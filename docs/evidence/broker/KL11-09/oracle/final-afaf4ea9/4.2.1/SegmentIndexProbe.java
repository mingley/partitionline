/* Execute independently pinned Apache segment/index methods on bounded data. */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.TimestampOffset;

public final class SegmentIndexProbe {
    private static final long[] TIMES = {1000, 1007, 1003, 1007, 995, 1010, 1008, 1010, 1006, 1012, 1012, 1000};
    private static final long[] QUERIES = {-1, 0, 994, 995, 999, 1000, 1001, 1003, 1006, 1007, 1008, 1010, 1011, 1012, 1013, Long.MAX_VALUE};
    private static int assertions;
    private SegmentIndexProbe() { }
    private static void require(boolean value, String reason) {
        assertions++;
        if (!value) throw new AssertionError(reason);
    }
    private static byte[] text(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] data = new byte[buffer.remaining()]; buffer.get(data); return data;
    }
    private static String hex(byte[] data) { return HexFormat.of().formatHex(data); }
    private static String sha(byte[] data) throws Exception { return hex(MessageDigest.getInstance("SHA-256").digest(data)); }
    private static MemoryRecords batch(int base) {
        return MemoryRecords.withRecords(base, Compression.NONE,
            new SimpleRecord(TIMES[base], text("key" + base), text("value" + base)),
            new SimpleRecord(TIMES[base + 1], text("key" + (base + 1)), text("value" + (base + 1))));
    }
    private static byte[] join(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length); return result;
    }
    private static List<LogSegment> open(Path directory, boolean populate) throws Exception {
        List<LogSegment> segments = new ArrayList<>();
        // Apache validates a 1 MiB target minimum; manual boundaries below
        // keep actual files under 4 KiB without claiming an automatic roll.
        LogConfig config = new LogConfig(Map.of("segment.bytes", 1_048_576, "segment.index.bytes", 256, "index.interval.bytes", 1));
        for (int base = 0; base < TIMES.length; base += 4) {
            LogSegment segment = LogSegment.open(directory.toFile(), base, config, Time.SYSTEM, 0, false);
            segments.add(segment);
            if (populate) {
                segment.append(base + 1, batch(base)); segment.append(base + 3, batch(base + 2));
                segment.onBecomeInactiveSegment(); segment.flush();
            }
        }
        return segments;
    }
    private static void close(List<LogSegment> segments) throws Exception {
        for (LogSegment segment : segments) segment.close();
    }
    private static String read(LogSegment segment, int base, long offset, int maxBytes, boolean minOne) throws Exception {
        FetchDataInfo result = segment.read(offset, maxBytes, Optional.of((long) segment.size()), minOne);
        if (offset >= base + 4) {
            require(result == null, "segment end must not return data");
            return "{\"segment_base\":" + base + ",\"offset\":" + offset + ",\"max_bytes\":" + maxBytes + ",\"min_one\":" + minOne + ",\"result\":null}";
        }
        require(result != null, "in-range read must return metadata");
        int length = result.records.sizeInBytes(); require(length <= 4096, "bounded actual read allocation");
        ByteBuffer buffer = ByteBuffer.allocate(length); ((FileRecords) result.records).readInto(buffer, 0);
        byte[] actual = buffer.array();
        byte[] expected = offset < base + 2 ? join(bytes(batch(base)), bytes(batch(base + 2))) : bytes(batch(base + 2));
        if (maxBytes == 1) expected = minOne ? bytes(batch(offset < base + 2 ? base : base + 2)) : Arrays.copyOf(expected, 1);
        require(Arrays.equals(actual, expected), "component slice / min-one byte preservation");
        return "{\"segment_base\":" + base + ",\"offset\":" + offset + ",\"max_bytes\":" + maxBytes + ",\"min_one\":" + minOne
            + ",\"bytes\":" + length + ",\"first_entry_incomplete\":" + result.firstEntryIncomplete
            + ",\"actual_payload_hex\":\"" + hex(actual) + "\"}";
    }
    private static String capture(List<LogSegment> segments) throws Exception {
        List<String> reads = new ArrayList<>(); List<String> searches = new ArrayList<>(); List<String> indexes = new ArrayList<>();
        for (int n = 0; n < segments.size(); n++) {
            LogSegment segment = segments.get(n); int base = n * 4;
            require(segment.readNextOffset() == base + 4, "reopened segment logical end");
            require(segment.offsetIndex().entries() > 0 && segment.timeIndex().entries() > 0, "actual sparse indexes populated");
            for (long offset = base; offset <= base + 4; offset++) {
                OffsetPosition hint = segment.offsetIndex().lookup(offset);
                require(hint.offset() <= offset && hint.position() >= 0 && hint.position() < segment.size(), "bounded offset-index predecessor");
                indexes.add("{\"segment_base\":" + base + ",\"query_offset\":" + offset + ",\"hint_offset\":" + hint.offset() + ",\"hint_position\":" + hint.position() + "}");
                reads.add(read(segment, base, offset, 4096, true));
            }
            reads.add(read(segment, base, base, 1, true)); reads.add(read(segment, base, base, 1, false));
            for (long target : QUERIES) {
                TimestampOffset hint = segment.timeIndex().lookup(target);
                require(hint.timestamp() <= target && hint.offset() >= base && hint.offset() < base + 4, "bounded prefix maximum time hint");
                indexes.add("{\"segment_base\":" + base + ",\"query_timestamp\":" + target + ",\"hint_timestamp\":" + hint.timestamp() + ",\"hint_offset\":" + hint.offset() + "}");
            }
        }
        for (long target : QUERIES) {
            for (long from : new long[]{0, 1, 2, 4, 5, 8, 9, 10, 11, 12}) {
                Optional<FileRecords.TimestampAndOffset> actual = Optional.empty();
                for (LogSegment segment : segments) {
                    actual = segment.findOffsetByTimestamp(target, from);
                    if (actual.isPresent()) break;
                }
                long expectedOffset = -1;
                for (int i = (int) from; i < TIMES.length; i++) {
                    if (TIMES[i] >= target) { expectedOffset = i; break; }
                }
                require(actual.isPresent() == (expectedOffset >= 0), "linear first-match presence across segments");
                if (actual.isPresent()) require(actual.get().offset == expectedOffset && actual.get().timestamp == TIMES[(int) expectedOffset], "equal/regressing timestamp first-match offset");
                searches.add("{\"query_timestamp\":" + target + ",\"from_offset\":" + from + ",\"result\":"
                    + (actual.isEmpty() ? "null" : "{\"timestamp\":" + actual.get().timestamp + ",\"offset\":" + actual.get().offset + "}") + "}");
            }
        }
        return "{\"reads\":[" + String.join(",", reads) + "],\"timestamp_searches\":[" + String.join(",", searches)
            + "],\"index_hints\":[" + String.join(",", indexes) + "]}";
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("scratch-directory output-json");
        Path directory = Path.of(args[0]); Files.createDirectories(directory);
        List<LogSegment> initial = open(directory, true);
        String before;
        try { before = capture(initial); } finally { close(initial); }
        List<String> files = new ArrayList<>();
        for (int base = 0; base < TIMES.length; base += 4) {
            byte[] actual = Files.readAllBytes(directory.resolve(String.format("%020d.log", base)));
            require(Arrays.equals(actual, join(bytes(batch(base)), bytes(batch(base + 2)))), "durable log bytes preserve assigned batches");
            for (String suffix : new String[]{"log", "index", "timeindex"}) {
                Path file = directory.resolve(String.format("%020d.%s", base, suffix));
                byte[] data = Files.readAllBytes(file); require(data.length <= 4096, "bounded retained files");
                files.add("{\"name\":\"" + file.getFileName() + "\",\"bytes\":" + data.length + ",\"sha256\":\"" + sha(data) + "\"}");
            }
        }
        List<LogSegment> reopened = open(directory, false);
        String after;
        try { after = capture(reopened); } finally { close(reopened); }
        require(before.equals(after), "all actual index hints, seeks and read bytes unchanged after close/reopen");
        String output = "{\"scope\":\"Actual Apache LogSegment/OffsetIndex/TimeIndex/FileRecords methods on three manually created segments; no automatic Apache rolling, Kafka wire broker, Rust code, retention or performance measurement. Negative timestamp queries are component searches, not ListOffsets sentinel dispatch.\","
            + "\"records\":12,\"segments\":3,\"assertions\":" + assertions + ",\"close_reopen_equal\":true,\"files\":[" + String.join(",", files) + "],\"before\":" + before + ",\"after\":" + after + "}\n";
        Files.writeString(Path.of(args[1]), output);
        System.out.println("{\"segments\":3,\"assertions\":" + assertions + ",\"close_reopen_equal\":true}");
    }
}
