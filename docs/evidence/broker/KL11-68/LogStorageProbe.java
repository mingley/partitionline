/* Execute pinned Apache LogSegment/FileRecords boundary and timestamp methods. */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogSegment;

public final class LogStorageProbe {
    private LogStorageProbe() { }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static MemoryRecords first() {
        return MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord(1000L, (byte[]) null, (byte[]) null),
            new SimpleRecord(1007L, new byte[0], utf8("payload"), new Header[]{new RecordHeader("dup", utf8("a")), new RecordHeader("dup", null)}),
            new SimpleRecord(1003L, utf8("key2"), new byte[0]));
    }
    private static MemoryRecords second() {
        return MemoryRecords.withRecords(3L, Compression.NONE, new SimpleRecord(1010L, utf8("key3"), utf8("value3")));
    }
    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate(); byte[] data = new byte[buffer.remaining()]; buffer.get(data); return data;
    }
    private static String read(LogSegment segment, long offset, int maxBytes, boolean minOne) throws Exception {
        FetchDataInfo result = segment.read(offset, maxBytes, Optional.of((long) segment.size()), minOne);
        if (result == null) return "{\"offset\":" + offset + ",\"max_bytes\":" + maxBytes + ",\"min_one_message\":" + minOne + ",\"result\":null}";
        int length = result.records.sizeInBytes();
        if (length > 128 * 1024) throw new AssertionError("probe allocation limit");
        ByteBuffer buffer = ByteBuffer.allocate(length);
        ((FileRecords) result.records).readInto(buffer, 0);
        return "{\"offset\":" + offset + ",\"max_bytes\":" + maxBytes + ",\"min_one_message\":" + minOne
            + ",\"bytes\":" + length + ",\"first_entry_incomplete\":" + result.firstEntryIncomplete
            + ",\"actual_payload_hex\":\"" + HexFormat.of().formatHex(buffer.array()) + "\"}";
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("scratch-directory output-json");
        Path directory = Path.of(args[0]); Files.createDirectories(directory);
        MemoryRecords first = first(); MemoryRecords second = second();
        List<String> reads = new ArrayList<>(); List<String> timestamps = new ArrayList<>();
        try (LogSegment segment = LogSegment.open(directory.toFile(), 0, new LogConfig(Map.of("segment.bytes", 1_048_576)), Time.SYSTEM, 0, false)) {
            segment.append(2, first); segment.append(3, second); segment.flush();
            for (long offset : new long[]{-1, 0, 1, 2, 3, 4, 5, Long.MAX_VALUE}) {
                reads.add(read(segment, offset, 4096, true));
            }
            reads.add(read(segment, 0, 1, true));
            reads.add(read(segment, 0, 1, false));
            reads.add(read(segment, 0, first.sizeInBytes() + 1, true));
            reads.add(read(segment, 3, 1, true));
            for (long timestamp : new long[]{-10, -6, -5, -4, -3, -2, -1, 0, 999, 1000, 1001, 1003, 1007, 1008, 1010, 1011, Long.MAX_VALUE}) {
                Optional<FileRecords.TimestampAndOffset> result = segment.findOffsetByTimestamp(timestamp, 0);
                timestamps.add("{\"query_timestamp\":" + timestamp + ",\"method\":\"LogSegment.findOffsetByTimestamp (not ListOffsets special-sentinel dispatch)\",\"result\":"
                    + (result.isEmpty() ? "null" : "{\"timestamp\":" + result.get().timestamp + ",\"offset\":" + result.get().offset + "}") + "}");
            }
        }
        Files.writeString(Path.of(args[1]), "{\"scope\":\"Actual Apache storage components only; no Apache broker or Rust handler executed. Negative segment reads do not establish LocalLog/broker acceptance; LocalLog separately rejects offsets outside retained segment range.\","
            + "\"first_batch_hex\":\"" + HexFormat.of().formatHex(bytes(first)) + "\",\"second_batch_hex\":\"" + HexFormat.of().formatHex(bytes(second))
            + "\",\"reads\":[" + String.join(",", reads) + "],\"timestamp_searches\":[" + String.join(",", timestamps) + "]}\n");
        System.out.println("{\"reads\":" + reads.size() + ",\"timestamp_searches\":" + timestamps.size() + "}");
    }
}
