import java.io.IOException;
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
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.Cleaner;
import org.apache.kafka.storage.internals.log.CleanerStats;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogToClean;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.SkimpyOffsetMap;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.utils.Throttler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

/** Actual official Cleaner and record filtering; no Rust implementation or broker emulator. */
public final class ApacheCompactionProbe {
    private static int checks;
    private static final List<String> cases = new ArrayList<>();
    private static Path output;
    private static boolean wrongNull;
    private static boolean wrongEquality;
    private static boolean wrongEmpty;

    private ApacheCompactionProbe() { }

    private static String quote(String text) {
        return text == null ? "null" : "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
    private static String hex(byte[] bytes) { return bytes == null ? "null" : quote(HexFormat.of().formatHex(bytes)); }
    private static byte[] bytes(String value) { return value == null ? null : value.getBytes(StandardCharsets.UTF_8); }
    private static byte[] contents(ByteBuffer value) {
        if (value == null) return null;
        ByteBuffer copy = value.duplicate(); byte[] result = new byte[copy.remaining()]; copy.get(result); return result;
    }
    private static void check(String name, Object actual, Object expected) {
        boolean passed = actual.equals(expected); checks++;
        System.out.println("{\"case\":" + quote(name) + ",\"actual\":" + quote(actual.toString())
            + ",\"expected\":" + quote(expected.toString()) + ",\"passed\":" + passed + "}");
        if (!passed) throw new AssertionError(name);
    }
    private static SimpleRecord record(long time, String key, String value) {
        return new SimpleRecord(time, bytes(key), bytes(value), new Header[] {
            new RecordHeader("dup", bytes("a")), new RecordHeader("dup", null)});
    }
    private static MemoryRecords mixed() {
        return MemoryRecords.withRecords(0, Compression.NONE,
            record(1000, "a", "old"), record(1007, null, "null-key"), record(1003, "a", "new"),
            record(1007, "b", "old"), record(1010, "b", null), record(1011, "", ""),
            record(1012, null, null), record(1013, "c", null));
    }
    private static byte[] data(MemoryRecords records) { return contents(records.buffer()); }
    private static MemoryRecords parse(byte[] bytes) { return MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)); }
    private static List<Long> offsets(MemoryRecords records) {
        List<Long> result = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) for (Record record : batch) result.add(record.offset());
        return result;
    }
    private static MutableRecordBatch first(MemoryRecords records) { return records.batches().iterator().next(); }
    private static void capture(String name, byte[] bytes, String origin, String policy) throws Exception {
        check(name + ".file-budget", bytes.length <= 16384, true);
        Files.write(output.resolve(name + ".bin"), bytes);
        MemoryRecords records = parse(bytes);
        check(name + ".full-consumption", records.validBytes(), bytes.length);
        List<String> batches = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            batch.ensureValid();
            List<String> rows = new ArrayList<>();
            for (Record record : batch) {
                record.ensureValid();
                List<String> headers = new ArrayList<>();
                for (Header header : record.headers()) headers.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":" + hex(header.value()) + "}");
                rows.add("{\"offset\":" + record.offset() + ",\"timestamp\":" + record.timestamp()
                    + ",\"key_hex\":" + hex(contents(record.key())) + ",\"value_hex\":" + hex(contents(record.value()))
                    + ",\"headers\":[" + String.join(",", headers) + "]}");
            }
            ByteBuffer serialized = ByteBuffer.allocate(batch.sizeInBytes()); batch.writeTo(serialized); serialized.flip();
            batches.add("{\"base_offset\":" + batch.baseOffset() + ",\"last_offset\":" + batch.lastOffset()
                + ",\"logical_span\":" + (batch.lastOffset() - batch.baseOffset() + 1)
                + ",\"record_count\":" + batch.countOrNull() + ",\"bytes\":" + batch.sizeInBytes()
                + ",\"attributes\":" + serialized.getShort(21) + ",\"base_timestamp\":" + serialized.getLong(27)
                + ",\"max_timestamp\":" + batch.maxTimestamp() + ",\"delete_horizon_ms\":"
                + (batch.deleteHorizonMs().isPresent() ? Long.toString(batch.deleteHorizonMs().getAsLong()) : "null")
                + ",\"producer_id\":" + batch.producerId() + ",\"records\":[" + String.join(",", rows) + "]}");
        }
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        cases.add("{\"name\":" + quote(name) + ",\"file\":" + quote(name + ".bin") + ",\"bytes\":" + bytes.length
            + ",\"sha256\":" + quote(digest) + ",\"origin\":" + quote(origin) + ",\"handler_policy\":" + quote(policy)
            + ",\"batches\":[" + String.join(",", batches) + "]}");
    }
    private static MemoryRecords filtered(MemoryRecords input, Set<Long> keep, long now,
                                          MemoryRecords.RecordFilter.BatchRetention retention) {
        MemoryRecords.RecordFilter filter = new MemoryRecords.RecordFilter(now, 1000) {
            @Override public BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new BatchRetentionResult(retention, false);
            }
            @Override public boolean shouldRetainRecord(RecordBatch batch, Record record) {
                return keep.contains(record.offset());
            }
        };
        MemoryRecords.FilterResult result = input.filterTo(filter, ByteBuffer.allocate(16384), BufferSupplier.NO_CACHING);
        ByteBuffer buffer = result.outputBuffer(); buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }
    private static void filters() throws Exception {
        MemoryRecords input = mixed();
        capture("ordinary-mixed-input", data(input), "Actual Apache MemoryRecords.withRecords", "contiguous_produce_and_sparse_read");
        MemoryRecords sparse = filtered(input, Set.of(2L, 4L, 5L, 7L), 2000,
            MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.first.offsets", offsets(sparse), List.of(2L, 4L, 5L, 7L));
        check("filter.first.delete-horizon", first(sparse).deleteHorizonMs().orElse(-1), 3000L);
        check("filter.first.original-extent", first(sparse).lastOffset(), 7L);
        check("filter.first.timestamps", first(sparse).maxTimestamp(), 1013L);
        capture("filter-first-horizon-sparse", data(sparse), "Actual Apache filterTo with declared offset-selection predicate", "sparse_read_only");
        MemoryRecords noTombstones = filtered(sparse, Set.of(2L, 5L), 3000,
            MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.expired.offsets", offsets(noTombstones), List.of(2L, 5L));
        check("filter.expired.horizon-preserved", first(noTombstones).deleteHorizonMs().orElse(-1), 3000L);
        check("filter.expired.max-recomputed", first(noTombstones).maxTimestamp(), 1011L);
        capture("filter-horizon-no-tombstones-sparse", data(noTombstones), "Actual Apache filterTo with declared offset-selection predicate", "sparse_read_only");
        MemoryRecords empty = filtered(sparse, Set.of(), 3000, MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.empty.bytes", empty.sizeInBytes(), wrongEmpty ? 0 : 61);
        check("filter.empty.count", first(empty).countOrNull(), 0);
        check("filter.empty.extent", first(empty).lastOffset(), 7L);
        check("filter.empty.horizon-cleared", first(empty).deleteHorizonMs().isEmpty(), true);
        check("filter.empty.base-time", ByteBuffer.wrap(data(empty)).getLong(27), -1L);
        check("filter.empty.max-time", first(empty).maxTimestamp(), 1013L);
        capture("filter-retain-empty-61", data(empty), "Actual Apache filterTo RETAIN_EMPTY", "sparse_empty_read_only");
        MemoryRecords gone = filtered(input, Set.of(), 2000, MemoryRecords.RecordFilter.BatchRetention.DELETE_EMPTY);
        check("filter.delete-empty.bytes", gone.sizeInBytes(), 0);
        capture("filter-delete-empty-output", data(gone), "Actual Apache filterTo DELETE_EMPTY", "no_batch_output");
        MemoryRecords ordinary = MemoryRecords.withRecords(32, Compression.NONE,
            record(1000, "a", "x"), record(1010, "b", "y"));
        MemoryRecords unchanged = filtered(ordinary, Set.of(32L, 33L), 2000, MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.unchanged.exact-bytes", Arrays.equals(data(ordinary), data(unchanged)), true);
        capture("filter-ordinary-unchanged", data(unchanged), "Actual Apache filterTo unchanged fast path", "contiguous_produce_and_sparse_read");
        MemoryRecords leading = filtered(ordinary, Set.of(33L), 2000, MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.leading.base-preserved", first(leading).baseOffset(), 32L);
        check("filter.leading.base-time", ByteBuffer.wrap(data(leading)).getLong(27), 1010L);
        capture("filter-leading-gap", data(leading), "Actual Apache filterTo declared offset-selection", "sparse_read_only");
        MemoryRecords trailing = filtered(ordinary, Set.of(32L), 2000, MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.trailing.last-preserved", first(trailing).lastOffset(), 33L);
        capture("filter-trailing-gap", data(trailing), "Actual Apache filterTo declared offset-selection", "sparse_read_only");
        MemoryRecords emptyKey = filtered(input, Set.of(5L), 2000, MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.empty-key.retained", offsets(emptyKey), List.of(5L));
        check("filter.empty-key.present", first(emptyKey).iterator().next().hasKey(), true);
        capture("filter-empty-key-sparse", data(emptyKey), "Actual Apache filterTo retains a present zero-length key/value", "sparse_read_only");
        MemoryRecords unknown = MemoryRecords.withRecords(0, Compression.NONE,
            record(-1, "u", null), record(-1, "v", "value"));
        MemoryRecords unknownHorizon = filtered(unknown, Set.of(0L, 1L), 2000, MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY);
        check("filter.unknown-time.horizon", first(unknownHorizon).deleteHorizonMs().orElse(-1), 3000L);
        check("filter.unknown-time.max", first(unknownHorizon).maxTimestamp(), -1L);
        capture("filter-unknown-time-horizon", data(unknownHorizon), "Actual Apache filterTo unknown record times/tombstone", "sparse_read_only");
    }
    private static final class Clock implements Time {
        private long now = 2000;
        @Override public long milliseconds() { return now; }
        @Override public long nanoseconds() { return now * 1000000; }
        @Override public void sleep(long ms) { now += ms; }
        @Override public void waitObject(Object object, Supplier<Boolean> condition, long deadline) {
            throw new UnsupportedOperationException("No waiting clock scenario");
        }
    }
    private static final class Fixture implements AutoCloseable {
        final Path dir;
        final Clock clock = new Clock();
        final KafkaScheduler scheduler = new KafkaScheduler(1, true, "compaction-oracle-");
        final BrokerTopicStats stats = new BrokerTopicStats(false);
        final LogConfig config;
        final Cleaner cleaner;
        UnifiedLog log;
        Fixture(Path root, String name) throws Exception {
            dir = root.resolve(name).resolve("alpha-0"); scheduler.startup();
            config = config("delete"); log = open(0);
            cleaner = new Cleaner(0, new SkimpyOffsetMap(65536, "MD5"), 16384, 16384, 0.9,
                new Throttler(Double.MAX_VALUE, 300, "compaction-oracle-io", "bytes", clock), clock,
                topicPartition -> { });
        }
        private LogConfig config(String cleanup) {
            return new LogConfig(Map.of("cleanup.policy", cleanup, "internal.segment.bytes", 16384,
                "segment.index.bytes", 1024, "index.interval.bytes", 1, "segment.ms", Long.MAX_VALUE,
                "delete.retention.ms", 1000L, "file.delete.delay.ms", 60000L,
                "message.timestamp.before.max.ms", Long.MAX_VALUE, "message.timestamp.after.max.ms", Long.MAX_VALUE));
        }
        private UnifiedLog open(long end) throws IOException {
            return UnifiedLog.create(dir.toFile(), config, 0, end, scheduler, stats, clock, 60000,
                new ProducerStateManagerConfig(60000, false), 60000, new LogDirFailureChannel(1),
                true, Optional.of(new Uuid(0, 2)));
        }
        void append(MemoryRecords input) throws IOException { log.appendAsLeader(input, 0); }
        byte[] prefix(long upper) throws IOException {
            List<byte[]> pieces = new ArrayList<>(); int size = 0;
            for (LogSegment segment : log.logSegments()) if (segment.baseOffset() < upper) {
                byte[] data = Files.readAllBytes(segment.log().file().toPath());
                pieces.add(data); size += data.length;
            }
            if (size > 16384) throw new AssertionError("component prefix byte bound");
            ByteBuffer result = ByteBuffer.allocate(size); for (byte[] piece : pieces) result.put(piece); return result.array();
        }
        byte[] active(long base) throws IOException {
            for (LogSegment segment : log.logSegments()) if (segment.baseOffset() == base)
                return Files.readAllBytes(segment.log().file().toPath());
            throw new AssertionError("active segment missing");
        }
        CleanerStats clean(long upper, long now) throws Exception {
            clock.now = now; log.updateConfig(config("compact"));
            Map.Entry<Long, CleanerStats> result = cleaner.doClean(new LogToClean(log, 0, upper, true), now);
            check("clean.boundary." + upper + "." + now, result.getKey(), upper);
            return result.getValue();
        }
        void reopen(long end) throws IOException { log.flush(true); log.close(); log = open(end); log.updateHighWatermark(end); }
        @Override public void close() {
            log.close(); stats.close();
            try { scheduler.shutdown(); } catch (InterruptedException failure) {
                Thread.currentThread().interrupt(); throw new IllegalStateException(failure);
            }
        }
    }
    private static void cleaners(Path root) throws Exception {
        try (Fixture fixture = new Fixture(root, "mixed")) {
            fixture.append(mixed()); fixture.log.roll();
            fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1014, "a", "protected"), record(1015, null, "active-null")));
            fixture.log.updateHighWatermark(10); byte[] active = fixture.active(8);
            capture("cleaner-mixed-active-protected", active, "Actual UnifiedLog active8–9 bytes; not cleaned", "contiguous_produce_and_sparse_read");
            capture("cleaner-mixed-input", fixture.prefix(8), "Actual UnifiedLog pre-compaction selected bytes", "contiguous_produce_and_sparse_read");
            CleanerStats stats = fixture.clean(8, 2000);
            MemoryRecords first = parse(fixture.prefix(8));
            check("cleaner.null-keys.invalid", stats.invalidMessagesRead(), wrongNull ? 0L : 2L);
            check("cleaner.first.offsets", offsets(first), List.of(2L, 4L, 5L, 7L));
            check("cleaner.first.horizon", first(first).deleteHorizonMs().orElse(-1), 3000L);
            capture("cleaner-first-horizon-sparse", data(first), "Actual Cleaner.doClean map/filter/segment swap", "sparse_read_only");
            check("cleaner.first.active-unchanged", Arrays.equals(active, fixture.active(8)), true);
            fixture.clean(8, 2999); check("cleaner.before-horizon.offsets", offsets(parse(fixture.prefix(8))), List.of(2L, 4L, 5L, 7L));
            capture("cleaner-before-horizon-sparse", fixture.prefix(8), "Actual Cleaner.doClean before tombstone horizon", "sparse_read_only");
            fixture.clean(8, 3000); check("cleaner.at-horizon.offsets", offsets(parse(fixture.prefix(8))), wrongEquality ? List.of(2L, 4L, 5L, 7L) : List.of(2L, 5L));
            capture("cleaner-equal-horizon-sparse", fixture.prefix(8), "Actual Cleaner.doClean equality expires tombstones", "sparse_read_only");
            fixture.clean(8, 3001); capture("cleaner-after-horizon-sparse", fixture.prefix(8), "Actual Cleaner.doClean after horizon", "sparse_read_only");
            check("cleaner.end-preserved", fixture.log.logEndOffset(), 10L);
            check("cleaner.final.active-unchanged", Arrays.equals(active, fixture.active(8)), true);
            byte[] before = fixture.prefix(8); fixture.reopen(10);
            check("cleaner.reopen.prefix-bytes", Arrays.equals(before, fixture.prefix(8)), true);
            check("cleaner.reopen.active-bytes", Arrays.equals(active, fixture.active(8)), true);
            check("cleaner.reopen.end", fixture.log.logEndOffset(), 10L);
        }
        try (Fixture fixture = new Fixture(root, "allremoved")) {
            fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1000, "a", "old0"), record(1001, "a", "old1")));
            fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1002, "a", null)));
            fixture.log.roll(); fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1003, "a", "active")));
            fixture.log.updateHighWatermark(4); byte[] active = fixture.active(3);
            capture("cleaner-allremoved-active-protected", active, "Actual UnifiedLog active3 bytes; not cleaned", "contiguous_produce_and_sparse_read");
            capture("cleaner-allremoved-input", fixture.prefix(3), "Actual UnifiedLog two ordinary pre-clean batches", "contiguous_produce_and_sparse_read");
            fixture.clean(3, 2000); MemoryRecords cleaned = parse(fixture.prefix(3));
            check("cleaner.intermediate-empty-dropped", first(cleaned).baseOffset(), 2L);
            check("cleaner.last-tombstone.offsets", offsets(cleaned), List.of(2L));
            capture("cleaner-intermediate-empty-dropped", data(cleaned), "Actual Cleaner drops ordinary intermediate empty batch; retains last cleaning-round batch", "sparse_read_only");
            fixture.clean(3, 3000); MemoryRecords empty = parse(fixture.prefix(3));
            check("cleaner.last-empty.bytes", empty.sizeInBytes(), 61);
            check("cleaner.last-empty.base", first(empty).baseOffset(), 2L);
            check("cleaner.last-empty.last", first(empty).lastOffset(), 2L);
            check("cleaner.last-empty.count", first(empty).countOrNull(), 0);
            check("cleaner.last-empty.base-time", ByteBuffer.wrap(data(empty)).getLong(27), -1L);
            capture("cleaner-last-empty-61", data(empty), "Actual Cleaner last cleaning-round ordinary empty batch retained", "sparse_empty_read_only");
            check("cleaner.allremoved.active", Arrays.equals(active, fixture.active(3)), true);
            fixture.reopen(4); check("cleaner.allremoved.reopen-end", fixture.log.logEndOffset(), 4L);
        }
        try (Fixture fixture = new Fixture(root, "nullonly")) {
            fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1000, null, "value"), record(1001, null, null)));
            fixture.log.roll(); fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1002, "active", "value")));
            fixture.log.updateHighWatermark(3);
            capture("cleaner-nullonly-input", fixture.prefix(2), "Actual UnifiedLog existing null-key records before compact policy", "contiguous_produce_and_sparse_read");
            capture("cleaner-nullonly-active-protected", fixture.active(2), "Actual UnifiedLog active2 bytes; not cleaned", "contiguous_produce_and_sparse_read");
            CleanerStats stats = fixture.clean(2, 2000); MemoryRecords empty = parse(fixture.prefix(2));
            check("cleaner.nullonly.invalid", stats.invalidMessagesRead(), 2L);
            check("cleaner.nullonly.empty", empty.sizeInBytes(), 61);
            check("cleaner.nullonly.extent", first(empty).lastOffset(), 1L);
            capture("cleaner-nullonly-empty-61", data(empty), "Actual Cleaner discards null keys but preserves last round extent", "sparse_empty_read_only");
        }
        try (Fixture fixture = new Fixture(root, "protectedsealed")) {
            fixture.append(mixed()); fixture.log.roll();
            fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1014, "a", "sealed-protected"), record(1015, "b", "sealed-protected")));
            fixture.log.roll(); fixture.append(MemoryRecords.withRecords(Compression.NONE, record(1016, "a", "active"), record(1017, "b", "active")));
            fixture.log.updateHighWatermark(12);
            byte[] sealed = fixture.active(8), active = fixture.active(10);
            capture("cleaner-protected-sealed-input", fixture.prefix(8), "Actual UnifiedLog bounded cleanable prefix0–7", "contiguous_produce_and_sparse_read");
            capture("cleaner-protected-sealed-suffix", sealed, "Actual UnifiedLog sealed8–9 beyond cleaning window; not cleaned", "contiguous_produce_and_sparse_read");
            capture("cleaner-protected-active-suffix", active, "Actual UnifiedLog active10–11; not cleaned", "contiguous_produce_and_sparse_read");
            fixture.clean(8, 2000);
            check("cleaner.protected-sealed.offsets", offsets(parse(fixture.prefix(8))), List.of(2L, 4L, 5L, 7L));
            check("cleaner.protected-sealed.exact-bytes", Arrays.equals(sealed, fixture.active(8)), true);
            check("cleaner.protected-active.exact-bytes", Arrays.equals(active, fixture.active(10)), true);
            check("cleaner.protected.end", fixture.log.logEndOffset(), 12L);
            capture("cleaner-protected-sealed-first-sparse", fixture.prefix(8), "Actual Cleaner.doClean bounded prefix with untouched sealed/active suffixes", "sparse_read_only");
            fixture.reopen(12);
            check("cleaner.protected-reopen.sealed", Arrays.equals(sealed, fixture.active(8)), true);
            check("cleaner.protected-reopen.active", Arrays.equals(active, fixture.active(10)), true);
            check("cleaner.protected-reopen.end", fixture.log.logEndOffset(), 12L);
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) throw new IllegalArgumentException("release work-root output-directory [negative-control]");
        wrongNull = args.length == 4 && args[3].equals("--wrong-null");
        wrongEquality = args.length == 4 && args[3].equals("--wrong-equality");
        wrongEmpty = args.length == 4 && args[3].equals("--wrong-empty");
        output = Path.of(args[2]); Files.createDirectories(output);
        filters(); cleaners(Path.of(args[1]));
        Files.writeString(output.resolve("goldens.json"), "{\"schema_version\":1,\"release\":" + quote(args[0])
            + ",\"passed\":true,\"assertions\":" + checks + ",\"cases\":[" + String.join(",", cases) + "]}\n");
        System.out.println("{\"passed\":true,\"checks\":" + checks + ",\"cases\":" + cases.size() + "}");
    }
}
