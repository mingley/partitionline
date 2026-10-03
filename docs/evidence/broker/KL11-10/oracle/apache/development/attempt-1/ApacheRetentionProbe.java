import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

/** Executes official storage classes; no replacement implementation or broker process. */
public final class ApacheRetentionProbe {
    private static int checks;

    private ApacheRetentionProbe() { }

    private static void check(String name, Object actual, Object expected) {
        boolean passed = actual.equals(expected);
        System.out.println("{\"case\":\"" + name + "\",\"actual\":\"" + actual
            + "\",\"expected\":\"" + expected + "\",\"passed\":" + passed + "}");
        checks++;
        if (!passed) throw new AssertionError(name);
    }

    /** An input clock through Apache's documented public Time interface. */
    private static final class Clock implements Time {
        private volatile long now;

        Clock(long now) { this.now = now; }
        void set(long value) { now = value; }
        @Override public long milliseconds() { return now; }
        @Override public long nanoseconds() { return now * 1_000_000; }
        @Override public void sleep(long ms) { now += ms; }
        @Override public void waitObject(Object object, Supplier<Boolean> condition, long deadline)
            throws InterruptedException {
            throw new UnsupportedOperationException("No component scenario uses clock waiting");
        }
    }

    private static final class Fixture implements AutoCloseable {
        final Path dir;
        final Clock clock;
        final KafkaScheduler scheduler;
        final BrokerTopicStats stats;
        final LogConfig config;
        UnifiedLog log;

        Fixture(Path root, String name, long retentionMs, long retentionBytes) throws IOException {
            dir = root.resolve(name).resolve("alpha-0");
            clock = new Clock(1050);
            scheduler = new KafkaScheduler(1, true, "retention-oracle-");
            scheduler.startup();
            stats = new BrokerTopicStats(false);
            config = new LogConfig(Map.of(
                "internal.segment.bytes", 120,
                "segment.index.bytes", 1024,
                "index.interval.bytes", 1,
                "segment.ms", Long.MAX_VALUE,
                "retention.ms", retentionMs,
                "retention.bytes", retentionBytes,
                "file.delete.delay.ms", 60_000L,
                "message.timestamp.before.max.ms", Long.MAX_VALUE,
                "message.timestamp.after.max.ms", Long.MAX_VALUE));
            log = open(0, 0);
        }

        private UnifiedLog open(long suppliedFloor, long recoveryPoint) throws IOException {
            return UnifiedLog.create(dir.toFile(), config, suppliedFloor, recoveryPoint, scheduler,
                stats, clock, 60_000, new ProducerStateManagerConfig(60_000, false),
                60_000, new LogDirFailureChannel(1), true, Optional.of(new Uuid(0, 2)));
        }

        void seed(Path seeds) throws IOException {
            for (String name : List.of("log-batch-0.bin", "log-batch-3.bin")) {
                byte[] bytes = Files.readAllBytes(seeds.resolve(name));
                // Actual Produce input base is zero. BaseOffset lies outside the batch CRC.
                ByteBuffer.wrap(bytes).putLong(0, 0);
                log.appendAsLeader(MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)), 0);
            }
            log.updateHighWatermark(4);
        }

        void reopen(long suppliedFloor) throws IOException {
            long durableEnd = log.logEndOffset();
            log.flush(true);
            log.close();
            log = open(suppliedFloor, durableEnd);
            log.updateHighWatermark(durableEnd);
        }

        @Override public void close() {
            log.close();
            stats.close();
            try {
                scheduler.shutdown();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        }
    }

    private static String bases(UnifiedLog log) {
        List<Long> result = new ArrayList<>();
        for (LogSegment segment : log.logSegments()) result.add(segment.baseOffset());
        return result.toString();
    }

    private static long timestampOffset(UnifiedLog log, long timestamp) {
        return log.fetchOffsetByTimestamp(timestamp, Optional.empty()).timestampAndOffsetOpt()
            .map(result -> result.offset).orElse(-1L);
    }

    private static String readBatches(UnifiedLog log, long offset) throws IOException {
        List<String> result = new ArrayList<>();
        for (RecordBatch batch : log.read(offset, 4096, FetchIsolation.HIGH_WATERMARK, true).records.batches()) {
            result.add(batch.baseOffset() + ":" + batch.lastOffset());
        }
        return result.toString();
    }

    private static void outOfRange(String name, UnifiedLog log, long offset) throws IOException {
        boolean rejected = false;
        try {
            log.read(offset, 4096, FetchIsolation.HIGH_WATERMARK, true);
        } catch (OffsetOutOfRangeException expected) {
            rejected = true;
        }
        check(name, rejected, true);
    }

    private static void floors(Path root, Path seeds, boolean wrongFloor) throws IOException {
        try (Fixture fixture = new Fixture(root, "floor", -1, -1)) {
            UnifiedLog log = fixture.log;
            fixture.seed(seeds);
            check("seed.segment-bases", bases(log), "[0, 3]");
            check("seed.bytes", log.size(), 182L);
            check("seed.end", log.logEndOffset(), 4L);
            check("seed.hw", log.highWatermark(), 4L);
            check("floor.timestamp-before", timestampOffset(log, 1004), 1L);
            check("floor.advance-inside-batch", log.maybeIncrementLogStartOffset(2,
                LogStartOffsetIncrementReason.ClientRecordDeletion), true);
            check("floor.logical", log.logStartOffset(), wrongFloor ? 1L : 2L);
            check("floor.timestamp-after", timestampOffset(log, 1004), 3L);
            check("floor.earliest", timestampOffset(log, -2), 2L);
            check("floor.latest", timestampOffset(log, -1), 4L);
            outOfRange("floor.read-before", log, 1);
            check("floor.read-containing-whole-batch", readBatches(log, 2), "[0:2]");
            check("floor.regression-noop", log.maybeIncrementLogStartOffset(1,
                LogStartOffsetIncrementReason.ClientRecordDeletion), false);
            check("floor.equal-noop", log.maybeIncrementLogStartOffset(2,
                LogStartOffsetIncrementReason.ClientRecordDeletion), false);
            boolean aboveHw = false;
            try {
                log.maybeIncrementLogStartOffset(5, LogStartOffsetIncrementReason.ClientRecordDeletion);
            } catch (OffsetOutOfRangeException expected) {
                aboveHw = true;
            }
            check("floor.above-hw-reject", aboveHw, true);
            check("floor.above-hw-no-mutation", log.logStartOffset(), 2L);
            check("floor.whole-first-still-selected", log.deleteOldSegments(), 0);
            check("floor.supply-checkpoint-on-reopen", log.logStartOffset(), 2L);
            fixture.reopen(2);
            log = fixture.log;
            check("floor.reopened-supplied-floor", log.logStartOffset(), 2L);
            check("floor.reopened-end", log.logEndOffset(), 4L);
            outOfRange("floor.reopened-read-before", log, 1);
            check("floor.reopened-timestamp", timestampOffset(log, 1004), 3L);
            check("floor.advance-boundary", log.maybeIncrementLogStartOffset(3,
                LogStartOffsetIncrementReason.ClientRecordDeletion), true);
            check("floor.boundary-cleanup", log.deleteOldSegments(), 1);
            check("floor.boundary-survivor", bases(log), "[3]");
            check("floor.boundary-bytes", log.size(), 78L);
            check("floor.advance-hw", log.maybeIncrementLogStartOffset(4,
                LogStartOffsetIncrementReason.ClientRecordDeletion), true);
            check("floor.at-hw-read-empty", readBatches(log, 4), "[]");
        }
        // This factory receives a checkpoint. It does not recover a logical floor from data alone.
        try (Fixture fixture = new Fixture(root, "unsupplied-checkpoint", -1, -1)) {
            fixture.seed(seeds);
            fixture.log.maybeIncrementLogStartOffset(2, LogStartOffsetIncrementReason.ClientRecordDeletion);
            fixture.reopen(0);
            check("checkpoint.unsupplied-floor-not-durable-receipt", fixture.log.logStartOffset(), 0L);
        }
    }

    private static void ages(Path root, Path seeds, boolean wrongAge) throws IOException {
        try (Fixture fixture = new Fixture(root, "age", 100, -1)) {
            fixture.seed(seeds);
            fixture.clock.set(1107);
            check("age.strict-equality-keeps", fixture.log.deleteOldSegments(), wrongAge ? 1 : 0);
            check("age.equal-floor", fixture.log.logStartOffset(), 0L);
            fixture.clock.set(1108);
            check("age.one-ms-over-deletes", fixture.log.deleteOldSegments(), 1);
            check("age.floor-boundary", fixture.log.logStartOffset(), 3L);
            check("age.end-unchanged", fixture.log.logEndOffset(), 4L);
            check("age.hw-unchanged", fixture.log.highWatermark(), 4L);
            check("age.survivor", bases(fixture.log), "[3]");
            check("age.survivor-bytes", fixture.log.size(), 78L);
            fixture.reopen(3);
            check("age.reopened-physical-boundary", fixture.log.logStartOffset(), 3L);
            outOfRange("age.reopened-read-deleted", fixture.log, 2);
        }
        try (Fixture fixture = new Fixture(root, "hw-protection", 100, -1)) {
            fixture.seed(seeds);
            fixture.log.updateHighWatermark(2);
            fixture.clock.set(1108);
            check("hw.partial-batch-protected", fixture.log.deleteOldSegments(), 0);
            check("hw.no-floor-advance", fixture.log.logStartOffset(), 0L);
            fixture.log.updateHighWatermark(3);
            check("hw.boundary-allows-first", fixture.log.deleteOldSegments(), 1);
            check("hw.floor-is-confirmed-boundary", fixture.log.logStartOffset(), 3L);
        }
        try (Fixture fixture = new Fixture(root, "active-expiry", 100, -1)) {
            fixture.seed(seeds);
            fixture.clock.set(1111);
            check("age.all-data-segments-expire", fixture.log.deleteOldSegments(), 2);
            check("age.empty-successor-at-hw", bases(fixture.log), "[4]");
            check("age.full-floor", fixture.log.logStartOffset(), 4L);
            check("age.full-end", fixture.log.logEndOffset(), 4L);
            check("age.empty-successor-size", fixture.log.size(), 0L);
            check("age.empty-successor-not-deleted", fixture.log.deleteOldSegments(), 0);
        }
    }

    private static void sizes(Path root, Path seeds, boolean wrongSize) throws IOException {
        for (long target : List.of(79L, 78L)) {
            try (Fixture fixture = new Fixture(root, "size-" + target, -1, target)) {
                fixture.seed(seeds);
                int expected = target == 78 ? 1 : 0;
                check("size.target-" + target + "-whole-removal", fixture.log.deleteOldSegments(),
                    wrongSize && target == 79 ? 1 : expected);
                check("size.target-" + target + "-floor", fixture.log.logStartOffset(), target == 78 ? 3L : 0L);
                check("size.target-" + target + "-remaining-bytes", fixture.log.size(), target == 78 ? 78L : 182L);
                check("size.target-" + target + "-end", fixture.log.logEndOffset(), 4L);
                check("size.target-" + target + "-hw", fixture.log.highWatermark(), 4L);
            }
        }
    }

    private static void noTimestamp(Path root, boolean wrongUnknown) throws IOException {
        try (Fixture fixture = new Fixture(root, "unknown-timestamp", 100, -1)) {
            fixture.clock.set(9000);
            MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(RecordBatch.NO_TIMESTAMP, new byte[] {1}));
            // Replication-origin component input preserves the actual NO_TIMESTAMP sentinel.
            fixture.log.appendAsFollower(records, 0);
            LogSegment first = fixture.log.activeSegment();
            fixture.log.roll();
            fixture.log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(10_000, new byte[] {2})), 0);
            fixture.log.updateHighWatermark(2);
            first.setLastModified(9000);
            check("unknown.actual-negative-record-maximum", first.maxTimestampSoFar(), -1L);
            check("unknown.actual-record-timestamp-absent", first.largestRecordTimestamp().isEmpty(), true);
            check("unknown.apache-effective-mtime", first.largestTimestamp(), 9000L);
            fixture.clock.set(9100);
            check("unknown.mtime-strict-equality-keeps", fixture.log.deleteOldSegments(), 0);
            fixture.clock.set(9101);
            check("unknown.apache-mtime-one-ms-over-deletes", fixture.log.deleteOldSegments(), wrongUnknown ? 0 : 1);
            check("unknown.apache-floor", fixture.log.logStartOffset(), 1L);
            check("unknown.next-future-timestamp-kept", bases(fixture.log), "[1]");
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) throw new IllegalArgumentException("root seeds [wrong-case]");
        Path root = Path.of(args[0]);
        Path seeds = Path.of(args[1]);
        String control = args.length == 3 ? args[2] : "";
        Files.createDirectories(root);
        floors(root, seeds, control.equals("--wrong-floor"));
        ages(root, seeds, control.equals("--wrong-age"));
        sizes(root, seeds, control.equals("--wrong-size"));
        noTimestamp(root, control.equals("--wrong-unknown-age"));
        System.out.println("{\"summary\":true,\"passed\":true,\"checks\":" + checks + "}");
    }
}
