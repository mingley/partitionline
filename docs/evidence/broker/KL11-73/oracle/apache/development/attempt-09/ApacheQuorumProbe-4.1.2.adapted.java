package org.apache.kafka.raft;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.IdentitySerde;
import kafka.raft.KafkaMetadataLog;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.util.KafkaScheduler;

/**
 * Executes official Apache classes, with assertion adaptations from pinned tests.
 * Package placement permits the protected LeaderState constructor without reflection.
 * A real accumulator/metrics replace upstream Mockito setup. This is a component
 * oracle: typed ACK routing, durable quorum histories and Kafka network operation
 * are independently checked elsewhere, not inferred from these assertions.
 */
public final class ApacheQuorumProbe {
    private static int checks;

    private ApacheQuorumProbe() { }

    private static void equal(String name, long actual, long expected) {
        boolean passed = actual == expected;
        System.out.println("{\"case\":\"" + name + "\",\"actual\":" + actual
            + ",\"expected\":" + expected + ",\"passed\":" + passed + "}");
        checks++;
        if (!passed) throw new AssertionError(name + ": " + actual + " != " + expected);
    }

    private static void yes(String name, boolean actual, boolean expected) {
        equal(name, actual ? 1 : 0, expected ? 1 : 0);
    }

    private static void rejects(String name, Class<? extends RuntimeException> expected, Runnable action) {
        String actual = "none";
        try { action.run(); } catch (RuntimeException e) { actual = e.getClass().getSimpleName(); }
        boolean passed = actual.equals(expected.getSimpleName());
        System.out.println("{\"case\":\"" + name + "\",\"actual_exception\":\"" + actual
            + "\",\"expected_exception\":\"" + expected.getSimpleName()
            + "\",\"passed\":" + passed + "}");
        checks++;
        if (!passed) throw new AssertionError(name + ": unexpected exception " + actual);
    }

    private static void contents(String name, KafkaMetadataLog log, Isolation isolation, String expected) {
        StringBuilder actual = new StringBuilder();
        for (var batch : log.read(0, isolation).records.batches()) {
            batch.ensureValid();
            for (var record : batch) {
                ByteBuffer value = record.value();
                byte[] bytes = new byte[value.remaining()];
                value.get(bytes);
                actual.append(record.offset()).append(':')
                    .append(new String(bytes, StandardCharsets.UTF_8)).append(':')
                    .append(record.timestamp()).append(';');
            }
        }
        boolean passed = actual.toString().equals(expected);
        System.out.println("{\"case\":\"" + name + "\",\"actual_records\":\"" + actual
            + "\",\"expected_records\":\"" + expected + "\",\"passed\":" + passed + "}");
        checks++;
        if (!passed) throw new AssertionError(name + ": record values/offsets/order/timestamps differ");
    }

    private static ReplicaKey key(int id) {
        return ReplicaKey.of(id, new Uuid(1, id + 1L));
    }

    private static final class Leader implements AutoCloseable {
        private final Metrics metrics = new Metrics();
        private final VoterSet voters;
        private final LeaderState<ByteBuffer> state;

        Leader(int voterCount, long epochStartOffset) {
            Map<Integer, VoterSet.VoterNode> nodes = new LinkedHashMap<>();
            for (int id = 0; id < voterCount; id++) {
                nodes.put(id, VoterSet.VoterNode.of(key(id), Endpoints.empty(),
                    new SupportedVersionRange((short) 0, (short) 1)));
            }
            voters = VoterSet.fromMap(nodes);
            BatchAccumulator<ByteBuffer> accumulator = new BatchAccumulator<>(
                5, epochStartOffset, 0, 4096, 2, MemoryPool.NONE,
                Time.SYSTEM, Compression.NONE, IdentitySerde.INSTANCE);
            state = new LeaderState<>(Time.SYSTEM, nodes.get(0), 5,
                epochStartOffset, voters, OptionalLong.of(0), KRaftVersion.KRAFT_VERSION_1,
                voters.voterIds(), accumulator, 2000, new LogContext(),
                new KafkaRaftMetrics(metrics, "independent-quorum"));
        }

        boolean local(long end) { return state.updateLocalState(new LogOffsetMetadata(end), voters); }
        boolean remote(int id, long end) {
            return state.updateReplicaState(key(id), Time.SYSTEM.milliseconds(), new LogOffsetMetadata(end));
        }
        long highWatermark() { return state.highWatermark().map(LogOffsetMetadata::offset).orElse(-1L); }
        @Override public void close() { state.close(); metrics.close(); }
    }

    private static void leaderCases(boolean deliberatelyWrongBarrier) {
        try (Leader leader = new Leader(1, 10)) {
            equal("one.initial-unknown", leader.highWatermark(), -1);
            yes("one.at-epoch-start-no-commit", leader.local(10), false);
            equal("one.current-epoch-barrier", leader.highWatermark(), deliberatelyWrongBarrier ? 10 : -1);
            yes("one.new-epoch-record-commits", leader.local(11), true);
            equal("one.committed-end", leader.highWatermark(), 11);
            yes("one.idempotent-local-update", leader.local(11), false);
            rejects("one.local-end-cannot-regress", IllegalStateException.class, () -> leader.local(10));
        }
        try (Leader leader = new Leader(3, 10)) {
            yes("three.local-only-no-majority", leader.local(15), false);
            yes("three.remote-at-old-boundary", leader.remote(1, 10), false);
            equal("three.previous-epoch-not-exposed", leader.highWatermark(), -1);
            yes("three.first-current-epoch-majority", leader.remote(1, 11), true);
            equal("three.majority-end11", leader.highWatermark(), 11);
            yes("three.duplicate-ack-not-extra-vote", leader.remote(1, 11), false);
            yes("three.observer-not-a-vote", leader.remote(99, 15), false);
            equal("three.observer-no-advance", leader.highWatermark(), 11);
            yes("three.majority-advances", leader.remote(2, 13), true);
            equal("three.majority-end13", leader.highWatermark(), 13);
            yes("three.remote-regression-keeps-commit", leader.remote(2, 12), false);
            equal("three.commit-monotonic", leader.highWatermark(), 13);
            rejects("three.remote-local-id-forbidden", IllegalStateException.class, () -> leader.remote(0, 15));
        }
        try (Leader leader = new Leader(5, 10)) {
            yes("five.local-only", leader.local(20), false);
            yes("five.two-of-five-insufficient", leader.remote(1, 19), false);
            equal("five.no-majority-yet", leader.highWatermark(), -1);
            yes("five.three-of-five-majority", leader.remote(2, 18), true);
            equal("five.median-end18", leader.highWatermark(), 18);
            yes("five.duplicate-not-majority-change", leader.remote(2, 18), false);
        }
        // The bare component assumes caller-validated replication responses.
        // Deliberately violate that input precondition to expose the boundary:
        // this is not a valid KafkaRaftClient transport history and must not be
        // copied into the typed partitionline ACK admission policy.
        try (Leader leader = new Leader(3, 10)) {
            yes("component-boundary.local-only", leader.local(15), false);
            yes("component-boundary.first-over-end-input", leader.remote(1, 100), true);
            equal("component-boundary.first-median-capped-by-local", leader.highWatermark(), 15);
            yes("component-boundary.second-over-end-input", leader.remote(2, 100), true);
            equal("component-boundary.bare-component-permits-over-local-end", leader.highWatermark(), 100);
        }
    }

    private static void followerCases() {
        FollowerState state = new FollowerState(Time.SYSTEM, 5, 1, Endpoints.empty(),
            Optional.empty(), Set.of(0, 1, 2), Optional.empty(), 2000, new LogContext());
        yes("follower.initial-zero", state.updateHighWatermark(OptionalLong.of(0)), true);
        rejects("follower.unknown-cannot-erase-known", IllegalArgumentException.class,
            () -> state.updateHighWatermark(OptionalLong.empty()));
        rejects("follower.negative-known-watermark", IllegalArgumentException.class,
            () -> state.updateHighWatermark(OptionalLong.of(-1)));
        yes("follower.advance", state.updateHighWatermark(OptionalLong.of(2)), true);
        yes("follower.repeat-idempotent", state.updateHighWatermark(OptionalLong.of(2)), false);
        rejects("follower.regression-rejected", IllegalArgumentException.class,
            () -> state.updateHighWatermark(OptionalLong.of(1)));
        equal("follower.committed-end-kept", state.highWatermark().orElseThrow().offset(), 2);
        state.close();
    }

    private static MemoryRecords records(long start, int epoch, String value) {
        return MemoryRecords.withRecords(start, Compression.NONE, epoch,
            new SimpleRecord(1000L, value.getBytes(StandardCharsets.UTF_8)));
    }

    private static KafkaMetadataLog open(Path directory, KafkaScheduler scheduler) throws java.io.IOException {
        Files.createDirectories(directory);
        MetadataLogConfig config = new MetadataLogConfig(new AbstractConfig(
            MetadataLogConfig.CONFIG_DEF, Map.of(), false));
        return KafkaMetadataLog.apply(new TopicPartition("__cluster_metadata", 0), Uuid.METADATA_TOPIC_ID,
            directory.toFile(), Time.SYSTEM, scheduler, config, 0);
    }

    private static void truncationCases(Path work) throws java.io.IOException, InterruptedException {
        Path directory = work.resolve("__cluster_metadata-0");
        if (Files.exists(directory)) throw new IllegalStateException("fresh oracle directory required");
        KafkaScheduler scheduler = new KafkaScheduler(1);
        scheduler.startup();
        try {
            try (KafkaMetadataLog log = open(directory, scheduler)) {
                log.appendAsLeader(records(log.endOffset().offset(), 5, "a"), 5);
                log.appendAsLeader(records(log.endOffset().offset(), 5, "b"), 5);
                log.appendAsLeader(records(log.endOffset().offset(), 5, "uncommitted-c"), 5);
                equal("log.append-end", log.endOffset().offset(), 3);
                log.updateHighWatermark(new LogOffsetMetadata(2));
                equal("log.high-watermark2", log.highWatermark().offset(), 2);
                contents("log.committed-prefix-exact-before-truncate", log, Isolation.COMMITTED, "0:a:1000;1:b:1000;");
                rejects("log.truncate-below-commit-rejected", IllegalArgumentException.class, () -> log.truncateTo(1));
                equal("log.rejected-truncation-keeps-end", log.endOffset().offset(), 3);
                log.truncateTo(2);
                equal("log.truncate-at-commit-removes-only-suffix", log.endOffset().offset(), 2);
                equal("log.truncation-keeps-high-watermark", log.highWatermark().offset(), 2);
                contents("log.committed-prefix-exact-after-truncate", log, Isolation.COMMITTED, "0:a:1000;1:b:1000;");
                log.initializeLeaderEpoch(6);
                log.appendAsLeader(records(log.endOffset().offset(), 6, "uncommitted-new-epoch"), 6);
                equal("log.new-epoch-suffix", log.endOffset().offset(), 3);
                equal("log.epoch5-end", log.endOffsetForEpoch(5).offset(), 2);
                equal("log.truncate-divergent-epoch-to-matching-end",
                    log.truncateToEndOffset(new OffsetAndEpoch(100, 5)), 2);
                equal("log.divergent-suffix-removed", log.endOffset().offset(), 2);
                rejects("log.divergent-truncation-cannot-remove-commit", IllegalArgumentException.class,
                    () -> log.truncateToEndOffset(new OffsetAndEpoch(1, 5)));
                log.flush(true);
            }
            try (KafkaMetadataLog reopened = open(directory, scheduler)) {
                equal("log.synced-truncation-survives-reopen", reopened.endOffset().offset(), 2);
                equal("log.reopened-epoch", reopened.lastFetchedEpoch(), 5);
                // Apache reconstructs quorum HW separately after reopening; this
                // storage component does not prove replicated durable commit.
                equal("log.reopened-high-watermark-component-reset", reopened.highWatermark().offset(), 0);
                contents("log.synced-exact-prefix-survives-reopen", reopened, Isolation.UNCOMMITTED, "0:a:1000;1:b:1000;");
            }
        } finally { scheduler.shutdown(); }
    }

    public static void main(String[] args) throws java.io.IOException, InterruptedException {
        if (args.length != 1 && args.length != 2) throw new IllegalArgumentException("work [--wrong-barrier]");
        leaderCases(args.length == 2 && args[1].equals("--wrong-barrier"));
        followerCases();
        truncationCases(Path.of(args[0]));
        System.out.println("{\"summary\":true,\"checks\":" + checks + ",\"passed\":true}");
    }
}
