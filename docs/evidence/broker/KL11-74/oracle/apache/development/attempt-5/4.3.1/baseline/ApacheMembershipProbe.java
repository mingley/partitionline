package org.apache.kafka.raft;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.internals.AddVoterHandler;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.IdentitySerde;
import org.apache.kafka.raft.internals.KRaftControlRecordStateMachine;
import org.apache.kafka.raft.internals.KafkaRaftLog;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.raft.internals.RemoveVoterHandler;
import org.apache.kafka.raft.internals.RequestSender;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.util.KafkaScheduler;

/** Actual Apache handlers, real control-record log and explicitly local transport adapter. */
public final class ApacheMembershipProbe {
    private static int checks;
    private static int captures;
    private static Path output;
    private static final ListenerName LISTENER = ListenerName.normalised("CONTROLLER");

    private ApacheMembershipProbe() { }

    private static String quote(Object value) {
        return "\"" + String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r") + "\"";
    }

    private static void check(String name, Object actual, Object expected) {
        boolean passed = actual.equals(expected);
        checks++;
        System.out.println("{\"event\":\"assertion\",\"case\":" + quote(name) + ",\"actual\":"
            + quote(actual) + ",\"expected\":" + quote(expected) + ",\"passed\":" + passed + "}");
        if (!passed) throw new AssertionError(name + ": " + actual + " != " + expected);
    }

    private static void rejects(String name, Class<? extends RuntimeException> expected, Runnable action) {
        String outcome = "none";
        try { action.run(); } catch (RuntimeException error) { outcome = error.getClass().getSimpleName(); }
        check(name, outcome, expected.getSimpleName());
    }

    private static final class Clock implements Time {
        private long now = 100;
        @Override public long milliseconds() { return now; }
        @Override public long nanoseconds() { return now * 1_000_000; }
        @Override public void sleep(long ms) { now += ms; }
        @Override public void waitObject(Object object, Supplier<Boolean> condition, long deadline)
            throws InterruptedException {
            throw new UnsupportedOperationException("No component scenario waits on clock objects");
        }
    }

    /** Captures the handler's discovery call; does not send any network request. */
    private static final class Sender implements RequestSender {
        private int sends;
        private boolean available = true;
        @Override public ListenerName listenerName() { return LISTENER; }
        @Override public OptionalLong send(Node destination, Supplier<ApiMessage> request, long now) {
            sends++;
            check("discovery.request-is-api18", request.get().apiKey(), (short) 18);
            check("discovery.synthetic-loopback-endpoint",
                destination.host().equals("127.0.0.1") || destination.host().equals("localhost"), true);
            check("discovery.synthetic-port", destination.port(), 19200 + destination.id());
            return available ? OptionalLong.of(1000) : OptionalLong.empty();
        }
    }

    private static String keyJson(ReplicaKey key) {
        Uuid directory = key.directoryId().orElseThrow();
        return "{\"id\":" + key.id() + ",\"directory_id\":"
            + quote(String.format("%016x%016x", directory.getMostSignificantBits(), directory.getLeastSignificantBits())) + "}";
    }

    private static String votersJson(VoterSet voters) {
        List<String> rows = new ArrayList<>();
        voters.voterKeys().stream().sorted().forEach(key -> rows.add(keyJson(key)));
        return "[" + String.join(",", rows) + "]";
    }

    private static ReplicaKey key(int id) { return ReplicaKey.of(id, new Uuid(1, id + 1L)); }
    private static VoterSet.VoterNode voter(int id) {
        return VoterSet.VoterNode.of(key(id), Endpoints.fromInetSocketAddresses(
            Map.of(LISTENER, new InetSocketAddress("127.0.0.1", 19200 + id))),
            new SupportedVersionRange((short) 0, (short) 1));
    }
    private static VoterSet voters(int count) {
        Map<Integer, VoterSet.VoterNode> map = new LinkedHashMap<>();
        for (int id = 0; id < count; id++) map.put(id, voter(id));
        return VoterSet.fromMap(map);
    }

    private static MemoryRecords bootstrap(VoterSet voters, boolean feature, boolean configuration) {
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(ByteBuffer.allocate(4096),
            RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0, 1000,
            RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
            false, true, 5, 4096)) {
            builder.appendKRaftVersionMessage(1000,
                new KRaftVersionRecord().setVersion((short) 0).setKRaftVersion((short) (feature ? 1 : 0)));
            if (configuration) builder.appendVotersMessage(1000, voters.toVotersRecord((short) 0));
            return builder.build();
        }
    }

    private static final class Harness implements AutoCloseable {
        private final String name;
        private final Clock clock = new Clock();
        private final Sender sender = new Sender();
        private final Metrics metrics = new Metrics();
        private final KafkaScheduler scheduler = new KafkaScheduler(1);
        private final BufferSupplier buffers = BufferSupplier.create();
        private final KafkaRaftLog log;
        private final BatchAccumulator<ByteBuffer> accumulator;
        private final LeaderState<ByteBuffer> leader;
        private final KRaftControlRecordStateMachine control;
        private final AddVoterHandler add;
        private final RemoveVoterHandler remove;
        private final Path directory;
        private final VoterSet initial;

        Harness(Path root, String name, int count, boolean feature, boolean configuration) throws Exception {
            this.name = name;
            directory = root.resolve(name).resolve("__cluster_metadata-0");
            Files.createDirectories(directory);
            initial = voters(count);
            scheduler.startup();
            log = openLog();
            MemoryRecords boot = bootstrap(initial, feature, configuration);
            log.appendAsLeader(boot, 5);
            log.flush(true);
            capture("bootstrap", boot);
            long end = log.endOffset().offset();
            accumulator = new BatchAccumulator<>(5, end, 0, 4096, 8, MemoryPool.NONE,
                clock, Compression.NONE, IdentitySerde.INSTANCE);
            KafkaRaftMetrics raftMetrics = new KafkaRaftMetrics(metrics, "membership-" + name);
            leader = new LeaderState<>(clock, voter(0), 5, 0, initial,
                configuration ? OptionalLong.of(1) : OptionalLong.empty(),
                feature ? KRaftVersion.KRAFT_VERSION_1 : KRaftVersion.KRAFT_VERSION_0,
                initial.voterIds(), accumulator, 2000, new LogContext(), raftMetrics);
            control = new KRaftControlRecordStateMachine(initial, log, IdentitySerde.INSTANCE, buffers,
                4096, new LogContext(), raftMetrics, ignored -> { });
            control.updateState();
            add = new AddVoterHandler(control, sender, clock, new LogContext());
            remove = new RemoveVoterHandler(OptionalInt.of(0), key(0).directoryId().orElseThrow(),
                control, clock, 1000, new LogContext());
            leader.updateLocalState(log.endOffset(), control.lastVoterSet());
            state("initial");
        }

        private KafkaRaftLog openLog() throws java.io.IOException {
            MetadataLogConfig config = new MetadataLogConfig(new AbstractConfig(MetadataLogConfig.CONFIG_DEF, Map.of(), false));
            return KafkaRaftLog.createLog(new TopicPartition("__cluster_metadata", 0), Uuid.METADATA_TOPIC_ID,
                directory.toFile(), clock, scheduler, config, 0);
        }

        private void capture(String operation, MemoryRecords records) throws java.io.IOException {
            ByteBuffer value = records.buffer().duplicate();
            byte[] raw = new byte[value.remaining()];
            value.get(raw);
            String filename = name + "-" + captures++ + "-" + operation + ".control.bin";
            Files.write(output.resolve(filename), raw);
            System.out.println("{\"event\":\"control-batch\",\"scenario\":" + quote(name)
                + ",\"operation\":" + quote(operation) + ",\"file\":" + quote(filename)
                + ",\"bytes\":" + raw.length + "}");
        }

        void state(String operation) {
            System.out.println("{\"event\":\"state\",\"scenario\":" + quote(name)
                + ",\"operation\":" + quote(operation) + ",\"now_ms\":" + clock.now
                + ",\"voters\":" + votersJson(control.lastVoterSet()) + ",\"latest_voters_offset\":"
                + control.lastVoterSetOffset().orElse(-2) + ",\"hw\":" + hw() + ",\"leo\":"
                + log.endOffset().offset() + ",\"pending\":" + leader.isOperationPending(clock.now)
                + ",\"leader_resign\":" + leader.isResignRequested() + "}");
        }

        void action(String operation, String inputs) {
            System.out.println("{\"event\":\"input\",\"scenario\":" + quote(name)
                + ",\"operation\":" + quote(operation) + ",\"now_ms\":" + clock.now
                + ",\"inputs\":" + inputs + "}");
        }
        long hw() { return leader.highWatermark().map(LogOffsetMetadata::offset).orElse(-1L); }
        void remote(int id, long end, long now) {
            clock.now = now;
            action("replica-progress", "{\"replica\":" + keyJson(key(id)) + ",\"end\":" + end + "}");
            leader.updateReplicaState(key(id), now, new LogOffsetMetadata(end));
            log.updateHighWatermark(new LogOffsetMetadata(Math.max(0, hw())));
            add.highWatermarkUpdated(leader);
            remove.highWatermarkUpdated(leader);
            state("progress-" + id + "-" + end);
        }
        void committedInitial() {
            if (initial.size() > 1) remote(1, log.endOffset().offset(), 105);
            check(name + ".prior-config-committed", hw(), log.endOffset().offset());
        }
        CompletableFuture<AddRaftVoterResponseData> requestAdd(int id) {
            action("add-request", "{\"replica\":" + keyJson(key(id)) + ",\"ack_when_committed\":true}");
            return add.handleAddVoterRequest(leader, key(id), voter(id).listeners(), true, clock.now);
        }
        CompletableFuture<RemoveRaftVoterResponseData> requestRemove(ReplicaKey key) {
            action("remove-request", "{\"replica\":" + keyJson(key) + "}");
            return remove.handleRemoveVoterRequest(leader, key, clock.now);
        }
        void featureReply(int source, Optional<ApiVersionsResponseData.SupportedFeatureKey> range) {
            action("feature-reply", "{\"source_id\":" + source + ",\"feature_present\":" + range.isPresent()
                + ",\"min\":" + range.map(r -> (int) r.minVersion()).orElse(-1)
                + ",\"max\":" + range.map(r -> (int) r.maxVersion()).orElse(-1) + "}");
            add.handleApiVersionsResponse(leader, new Node(source, "127.0.0.1", 19200 + source), Errors.NONE, range, clock.now);
        }
        void drain() throws java.io.IOException {
            accumulator.forceDrain();
            for (BatchAccumulator.CompletedBatch<ByteBuffer> batch : accumulator.drain()) {
                try {
                    log.appendAsLeader(batch.data, 5);
                    capture("appended", batch.data);
                } finally { batch.release(); }
            }
            log.flush(true);
            control.updateState();
            leader.updateLocalState(log.endOffset(), control.lastVoterSet());
            state("local-appended");
        }
        void rawConfiguration(VoterSet replacement) throws java.io.IOException {
            action("raw-helper-configuration", "{\"voters\":" + votersJson(replacement) + "}");
            try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(ByteBuffer.allocate(4096),
                RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
                log.endOffset().offset(), 1000, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE, false, true, 5, 4096)) {
                builder.appendVotersMessage(1000, replacement.toVotersRecord((short) 0));
                MemoryRecords batch = builder.build(); log.appendAsLeader(batch, 5);
                capture("raw-helper-configuration", batch);
            }
            log.flush(true); control.updateState(); state("raw-helper-applied");
        }
        void truncateUncommitted(long end) {
            action("truncate-uncommitted", "{\"end\":" + end + "}");
            log.truncateTo(end); control.truncateNewEntries(end); state("truncated");
        }
        void replay() throws java.io.IOException {
            log.flush(true);
            long expectedEnd = log.endOffset().offset();
            log.close();
            try (KafkaRaftLog reopened = openLog()) {
                KRaftControlRecordStateMachine state = new KRaftControlRecordStateMachine(initial, reopened,
                    IdentitySerde.INSTANCE, buffers, 4096, new LogContext(),
                    new KafkaRaftMetrics(metrics, "reopened-" + name), ignored -> { });
                state.updateState();
                check(name + ".replay-voter-set", state.lastVoterSet(), control.lastVoterSet());
                check(name + ".replay-config-offset", state.lastVoterSetOffset(), control.lastVoterSetOffset());
                check(name + ".reopen-end", reopened.endOffset().offset(), expectedEnd);
                check(name + ".storage-HW-not-quorum-proof", reopened.highWatermark().offset(), 0L);
            }
        }
        @Override public void close() {
            leader.close(); log.close(); buffers.close(); metrics.close();
            try { scheduler.shutdown(); } catch (InterruptedException error) {
                Thread.currentThread().interrupt(); throw new IllegalStateException(error);
            }
        }
    }

    private static Optional<ApiVersionsResponseData.SupportedFeatureKey> supported() {
        return Optional.of(new ApiVersionsResponseData.SupportedFeatureKey().setName("kraft.version")
            .setMinVersion((short) 0).setMaxVersion((short) 1));
    }

    private static void addCases(Path root, boolean wrongMajority) throws Exception {
        try (Harness h = new Harness(root, "add-new-majority", 3, true, true)) {
            h.committedInitial();
            h.remote(3, 2, 106);
            check("add.observer-caught-up", h.leader.isReplicaCaughtUp(key(3), 106), true);
            CompletableFuture<AddRaftVoterResponseData> future = h.requestAdd(3);
            check("add.pending-discovery", future.isDone(), false);
            check("add.remove-during-pending-add", h.requestRemove(key(2)).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.featureReply(9, supported());
            check("add.wrong-source-ignored", future.isDone(), false);
            h.featureReply(3, supported());
            h.drain();
            check("add.new-set-active-before-commit", h.control.lastVoterSet().size(), 4);
            check("add.only-old-majority", future.isDone(), wrongMajority);
            h.remote(1, 3, 107);
            check("add.two-of-four-insufficient", future.isDone(), false);
            h.remote(3, 3, 108);
            check("add.new-majority-commits", future.join().errorCode(), Errors.NONE.code());
            check("add.committed-end", h.hw(), 3L);
            check("add.pending-cleared", h.leader.isOperationPending(108), false);
            h.replay();
        }
        try (Harness h = new Harness(root, "add-prerequisites", 3, true, true)) {
            check("add.no-current-epoch-HW", h.requestAdd(3).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.committedInitial();
            check("add.duplicate-id", h.requestAdd(1).join().errorCode(), Errors.DUPLICATE_VOTER.code());
            rejects("component.missing-listener-throws", IllegalArgumentException.class,
                () -> h.add.handleAddVoterRequest(h.leader, key(3), Endpoints.empty(), true, h.clock.now));
            h.sender.available = false;
            check("add.discovery-unavailable", h.requestAdd(3).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
        }
        try (Harness h = new Harness(root, "add-no-feature", 3, false, true)) {
            h.committedInitial();
            check("add.cluster-feature-zero", h.requestAdd(3).join().errorCode(), Errors.UNSUPPORTED_VERSION.code());
        }
        try (Harness h = new Harness(root, "add-no-config", 3, true, false)) {
            h.committedInitial();
            check("add.no-prior-config-entry", h.requestAdd(3).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
        }
        for (boolean missing : new boolean[] {true, false}) {
            try (Harness h = new Harness(root, "add-feature-" + missing, 3, true, true)) {
                h.committedInitial(); h.remote(3, 2, 106);
                CompletableFuture<AddRaftVoterResponseData> future = h.requestAdd(3);
                h.featureReply(3, missing ? Optional.empty() : Optional.of(
                    new ApiVersionsResponseData.SupportedFeatureKey().setName("kraft.version").setMinVersion((short) 2).setMaxVersion((short) 3)));
                check("add.missing-or-incompatible-feature-" + missing, future.join().errorCode(), Errors.INVALID_REQUEST.code());
            }
        }
        try (Harness h = new Harness(root, "add-lagging", 3, true, true)) {
            h.committedInitial();
            CompletableFuture<AddRaftVoterResponseData> future = h.requestAdd(3);
            h.featureReply(3, supported());
            check("add.no-catch-up-history", future.join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
        }
        try (Harness h = new Harness(root, "add-timeout-uncommitted", 3, true, true)) {
            h.committedInitial(); h.remote(3, 2, 106);
            CompletableFuture<AddRaftVoterResponseData> future = h.requestAdd(3);
            check("add.concurrent-request", h.requestAdd(4).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.featureReply(3, supported()); h.drain();
            h.clock.now = 1106;
            h.leader.maybeExpirePendingOperation(1106);
            check("add.timeout-after-config-append", future.join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            check("add.timed-out-config-stays-active", h.control.lastVoterSet().size(), 4);
            check("add.uncommitted-prior-config-blocks-retry", h.requestAdd(4).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            check("remove.uncommitted-prior-config-blocks-retry", h.requestRemove(key(2)).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.replay();
        }
        try (Harness h = new Harness(root, "add-expired-probe", 3, true, true)) {
            h.committedInitial(); h.remote(3, 2, 106);
            CompletableFuture<AddRaftVoterResponseData> future = h.requestAdd(3);
            h.clock.now = 1106; h.leader.maybeExpirePendingOperation(1106);
            check("add.expired-discovery-probe", future.join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.featureReply(3, supported()); h.drain();
            check("add.expired-reply-does-not-append", h.log.endOffset().offset(), 2L);
            check("add.expired-reply-keeps-prior-config", h.control.lastVoterSet().size(), 3);
        }
        // BEGIN_NEWER_EARLY_ACK
        try (Harness h = new Harness(root, "add-newer-early-ack", 3, true, true)) {
            h.committedInitial(); h.remote(3, 2, 106);
            h.action("add-request", "{\"replica\":" + keyJson(key(3)) + ",\"ack_when_committed\":false}");
            CompletableFuture<AddRaftVoterResponseData> future = h.add.handleAddVoterRequest(
                h.leader, key(3), voter(3).listeners(), false, h.clock.now);
            h.featureReply(3, supported());
            check("newer.early-ack-before-drain", future.join().errorCode(), Errors.NONE.code());
            h.drain();
            check("newer.early-ack-HW-remains-prior", h.hw(), 2L);
            check("newer.early-ack-keeps-pending", h.leader.isOperationPending(106), true);
            check("newer.early-ack-next-config-blocked", h.requestAdd(4).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
        }
        // END_NEWER_EARLY_ACK
        try (Harness h = new Harness(root, "catch-up-component-boundary", 3, true, true)) {
            h.committedInitial(); h.remote(3, 2, 106);
            h.remote(3, 1, 107);
            check("component.regressed-offset-retains-catch-up", h.leader.isReplicaCaughtUp(key(3), 107), true);
            check("component.before-one-hour", h.leader.isReplicaCaughtUp(key(3), 3600106), true);
            check("component.exact-one-hour-expires", h.leader.isReplicaCaughtUp(key(3), 3600107), false);
            check("component.wrong-directory-not-caught-up", h.leader.isReplicaCaughtUp(ReplicaKey.of(3, new Uuid(9, 9)), 107), false);
        }
    }

    private static void removeCases(Path root, boolean wrongResign) throws Exception {
        try (Harness h = new Harness(root, "remove-leader", 3, true, true)) {
            h.committedInitial(); h.remote(2, 2, 106);
            CompletableFuture<RemoveRaftVoterResponseData> future = h.requestRemove(key(0));
            h.drain();
            check("remove.leader-removed-before-commit", h.control.lastVoterSet().isVoter(key(0)), false);
            check("remove.no-early-resign", h.leader.isResignRequested(), wrongResign);
            h.remote(1, 3, 107);
            check("remove.one-of-two-insufficient-no-self-vote", future.isDone(), false);
            h.remote(2, 3, 108);
            check("remove.new-majority-commits", future.join().errorCode(), Errors.NONE.code());
            check("remove.resign-after-commit", h.leader.isResignRequested(), true);
            check("remove.committed-end", h.hw(), 3L);
            h.replay();
        }
        try (Harness h = new Harness(root, "remove-follower", 3, true, true)) {
            h.committedInitial();
            check("remove.wrong-directory", h.requestRemove(ReplicaKey.of(2, new Uuid(9, 9))).join().errorCode(), Errors.VOTER_NOT_FOUND.code());
            check("remove.unknown-id", h.requestRemove(key(9)).join().errorCode(), Errors.VOTER_NOT_FOUND.code());
            CompletableFuture<RemoveRaftVoterResponseData> future = h.requestRemove(key(2)); h.drain();
            h.remote(2, 3, 106);
            check("remove.removed-voter-no-contribution", future.isDone(), false);
            check("remove.add-during-pending-remove", h.requestAdd(3).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.remote(1, 3, 107);
            check("remove.remaining-majority-commits", future.join().errorCode(), Errors.NONE.code());
            check("remove.retained-leader-does-not-resign", h.leader.isResignRequested(), false);
        }
        try (Harness h = new Harness(root, "remove-prerequisites", 3, true, true)) {
            check("remove.no-current-epoch-HW", h.requestRemove(key(2)).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.committedInitial();
            CompletableFuture<RemoveRaftVoterResponseData> future = h.requestRemove(key(2)); h.drain();
            check("remove.concurrent-request", h.requestRemove(key(1)).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            h.clock.now = 1105; h.leader.maybeExpirePendingOperation(1105);
            check("remove.timeout-uncommitted", future.join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
            check("remove.timed-out-config-stays-active", h.control.lastVoterSet().size(), 2);
            h.replay();
        }
        try (Harness h = new Harness(root, "remove-no-feature", 3, false, true)) {
            h.committedInitial();
            check("remove.cluster-feature-zero", h.requestRemove(key(2)).join().errorCode(), Errors.UNSUPPORTED_VERSION.code());
        }
        try (Harness h = new Harness(root, "remove-no-config", 3, true, false)) {
            h.committedInitial();
            check("remove.no-prior-config-entry", h.requestRemove(key(2)).join().errorCode(), Errors.REQUEST_TIMED_OUT.code());
        }
        try (Harness h = new Harness(root, "remove-only-voter", 1, true, true)) {
            h.committedInitial();
            check("remove.cannot-empty-voter-set", h.requestRemove(key(0)).join().errorCode(), Errors.VOTER_NOT_FOUND.code());
        }
    }

    private static void historyCases(Path root, boolean wrongHistory) throws Exception {
        try (Harness h = new Harness(root, "history-permissive-disjoint", 3, true, true)) {
            h.committedInitial();
            Map<Integer, VoterSet.VoterNode> replacements = Map.of(10, voter(10), 11, voter(11), 12, voter(12));
            VoterSet disjoint = VoterSet.fromMap(replacements);
            check("history.disjoint-majorities", h.initial.hasOverlappingMajority(disjoint), false);
            h.rawConfiguration(disjoint);
            check("history.bare-helper-accepts-disjoint", h.control.lastVoterSet(), wrongHistory ? h.initial : disjoint);
            check("history.bare-helper-uncommitted-active", h.control.lastVoterSetOffset().orElseThrow(), 2L);
            h.replay();
        }
        try (Harness h = new Harness(root, "history-truncate-uncommitted", 3, true, true)) {
            h.committedInitial(); h.remote(3, 2, 106);
            CompletableFuture<AddRaftVoterResponseData> future = h.requestAdd(3);
            h.featureReply(3, supported()); h.drain();
            check("history.pending-before-truncation", future.isDone(), false);
            h.truncateUncommitted(2);
            check("history.truncate-restores-prior-voters", h.control.lastVoterSet(), h.initial);
            check("history.truncate-restores-prior-config-offset", h.control.lastVoterSetOffset().orElseThrow(), 1L);
            h.replay();
        }
    }

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); output = Path.of(args[1]);
        Files.createDirectories(root); Files.createDirectories(output);
        addCases(root, args.length > 2 && args[2].equals("wrong-old-majority"));
        removeCases(root, args.length > 2 && args[2].equals("wrong-early-resign"));
        historyCases(root, args.length > 2 && args[2].equals("wrong-history-rejects"));
        System.out.println("{\"event\":\"summary\",\"passed\":true,\"assertions\":" + checks + "}");
    }
}
