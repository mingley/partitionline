import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.ElectionState;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.raft.QuorumStateStore;
import org.apache.kafka.server.common.KRaftVersion;

/** Executes pinned Apache handlers on explicit synthetic log/store/network dependencies. */
public final class ControllerOracle {
    private ControllerOracle() { }
    private static final String TOPIC = "__cluster_metadata";
    private static final ListenerName LISTENER = new ListenerName("CONTROLLER");
    private static final int CORRELATION = 77;
    private static final class Clock implements Time {
        @Override public long milliseconds() { return 0; }
        @Override public long nanoseconds() { return 0; }
        @Override public void sleep(long ms) { throw new IllegalStateException("No sleep"); }
        @Override public void waitObject(Object object, Supplier<Boolean> condition, long deadline) {
            throw new IllegalStateException("No waits permitted");
        }
    }
    private static final class Store implements QuorumStateStore {
        private ElectionState state = ElectionState.withUnknownLeader(0, Set.of(1, 2, 3));
        private final List<String> writes = new ArrayList<>();
        @Override public Optional<ElectionState> readElectionState() { return Optional.ofNullable(state); }
        @Override public void writeElectionState(ElectionState value, KRaftVersion version) {
            state = value; writes.add(value.toString());
        }
        @Override public Path path() { return Path.of("synthetic-no-filesystem-store"); }
        @Override public void clear() { state = null; }
    }
    private static final class Context implements AutoCloseable {
        private long end;
        private int logEpoch;
        private final Store store = new Store();
        private final Metrics metrics = new Metrics();
        private final Object client;
        private Context(int initialEpoch) throws Exception {
            store.state = ElectionState.withUnknownLeader(initialEpoch, Set.of(1, 2, 3));
            Class<?> raft = Class.forName("org.apache.kafka.raft.KafkaRaftClient");
            Constructor<?> constructor = Arrays.stream(raft.getConstructors())
                .filter(c -> c.getParameterCount() == 14).findFirst().orElseThrow();
            QuorumConfig config = new QuorumConfig(new AbstractConfig(QuorumConfig.CONFIG_DEF,
                Map.of("controller.quorum.election.timeout.ms", 5,
                    "controller.quorum.fetch.timeout.ms", 5,
                    "controller.quorum.election.backoff.max.ms", 1000), false));
            Class<?>[] types = constructor.getParameterTypes(); Object[] values = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                Class<?> type = types[i];
                values[i] = switch (type.getSimpleName()) {
                    case "OptionalInt" -> OptionalInt.of(1);
                    case "Uuid" -> new Uuid(1, 1);
                    case "Time" -> new Clock();
                    case "LogContext" -> type.getConstructor().newInstance();
                    case "boolean" -> true;
                    case "String" -> "cluster-fixed";
                    case "Collection" -> List.of();
                    case "Endpoints" -> type.getMethod("empty").invoke(null);
                    case "SupportedVersionRange" -> type.getConstructor(short.class, short.class)
                        .newInstance((short) 0, (short) 0);
                    case "QuorumConfig" -> config;
                    default -> proxy(type);
                };
            }
            client = constructor.newInstance(values);
            Method initialize = Arrays.stream(raft.getMethods()).filter(m -> m.getName().equals("initialize"))
                .findFirst().orElseThrow();
            Map<Integer, InetSocketAddress> voters = Map.of(
                1, new InetSocketAddress("127.0.0.1", 9101),
                2, new InetSocketAddress("127.0.0.1", 9102),
                3, new InetSocketAddress("127.0.0.1", 9103));
            initialize.invoke(client, voters, store, metrics, proxy(initialize.getParameterTypes()[3]));
        }
        private Object proxy(Class<?> type) {
            return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (p, m, args) ->
                switch (m.getName()) {
                    case "toString" -> "SyntheticBounded" + type.getSimpleName();
                    case "hashCode" -> 7;
                    case "equals" -> p == args[0];
                    case "topicPartition" -> new TopicPartition(TOPIC, 0);
                    case "topicId" -> new Uuid(0, 1);
                    case "endOffset", "highWatermark" -> new LogOffsetMetadata(end);
                    case "startOffset" -> 0L;
                    case "lastFetchedEpoch" -> logEpoch;
                    case "latestSnapshotId", "earliestSnapshotId", "latestSnapshot", "earliestSnapshot",
                        "maybeClean" -> Optional.empty();
                    case "listenerName" -> LISTENER;
                    case "newCorrelationId" -> CORRELATION;
                    case "close", "flush", "initializeLeaderEpoch", "setIgnoredStaticVoters" -> null;
                    default -> throw new IllegalStateException("Unexpected synthetic dependency call: " + m);
                });
        }
        private ApiMessage invoke(ApiMessage request, short version) throws Exception {
            Class<?> inbound = Class.forName("org.apache.kafka.raft.RaftRequest$Inbound");
            Object meta = inbound.getConstructor(ListenerName.class, int.class, short.class, ApiMessage.class, long.class)
                .newInstance(LISTENER, CORRELATION, version, request, 0L);
            String methodName = switch (request.apiKey()) {
                case 52 -> "handleVoteRequest";
                case 53 -> "handleBeginQuorumEpochRequest";
                case 54 -> "handleEndQuorumEpochRequest";
                default -> throw new IllegalArgumentException("Not a Raft handler");
            };
            Method method = request.apiKey() == 52
                ? client.getClass().getDeclaredMethod(methodName, inbound)
                : client.getClass().getDeclaredMethod(methodName, inbound, long.class);
            method.setAccessible(true);
            return (ApiMessage) (request.apiKey() == 52 ? method.invoke(client, meta) : method.invoke(client, meta, 0L));
        }
        private String remainingFetch() throws Exception {
            Field field = client.getClass().getDeclaredField("quorum"); field.setAccessible(true);
            Object quorum = field.get(client);
            if (!(Boolean) quorum.getClass().getMethod("isFollower").invoke(quorum)) return "null";
            Object follower = quorum.getClass().getMethod("followerStateOrThrow").invoke(quorum);
            return quorum.getClass().getMethod("followerStateOrThrow").getReturnType()
                .getMethod("remainingFetchTimeMs", long.class).invoke(follower, 0L).toString();
        }
        @Override public void close() { metrics.close(); }
    }
    private record Case(String name, ApiMessage request, short version, String setup,
            String policy, String term, String vote, String leader, String deadline) { }
    private static Case live(String name, ApiMessage request, String setup) {
        return new Case(name, request, (short) 0, setup, "actual-handler", "-", "-", "-", "-");
    }
    private static Case local(String name, ApiMessage request, String setup, String policy) {
        return new Case(name, request, (short) 0, setup, policy, "-", "-", "-", "-");
    }
    private static VoteRequestData vote(String cluster, String topic, int partition, int candidate,
            int epoch, int lastEpoch, long end) {
        return new VoteRequestData().setClusterId(cluster).setTopics(List.of(new VoteRequestData.TopicData()
            .setTopicName(topic).setPartitions(List.of(new VoteRequestData.PartitionData()
                .setPartitionIndex(partition).setReplicaId(candidate).setReplicaEpoch(epoch)
                .setLastOffsetEpoch(lastEpoch).setLastOffset(end)))));
    }
    private static BeginQuorumEpochRequestData begin(String cluster, int leader, int epoch) {
        return new BeginQuorumEpochRequestData().setClusterId(cluster).setTopics(List.of(
            new BeginQuorumEpochRequestData.TopicData().setTopicName(TOPIC).setPartitions(List.of(
                new BeginQuorumEpochRequestData.PartitionData().setPartitionIndex(0)
                    .setLeaderId(leader).setLeaderEpoch(epoch)))));
    }
    private static EndQuorumEpochRequestData end(String cluster, int leader, int epoch, List<Integer> successors) {
        return new EndQuorumEpochRequestData().setClusterId(cluster).setTopics(List.of(
            new EndQuorumEpochRequestData.TopicData().setTopicName(TOPIC).setPartitions(List.of(
                new EndQuorumEpochRequestData.PartitionData().setPartitionIndex(0)
                    .setLeaderId(leader).setLeaderEpoch(epoch).setPreferredSuccessors(successors)))));
    }
    private static List<Case> cases() {
        List<Case> out = new ArrayList<>();
        for (short v = 0; v <= 4; v++) out.add(new Case("api-versions-" + v,
            new ApiVersionsRequestData().setClientSoftwareName("controller-oracle").setClientSoftwareVersion("1.0"),
            v, "fresh", "controller-profile-advertisement", "1", "none", "none", "5"));
        out.add(new Case("vote-positive", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 0), (short) 0,
            "fresh", "actual-handler", "2", "2", "none", "5"));
        out.add(live("vote-null-cluster", vote(null, TOPIC, 0, 2, 1, 0, 0), "fresh"));
        out.add(live("vote-cluster-mismatch", vote("other", TOPIC, 0, 2, 1, 0, 0), "fresh"));
        out.add(live("vote-topic-mismatch", vote("cluster-fixed", "other", 0, 2, 1, 0, 0), "fresh"));
        out.add(live("vote-partition-mismatch", vote("cluster-fixed", TOPIC, 1, 2, 1, 0, 0), "fresh"));
        out.add(live("vote-empty-topics", new VoteRequestData().setClusterId("cluster-fixed"), "fresh"));
        VoteRequestData two = vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 0);
        two.setTopics(List.of(two.topics().get(0), two.topics().get(0).duplicate()));
        out.add(live("vote-two-topics", two, "fresh"));
        out.add(live("vote-negative-candidate", vote("cluster-fixed", TOPIC, 0, -1, 1, 0, 0), "fresh"));
        out.add(local("vote-nonmember", vote("cluster-fixed", TOPIC, 0, 99, 1, 0, 0), "fresh", "fixed-membership"));
        out.add(live("vote-negative-offset", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, -1), "fresh"));
        out.add(live("vote-negative-log-epoch", vote("cluster-fixed", TOPIC, 0, 2, 1, -1, 0), "fresh"));
        out.add(live("vote-log-epoch-equals-election", vote("cluster-fixed", TOPIC, 0, 2, 1, 1, 1), "fresh"));
        out.add(live("vote-negative-election", vote("cluster-fixed", TOPIC, 0, 2, -1, 0, 0), "fresh"));
        out.add(local("vote-nonzero-epoch-empty-offset", vote("cluster-fixed", TOPIC, 0, 2, 2, 1, 0),
            "fresh", "reserved-empty-log-coordinate"));
        out.add(live("vote-epoch-zero-nonempty-equal", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 5), "log:0:5"));
        out.add(live("vote-epoch-zero-nonempty-stale", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 4), "log:0:5"));
        out.add(live("vote-higher-log-epoch", vote("cluster-fixed", TOPIC, 0, 2, 2, 1, 1), "log:0:5"));
        out.add(live("vote-lower-log-epoch", vote("cluster-fixed", TOPIC, 0, 2, 2, 0, 100), "log:1:1"));
        out.add(live("vote-stale-election", vote("cluster-fixed", TOPIC, 0, 3, 0, 0, 0), "req:vote-positive"));
        out.add(live("vote-fenced-election", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 0), "req:begin-higher-leader"));
        out.add(live("vote-repeat-same-candidate", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 0), "req:vote-positive"));
        out.add(live("vote-other-candidate-same-election", vote("cluster-fixed", TOPIC, 0, 3, 1, 0, 0), "req:vote-positive"));
        out.add(live("vote-known-leader", vote("cluster-fixed", TOPIC, 0, 3, 1, 0, 0), "req:begin-positive"));
        out.add(new Case("begin-positive", begin("cluster-fixed", 2, 1), (short) 0, "fresh", "actual-handler",
            "2", "none", "2", "5"));
        out.add(live("begin-null-cluster", begin(null, 2, 1), "fresh"));
        out.add(live("begin-cluster-mismatch", begin("other", 2, 1), "fresh"));
        BeginQuorumEpochRequestData wrongBeginTopic = begin("cluster-fixed", 2, 1);
        wrongBeginTopic.topics().get(0).setTopicName("other");
        out.add(live("begin-topic-mismatch", wrongBeginTopic, "fresh"));
        BeginQuorumEpochRequestData wrongBeginPartition = begin("cluster-fixed", 2, 1);
        wrongBeginPartition.topics().get(0).partitions().get(0).setPartitionIndex(1);
        out.add(live("begin-partition-mismatch", wrongBeginPartition, "fresh"));
        out.add(live("begin-empty-topics", new BeginQuorumEpochRequestData().setClusterId("cluster-fixed"), "fresh"));
        out.add(live("begin-negative-leader", begin("cluster-fixed", -1, 1), "fresh"));
        out.add(local("begin-nonmember", begin("cluster-fixed", 99, 1), "fresh", "fixed-membership"));
        out.add(live("begin-stale-election", begin("cluster-fixed", 3, 0), "req:begin-positive"));
        out.add(live("begin-repeat", begin("cluster-fixed", 2, 1), "req:begin-positive"));
        out.add(local("begin-conflicting-leader", begin("cluster-fixed", 3, 1), "req:begin-positive", "leader-conflict-error"));
        out.add(live("begin-higher-leader", begin("cluster-fixed", 3, 2), "req:begin-positive"));
        out.add(live("begin-after-vote-same-candidate", begin("cluster-fixed", 2, 1), "req:vote-positive"));
        out.add(new Case("begin-after-vote-other-candidate", begin("cluster-fixed", 3, 1), (short) 0,
            "req:vote-positive", "actual-handler", "2", "2", "3", "5"));
        for (List<Integer> successors : List.of(List.<Integer>of(), List.of(1, 3), List.of(3, 1), List.of(3, 1, 2), List.of(2, 3, 1), List.of(2, 3))) {
            String name = successors.isEmpty() ? "end-empty" : successors.equals(List.of(1, 3)) ? "end-rank-zero"
                : successors.equals(List.of(3, 1)) ? "end-rank-one" : successors.equals(List.of(3, 1, 2)) ? "end-rank-one-size-three"
                : successors.equals(List.of(2, 3, 1)) ? "end-rank-two" : "end-absent";
            String deadline = successors.isEmpty() || successors.get(0) == 1 ? "0"
                : successors.size() == 2 && successors.contains(1) ? "500"
                : successors.equals(List.of(3, 1, 2)) ? "250" : successors.contains(1) ? "500" : "1000";
            out.add(new Case(name, end("cluster-fixed", 2, 1, successors), (short) 0, "req:begin-positive",
                "actual-handler", "2", "none", "2", deadline));
        }
        out.add(live("end-fresh-transitions-to-follower", end("cluster-fixed", 2, 1, List.of(1)), "fresh"));
        out.add(live("end-null-cluster", end(null, 2, 1, List.of(1)), "req:begin-positive"));
        out.add(live("end-cluster-mismatch", end("other", 2, 1, List.of(1)), "req:begin-positive"));
        EndQuorumEpochRequestData wrongEndTopic = end("cluster-fixed", 2, 1, List.of(1));
        wrongEndTopic.topics().get(0).setTopicName("other");
        out.add(live("end-topic-mismatch", wrongEndTopic, "fresh"));
        EndQuorumEpochRequestData wrongEndPartition = end("cluster-fixed", 2, 1, List.of(1));
        wrongEndPartition.topics().get(0).partitions().get(0).setPartitionIndex(1);
        out.add(live("end-partition-mismatch", wrongEndPartition, "fresh"));
        out.add(live("end-empty-topics", new EndQuorumEpochRequestData().setClusterId("cluster-fixed"), "fresh"));
        out.add(local("end-nonmember-leader", end("cluster-fixed", 99, 1, List.of(1)), "fresh", "fixed-membership"));
        out.add(local("end-self-leader", end("cluster-fixed", 1, 1, List.of(1)), "fresh", "remote-end-leader-required"));
        out.add(live("end-stale-election", end("cluster-fixed", 2, 0, List.of(1)), "req:begin-positive"));
        out.add(live("end-negative-leader", end("cluster-fixed", -1, 1, List.of(1)), "req:begin-positive"));
        out.add(local("end-conflicting-leader", end("cluster-fixed", 3, 1, List.of(1)), "req:begin-positive", "leader-conflict-error"));
        out.add(local("end-duplicate-successors", end("cluster-fixed", 2, 1, List.of(1, 1)), "req:begin-positive", "unique-fixed-successors"));
        out.add(local("end-nonmember-successor", end("cluster-fixed", 2, 1, List.of(99, 1)), "req:begin-positive", "unique-fixed-successors"));
        for (int n : new int[]{31, 32, 33}) {
            List<Integer> successors = new ArrayList<>(); successors.add(3); successors.add(1);
            while (successors.size() < n) successors.add(3);
            out.add(local("end-successor-count-" + n, end("cluster-fixed", 2, 1, successors),
                "req:begin-positive", n > 31 ? "close-successor-bound" : "unique-fixed-successors"));
        }
        out.add(new Case("vote-unsupported-v1", vote("cluster-fixed", TOPIC, 0, 2, 1, 0, 0), (short) 1,
            "fresh", "close-modern-profile", "1", "none", "none", "5"));
        out.add(new Case("begin-unsupported-v1", begin("cluster-fixed", 2, 1), (short) 1,
            "fresh", "close-modern-profile", "1", "none", "none", "5"));
        out.add(new Case("end-unsupported-v1", end("cluster-fixed", 2, 1, List.of()), (short) 1,
            "fresh", "close-modern-profile", "1", "none", "none", "5"));
        return out;
    }
    private static String quote(String value) {
        if (value == null) return "null";
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\"";
    }
    private static byte[] encode(Message data, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(data.size(cache, version));
        data.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) throw new AssertionError("serializer underwrite");
        return buffer.array();
    }
    private static byte[] join(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length); System.arraycopy(b, 0, result, a.length, b.length); return result;
    }
    private static byte[] frame(byte[] payload) { return join(ByteBuffer.allocate(4).putInt(payload.length).array(), payload); }
    private static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static ApiVersionsResponseData advertisement() {
        ApiVersionsResponseData response = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersionCollection versions = new ApiVersionsResponseData.ApiVersionCollection();
        for (int key : new int[]{18, 52, 53, 54}) versions.add(new ApiVersionsResponseData.ApiVersion()
            .setApiKey((short) key).setMinVersion((short) 0).setMaxVersion((short) (key == 18 ? 4 : 0)));
        return response.setApiKeys(versions);
    }
    private static ApiMessage policyError(Case c) {
        int epoch = c.setup().contains("req:") ? 1 : 0;
        int leader = c.setup().contains("begin-positive") ? 2 : -1;
        return switch (c.request().apiKey()) {
            case 52 -> new VoteResponseData().setTopics(List.of(new VoteResponseData.TopicData().setTopicName(TOPIC)
                .setPartitions(List.of(new VoteResponseData.PartitionData().setPartitionIndex(0)
                    .setErrorCode((short) 42).setLeaderEpoch(epoch).setLeaderId(leader).setVoteGranted(false)))));
            case 53 -> new BeginQuorumEpochResponseData().setTopics(List.of(new BeginQuorumEpochResponseData.TopicData()
                .setTopicName(TOPIC).setPartitions(List.of(new BeginQuorumEpochResponseData.PartitionData()
                    .setPartitionIndex(0).setErrorCode((short) 42).setLeaderEpoch(epoch).setLeaderId(leader)))));
            case 54 -> new EndQuorumEpochResponseData().setTopics(List.of(new EndQuorumEpochResponseData.TopicData()
                .setTopicName(TOPIC).setPartitions(List.of(new EndQuorumEpochResponseData.PartitionData()
                    .setPartitionIndex(0).setErrorCode((short) 42).setLeaderEpoch(epoch).setLeaderId(leader)))));
            default -> throw new IllegalArgumentException("No profile error for API");
        };
    }
    private static void decodeResponse(ApiKeys key, short version, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        ResponseHeader header = ResponseHeader.parse(buffer, key.responseHeaderVersion(version));
        if (header.correlationId() != CORRELATION) throw new AssertionError("correlation");
        AbstractResponse parsed = AbstractResponse.parseResponse(key, new ByteBufferAccessor(buffer), version);
        if (buffer.hasRemaining()) throw new AssertionError("trailing response bytes");
        byte[] reconstructed = join(encode(header.data(), header.headerVersion()), encode(parsed.data(), version));
        if (!Arrays.equals(payload, reconstructed)) throw new AssertionError("response round trip");
    }
    private static void assertApacheOutcome(Case c, ApiMessage actual, String failure, String fetch) {
        if (c.name().equals("begin-conflicting-leader") || c.name().equals("end-conflicting-leader")) {
            if (failure == null || !failure.startsWith("java.lang.IllegalStateException:"))
                throw new AssertionError("Expected authentic same-epoch leader conflict exception");
        }
        if (c.request().apiKey() == 52 && actual != null) {
            VoteResponseData data = (VoteResponseData) actual;
            if (List.of("vote-positive", "vote-nonmember", "vote-nonzero-epoch-empty-offset",
                    "vote-epoch-zero-nonempty-equal", "vote-higher-log-epoch", "vote-repeat-same-candidate").contains(c.name())) {
                if (!data.topics().get(0).partitions().get(0).voteGranted())
                    throw new AssertionError("Expected authentic Apache grant: " + c.name());
            }
            if (List.of("vote-epoch-zero-nonempty-stale", "vote-lower-log-epoch",
                    "vote-other-candidate-same-election", "vote-known-leader").contains(c.name())) {
                if (data.topics().get(0).partitions().get(0).voteGranted())
                    throw new AssertionError("Expected authentic Apache denial: " + c.name());
            }
            if (c.name().equals("vote-fenced-election")
                    && data.topics().get(0).partitions().get(0).errorCode() != 74)
                throw new AssertionError("Expected authentic fenced epoch74");
        }
        if (c.name().equals("begin-after-vote-other-candidate")) {
            BeginQuorumEpochResponseData data = (BeginQuorumEpochResponseData) actual;
            if (data.topics().get(0).partitions().get(0).leaderId() != 3
                    || data.topics().get(0).partitions().get(0).errorCode() != 0)
                throw new AssertionError("Actual Apache accepts elected leader despite earlier different vote");
        }
        if (c.request().apiKey() == 54 && c.policy().equals("actual-handler") && !c.deadline().equals("-")) {
            if (!fetch.equals(c.deadline())) throw new AssertionError("Actual Apache End backoff differs: " + c.name());
        }
        if (c.name().startsWith("end-successor-count-")) {
            String expected = c.name().endsWith("33") ? "1000" : "0";
            if (!fetch.equals(expected)) throw new AssertionError("Actual Java shift masking counterexample differs");
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) throw new IllegalArgumentException("OUTPUT RELEASE [ACTUAL_RESPONSES]");
        Path output = Path.of(args[0]); Files.createDirectories(output);
        List<Case> cases = cases(); Map<String, Case> byName = new LinkedHashMap<>();
        for (Case c : cases) if (byName.put(c.name(), c) != null) throw new AssertionError("duplicate case");
        List<String> observations = new ArrayList<>(); StringBuilder table = new StringBuilder(
            "name\tkey\tversion\tcorrelation\trequest_file\tresponse_file\tsetup\texpected_disposition\tcore_term\tvoted_for\tleader\tdeadline_ms\n");
        int decodedActual = 0;
        int decodedTransport = 0;
        for (Case c : cases) {
            ApiKeys key = ApiKeys.forId(c.request().apiKey());
            RequestHeader requestHeader = new RequestHeader(key, c.version(), "controller-oracle", CORRELATION);
            byte[] request = join(encode(requestHeader.data(), requestHeader.headerVersion()), encode(c.request(), c.version()));
            ByteBuffer rb = ByteBuffer.wrap(request); RequestHeader parsedHeader = RequestHeader.parse(rb);
            ApiMessage parsedRequest = AbstractRequest.parseRequest(key, c.version(), new ByteBufferAccessor(rb)).request.data();
            if (rb.hasRemaining() || !Arrays.equals(encode(c.request(), c.version()), encode(parsedRequest, c.version())))
                throw new AssertionError("request round trip");
            ApiMessage actual = null; String failure = null; String fetch = "null"; List<String> writes = List.of();
            int initialEpoch = c.setup().startsWith("log:") ? Integer.parseInt(c.setup().split(":")[1]) : 0;
            if (key.id != 18) try (Context context = new Context(initialEpoch)) {
                if (!c.setup().equals("fresh")) for (String action : c.setup().split("\\|")) {
                    String[] parts = action.split(":");
                    if (parts[0].equals("log")) { context.logEpoch = Integer.parseInt(parts[1]); context.end = Long.parseLong(parts[2]); }
                    else if (parts[0].equals("req")) context.invoke(byName.get(parts[1]).request(), (short) 0);
                    else throw new AssertionError("unknown setup");
                }
                try { actual = context.invoke(parsedRequest, c.version()); }
                catch (InvocationTargetException exception) { failure = exception.getCause().toString(); }
                fetch = context.remainingFetch(); writes = new ArrayList<>(context.store.writes);
            }
            if (key.id != 18) assertApacheOutcome(c, actual, failure, fetch);
            boolean closes = c.policy().startsWith("close-");
            ApiMessage response = closes ? null : key.id == 18 ? advertisement()
                : c.policy().equals("actual-handler") ? actual : policyError(c);
            if (!closes && response == null) throw new AssertionError("Unexpected actual handler failure " + c.name() + ": " + failure);
            short rhv = key.responseHeaderVersion(c.version());
            byte[] responseBytes = response == null ? null
                : join(encode(new ResponseHeader(CORRELATION, rhv).data(), rhv), encode(response, c.version()));
            byte[] actualBytes = actual == null ? null
                : join(encode(new ResponseHeader(CORRELATION, rhv).data(), rhv), encode(actual, c.version()));
            if (responseBytes != null) decodeResponse(key, c.version(), responseBytes);
            String requestFile = c.name() + ".request.bin", responseFile = closes ? "-" : c.name() + ".response.bin";
            Files.write(output.resolve(requestFile), request); Files.write(output.resolve(c.name() + ".request.frame.bin"), frame(request));
            if (responseBytes != null) { Files.write(output.resolve(responseFile), responseBytes);
                Files.write(output.resolve(c.name() + ".response.frame.bin"), frame(responseBytes)); }
            if (actualBytes != null) Files.write(output.resolve(c.name() + ".apache-response.bin"), actualBytes);
            if (args.length == 3 && responseBytes != null) {
                Path actualFile = Path.of(args[2]).resolve(args[1]).resolve(c.name() + ".actual-response.bin");
                byte[] rust = Files.readAllBytes(actualFile); decodeResponse(key, c.version(), rust);
                if (!Arrays.equals(rust, responseBytes)) throw new AssertionError("Rust response mismatch " + c.name());
                decodedActual++;
                if (c.setup().equals("fresh") && (key.id == 18 || c.name().equals("vote-positive"))) {
                    Path transport = Path.of(args[2]).resolve("transport").resolve(args[1]);
                    byte[] tcp = Files.readAllBytes(transport.resolve(c.name() + ".actual-response.bin"));
                    byte[] tcpFrame = Files.readAllBytes(transport.resolve(c.name() + ".actual-frame.bin"));
                    decodeResponse(key, c.version(), tcp);
                    if (!Arrays.equals(tcp, responseBytes) || !Arrays.equals(tcpFrame, frame(tcp)))
                        throw new AssertionError("Rust TCP frame mismatch " + c.name());
                    decodedTransport++;
                }
            }
            table.append(String.join("\t", c.name(), Short.toString(key.id), Short.toString(c.version()), "77", requestFile,
                responseFile, c.setup(), closes ? "close" : "response", c.term(), c.vote(), c.leader(), c.deadline())).append('\n');
            String rawWrites = String.join(",", writes.stream().map(ControllerOracle::quote).toList());
            observations.add("{\"name\":" + quote(c.name()) + ",\"key\":" + key.id + ",\"version\":" + c.version()
                + ",\"request_header_version\":" + parsedHeader.headerVersion() + ",\"response_header_version\":" + rhv
                + ",\"setup\":" + quote(c.setup()) + ",\"policy\":" + quote(c.policy())
                + ",\"request_sha256\":" + quote(hash(request)) + ",\"response_sha256\":" + (responseBytes == null ? "null" : quote(hash(responseBytes)))
                + ",\"actual_apache_response\":" + quote(actual == null ? null : actual.toString())
                + ",\"actual_apache_exception\":" + quote(failure) + ",\"actual_apache_response_hex\":"
                + quote(actualBytes == null ? null : HexFormat.of().formatHex(actualBytes))
                + ",\"apache_remaining_fetch_ms\":" + fetch + ",\"apache_store_writes\":[" + rawWrites + "]}");
        }
        Files.writeString(output.resolve("cases.tsv"), table);
        Files.writeString(output.resolve("observations.json"), "{\"schema_version\":1,\"release\":" + quote(args[1])
            + ",\"case_count\":" + cases.size() + ",\"decoded_actual_rust_responses\":" + decodedActual
            + ",\"decoded_actual_tcp_responses\":" + decodedTransport
            + ",\"cases\":[" + String.join(",\n", observations) + "]}\n");
        System.out.println("Apache controller oracle " + args[1] + ": " + cases.size() + " cases, Rust decoded " + decodedActual);
    }
}
