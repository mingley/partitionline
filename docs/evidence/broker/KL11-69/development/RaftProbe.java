import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.ElectionState;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.raft.QuorumStateStore;
import org.apache.kafka.server.common.KRaftVersion;

/** Scratch proof of actual handler construction, without network or disk log. */
public final class RaftProbe {
    private RaftProbe() { }
    private static final ListenerName LISTENER = new ListenerName("CONTROLLER");
    private static final class Clock implements Time {
        private long now;
        @Override public long milliseconds() { return now; }
        @Override public long nanoseconds() { return now * 1000000; }
        @Override public void sleep(long ms) { now += ms; }
        @Override public void waitObject(Object object, Supplier<Boolean> condition, long deadline) {
            throw new IllegalStateException("No waits permitted in isolated oracle");
        }
    }
    private static final class Store implements QuorumStateStore {
        private ElectionState state = ElectionState.withUnknownLeader(0, Set.of(1,2,3));
        @Override public Optional<ElectionState> readElectionState() { return Optional.ofNullable(state); }
        @Override public void writeElectionState(ElectionState value, KRaftVersion version) { state=value; }
        @Override public Path path() { return Path.of("synthetic-no-filesystem-store"); }
        @Override public void clear() { state=null; }
    }
    private static Object proxy(Class<?> type) {
        return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (p,m,args) -> {
            return switch (m.getName()) {
                case "toString" -> "SyntheticBounded" + type.getSimpleName();
                case "hashCode" -> 7;
                case "equals" -> p == args[0];
                case "topicPartition" -> new TopicPartition("__cluster_metadata",0);
                case "topicId" -> new Uuid(0,1);
                case "endOffset", "highWatermark" -> new LogOffsetMetadata(0);
                case "startOffset" -> 0L;
                case "lastFetchedEpoch" -> 0;
                case "latestSnapshotId", "earliestSnapshotId", "latestSnapshot", "earliestSnapshot", "maybeClean" -> Optional.empty();
                case "listenerName" -> LISTENER;
                case "newCorrelationId" -> 77;
                case "close", "flush", "initializeLeaderEpoch", "setIgnoredStaticVoters" -> null;
                default -> throw new IllegalStateException("Unexpected synthetic dependency call: " + type + "." + m);
            };
        });
    }
    private static Object construct() throws Exception {
        Class<?> raft=Class.forName("org.apache.kafka.raft.KafkaRaftClient");
        Constructor<?> constructor=Arrays.stream(raft.getConstructors()).filter(c->c.getParameterCount()==14).findFirst().orElseThrow();
        Object[] args=new Object[14]; Clock clock=new Clock();
        QuorumConfig config=new QuorumConfig(new AbstractConfig(QuorumConfig.CONFIG_DEF,
            Map.of("controller.quorum.election.timeout.ms",5,"controller.quorum.fetch.timeout.ms",5,
                "controller.quorum.election.backoff.max.ms",1000), false));
        Class<?>[] types=constructor.getParameterTypes();
        for(int i=0;i<types.length;i++) {
            Class<?> type=types[i];
            args[i]=switch(type.getSimpleName()) {
                case "OptionalInt" -> OptionalInt.of(1);
                case "Uuid" -> new Uuid(1,1);
                case "Time" -> clock;
                case "LogContext" -> type.getConstructor().newInstance();
                case "boolean" -> true;
                case "String" -> "cluster-fixed";
                case "Collection" -> List.of();
                case "Endpoints" -> type.getMethod("empty").invoke(null);
                case "SupportedVersionRange" -> type.getConstructor(short.class,short.class).newInstance((short)0,(short)0);
                case "QuorumConfig" -> config;
                default -> proxy(type);
            };
        }
        Object client=constructor.newInstance(args);
        Method initialize=Arrays.stream(raft.getMethods()).filter(m->m.getName().equals("initialize")).findFirst().orElseThrow();
        Map<Integer,InetSocketAddress> voters=Map.of(1,new InetSocketAddress("127.0.0.1",9101),2,new InetSocketAddress("127.0.0.1",9102),3,new InetSocketAddress("127.0.0.1",9103));
        Object[] init={voters,new Store(),new Metrics(),proxy(initialize.getParameterTypes()[3])};
        initialize.invoke(client,init);
        return client;
    }
    public static void main(String[] args) throws Exception {
        Object client=construct();
        VoteRequestData.PartitionData part=new VoteRequestData.PartitionData().setPartitionIndex(0).setReplicaId(2).setReplicaEpoch(1).setLastOffsetEpoch(0).setLastOffset(0);
        VoteRequestData request=new VoteRequestData().setClusterId("cluster-fixed").setTopics(List.of(new VoteRequestData.TopicData().setTopicName("__cluster_metadata").setPartitions(List.of(part))));
        Class<?> inbound=Class.forName("org.apache.kafka.raft.RaftRequest$Inbound");
        Object meta=inbound.getConstructor(ListenerName.class,int.class,short.class,ApiMessage.class,long.class).newInstance(LISTENER,77,(short)0,request,0L);
        Method handler=client.getClass().getDeclaredMethod("handleVoteRequest",inbound);handler.setAccessible(true);
        System.out.println(handler.invoke(client,meta));
        Method backoff=client.getClass().getDeclaredMethod("strictExponentialElectionBackoffMs",int.class,int.class);backoff.setAccessible(true);
        for(int count: new int[]{0,1,2,3,31,32,33})
            for(int rank: new int[]{0,1,-1,count})
                System.out.println("backoff " + rank + " " + count + " " + backoff.invoke(client,rank,count));
    }
}
