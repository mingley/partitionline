/* Genuine bounded official public Admin operations; scripted wire-peer scope. */
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.common.TopicPartition;

public final class PublicAdminProbe {
    private PublicAdminProbe() { }
    private static String quote(String value) {
        if (value==null) return "null";
        return "\""+value.replace("\\","\\\\").replace("\"","\\\"")+"\"";
    }
    private static String officialOffsetAccessor(Object info) {
        String release = org.apache.kafka.common.utils.AppInfoParser.getVersion();
        String expectedType;
        String accessor;
        switch (release) {
            case "4.1.2":
                expectedType = "org.apache.kafka.clients.consumer.OffsetAndMetadata";
                accessor = "offset";
                break;
            case "4.2.1":
            case "4.3.1":
                expectedType = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo";
                accessor = "startOffset";
                break;
            default:
                throw new AssertionError("unqualified official release");
        }
        if (!info.getClass().getName().equals(expectedType)) {
            throw new AssertionError("official public result type differs from pinned release");
        }
        return accessor;
    }
    private static long officialOffset(Object info) throws Exception {
        Object value = info.getClass().getMethod(officialOffsetAccessor(info)).invoke(info);
        if (!(value instanceof Long)) throw new AssertionError("official long offset accessor");
        return (Long) value;
    }
    private static String officialLeaderEpoch(Object info) throws Exception {
        officialOffsetAccessor(info);
        Object value = info.getClass().getMethod("leaderEpoch").invoke(info);
        if (!(value instanceof Optional<?>)) throw new AssertionError("official Optional leader epoch");
        Optional<?> epoch = (Optional<?>) value;
        if (epoch.isPresent() && !(epoch.get() instanceof Integer)) {
            throw new AssertionError("official integer leader epoch");
        }
        return epoch.isPresent() ? epoch.get().toString() : "null";
    }
    private static String officialLagGetter(Object info) throws Exception {
        officialOffsetAccessor(info);
        if (org.apache.kafka.common.utils.AppInfoParser.getVersion().equals("4.1.2")) {
            try {
                info.getClass().getMethod("lag");
            } catch (NoSuchMethodException expectedAbsent) {
                return "absent-on-this-official-release";
            }
            throw new AssertionError("unexpected lag accessor on pinned older public type");
        }
        return "actual-official-public-Optional";
    }
    private static String officialLag(Object info) throws Exception {
        if (officialLagGetter(info).equals("absent-on-this-official-release")) return "null";
        Object value = info.getClass().getMethod("lag").invoke(info);
        if (!(value instanceof Optional<?>)) throw new AssertionError("official Optional lag");
        Optional<?> lag = (Optional<?>) value;
        if (lag.isPresent() && !(lag.get() instanceof Long)) {
            throw new AssertionError("official long lag");
        }
        return lag.isPresent() ? lag.get().toString() : "null";
    }
    public static void main(String[] args) throws Exception {
        if (args.length!=4) throw new IllegalArgumentException("bootstrap abort|share expected-category output");
        Properties config=new Properties();
        config.setProperty("bootstrap.servers",args[0]);
        config.setProperty("client.id","public-capability-probe");
        config.setProperty("request.timeout.ms","1000");
        config.setProperty("default.api.timeout.ms","3000");
        config.setProperty("retry.backoff.ms","5");
        config.setProperty("retry.backoff.max.ms","10");
        String result="completed",type=null,offset="null",lag="null",valueType=null,offsetGetter=null,leaderEpoch="null";
        Admin admin=Admin.create(config);
        try {
            if (args[1].equals("abort")) {
                admin.abortTransaction(new AbortTransactionSpec(new TopicPartition("t",0),1000,(short)2,7))
                    .all().get(4,TimeUnit.SECONDS);
            } else if (args[1].equals("share")) {
                Map<TopicPartition,?> values=admin.listShareGroupOffsets(
                    Map.of("g",new ListShareGroupOffsetsSpec())).all().get(4,TimeUnit.SECONDS).get("g");
                Object info=values.get(new TopicPartition("t",0));
                if (info==null) throw new AssertionError("expected public target offset");
                offset=Long.toString(officialOffset(info));
                lag=officialLag(info);
                valueType=info.getClass().getName();
                offsetGetter=officialOffsetAccessor(info);
                leaderEpoch=officialLeaderEpoch(info);
            } else throw new IllegalArgumentException("bounded operation enum");
        } catch (ExecutionException failure) {
            result="failed";type=failure.getCause().getClass().getName();
        } finally {
            admin.close(Duration.ofSeconds(1));
        }
        boolean passed=args[2].equals(result) || args[2].equals(type);
        Files.writeString(Path.of(args[3]),"{\"scope\":\"genuine official public Admin against bounded scripted peer; no broker transaction or share-state claim\",\"operation\":"
            +quote(args[1])+",\"outcome\":"+quote(result)+",\"failure_type\":"+quote(type)
            +",\"start_offset\":"+offset+",\"lag\":"+lag
            +",\"public_result_type\":"+quote(valueType)+",\"offset_getter\":"+quote(offsetGetter)
            +",\"leader_epoch\":"+leaderEpoch+",\"passed\":"+passed+"}\n");
        if (!passed) throw new AssertionError("predeclared public outcome differs");
    }
}
