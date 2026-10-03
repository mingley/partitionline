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
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.common.TopicPartition;

public final class PublicAdminProbe {
    private PublicAdminProbe() { }
    private static String quote(String value) {
        if (value==null) return "null";
        return "\""+value.replace("\\","\\\\").replace("\"","\\\"")+"\"";
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
        String result="completed",type=null,offset="null",lag="null";
        Admin admin=Admin.create(config);
        try {
            if (args[1].equals("abort")) {
                admin.abortTransaction(new AbortTransactionSpec(new TopicPartition("t",0),1000,(short)2,7))
                    .all().get(4,TimeUnit.SECONDS);
            } else if (args[1].equals("share")) {
                Map<TopicPartition,SharePartitionOffsetInfo> values=admin.listShareGroupOffsets(
                    Map.of("g",new ListShareGroupOffsetsSpec())).all().get(4,TimeUnit.SECONDS).get("g");
                SharePartitionOffsetInfo info=values.get(new TopicPartition("t",0));
                if (info==null) throw new AssertionError("expected public target offset");
                offset=Long.toString(info.startOffset());
                try {
                    Object value=info.getClass().getMethod("lag").invoke(info);
                    if (!(value instanceof Optional<?>)) throw new AssertionError("official public Optional lag");
                    Optional<?> optional=(Optional<?>)value;
                    lag=optional.isPresent() ? optional.get().toString() : "null";
                } catch (NoSuchMethodException older) { }
            } else throw new IllegalArgumentException("bounded operation enum");
        } catch (ExecutionException failure) {
            result="failed";type=failure.getCause().getClass().getName();
        } finally {
            admin.close(Duration.ofSeconds(1));
        }
        boolean passed=args[2].equals(result) || args[2].equals(type);
        Files.writeString(Path.of(args[3]),"{\"scope\":\"genuine official public Admin against bounded scripted peer; no broker transaction or share-state claim\",\"operation\":"
            +quote(args[1])+",\"outcome\":"+quote(result)+",\"failure_type\":"+quote(type)
            +",\"start_offset\":"+offset+",\"lag\":"+lag+",\"passed\":"+passed+"}\n");
        if (!passed) throw new AssertionError("predeclared public outcome differs");
    }
}
