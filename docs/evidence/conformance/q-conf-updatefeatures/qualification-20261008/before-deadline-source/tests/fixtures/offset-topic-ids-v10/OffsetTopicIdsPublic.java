import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Actual name-based Admin and manual Consumer offset calls. */
public final class OffsetTopicIdsPublic {
    private OffsetTopicIdsPublic() { }
    private static Throwable root(Throwable error) {
        while(error.getCause()!=null)error=error.getCause();return error;
    }
    private static void verify(Map<TopicPartition,OffsetAndMetadata> offsets) {
        var value=offsets.get(new TopicPartition("topic",0));
        if(offsets.size()!=1 || value==null || value.offset()!=123 || !value.leaderEpoch().equals(Optional.of(4)) || !value.metadata().equals(""))
            throw new AssertionError("public offset projection differs: "+offsets);
    }
    public static void main(String[] args)throws Exception {
        if(args.length!=4)throw new IllegalArgumentException("bootstrap, admin/consumer, supported/unsupported, receipt");
        Properties props=new Properties();props.put("bootstrap.servers",args[0]);props.put("client.id","offset-id-java");
        props.put("request.timeout.ms","1000");props.put("default.api.timeout.ms","3000");props.put("retry.backoff.ms","1");props.put("retry.backoff.max.ms","1");
        props.put("reconnect.backoff.ms","1");props.put("reconnect.backoff.max.ms","1");props.put("enable.unstable.api.versions","true");
        var tp=new TopicPartition("topic",0);var value=new OffsetAndMetadata(123,Optional.of(4),"");
        Throwable failure=null;int successfulCalls=0;
        if(args[1].equals("admin") || args[1].equals("batch")) {
            Admin admin=Admin.create(props);
            try {
                if(args[1].equals("batch")) {
                    var result=admin.listConsumerGroupOffsets(Map.of("group",new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(tp)),
                        "other",new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(tp))),new ListConsumerGroupOffsetsOptions().requireStable(true).timeoutMs(3000));
                    verify(result.partitionsToOffsetAndMetadata("other").get(5,TimeUnit.SECONDS));
                    verify(result.partitionsToOffsetAndMetadata("group").get(5,TimeUnit.SECONDS));successfulCalls++;
                } else {
                    admin.alterConsumerGroupOffsets("group",Map.of(tp,value),new AlterConsumerGroupOffsetsOptions().timeoutMs(3000)).all().get(5,TimeUnit.SECONDS);successfulCalls++;
                    verify(admin.listConsumerGroupOffsets(Map.of("group",new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(tp))),new ListConsumerGroupOffsetsOptions().requireStable(true).timeoutMs(3000))
                        .partitionsToOffsetAndMetadata("group").get(5,TimeUnit.SECONDS));successfulCalls++;
                }
            } catch(Exception error) { failure=root(error); }
            finally { admin.close(Duration.ofSeconds(2)); }
        } else if(args[1].equals("consumer")) {
            props.put("group.id","group");props.put("enable.auto.commit","false");props.put("group.protocol","classic");
            var consumer=new KafkaConsumer<byte[],byte[]>(props,new ByteArrayDeserializer(),new ByteArrayDeserializer());
            try {
                consumer.assign(List.of(tp));consumer.commitSync(Map.of(tp,value),Duration.ofSeconds(3));successfulCalls++;
                verify(consumer.committed(Set.of(tp),Duration.ofSeconds(3)));successfulCalls++;
            } catch(Exception error) { failure=root(error); }
            finally { consumer.close(CloseOptions.timeout(Duration.ofSeconds(2))); }
        } else throw new IllegalArgumentException("unknown public caller");
        if(args[2].equals("supported")) {
            if(failure!=null)throw new AssertionError("supported public call failed",failure);
            if(successfulCalls!=(args[1].equals("batch")?1:2))throw new AssertionError("missing public calls");
        } else if(!args[2].equals("unsupported") || !(failure instanceof UnsupportedVersionException) || successfulCalls!=0) {
            throw new AssertionError("expected actual public factory refusal",failure);
        }
        long alive=Thread.getAllStackTraces().keySet().stream().filter(thread->thread.isAlive()
            && (thread.getName().startsWith("kafka-admin-client-thread") || thread.getName().startsWith("kafka-coordinator-heartbeat-thread")
                || thread.getName().startsWith("kafka-consumer-network-thread"))).count();
        if(alive!=0)throw new AssertionError("live Kafka threads: "+alive);
        String receipt="{\"successful_public_calls\":"+successfulCalls+",\"failure\":"+(failure==null?"null":"\""+failure.getClass().getName()+"\"")+",\"live_kafka_threads\":0}";
        Files.writeString(Path.of(args[3]),receipt+"\n");System.out.println(receipt);
    }
}
