import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Actual public calls, including the SDK's legacy Metadata builder policy. */
public final class LegacyDiscoveryPublic {
    private LegacyDiscoveryPublic() { }
    private static Throwable root(Throwable error) { while(error.getCause()!=null)error=error.getCause();return error; }
    private static void offsets(Map<TopicPartition,OffsetAndMetadata> values) {
        var value=values.get(new TopicPartition("topic",0));
        if(values.size()!=1 || value==null || value.offset()!=123 || !value.leaderEpoch().equals(Optional.of(4)) || !value.metadata().equals(""))throw new AssertionError("offset fields differ: "+values);
    }
    public static void main(String[] args)throws Exception {
        if(args.length!=4)throw new IllegalArgumentException("bootstrap, driver, expected, receipt");
        Properties props=new Properties();props.put("bootstrap.servers",args[0]);props.put("client.id","legacy-discovery-java");
        props.put("request.timeout.ms","500");props.put("default.api.timeout.ms","1500");props.put("retry.backoff.ms","1");props.put("retry.backoff.max.ms","1");props.put("reconnect.backoff.ms","1");props.put("reconnect.backoff.max.ms","1");
        props.put("enable.unstable.api.versions","true");
        if(args[2].equals("deadline")) {props.put("request.timeout.ms","2500");props.put("default.api.timeout.ms","3000");}
        Throwable failure=null;int success=0;long operationNanos=0;
        switch(args[1]) {
            case "admin-offset" -> {
                Admin admin=Admin.create(props);
                long started=System.nanoTime();
                try {offsets(admin.listConsumerGroupOffsets(Map.of("group",new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(new TopicPartition("topic",0)))),new ListConsumerGroupOffsetsOptions().timeoutMs(1500)).partitionsToOffsetAndMetadata("group").get(3,TimeUnit.SECONDS));success++;}
                catch(Exception error){failure=root(error);}finally{operationNanos=System.nanoTime()-started;admin.close(Duration.ofSeconds(1));}
            }
            case "consumer-offset", "consumer-metadata" -> {
                props.put("group.id","group");props.put("group.protocol","classic");props.put("enable.auto.commit","false");props.put("allow.auto.create.topics","true");
                var consumer=new KafkaConsumer<byte[],byte[]>(props,new ByteArrayDeserializer(),new ByteArrayDeserializer());
                long started=System.nanoTime();
                try {if(args[1].equals("consumer-offset"))offsets(consumer.committed(Set.of(new TopicPartition("topic",0)),Duration.ofMillis(1500)));
                    else if(consumer.partitionsFor("topic",Duration.ofMillis(1500)).size()!=1)throw new AssertionError("missing partition");success++;}
                catch(Exception error){failure=root(error);}finally{operationNanos=System.nanoTime()-started;consumer.close(CloseOptions.timeout(Duration.ofSeconds(1)));}
            }
            case "producer-metadata" -> {
                props.put("enable.idempotence","false");props.put("max.block.ms","1500");
                var producer=new KafkaProducer<byte[],byte[]>(props,new ByteArraySerializer(),new ByteArraySerializer());
                long started=System.nanoTime();
                try {if(producer.partitionsFor("topic").size()!=1)throw new AssertionError("missing partition");success++;}
                catch(Exception error){failure=root(error);}finally{operationNanos=System.nanoTime()-started;producer.close(Duration.ofSeconds(1));}
            }
            default -> throw new IllegalArgumentException("unknown driver");
        }
        if(args[2].equals("supported")) {if(failure!=null || success!=1)throw new AssertionError("public discovery failed",failure);}
        else if(args[2].equals("terminal")) {if(success!=0 || !(failure instanceof GroupAuthorizationException))throw new AssertionError("expected GROUP authorization error",failure);}
        else if(args[2].equals("deadline")) {
            // Admin cancels an in-flight coordinator request at the call deadline
            // and reports DisconnectException; Consumer reports TimeoutException.
            boolean expected=args[1].equals("admin-offset") ? failure instanceof DisconnectException : failure instanceof TimeoutException;
            if(success!=0 || !expected || operationNanos<TimeUnit.MILLISECONDS.toNanos(1300) || operationNanos>TimeUnit.MILLISECONDS.toNanos(2500))throw new AssertionError("expected original discovery deadline: elapsed="+operationNanos, failure);
        }
        else if(args[2].equals("not-coordinator")) {if(success!=0 || !(failure instanceof NotCoordinatorException))throw new AssertionError("expected reference FindCoordinator policy",failure);}
        else if(!args[2].equals("legacy-policy") || success!=0 || !(failure instanceof UnsupportedVersionException || failure instanceof TimeoutException))throw new AssertionError("expected legacy factory policy",failure);
        long alive=Thread.getAllStackTraces().keySet().stream().filter(t->t.isAlive() && (t.getName().startsWith("kafka-admin-client-thread") || t.getName().startsWith("kafka-producer-network-thread") || t.getName().startsWith("kafka-consumer-network-thread") || t.getName().startsWith("kafka-coordinator-heartbeat-thread"))).count();
        if(alive!=0)throw new AssertionError("live Kafka threads");
        String receipt="{\"successful_public_calls\":"+success+",\"operation_elapsed_ns\":"+operationNanos+",\"failure\":"+(failure==null?"null":"\""+failure.getClass().getName()+"\"")+",\"live_kafka_threads\":0}";
        Files.writeString(Path.of(args[3]),receipt+"\n");System.out.println(receipt);
    }
}
