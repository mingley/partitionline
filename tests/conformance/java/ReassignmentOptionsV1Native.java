/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Actual reference-cluster state checks and a throttled pending move. */
public class ReassignmentOptionsV1Native {
    static final Map<String,String> PINS = Map.of(
        "4.1.2", "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
        "4.2.1", "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
        "4.3.1", "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36");
    static <T> T await(java.util.concurrent.Future<T> future) throws Exception {
        return future.get(15,TimeUnit.SECONDS);
    }
    static short result(java.util.concurrent.Future<Void> future) throws Exception {
        try { await(future); return 0; }
        catch (ExecutionException error) { return Errors.forException(error.getCause()).code(); }
    }
    static Map<TopicPartition,Optional<NewPartitionReassignment>> input(String topic) {
        return Map.of(new TopicPartition(topic,0),Optional.of(new NewPartitionReassignment(List.of(2,3,1))),
                      new TopicPartition(topic,1),Optional.of(new NewPartitionReassignment(List.of(2,3))),
                      new TopicPartition(topic,2),Optional.empty());
    }
    static void policy(Admin admin,String topic,boolean allow) throws Exception {
        var outcomes=admin.alterPartitionReassignments(input(topic),
            new AlterPartitionReassignmentsOptions().allowReplicationFactorChange(allow).timeoutMs(10000)).values();
        for(var entry:outcomes.entrySet()) {
            short actual=result(entry.getValue());
            int expected=entry.getKey().partition()==2 ? 85 : !allow && entry.getKey().partition()==1 ? 38 : 0;
            System.out.println("{\"partition\":"+entry.getKey().partition()+",\"error_code\":"+actual+",\"allow\":"+allow+"}");
            if(actual!=expected)throw new AssertionError("partition "+entry.getKey()+": expected "+expected+" got "+actual);
        }
    }
    static void verify(Admin admin,String topic,boolean allow) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while(System.nanoTime()<deadline) {
            var description=await(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
            var first=description.partitions().get(0).replicas().stream().map(node->node.id()).toList();
            var second=description.partitions().get(1).replicas().stream().map(node->node.id()).toList();
            var third=description.partitions().get(2).replicas().stream().map(node->node.id()).toList();
            if(first.equals(List.of(2,3,1)) && second.equals(allow ? List.of(2,3) : List.of(1,2,3)) && third.equals(List.of(1,2,3))) {
                System.out.println("{\"replica_state\":["+first+","+second+","+third+"],\"allow\":"+allow+"}");return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("reference replica state does not match policy");
    }
    static void preparePending(Admin admin,Properties config,String topic) throws Exception {
        await(admin.createTopics(List.of(new NewTopic(topic,Map.of(0,List.of(1,2)))
            .configs(Map.of("follower.replication.throttled.replicas","0:3","leader.replication.throttled.replicas","0:1")))).all());
        await(admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.BROKER,"3"),
            List.of(new AlterConfigOp(new ConfigEntry("follower.replication.throttled.rate","1"),AlterConfigOp.OpType.SET)))).all());
        var producerConfig=new Properties();producerConfig.putAll(config);
        producerConfig.setProperty("key.serializer",ByteArraySerializer.class.getName());
        producerConfig.setProperty("value.serializer",ByteArraySerializer.class.getName());
        producerConfig.setProperty("acks","all");producerConfig.setProperty("linger.ms","0");
        producerConfig.setProperty("delivery.timeout.ms","30000");
        try(var producer=new KafkaProducer<byte[],byte[]>(producerConfig)) {
            byte[] value=new byte[65536];
            for(int i=0;i<128;i++) {value[0]=(byte)i;await(producer.send(new ProducerRecord<>(topic,0,null,value)));}
            producer.flush();
        }
        var partition=new TopicPartition(topic,0);
        await(admin.alterPartitionReassignments(Map.of(partition,Optional.of(new NewPartitionReassignment(List.of(2,3))))).all());
        var pending=await(admin.listPartitionReassignments(Set.of(partition)).reassignments());
        if(!pending.containsKey(partition) || !pending.get(partition).addingReplicas().contains(3))
            throw new AssertionError("seed move already completed; pending cancellation not exercised");
        System.out.println("{\"pending_observed\":true,\"seed_records\":128,\"seed_value_bytes\":65536,\"adding_replicas\":"+pending.get(partition).addingReplicas()+",\"removing_replicas\":"+pending.get(partition).removingReplicas()+"}");
    }
    static void cancelPending(Admin admin,String topic) throws Exception {
        var partition=new TopicPartition(topic,0);
        await(admin.alterPartitionReassignments(Map.of(partition,Optional.empty()),
            new AlterPartitionReassignmentsOptions().allowReplicationFactorChange(false).timeoutMs(10000)).all());
    }
    static void verifyCancelled(Admin admin,String topic) throws Exception {
        var partition=new TopicPartition(topic,0);
        if(!await(admin.listPartitionReassignments(Set.of(partition)).reassignments()).isEmpty())throw new AssertionError("move still pending");
        var replicas=await(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic).partitions().get(0).replicas().stream().map(node->node.id()).toList();
        if(replicas.size()!=2 || !new java.util.HashSet<>(replicas).equals(Set.of(1,2)))
            throw new AssertionError("cancellation changed original replica membership: "+replicas);
        System.out.println("{\"pending_entries\":0,\"cancelled_replicas\":"+replicas+"}");
    }
    public static void main(String[]args) throws Exception {
        if(args.length!=5)throw new IllegalArgumentException("release jar bootstrap topic mode");
        var loaded=Path.of(Admin.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if(!Files.isSameFile(loaded,Path.of(args[1])) || !HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(loaded))).equals(PINS.get(args[0])))
            throw new AssertionError("unmatched loaded Apache jar");
        var config=new Properties();config.setProperty("bootstrap.servers",args[2]);
        config.setProperty("request.timeout.ms","10000");config.setProperty("default.api.timeout.ms","15000");
        String topic=args[3],mode=args[4];
        try(Admin admin=Admin.create(config)) {
            switch(mode) {
                case "prepare" -> await(admin.createTopics(List.of(new NewTopic(topic,Map.of(0,List.of(1,2,3),1,List.of(1,2,3),2,List.of(1,2,3))))).all());
                case "false" -> {policy(admin,topic,false);verify(admin,topic,false);}
                case "true" -> {policy(admin,topic,true);verify(admin,topic,true);}
                case "verify-false" -> verify(admin,topic,false);
                case "verify-true" -> verify(admin,topic,true);
                case "prepare-pending" -> preparePending(admin,config,topic);
                case "cancel-pending" -> {cancelPending(admin,topic);verifyCancelled(admin,topic);}
                case "verify-cancelled" -> verifyCancelled(admin,topic);
                default -> throw new IllegalArgumentException("mode");
            }
            admin.close(Duration.ofSeconds(2));
        }
        System.out.println("{\"status\":\"pass\",\"release\":\""+args[0]+"\",\"topic\":\""+topic+"\",\"mode\":\""+mode+"\",\"admin_closed\":true}");
    }
}
