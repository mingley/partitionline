import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

/** Create an owned topic and report its observed metadata and empty offsets. */
public final class RuntimeTopic {
    static Properties properties(String bootstrap) {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", bootstrap);
        p.setProperty("request.timeout.ms", "10000");
        p.setProperty("default.api.timeout.ms", "15000");
        return p;
    }
    public static void delete(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(properties(bootstrap))) {
            admin.deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
        }
        System.out.printf("{\"status\":\"deletion_requested\",\"topic\":\"%s\",\"admin_closed\":true}%n", topic);
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("bootstrap topic partitions");
        int count = Integer.parseInt(args[2]);
        try (Admin admin = Admin.create(properties(args[0]))) {
            admin.createTopics(List.of(new NewTopic(args[1], count, (short) 1)
                    .configs(Map.of("min.insync.replicas", "1", "retention.ms", "-1"))))
                    .all().get(15, TimeUnit.SECONDS);
            var description = admin.describeTopics(List.of(args[1])).allTopicNames()
                    .get(15, TimeUnit.SECONDS).get(args[1]);
            if (description.partitions().size() != count) throw new AssertionError("partition count differs");
            Map<TopicPartition, OffsetSpec> requested = new LinkedHashMap<>();
            List<String> offsets = new ArrayList<>();
            for (var partition : description.partitions()) {
                if (partition.leader() == null || partition.leader().id() != 1
                        || partition.replicas().size() != 1 || partition.isr().size() != 1) {
                    throw new AssertionError("leader, replication or ISR differs");
                }
                requested.put(new TopicPartition(args[1], partition.partition()), OffsetSpec.latest());
            }
            var actual = admin.listOffsets(requested).all().get(15, TimeUnit.SECONDS);
            for (var item : actual.entrySet()) {
                if (item.getValue().offset() != 0) throw new AssertionError("fresh topic is not empty");
                offsets.add("\"" + item.getKey().partition() + "\":" + item.getValue().offset());
            }
            var resource = new ConfigResource(ConfigResource.Type.TOPIC, args[1]);
            Config config = admin.describeConfigs(List.of(resource)).all().get(15, TimeUnit.SECONDS).get(resource);
            if (!config.get("min.insync.replicas").value().equals("1")
                    || !config.get("retention.ms").value().equals("-1")) {
                throw new AssertionError("actual topic policy differs");
            }
            System.out.printf("{\"status\":\"created\",\"partitions\":%d,\"replication_factor\":1,\"min_insync_replicas\":1,\"offsets\":{%s}}%n", count, String.join(",", offsets));
        }
    }
}
