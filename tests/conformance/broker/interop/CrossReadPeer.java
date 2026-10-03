/* Actual pinned Apache manual consumers independently read Rust/native Producer histories. */
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public final class CrossReadPeer {
    private static String topic;
    private static int count;
    private static final long TIME = 1_700_000_000_000L;
    private static final List<String> HISTORY = new ArrayList<>();
    private static int checks;
    private CrossReadPeer() { }
    private static void check(boolean condition, String label) { checks++; if (!condition) throw new AssertionError(label); }
    private static String quote(String value) {
        return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String hex(byte[] data) { return data == null ? null : HexFormat.of().formatHex(data); }
    private static String receipt(int partition, long offset, long timestamp, byte[] key, byte[] value, Iterable<Header> headers) throws Exception {
        List<String> fields = new ArrayList<>();
        for (Header header : headers) fields.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":" + quote(hex(header.value())) + "}");
        String content = "{\"topic\":" + quote(topic) + ",\"partition\":" + partition + ",\"offset\":" + offset + ",\"timestamp\":" + timestamp
            + ",\"key_hex\":" + quote(hex(key)) + ",\"value_hex\":" + quote(hex(value)) + ",\"headers\":[" + String.join(",", fields) + "]}";
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(utf8(content)));
        return "{\"sha256\":" + quote(hash) + ",\"record\":" + content + "}";
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("release port initial|restart state-directory topic count12or13");
        String release = args[0]; String phase = args[2]; Path state = Path.of(args[3]); Files.createDirectories(state);
        topic = args[4]; count = Integer.parseInt(args[5]);
        check(topic.length() <= 64 && topic.matches("[A-Za-z0-9._-]+") && (count == 12 || count == 13), "bounded cross-language topic/count");
        check(phase.equals("initial") || phase.equals("restart"), "explicit cross-language history phase"); boolean passed = false;
        Properties p = new Properties(); p.setProperty("bootstrap.servers", "127.0.0.1:" + args[1]);
        p.setProperty("client.id", "cross-history-java-" + release); p.setProperty("request.timeout.ms", "5000");
        p.setProperty("default.api.timeout.ms", "15000"); p.setProperty("enable.auto.commit", "false");
        p.setProperty("allow.auto.create.topics", "false"); p.setProperty("auto.offset.reset", "none");
        p.setProperty("max.poll.records", "100"); p.setProperty("fetch.min.bytes", "1"); p.setProperty("fetch.max.wait.ms", "20");
        p.setProperty("fetch.max.bytes", "65536"); p.setProperty("max.partition.fetch.bytes", "4096"); p.setProperty("isolation.level", "read_uncommitted");
        Properties adminProperties = new Properties();
        for (String name : List.of("bootstrap.servers", "client.id", "request.timeout.ms", "default.api.timeout.ms")) adminProperties.setProperty(name, p.getProperty(name));
        adminProperties.setProperty("retries", "0");
        try (Admin admin = Admin.create(adminProperties); KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            var described = admin.describeTopics(TopicCollection.ofTopicNames(List.of(topic))).allTopicNames().get(15, TimeUnit.SECONDS).get(topic);
            check(described != null && described.partitions().size() == 2 && !described.topicId().equals(Uuid.ZERO_UUID), "actual foreign topic UUID/partitions");
            Path identity = state.resolve(topic + ".uuid");
            if (Files.exists(identity)) check(described.topicId().equals(Uuid.fromString(Files.readString(identity))), "foreign topic UUID retained across readers/restart");
            else { check(phase.equals("initial"), "restart requires previously observed foreign UUID"); Files.writeString(identity, described.topicId().toString()); }
            HISTORY.add("{\"label\":\"foreign-topic-identity\",\"topic\":" + quote(topic) + ",\"uuid\":" + quote(described.topicId().toString()) + "}");
            Map<TopicPartition, Integer> next = new LinkedHashMap<>();
            for (int partition = 0; partition < 2; partition++) next.put(new TopicPartition(topic, partition), 0);
            List<TopicPartition> assignment = new ArrayList<>(next.keySet()); consumer.assign(assignment);
            for (TopicPartition partition : assignment) consumer.seek(partition, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20); int received = 0;
            while (received < 2 * count && System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition()); Integer ordinal = next.get(partition);
                    check(ordinal != null && ordinal < count && record.offset() == ordinal, "foreign-written actual Java Consumer exact offset/order/no duplicates");
                    String id = topic + ":" + record.partition() + ":" + ordinal;
                    byte[] key = ordinal % 3 == 0 ? null : utf8("key:" + id);
                    byte[] value = ordinal % 4 == 0 ? null : ordinal % 4 == 1 ? new byte[0] : utf8("value:" + id);
                    List<Header> headers = List.of(new RecordHeader("receipt", utf8(id)), new RecordHeader("dup", utf8("a")), new RecordHeader("dup", null));
                    String wanted = receipt(record.partition(), ordinal, TIME + record.partition() * 100 + ordinal, key, value, headers);
                    String observed = receipt(record.partition(), record.offset(), record.timestamp(), record.key(), record.value(), record.headers());
                    check(wanted.equals(observed), "foreign-written cross-SDK exact record hash/key/value/null/empty/timestamp/ordered duplicate headers");
                    HISTORY.add("{\"label\":\"foreign-written-java-read\",\"receipt\":" + observed + "}");
                    next.put(partition, ordinal + 1); received++;
                }
            }
            check(received == 2 * count, "all foreign-written records consumed by actual Java Consumer");
            var first = consumer.beginningOffsets(assignment, Duration.ofSeconds(10));
            var last = consumer.endOffsets(assignment, Duration.ofSeconds(10));
            for (TopicPartition partition : assignment) {
                check(first.get(partition) == 0 && last.get(partition) == count, "actual cross-SDK ListOffsets bounds");
                var timed = consumer.offsetsForTimes(Map.of(partition, TIME + partition.partition() * 100 + 5), Duration.ofSeconds(10)).get(partition);
                check(timed != null && timed.offset() == 5 && timed.timestamp() == TIME + partition.partition() * 100 + 5, "actual cross-SDK timestamp ListOffsets");
            }
            passed = true; System.out.println("{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"records\":" + (2 * count) + ",\"assertions\":" + checks + ",\"passed\":true}");
        } finally {
            Files.writeString(state.resolve(release + "-cross-read-" + phase + "-" + topic + ".json"), "{\"release\":" + quote(release) + ",\"phase\":" + quote(phase)
                + ",\"passed\":" + passed + ",\"assertions\":" + checks + ",\"ordinary_configuration\":\"Manual assignment; auto.commit=false; no group join; read_uncommitted; bounded fetch/heap.\",\"history\":[\n"
                + String.join(",\n", HISTORY) + "\n]}\n");
        }
    }
}
