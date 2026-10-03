/* Genuine public Apache Producer/Consumer/Admin ordinary compaction peer. */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class CompactionPeer {
    private static final String[] SCENARIOS = {"mixed", "removed", "nulls"};
    private static final long[][] TIMES = {
        {1000,1007,1003,1007,1010,1011,1012,1013,1014,1015},
        {1000,1001,1002,1003,1004}, {1000,1001,1002,1003}
    };
    private static final String[][] KEYS = {
        {"a",null,"a","b","b","",null,"c","a","a"},
        {"a","a","a","a","a"}, {null,null,"z","z"}
    };
    private static final String[][] VALUES = {
        {"o","v","n","o",null,"",null,null,"p","q"},
        {"o","n",null,"p","q"}, {"v",null,"p","q"}
    };
    private static final int[] INITIAL_END = {9,4,3};
    private static final List<String> HISTORY = new ArrayList<>();
    private static final Map<String,String> IDENTITIES = new LinkedHashMap<>();
    private static int assertions;
    private static int records;
    private static String bootstrap;
    private static String prefix;

    private CompactionPeer() { }
    private static void check(boolean condition, String label) {
        assertions++;
        if (!condition) throw new AssertionError(label);
    }
    private static String quote(String text) {
        return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    private static byte[] bytes(String text) { return text == null ? null : text.getBytes(StandardCharsets.UTF_8); }
    private static String hex(byte[] data) { return data == null ? "null" : quote(HexFormat.of().formatHex(data)); }
    private static String topic(int scenario) { return prefix + "-" + SCENARIOS[scenario]; }
    private static List<Header> headers() {
        return List.of(new RecordHeader("d", bytes("a")), new RecordHeader("d", null), new RecordHeader("e", bytes("")));
    }
    private static String receipt(String name, long offset, long time, byte[] key, byte[] value, Iterable<Header> input) {
        List<String> h = new ArrayList<>();
        for (Header header : input) h.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":" + hex(header.value()) + "}");
        return "{\"topic\":" + quote(name) + ",\"partition\":0,\"offset\":" + offset + ",\"timestamp\":" + time
            + ",\"key_hex\":" + hex(key) + ",\"value_hex\":" + hex(value) + ",\"headers\":[" + String.join(",", h) + "]}";
    }
    private static String expectedReceipt(int scenario, int offset) {
        return receipt(topic(scenario), offset, TIMES[scenario][offset], bytes(KEYS[scenario][offset]), bytes(VALUES[scenario][offset]), headers());
    }
    private static Properties properties() {
        Properties p = new Properties(); p.setProperty("bootstrap.servers", bootstrap); p.setProperty("client.id", "compaction-public-java");
        p.setProperty("request.timeout.ms", "5000"); p.setProperty("default.api.timeout.ms", "10000");
        p.setProperty("allow.auto.create.topics", "false"); return p;
    }
    private static void identities(Admin admin) throws Exception {
        List<String> names = Arrays.stream(SCENARIOS).map(s -> prefix + "-" + s).toList();
        var descriptions = admin.describeTopics(TopicCollection.ofTopicNames(names)).allTopicNames().get(10, TimeUnit.SECONDS);
        check(descriptions.size() == 3, "exact public topic description count");
        for (String name : names) {
            var description = descriptions.get(name);
            check(description != null && description.partitions().size() == 1, "one actual partition");
            var uuid = description.topicId();
            ByteBuffer raw = ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
            String id = HexFormat.of().formatHex(raw.array()); check(!id.equals("00000000000000000000000000000000"), "nonzero actual UUID");
            IDENTITIES.put(name, id);
        }
    }
    private static void produce(boolean seed) throws Exception {
        Properties p = properties(); p.remove("default.api.timeout.ms"); p.setProperty("enable.idempotence", "false");
        p.setProperty("acks", "1"); p.setProperty("retries", "0"); p.setProperty("compression.type", "none");
        p.setProperty("max.in.flight.requests.per.connection", "1"); p.setProperty("buffer.memory", "1048576");
        p.setProperty("batch.size", "128"); p.setProperty("linger.ms", "0"); p.setProperty("max.block.ms", "5000");
        p.setProperty("delivery.timeout.ms", "15000");
        try (Admin admin = Admin.create(properties())) {
            if (seed) {
                Collection<NewTopic> topics = Arrays.stream(SCENARIOS).map(s -> new NewTopic(prefix + "-" + s, 1, (short) 1)).toList();
                admin.createTopics(topics).all().get(10, TimeUnit.SECONDS);
            }
            identities(admin);
            try (KafkaProducer<byte[],byte[]> producer = new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer())) {
                for (int scenario = 0; scenario < 3; scenario++) {
                    int start = seed ? 0 : INITIAL_END[scenario]; int end = seed ? INITIAL_END[scenario] : start + 1;
                    for (int offset = start; offset < end; offset++) {
                        // Await and flush each send: all seed bytes originate in the public SDK, one record per batch.
                        var record = new ProducerRecord<>(topic(scenario), 0, TIMES[scenario][offset], bytes(KEYS[scenario][offset]), bytes(VALUES[scenario][offset]), headers());
                        var ack = producer.send(record).get(15, TimeUnit.SECONDS); producer.flush();
                        check(ack.offset() == offset && ack.partition() == 0 && ack.topic().equals(topic(scenario)), "actual public Producer offset/partition/topic");
                        HISTORY.add("{\"label\":\"public-producer-delivery\",\"record\":" + expectedReceipt(scenario, offset) + "}"); records++;
                    }
                }
            }
        }
    }
    private static int[] retained(int scenario, String stage) {
        return switch (stage) {
            case "initial" -> java.util.stream.IntStream.range(0, INITIAL_END[scenario]).toArray();
            case "first", "before-expiry" -> scenario == 0 ? new int[]{2,4,5,7,8} : scenario == 1 ? new int[]{2,3} : new int[]{2};
            case "expired", "restart" -> scenario == 0 ? new int[]{2,5,8} : scenario == 1 ? new int[]{3} : new int[]{2};
            case "appended" -> scenario == 0 ? new int[]{2,5,8,9} : scenario == 1 ? new int[]{3,4} : new int[]{2,3};
            default -> throw new IllegalArgumentException("unknown stage");
        };
    }
    private static void consume(String stage) throws Exception {
        try (Admin admin = Admin.create(properties())) { identities(admin); }
        Properties p = properties(); p.setProperty("enable.auto.commit", "false"); p.setProperty("auto.offset.reset", "none");
        p.setProperty("fetch.min.bytes", "1"); p.setProperty("fetch.max.wait.ms", "20"); p.setProperty("max.poll.records", "64");
        p.setProperty("fetch.max.bytes", "65536"); p.setProperty("max.partition.fetch.bytes", "4096"); p.setProperty("isolation.level", "read_uncommitted");
        try (KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            for (int scenario = 0; scenario < 3; scenario++) {
                TopicPartition tp = new TopicPartition(topic(scenario), 0); consumer.assign(List.of(tp));
                int end = INITIAL_END[scenario] + (stage.equals("appended") ? 1 : 0);
                check(consumer.beginningOffsets(List.of(tp)).get(tp) == 0, "compaction preserves logical floor0");
                check(consumer.endOffsets(List.of(tp)).get(tp) == end, "compaction preserves public end offset");
                int[] kept = retained(scenario, stage);
                int[] starts = {0, scenario == 0 ? 3 : 1, end};
                for (int start : starts) {
                    consumer.seek(tp, start);
                    int[] wanted = Arrays.stream(kept).filter(offset -> offset >= start).toArray(); int received = 0;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while ((received < wanted.length || consumer.position(tp) < end) && System.nanoTime() < deadline) {
                        for (ConsumerRecord<byte[],byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                            check(received < wanted.length, "no extra public consumer records");
                            int offset = wanted[received++];
                            check(record.offset() == offset && record.partition() == 0 && record.topic().equals(topic(scenario)), "public seek skips compacted holes");
                            check(record.timestampType() == TimestampType.CREATE_TIME, "preserved CreateTime type");
                            String observed = receipt(record.topic(), record.offset(), record.timestamp(), record.key(), record.value(), record.headers());
                            check(observed.equals(expectedReceipt(scenario, offset)), "exact retained keys/nulls/empty values/timestamps/ordered headers");
                            HISTORY.add("{\"label\":\"public-consumer-record\",\"seek\":" + start + ",\"stage\":" + quote(stage) + ",\"record\":" + observed + "}"); records++;
                        }
                    }
                    check(received == wanted.length && consumer.position(tp) == end, "public consumer completes at unchanged LEO across sparse/empty batches");
                    HISTORY.add("{\"label\":\"public-consumer-position\",\"topic\":" + quote(topic(scenario)) + ",\"seek\":" + start + ",\"position\":" + consumer.position(tp) + ",\"beginning_offset\":0,\"end_offset\":" + end + "}");
                    check(consumer.poll(Duration.ofMillis(30)).isEmpty(), "no duplicate records after terminal position");
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("bootstrap prefix seed|read|append stage release output-json");
        bootstrap = args[0]; prefix = args[1]; String operation = args[2]; String stage = args[3];
        check(bootstrap.length() <= 128 && prefix.matches("[a-z0-9-]{1,40}"), "bounded trusted peer arguments");
        boolean passed = false;
        try {
            switch (operation) {
                case "seed" -> produce(true);
                case "append" -> produce(false);
                case "read" -> consume(stage);
                default -> throw new IllegalArgumentException("operation");
            }
            passed = true;
        } finally {
            List<String> ids = new ArrayList<>(); IDENTITIES.forEach((name,id) -> ids.add(quote(name) + ":" + quote(id)));
            String output = "{\"schema_version\":1,\"peer\":\"apache-java\",\"release\":" + quote(args[4]) + ",\"operation\":" + quote(operation)
                + ",\"stage\":" + quote(stage) + ",\"prefix\":" + quote(prefix) + ",\"assertions\":" + assertions + ",\"records\":" + records
                + ",\"identities\":{" + String.join(",",ids) + "},\"history\":[" + String.join(",",HISTORY) + "],\"passed\":" + passed + "}\n";
            Files.writeString(Path.of(args[5]), output);
        }
    }
}
