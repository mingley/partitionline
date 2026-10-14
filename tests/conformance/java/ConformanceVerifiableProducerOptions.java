/* SPDX-License-Identifier: Apache-2.0 */
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache Produce v9 bodies for the KL01-26 verifiable Producer
 * option profile.
 *
 * Cells follow hashed VerifiableProducer.getValue / getKey / send create-time
 * and @JsonPropertyOrder({"timestamp","name"}): six records, throughput 5,
 * value-prefix 42, repeating-keys 2, first create-time 1650000000000.
 *
 * Record batches are built through reflection so the same source compiles
 * on 3.9.1–4.2.1 (public MemoryRecords) and 4.3.1 (internal package).
 */
public final class ConformanceVerifiableProducerOptions {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short PRODUCE_VERSION = 9;
    static final String TOPIC = "pl-options";
    static final int RECORD_COUNT = 6;
    static final int THROUGHPUT = 5;
    static final int VALUE_PREFIX = 42;
    static final int REPEATING_KEYS = 2;
    static final long CREATE_TIME = 1_650_000_000_000L;
    static final long LOG_APPEND_TIME_MS = 1_700_000_000_000L;
    static final long EVENT_TIMESTAMP_BASE = 1_700_000_100_000L;

    private ConformanceVerifiableProducerOptions() {}

    static String sha(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    static byte[] encode(Message data, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(data.size(cache, version));
        data.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) {
            throw new AssertionError("size/write mismatch version " + version);
        }
        return buffer.array();
    }

    static Class<?> firstClass(String... names) {
        for (String name : names) {
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException ignored) {
                // try the next public/internal record class
            }
        }
        throw new AssertionError("missing " + Arrays.toString(names));
    }

    static Object compressionNone() throws Exception {
        return Class.forName("org.apache.kafka.common.compress.Compression").getField("NONE").get(null);
    }

    static Class<?> memoryRecordsClass() {
        return firstClass(
                "org.apache.kafka.common.record.MemoryRecords",
                "org.apache.kafka.common.record.internal.MemoryRecords");
    }

    static Class<?> simpleRecordClass() {
        return firstClass(
                "org.apache.kafka.common.record.SimpleRecord",
                "org.apache.kafka.common.record.internal.SimpleRecord");
    }

    /** Hashed VerifiableProducer.getValue(valuePrefix, val). */
    static String getValue(int val) {
        return String.format("%d.%d", VALUE_PREFIX, val);
    }

    /** Hashed VerifiableProducer.getKey() for sequential send i. */
    static String getKey(int val) {
        return Integer.toString(val % REPEATING_KEYS);
    }

    static Object simpleRecord(long timestamp, byte[] key, byte[] value) throws Exception {
        return simpleRecordClass()
                .getConstructor(long.class, byte[].class, byte[].class)
                .newInstance(timestamp, key, value);
    }

    static Object optionRecords(int from, int count) throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> simple = simpleRecordClass();
        Object array = Array.newInstance(simple, count);
        for (int i = 0; i < count; i++) {
            int val = from + i;
            Array.set(
                    array,
                    i,
                    simpleRecord(
                            CREATE_TIME,
                            getKey(val).getBytes(StandardCharsets.UTF_8),
                            getValue(val).getBytes(StandardCharsets.UTF_8)));
        }
        Class<?> compression = Class.forName("org.apache.kafka.common.compress.Compression");
        Method method = recordsClass.getMethod("withRecords", long.class, compression, array.getClass());
        return method.invoke(null, (long) from, compressionNone(), array);
    }

    static void setRecords(Object holder, Object records) throws Exception {
        for (Method method : holder.getClass().getMethods()) {
            if (method.getName().equals("setRecords") && method.getParameterCount() == 1) {
                method.invoke(holder, records);
                return;
            }
        }
        throw new AssertionError(holder.getClass().getName() + ".setRecords missing");
    }

    static ProduceRequestData produceRequest(Object records) throws Exception {
        ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) -1)
                .setTimeoutMs(30_000)
                .setTransactionalId(null);
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName(TOPIC);
        ProduceRequestData.PartitionProduceData part = new ProduceRequestData.PartitionProduceData().setIndex(0);
        setRecords(part, records);
        topic.partitionData().add(part);
        req.topicData().add(topic);
        return req;
    }

    static ProduceResponseData produceResponse() {
        ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(0);
        ProduceResponseData.TopicProduceResponse topic = new ProduceResponseData.TopicProduceResponse().setName(TOPIC);
        ProduceResponseData.PartitionProduceResponse part = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0)
                .setErrorCode((short) 0)
                .setBaseOffset(0L)
                .setLogAppendTimeMs(LOG_APPEND_TIME_MS)
                .setLogStartOffset(0L);
        topic.partitionResponses().add(part);
        resp.responses().add(topic);
        return resp;
    }

    static void writeBytes(Path out, String name, byte[] bytes, List<String> rows) throws Exception {
        Files.write(out.resolve(name), bytes);
        rows.add("{\"file\":\"" + name + "\",\"sha256\":\"" + sha(bytes) + "\",\"bytes\":" + bytes.length + "}");
    }

    static String event(long timestamp, String name, String fields) {
        return "{\"timestamp\":" + timestamp + ",\"name\":\"" + name + '"' + fields + '}';
    }

    static void writeEvents(Path out) throws Exception {
        List<String> producer = new ArrayList<>();
        producer.add(event(EVENT_TIMESTAMP_BASE, "startup_complete", ""));
        for (int i = 0; i < RECORD_COUNT; i++) {
            producer.add(event(
                    EVENT_TIMESTAMP_BASE + 1 + i,
                    "producer_send_success",
                    ",\"key\":\"" + getKey(i) + "\",\"value\":\"" + getValue(i) + "\",\"topic\":\"" + TOPIC
                            + "\",\"partition\":0,\"offset\":" + i));
        }
        producer.add(event(EVENT_TIMESTAMP_BASE + 7, "shutdown_complete", ""));
        producer.add(event(
                EVENT_TIMESTAMP_BASE + 8,
                "tool_data",
                ",\"sent\":6,\"acked\":6,\"target_throughput\":5,\"avg_throughput\":0.0"));
        StringBuilder json = new StringBuilder();
        json.append("{\"json_property_order\":[\"timestamp\",\"name\"],\"topic\":\"")
                .append(TOPIC)
                .append("\",\"record_count\":")
                .append(RECORD_COUNT)
                .append(",\"producer\":[")
                .append(String.join(",", producer))
                .append("]}\n");
        Files.writeString(out.resolve("events.json"), json.toString());
    }

    static void writeOptions(Path out) throws Exception {
        StringBuilder records = new StringBuilder();
        for (int i = 0; i < RECORD_COUNT; i++) {
            if (i > 0) {
                records.append(',');
            }
            records.append("{\"i\":")
                    .append(i)
                    .append(",\"key\":\"")
                    .append(getKey(i))
                    .append("\",\"value\":\"")
                    .append(getValue(i))
                    .append("\",\"create_time\":")
                    .append(CREATE_TIME)
                    .append('}');
        }
        String options = "{\"topic\":\"" + TOPIC + "\",\"max_messages\":" + RECORD_COUNT
                + ",\"throughput\":" + THROUGHPUT + ",\"acks\":-1,\"message_create_time\":" + CREATE_TIME
                + ",\"value_prefix\":" + VALUE_PREFIX + ",\"repeating_keys\":" + REPEATING_KEYS
                + ",\"defaults\":{\"max-messages\":-1,\"throughput\":-1,\"acks\":-1,\"message-create-time\":-1}"
                + ",\"named_options\":[\"topic\",\"bootstrap-server\",\"max-messages\",\"throughput\",\"acks\","
                + "\"producer.config\",\"message-create-time\",\"value-prefix\",\"repeating-keys\"]"
                + ",\"records\":[" + records + "]}\n";
        Files.writeString(out.resolve("options.json"), options);
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;

        byte[] combined = encode(produceRequest(optionRecords(0, RECORD_COUNT)), PRODUCE_VERSION);
        byte[] response = encode(produceResponse(), PRODUCE_VERSION);
        writeBytes(out, "produce-request.bin", combined, rows);
        writeBytes(out, "produce-response.bin", response, rows);
        bodies += 2;
        for (int i = 0; i < RECORD_COUNT; i++) {
            writeBytes(
                    out,
                    String.format("r%02d-request.bin", i),
                    encode(produceRequest(optionRecords(i, 1)), PRODUCE_VERSION),
                    rows);
            bodies++;
        }

        writeEvents(out);
        writeOptions(out);

        String expected = "{\"topic\":\"" + TOPIC + "\",\"record_count\":" + RECORD_COUNT
                + ",\"throughput\":" + THROUGHPUT + ",\"value_prefix\":" + VALUE_PREFIX
                + ",\"repeating_keys\":" + REPEATING_KEYS + ",\"message_create_time\":" + CREATE_TIME
                + ",\"acks\":-1,\"keys\":[\"0\",\"1\",\"0\",\"1\",\"0\",\"1\"]"
                + ",\"values\":[\"42.0\",\"42.1\",\"42.2\",\"42.3\",\"42.4\",\"42.5\"]}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"produce_version\":" + PRODUCE_VERSION + ","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"record_count\":" + RECORD_COUNT + ","
                + "\"throughput\":" + THROUGHPUT + ","
                + "\"value_prefix\":" + VALUE_PREFIX + ","
                + "\"repeating_keys\":" + REPEATING_KEYS + ","
                + "\"message_create_time\":" + CREATE_TIME + ","
                + "\"generated_bodies\":" + bodies + ","
                + "\"unsupported_receipts\":0,"
                + "\"cells\":[" + String.join(",", rows) + "]}\n";
        Files.writeString(out.resolve("identity.json"), identity);
        System.out.println("{\"generated_bodies\":" + bodies + ",\"unsupported_receipts\":0}");
    }

    static <T> T first(Iterable<T> items) {
        for (T item : items) {
            return item;
        }
        throw new AssertionError("empty collection");
    }

    static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    static int countRecords(Object records) throws Exception {
        Iterable<?> it = (Iterable<?>) records.getClass().getMethod("records").invoke(records);
        int n = 0;
        for (Object ignored : it) {
            n++;
        }
        return n;
    }

    static String firstValue(Object records) throws Exception {
        Iterable<?> it = (Iterable<?>) records.getClass().getMethod("records").invoke(records);
        for (Object rec : it) {
            ByteBuffer val = (ByteBuffer) rec.getClass().getMethod("value").invoke(rec);
            if (val == null) {
                return null;
            }
            byte[] bytes = new byte[val.remaining()];
            val.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    static void decodeRust(String kind, short version, String hex) throws Exception {
        byte[] bytes = HexFormat.of().parseHex(hex);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        if (kind.equals("produce-request")) {
            ProduceRequestData req = new ProduceRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Produce request");
            }
            ProduceRequestData.TopicProduceData topic = first(req.topicData());
            Object records = first(topic.partitionData()).records();
            System.out.println("{\"ok\":true,\"kind\":\"produce-request\",\"acks\":" + req.acks()
                    + ",\"topic\":" + jsonString(topic.name()) + ",\"records\":" + countRecords(records)
                    + ",\"first_value\":" + jsonString(firstValue(records)) + "}");
            return;
        }
        if (kind.equals("produce-response")) {
            ProduceResponseData resp = new ProduceResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Produce response");
            }
            long base = first(first(resp.responses()).partitionResponses()).baseOffset();
            System.out.println("{\"ok\":true,\"kind\":\"produce-response\",\"base_offset\":" + base + "}");
            return;
        }
        throw new IllegalArgumentException("unknown kind " + kind);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("generate")) {
            generate(Path.of(args[1]), Path.of(args[2]));
            return;
        }
        if (args.length == 5 && args[0].equals("decode-rust")) {
            Path jar = Path.of(args[1]);
            Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
                throw new AssertionError("wrong pinned Apache jar");
            }
            decodeRust(args[2], Short.parseShort(args[3]), args[4]);
            return;
        }
        throw new IllegalArgumentException(
                "generate <jar> <outdir> | decode-rust <jar> <kind> <version> <hex>");
    }
}
