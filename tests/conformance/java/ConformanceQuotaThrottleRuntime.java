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
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.ProduceResponse;

/**
 * Independent Apache Produce v9 / Fetch v12 quota-throttle bodies for KL01-25.
 *
 * Cells follow hashed ProduceResponse.shouldClientThrottle (v6+),
 * FetchResponse.shouldClientThrottle (v8+), and FetchSessionHandler
 * empty-throttled-full reset to INITIAL. Zero and negative ThrottleTimeMs
 * never unmute a prior quota.
 *
 * Record batches are built through reflection so the same source compiles
 * on 3.9.1–4.2.1 (public MemoryRecords) and 4.3.1 (internal package).
 */
public final class ConformanceQuotaThrottleRuntime {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short PRODUCE_VERSION = 9;
    static final short FETCH_VERSION = 12;
    static final String TOPIC = "pl-quota";
    static final int SESSION_ID = 91;
    static final int PRODUCE_QUOTA_MS = 250;
    static final int FETCH_QUOTA_MS = 80;
    static final long LOG_APPEND_TIME_MS = 1_700_000_000_000L;

    private ConformanceQuotaThrottleRuntime() {}

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

    static Object simpleRecord(byte[] value) throws Exception {
        return simpleRecordClass().getConstructor(long.class, byte[].class).newInstance(0L, value);
    }

    static Object dataRecords(long baseOffset, byte[]... values) throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> simple = simpleRecordClass();
        Object array = Array.newInstance(simple, values.length);
        for (int i = 0; i < values.length; i++) {
            Array.set(array, i, simpleRecord(values[i]));
        }
        Class<?> compression = Class.forName("org.apache.kafka.common.compress.Compression");
        Method method = recordsClass.getMethod("withRecords", long.class, compression, array.getClass());
        return method.invoke(null, baseOffset, compressionNone(), array);
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

    static ProduceResponseData produceResponse(int throttle) {
        ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(throttle);
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

    static FetchRequestData fetchRequest() {
        FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10_485_760)
                .setIsolationLevel((byte) 0)
                .setSessionId(0)
                .setSessionEpoch(0)
                .setRackId("");
        FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic(TOPIC);
        topic.partitions()
                .add(new FetchRequestData.FetchPartition()
                        .setPartition(0)
                        .setFetchOffset(0L)
                        .setLogStartOffset(-1L)
                        .setPartitionMaxBytes(1_048_576)
                        .setCurrentLeaderEpoch(-1));
        req.topics().add(topic);
        return req;
    }

    static FetchResponseData.PartitionData okPartition(Object records) throws Exception {
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(1L)
                .setLastStableOffset(1L)
                .setLogStartOffset(0L);
        setRecords(part, records);
        return part;
    }

    static FetchResponseData fetchResponse(int throttle, int sessionId, FetchResponseData.PartitionData... partitions) {
        FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(throttle)
                .setErrorCode((short) 0)
                .setSessionId(sessionId);
        if (partitions.length > 0) {
            FetchResponseData.FetchableTopicResponse topic =
                    new FetchResponseData.FetchableTopicResponse().setTopic(TOPIC);
            topic.partitions().addAll(Arrays.asList(partitions));
            resp.responses().add(topic);
        }
        return resp;
    }

    static void writeBytes(Path out, String name, byte[] bytes, List<String> rows) throws Exception {
        Files.write(out.resolve(name), bytes);
        rows.add("{\"file\":\"" + name + "\",\"sha256\":\"" + sha(bytes) + "\",\"bytes\":" + bytes.length + "}");
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;
        Object produceRecords = dataRecords(0L, "q".getBytes(StandardCharsets.UTF_8));
        Object fetchRecords = dataRecords(0L, "a".getBytes(StandardCharsets.UTF_8));

        ProduceResponse produceWrap = new ProduceResponse(produceResponse(0));
        if (!produceWrap.shouldClientThrottle(PRODUCE_VERSION) || produceWrap.shouldClientThrottle((short) 5)) {
            throw new AssertionError("Produce shouldClientThrottle gates");
        }

        writeBytes(out, "p01-produce-request.bin", encode(produceRequest(produceRecords), PRODUCE_VERSION), rows);
        writeBytes(out, "p01-produce-zero-response.bin", encode(produceResponse(0), PRODUCE_VERSION), rows);
        bodies += 2;
        writeBytes(out, "p02-produce-quota-response.bin", encode(produceResponse(PRODUCE_QUOTA_MS), PRODUCE_VERSION), rows);
        bodies++;
        writeBytes(out, "p03-produce-negative-response.bin", encode(produceResponse(-1), PRODUCE_VERSION), rows);
        bodies++;

        writeBytes(out, "f01-fetch-request.bin", encode(fetchRequest(), FETCH_VERSION), rows);
        writeBytes(
                out,
                "f01-fetch-zero-response.bin",
                encode(fetchResponse(0, SESSION_ID, okPartition(fetchRecords)), FETCH_VERSION),
                rows);
        bodies += 2;
        writeBytes(
                out,
                "f02-fetch-quota-response.bin",
                encode(fetchResponse(FETCH_QUOTA_MS, SESSION_ID, okPartition(fetchRecords)), FETCH_VERSION),
                rows);
        bodies++;
        writeBytes(
                out,
                "f03-empty-throttled-response.bin",
                encode(fetchResponse(FETCH_QUOTA_MS, SESSION_ID), FETCH_VERSION),
                rows);
        bodies++;

        String expected = "{\"topic\":\"" + TOPIC + "\",\"produce_version\":" + PRODUCE_VERSION
                + ",\"fetch_version\":" + FETCH_VERSION + ",\"session_id\":" + SESSION_ID
                + ",\"produce_zero_throttle\":0,\"produce_quota_throttle\":" + PRODUCE_QUOTA_MS
                + ",\"produce_negative_throttle\":-1,\"fetch_zero_throttle\":0,\"fetch_quota_throttle\":"
                + FETCH_QUOTA_MS + ",\"fetch_empty_throttled\":" + FETCH_QUOTA_MS
                + ",\"produce_should_client_throttle_v9\":true,\"produce_should_client_throttle_v5\":false"
                + ",\"fetch_should_client_throttle_v12\":true,\"fetch_should_client_throttle_v7\":false}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"produce_version\":" + PRODUCE_VERSION + ","
                + "\"fetch_version\":" + FETCH_VERSION + ","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"session_id\":" + SESSION_ID + ","
                + "\"produce_quota_ms\":" + PRODUCE_QUOTA_MS + ","
                + "\"fetch_quota_ms\":" + FETCH_QUOTA_MS + ","
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

    static void decodeRust(String kind, short version, String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        if (kind.equals("produce-request")) {
            ProduceRequestData req = new ProduceRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Produce request");
            }
            ProduceRequestData.TopicProduceData topic = first(req.topicData());
            System.out.println("{\"ok\":true,\"kind\":\"produce-request\",\"acks\":" + req.acks()
                    + ",\"topic\":" + jsonString(topic.name()) + ",\"partitions\":"
                    + topic.partitionData().size() + "}");
            return;
        }
        if (kind.equals("produce-response")) {
            ProduceResponseData resp = new ProduceResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Produce response");
            }
            long base = first(first(resp.responses()).partitionResponses()).baseOffset();
            System.out.println("{\"ok\":true,\"kind\":\"produce-response\",\"throttle\":" + resp.throttleTimeMs()
                    + ",\"base_offset\":" + base + "}");
            return;
        }
        if (kind.equals("fetch-request")) {
            FetchRequestData req = new FetchRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Fetch request");
            }
            int partitions = req.topics().isEmpty() ? 0 : req.topics().get(0).partitions().size();
            System.out.println("{\"ok\":true,\"kind\":\"fetch-request\",\"session_id\":" + req.sessionId()
                    + ",\"epoch\":" + req.sessionEpoch() + ",\"partitions\":" + partitions + "}");
            return;
        }
        if (kind.equals("fetch-response")) {
            FetchResponseData resp = new FetchResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Fetch response");
            }
            System.out.println("{\"ok\":true,\"kind\":\"fetch-response\",\"throttle\":" + resp.throttleTimeMs()
                    + ",\"session_id\":" + resp.sessionId() + ",\"topics\":" + resp.responses().size() + "}");
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
