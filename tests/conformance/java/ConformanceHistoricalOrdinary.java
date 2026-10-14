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
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache ordinary Produce / Fetch / ListOffsets / Metadata bodies
 * plus Apache-shaped verifiable events for KL01-23.
 *
 * Record batches are built through reflection so the same source compiles
 * on 3.9.1–4.2.1 (public MemoryRecords) and 4.3.1 (internal package).
 */
public final class ConformanceHistoricalOrdinary {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short PRODUCE_VERSION = 9;
    static final short FETCH_VERSION = 12;
    static final short LIST_OFFSETS_VERSION = 5;
    static final short METADATA_VERSION = 9;
    static final String TOPIC = "pl-ordinary";
    static final int RECORD_COUNT = 25;
    static final long LATEST_TIMESTAMP = -1L;
    static final long LOG_APPEND_TIME_MS = 1_710_000_000_000L;
    static final long EVENT_TIMESTAMP_BASE = 1_710_000_000_000L;

    private ConformanceHistoricalOrdinary() {}

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

    static Object ordinaryRecords() throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> simple = simpleRecordClass();
        Object array = Array.newInstance(simple, RECORD_COUNT);
        for (int i = 0; i < RECORD_COUNT; i++) {
            Array.set(array, i, simpleRecord(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
        }
        Class<?> compression = Class.forName("org.apache.kafka.common.compress.Compression");
        Method method = recordsClass.getMethod("withRecords", long.class, compression, array.getClass());
        return method.invoke(null, 0L, compressionNone(), array);
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

    static FetchRequestData fetchRequest() {
        FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10_485_760)
                .setIsolationLevel((byte) 0)
                .setSessionId(0)
                .setSessionEpoch(-1)
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

    static FetchResponseData fetchResponse(Object records) throws Exception {
        FetchResponseData resp = new FetchResponseData().setThrottleTimeMs(0);
        FetchResponseData.FetchableTopicResponse topic =
                new FetchResponseData.FetchableTopicResponse().setTopic(TOPIC);
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(RECORD_COUNT)
                .setLastStableOffset(RECORD_COUNT)
                .setLogStartOffset(0L);
        setRecords(part, records);
        topic.partitions().add(part);
        resp.responses().add(topic);
        return resp;
    }

    static ListOffsetsRequestData listOffsetsRequest() {
        ListOffsetsRequestData req = new ListOffsetsRequestData()
                .setReplicaId(-1)
                .setIsolationLevel((byte) 0);
        ListOffsetsRequestData.ListOffsetsTopic topic = new ListOffsetsRequestData.ListOffsetsTopic().setName(TOPIC);
        topic.partitions()
                .add(new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(LATEST_TIMESTAMP)
                        .setCurrentLeaderEpoch(-1));
        req.topics().add(topic);
        return req;
    }

    static ListOffsetsResponseData listOffsetsResponse() {
        ListOffsetsResponseData resp = new ListOffsetsResponseData().setThrottleTimeMs(0);
        ListOffsetsResponseData.ListOffsetsTopicResponse topic =
                new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(TOPIC);
        topic.partitions()
                .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setTimestamp(LATEST_TIMESTAMP)
                        .setOffset(RECORD_COUNT)
                        .setLeaderEpoch(0));
        resp.topics().add(topic);
        return resp;
    }

    static MetadataRequestData metadataRequest() {
        MetadataRequestData req = new MetadataRequestData().setAllowAutoTopicCreation(false);
        req.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(TOPIC));
        return req;
    }

    static MetadataResponseData metadataResponse() {
        MetadataResponseData resp = new MetadataResponseData()
                .setThrottleTimeMs(0)
                .setClusterId("pl-ordinary-cluster")
                .setControllerId(1);
        resp.brokers()
                .add(new MetadataResponseData.MetadataResponseBroker()
                        .setNodeId(1)
                        .setHost("broker")
                        .setPort(9092)
                        .setRack(null));
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic()
                .setErrorCode((short) 0)
                .setName(TOPIC)
                .setIsInternal(false);
        MetadataResponseData.MetadataResponsePartition part = new MetadataResponseData.MetadataResponsePartition()
                .setErrorCode((short) 0)
                .setPartitionIndex(0)
                .setLeaderId(1)
                .setLeaderEpoch(7);
        part.replicaNodes().add(1);
        part.isrNodes().add(1);
        topic.partitions().add(part);
        resp.topics().add(topic);
        return resp;
    }

    static void writePair(Path out, String stem, byte[] req, byte[] resp, List<String> rows) throws Exception {
        Files.write(out.resolve(stem + "-request.bin"), req);
        Files.write(out.resolve(stem + "-response.bin"), resp);
        rows.add("{\"cell\":\"" + stem + "\",\"request_sha256\":\"" + sha(req)
                + "\",\"response_sha256\":\"" + sha(resp) + "\",\"request_bytes\":" + req.length
                + ",\"response_bytes\":" + resp.length + "}");
    }

    static void roundtrip(Message original, byte[] encoded, short version, String cell) {
        Message back;
        if (original instanceof ProduceRequestData) {
            back = new ProduceRequestData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof ProduceResponseData) {
            back = new ProduceResponseData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof FetchRequestData) {
            back = new FetchRequestData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof FetchResponseData) {
            back = new FetchResponseData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof ListOffsetsRequestData) {
            back = new ListOffsetsRequestData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof ListOffsetsResponseData) {
            back = new ListOffsetsResponseData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof MetadataRequestData) {
            back = new MetadataRequestData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else if (original instanceof MetadataResponseData) {
            back = new MetadataResponseData(new ByteBufferAccessor(ByteBuffer.wrap(encoded)), version);
        } else {
            throw new AssertionError("unknown message " + original.getClass().getName());
        }
        if (!Arrays.equals(encoded, encode(back, version))) {
            throw new AssertionError("Apache self-roundtrip failed " + cell);
        }
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
                    ",\"key\":null,\"value\":\"" + i + "\",\"topic\":\"" + TOPIC
                            + "\",\"partition\":0,\"offset\":" + i));
        }
        producer.add(event(EVENT_TIMESTAMP_BASE + 26, "shutdown_complete", ""));
        producer.add(event(
                EVENT_TIMESTAMP_BASE + 27,
                "tool_data",
                ",\"sent\":25,\"acked\":25,\"target_throughput\":-1,\"avg_throughput\":0.0"));

        List<String> consumer = new ArrayList<>();
        consumer.add(event(EVENT_TIMESTAMP_BASE + 28, "startup_complete", ""));
        consumer.add(event(
                EVENT_TIMESTAMP_BASE + 29,
                "partitions_assigned",
                ",\"partitions\":[{\"topic\":\"" + TOPIC + "\",\"partition\":0}]"));
        consumer.add(event(
                EVENT_TIMESTAMP_BASE + 30,
                "records_consumed",
                ",\"count\":25,\"partitions\":[{\"topic\":\"" + TOPIC
                        + "\",\"partition\":0,\"count\":25,\"minOffset\":0,\"maxOffset\":24}]"));
        for (int i = 0; i < RECORD_COUNT; i++) {
            consumer.add(event(
                    EVENT_TIMESTAMP_BASE + 31 + i,
                    "record_data",
                    ",\"key\":null,\"value\":\"" + i + "\",\"topic\":\"" + TOPIC
                            + "\",\"partition\":0,\"offset\":" + i));
        }
        consumer.add(event(
                EVENT_TIMESTAMP_BASE + 56,
                "offsets_committed",
                ",\"offsets\":[{\"topic\":\"" + TOPIC + "\",\"partition\":0,\"offset\":25}],\"success\":true"));
        consumer.add(event(
                EVENT_TIMESTAMP_BASE + 57,
                "partitions_revoked",
                ",\"partitions\":[{\"topic\":\"" + TOPIC + "\",\"partition\":0}]"));
        consumer.add(event(EVENT_TIMESTAMP_BASE + 58, "shutdown_complete", ""));

        StringBuilder json = new StringBuilder();
        json.append("{\"json_property_order\":[\"timestamp\",\"name\"],\"topic\":\"")
                .append(TOPIC)
                .append("\",\"record_count\":")
                .append(RECORD_COUNT)
                .append(",\"producer\":[")
                .append(String.join(",", producer))
                .append("],\"consumer\":[")
                .append(String.join(",", consumer))
                .append("]}\n");
        Files.writeString(out.resolve("events.json"), json.toString());
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;
        Object records = ordinaryRecords();

        ProduceRequestData produceReq = produceRequest(records);
        ProduceResponseData produceResp = produceResponse();
        byte[] produceReqBytes = encode(produceReq, PRODUCE_VERSION);
        byte[] produceRespBytes = encode(produceResp, PRODUCE_VERSION);
        roundtrip(produceReq, produceReqBytes, PRODUCE_VERSION, "produce-request");
        roundtrip(produceResp, produceRespBytes, PRODUCE_VERSION, "produce-response");
        writePair(out, "produce", produceReqBytes, produceRespBytes, rows);
        bodies += 2;

        FetchRequestData fetchReq = fetchRequest();
        FetchResponseData fetchResp = fetchResponse(records);
        byte[] fetchReqBytes = encode(fetchReq, FETCH_VERSION);
        byte[] fetchRespBytes = encode(fetchResp, FETCH_VERSION);
        roundtrip(fetchReq, fetchReqBytes, FETCH_VERSION, "fetch-request");
        roundtrip(fetchResp, fetchRespBytes, FETCH_VERSION, "fetch-response");
        writePair(out, "fetch", fetchReqBytes, fetchRespBytes, rows);
        bodies += 2;

        ListOffsetsRequestData listReq = listOffsetsRequest();
        ListOffsetsResponseData listResp = listOffsetsResponse();
        byte[] listReqBytes = encode(listReq, LIST_OFFSETS_VERSION);
        byte[] listRespBytes = encode(listResp, LIST_OFFSETS_VERSION);
        roundtrip(listReq, listReqBytes, LIST_OFFSETS_VERSION, "list-offsets-request");
        roundtrip(listResp, listRespBytes, LIST_OFFSETS_VERSION, "list-offsets-response");
        writePair(out, "list-offsets", listReqBytes, listRespBytes, rows);
        bodies += 2;

        MetadataRequestData metaReq = metadataRequest();
        MetadataResponseData metaResp = metadataResponse();
        byte[] metaReqBytes = encode(metaReq, METADATA_VERSION);
        byte[] metaRespBytes = encode(metaResp, METADATA_VERSION);
        roundtrip(metaReq, metaReqBytes, METADATA_VERSION, "metadata-request");
        roundtrip(metaResp, metaRespBytes, METADATA_VERSION, "metadata-response");
        writePair(out, "metadata", metaReqBytes, metaRespBytes, rows);
        bodies += 2;

        writeEvents(out);

        String expected = "{\"topic\":\"" + TOPIC + "\",\"record_count\":" + RECORD_COUNT
                + ",\"produce_acks\":-1,\"produce_base_offset\":0,\"fetch_offset\":0,"
                + "\"fetch_high_watermark\":" + RECORD_COUNT + ",\"list_offsets_timestamp\":-1,"
                + "\"list_offsets_offset\":" + RECORD_COUNT + ",\"metadata_leader_id\":1,"
                + "\"metadata_partitions\":1,\"verifiable_producer_successes\":" + RECORD_COUNT
                + ",\"verifiable_commit_offset\":" + RECORD_COUNT + "}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"produce_version\":" + PRODUCE_VERSION + ","
                + "\"fetch_version\":" + FETCH_VERSION + ","
                + "\"list_offsets_version\":" + LIST_OFFSETS_VERSION + ","
                + "\"metadata_version\":" + METADATA_VERSION + ","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"record_count\":" + RECORD_COUNT + ","
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
            System.out.println("{\"ok\":true,\"kind\":\"produce-response\",\"base_offset\":" + base + "}");
            return;
        }
        if (kind.equals("fetch-response")) {
            FetchResponseData resp = new FetchResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Fetch response");
            }
            FetchResponseData.PartitionData part = resp.responses().get(0).partitions().get(0);
            System.out.println("{\"ok\":true,\"kind\":\"fetch-response\",\"high_watermark\":"
                    + part.highWatermark() + ",\"first_error\":" + part.errorCode() + "}");
            return;
        }
        if (kind.equals("list-offsets-response")) {
            ListOffsetsResponseData resp = new ListOffsetsResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed ListOffsets response");
            }
            long offset = resp.topics().get(0).partitions().get(0).offset();
            System.out.println("{\"ok\":true,\"kind\":\"list-offsets-response\",\"offset\":" + offset + "}");
            return;
        }
        if (kind.equals("metadata-response")) {
            MetadataResponseData resp = new MetadataResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Metadata response");
            }
            int leader = first(first(resp.topics()).partitions()).leaderId();
            System.out.println("{\"ok\":true,\"kind\":\"metadata-response\",\"leader_id\":" + leader
                    + ",\"topics\":" + resp.topics().size() + "}");
            return;
        }
        throw new IllegalArgumentException("unknown kind " + kind);
    }

    static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' || c == '"') {
                out.append('\\').append(c);
            } else if (c < 0x20) {
                out.append(String.format("\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
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
