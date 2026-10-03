/* Actual pinned Apache ordinary producers/manual consumers and forced-version TCP peers. */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class OrdinaryPeer {
    private static final long TIME = 1_700_000_000_000L;
    private static final int COUNT = 12;
    private static final int RAW_COUNT = 33;
    private static final List<String> HISTORY = new ArrayList<>();
    private static String release;
    private static String prefix;
    private static int port;
    private static int correlation = 100;
    private static int assertions;
    private static boolean expectReadApis;
    private static Path state;
    private OrdinaryPeer() { }
    private static String quote(String value) {
        return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }
    private static void check(boolean value, String label) { assertions++; if (!value) throw new AssertionError(label); }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String hex(byte[] data) { return data == null ? null : HexFormat.of().formatHex(data); }
    private static byte[] bytes(ByteBuffer data) {
        if (data == null) return null;
        ByteBuffer copy = data.duplicate(); byte[] result = new byte[copy.remaining()]; copy.get(result); return result;
    }
    private static String hash(String text) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(utf8(text))); }
    private static String id(String topic, int partition, int index) { return topic + ":" + partition + ":" + index; }
    private static byte[] key(String id, int index) { return index % 3 == 0 ? null : utf8("key:" + id); }
    private static byte[] value(String id, int index) { return index % 4 == 0 ? null : index % 4 == 1 ? new byte[0] : utf8("value:" + id); }
    private static List<Header> headers(String id) { return List.of(new RecordHeader("receipt", utf8(id)), new RecordHeader("dup", utf8("a")), new RecordHeader("dup", null)); }
    private static String receipt(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value, Iterable<Header> headers) throws Exception {
        List<String> fields = new ArrayList<>();
        for (Header header : headers) fields.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":" + quote(hex(header.value())) + "}");
        String content = "{\"topic\":" + quote(topic) + ",\"partition\":" + partition + ",\"offset\":" + offset + ",\"timestamp\":" + timestamp
            + ",\"key_hex\":" + quote(hex(key)) + ",\"value_hex\":" + quote(hex(value)) + ",\"headers\":[" + String.join(",", fields) + "]}";
        return "{\"sha256\":" + quote(hash(content)) + ",\"record\":" + content + "}";
    }
    private static Properties properties() {
        Properties properties = new Properties(); properties.setProperty("bootstrap.servers", "127.0.0.1:" + port);
        properties.setProperty("client.id", "ordinary-java-" + release); properties.setProperty("request.timeout.ms", "5000");
        properties.setProperty("default.api.timeout.ms", "15000"); properties.setProperty("retry.backoff.ms", "50"); return properties;
    }
    private static Admin admin() { Properties p = properties(); p.setProperty("retries", "0"); return Admin.create(p); }
    private static Uuid describe(Admin admin, String topic, int partitions) throws Exception {
        TopicDescription description = admin.describeTopics(TopicCollection.ofTopicNames(List.of(topic))).allTopicNames().get(15, TimeUnit.SECONDS).get(topic);
        check(description != null && description.partitions().size() == partitions, "actual Admin topic partitions");
        check(!description.topicId().equals(Uuid.ZERO_UUID), "actual Admin allocated nonzero UUID"); return description.topicId();
    }
    private static void actualAppend() throws Exception {
        try (Admin admin = admin()) {
            for (String ack : List.of("1", "-1", "0")) {
                String topic = prefix + "-acks" + ack; int partitions = ack.equals("1") ? 2 : 1;
                admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(15, TimeUnit.SECONDS);
                Uuid id = describe(admin, topic, partitions); Files.writeString(state.resolve(topic + ".uuid"), id.toString());
                Properties p = properties(); p.remove("default.api.timeout.ms"); p.setProperty("enable.idempotence", "false");
                p.setProperty("acks", ack); p.setProperty("retries", "0"); p.setProperty("max.in.flight.requests.per.connection", "1");
                p.setProperty("compression.type", "none"); p.setProperty("batch.size", "16384"); p.setProperty("linger.ms", "5");
                p.setProperty("buffer.memory", "1048576"); p.setProperty("max.request.size", "1048576");
                p.setProperty("max.block.ms", "5000"); p.setProperty("delivery.timeout.ms", "15000");
                List<Future<RecordMetadata>> pending = new ArrayList<>(); List<String> expected = new ArrayList<>();
                try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer())) {
                    for (int partition = 0; partition < partitions; partition++) for (int index = 0; index < COUNT; index++) {
                        String rid = id(topic, partition, index); long timestamp = TIME + partition * 100 + index;
                        pending.add(producer.send(new ProducerRecord<>(topic, partition, timestamp, key(rid, index), value(rid, index), headers(rid))));
                        expected.add(receipt(topic, partition, index, timestamp, key(rid, index), value(rid, index), headers(rid)));
                    }
                    producer.flush();
                    for (int index = 0; index < pending.size(); index++) {
                        RecordMetadata metadata = pending.get(index).get(15, TimeUnit.SECONDS);
                        check(metadata.topic().equals(topic) && metadata.partition() == index / COUNT, "actual Producer assigned partition");
                        check(metadata.offset() == (ack.equals("0") ? -1 : index % COUNT), "actual Producer acknowledgment offset");
                        HISTORY.add("{\"label\":\"actual-Producer\",\"acks\":" + quote(ack) + ",\"acked_offset\":" + metadata.offset() + ",\"expected_receipt\":" + expected.get(index) + "}");
                    }
                }
            }
        }
    }
    private static KafkaConsumer<byte[], byte[]> consumer() {
        Properties p = properties(); p.setProperty("enable.auto.commit", "false"); p.setProperty("allow.auto.create.topics", "false");
        p.setProperty("auto.offset.reset", "none"); p.setProperty("max.poll.records", "100");
        p.setProperty("fetch.min.bytes", "1"); p.setProperty("fetch.max.wait.ms", "20");
        p.setProperty("fetch.max.bytes", "65536"); p.setProperty("max.partition.fetch.bytes", "4096");
        p.setProperty("isolation.level", "read_uncommitted");
        return new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    private static void actualFetch() throws Exception {
        try (Admin admin = admin(); KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            for (String ack : List.of("1", "-1", "0")) {
                String topic = prefix + "-acks" + ack; int partitions = ack.equals("1") ? 2 : 1;
                int count = COUNT + (Files.exists(state.resolve(topic + ".checkpoint")) ? 1 : 0);
                Uuid expectedId = Uuid.fromString(Files.readString(state.resolve(topic + ".uuid")));
                check(describe(admin, topic, partitions).equals(expectedId), "topic UUID retained before/after restart");
                List<TopicPartition> assignment = new ArrayList<>(); Map<TopicPartition, Integer> next = new LinkedHashMap<>();
                for (int partition = 0; partition < partitions; partition++) { TopicPartition tp = new TopicPartition(topic, partition); assignment.add(tp); next.put(tp, 0); }
                consumer.assign(assignment); for (TopicPartition tp : assignment) consumer.seek(tp, 0);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20); int received = 0;
                while (received < partitions * count && System.nanoTime() < deadline) {
                    ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[], byte[]> record : polled) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition()); Integer ordinal = next.get(tp);
                        check(ordinal != null && ordinal < count && record.offset() == ordinal, "exact actual Consumer per-partition offset/order/no duplicates");
                        String rid = id(topic, record.partition(), ordinal); long timestamp = TIME + record.partition() * 100 + ordinal;
                        String expected = receipt(topic, record.partition(), ordinal, timestamp, key(rid, ordinal), value(rid, ordinal), headers(rid));
                        String observed = receipt(record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value(), record.headers());
                        check(expected.equals(observed), "actual Consumer exact record hash/null/empty/key/value/timestamp/header identity");
                        next.put(tp, ordinal + 1); received++; HISTORY.add("{\"label\":\"actual-Consumer\",\"acks\":" + quote(ack) + ",\"receipt\":" + observed + "}");
                    }
                }
                check(received == partitions * count, "actual Consumer received every acknowledged/noack record");
                Map<TopicPartition, Long> first = consumer.beginningOffsets(assignment, Duration.ofSeconds(10));
                Map<TopicPartition, Long> last = consumer.endOffsets(assignment, Duration.ofSeconds(10));
                for (TopicPartition tp : assignment) {
                    check(first.get(tp) == 0 && last.get(tp) == count, "actual ListOffsets start/end");
                    var found = consumer.offsetsForTimes(Map.of(tp, TIME + tp.partition() * 100 + 5), Duration.ofSeconds(10)).get(tp);
                    check(found != null && found.offset() == 5 && found.timestamp() == TIME + tp.partition() * 100 + 5, "actual ListOffsets timestamp boundary");
                }
            }
        }
    }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache(); ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version); check(!buffer.hasRemaining(), "serializer full write"); return buffer.array();
    }
    private static byte[] frame(ApiKeys key, short version, ApiMessage data, int cid) {
        RequestHeader header = new RequestHeader(key, version, "ordinary-raw-" + release, cid);
        byte[] head = encode(header.data(), header.headerVersion()); byte[] body = encode(data, version);
        byte[] frame = Arrays.copyOf(head, head.length + body.length); System.arraycopy(body, 0, frame, head.length, body.length); return frame;
    }
    private static ApiMessage read(Socket socket, ApiKeys key, short version, int cid, byte[] request, String label) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream()); int size = input.readInt(); check(size >= 4 && size <= 128 * 1024, "response allocation bound");
        byte[] response = input.readNBytes(size); check(response.length == size, "response complete read");
        ByteBuffer buffer = ByteBuffer.wrap(response); check(ResponseHeader.parse(buffer, key.responseHeaderVersion(version)).correlationId() == cid, "correlation identity");
        ApiMessage data = AbstractResponse.parseResponse(key, new ByteBufferAccessor(buffer), version).data(); check(!buffer.hasRemaining(), "response complete parse");
        HISTORY.add("{\"label\":" + quote(label) + ",\"api_key\":" + key.id + ",\"api_version\":" + version + ",\"request_hex\":" + quote(hex(request))
            + ",\"response_hex\":" + quote(hex(response)) + ",\"apache_data\":" + quote(data.toString()) + "}"); return data;
    }
    private static void send(Socket socket, byte[] request) throws Exception { DataOutputStream output = new DataOutputStream(socket.getOutputStream()); output.writeInt(request.length); output.write(request); output.flush(); }
    private static ApiMessage call(ApiKeys key, short version, ApiMessage data, String label) throws Exception {
        int cid = correlation++; byte[] request = frame(key, version, data, cid);
        try (Socket socket = new Socket("127.0.0.1", port)) { socket.setSoTimeout(10_000); send(socket, request); return read(socket, key, version, cid, request, label); }
    }
    private static void assertProfile(ApiVersionsResponseData data) {
        Map<Integer, String> expected = new HashMap<>(); expected.put(0, "3:13"); expected.put(3, "0:13");
        expected.put(18, "0:4"); expected.put(19, "2:4"); expected.put(20, "1:6");
        if (expectReadApis) { expected.put(1, "4:6"); expected.put(2, "1:3"); }
        Map<Integer, String> actual = new HashMap<>();
        for (var entry : data.apiKeys()) check(actual.put((int) entry.apiKey(), entry.minVersion() + ":" + entry.maxVersion()) == null, "no duplicate advertised API key");
        check(data.errorCode() == 0 && actual.equals(expected), "exact actual on-wire five/seven API ordinary profile");
    }
    private static MemoryRecords ordinary(int ordinal) { return MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(TIME + ordinal, utf8("raw-key:" + ordinal), utf8("raw-value:" + ordinal), new Header[]{new RecordHeader("receipt", utf8(prefix + ":raw:" + ordinal))})); }
    private static ProduceRequestData produce(String topic, Uuid id, short acks, MemoryRecords records) {
        ProduceRequestData request = new ProduceRequestData().setTransactionalId(null).setAcks(acks).setTimeoutMs(5000);
        request.topicData().add(new ProduceRequestData.TopicProduceData().setName(topic).setTopicId(id)
            .setPartitionData(new ArrayList<>(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records))))); return request;
    }
    private static void rawAppend() throws Exception {
        String topic = prefix + "-raw"; Uuid topicId;
        try (Admin admin = admin()) { admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(15, TimeUnit.SECONDS); topicId = describe(admin, topic, 1); }
        Files.writeString(state.resolve(topic + ".uuid"), topicId.toString()); int ordinal = 0;
        for (short version = 3; version <= 13; version++) {
            for (short acks : new short[]{1, -1}) {
                ProduceResponseData response = (ProduceResponseData) call(ApiKeys.PRODUCE, version, produce(topic, topicId, acks, ordinary(ordinal)), "forced-produce-v" + version + "-acks" + acks);
                var result = response.responses().iterator().next().partitionResponses().get(0);
                check(result.errorCode() == 0 && result.baseOffset() == ordinal && result.logAppendTimeMs() == -1, "forced Produce durable sequential offset");
                if (version >= 5) check(result.logStartOffset() == 0, "forced Produce start0"); ordinal++;
            }
            int cid = correlation++; byte[] request = frame(ApiKeys.PRODUCE, version, produce(topic, topicId, (short) 0, ordinary(ordinal)), cid);
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(10_000); send(socket, request);
                int pingCid = correlation++; byte[] ping = frame(ApiKeys.API_VERSIONS, (short) 4,
                    new ApiVersionsRequestData().setClientSoftwareName("ordinary-peer").setClientSoftwareVersion(release), pingCid);
                send(socket, ping); ApiVersionsResponseData pong = (ApiVersionsResponseData) read(socket, ApiKeys.API_VERSIONS, (short) 4, pingCid, ping, "acks0-v" + version + "-same-channel-next-response");
                check(pong.errorCode() == 0, "acks0 successful channel remains open and sends no Produce response"); assertProfile(pong);
                HISTORY.add("{\"label\":\"acks0-success-no-response\",\"api_version\":" + version + ",\"request_hex\":" + quote(hex(request)) + ",\"assigned_offset\":" + ordinal + "}");
            }
            ordinal++;
            byte[] damaged = bytes(ordinary(0).buffer()); damaged[damaged.length - 1] ^= 1;
            ProduceResponseData corrupt = (ProduceResponseData) call(ApiKeys.PRODUCE, version, produce(topic, topicId, (short) 1,
                MemoryRecords.readableRecords(ByteBuffer.wrap(damaged))), "forced-produce-v" + version + "-bad-crc");
            var corruptPart = corrupt.responses().iterator().next().partitionResponses().get(0);
            check(corruptPart.errorCode() == 2 && corruptPart.baseOffset() == -1, "corrupt Produce rejected without offset receipt");
            byte[] noack = frame(ApiKeys.PRODUCE, version, produce("missing-topic", new Uuid(0, 99), (short) 0, ordinary(0)), correlation++);
            try (Socket socket = new Socket("127.0.0.1", port)) { socket.setSoTimeout(10_000); send(socket, noack); check(socket.getInputStream().read() == -1, "acks0 partition error clean EOF/no response"); }
            HISTORY.add("{\"label\":\"acks0-error-close\",\"api_version\":" + version + ",\"request_hex\":" + quote(hex(noack)) + ",\"outcome\":\"EOF\"}");
        }
        check(ordinal == RAW_COUNT, "raw receipt count33");
    }
    private static FetchRequestData fetch(String topic, long offset, int maxBytes) {
        FetchRequestData data = new FetchRequestData().setReplicaId(-1).setMaxWaitMs(0).setMinBytes(0).setMaxBytes(maxBytes).setIsolationLevel((byte) 0);
        data.topics().add(new FetchRequestData.FetchTopic().setTopic(topic).setPartitions(new ArrayList<>(List.of(
            new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(offset).setPartitionMaxBytes(maxBytes))))); return data;
    }
    private static void rawFetch() throws Exception {
        String topic = prefix + "-raw";
        int count = RAW_COUNT + (Files.exists(state.resolve(topic + ".checkpoint")) ? 1 : 0);
        try (Admin admin = admin()) { check(describe(admin, topic, 1).equals(Uuid.fromString(Files.readString(state.resolve(topic + ".uuid")))), "raw topic UUID retained"); }
        for (short version = 4; version <= 6; version++) {
            FetchResponseData response = (FetchResponseData) call(ApiKeys.FETCH, version, fetch(topic, 0, 65536), "forced-fetch-v" + version);
            var part = response.responses().get(0).partitions().get(0); check(part.errorCode() == 0 && part.highWatermark() == count && part.lastStableOffset() == count, "raw Fetch ordinary HW/LSO");
            if (version >= 5) check(part.logStartOffset() == 0, "raw Fetch retained start0"); int ordinal = 0;
            for (Record record : FetchResponse.recordsOrFail(part).records()) {
                MemoryRecords expected = ordinary(ordinal); Record er = expected.records().iterator().next();
                String observed = receipt(topic, 0, record.offset(), record.timestamp(), bytes(record.key()), bytes(record.value()), List.of(record.headers()));
                String wanted = receipt(topic, 0, ordinal, TIME + ordinal, bytes(er.key()), bytes(er.value()), List.of(er.headers()));
                check(observed.equals(wanted), "raw Fetch every ID/hash/header/offset/order receipt"); ordinal++;
                HISTORY.add("{\"label\":\"forced-fetch-record\",\"api_version\":" + version + ",\"receipt\":" + observed + "}");
            }
            check(ordinal == count, "raw Fetch all acknowledged/noack records; corruption did not append");
            FetchResponseData limited = (FetchResponseData) call(ApiKeys.FETCH, version, fetch(topic, 0, 1), "forced-fetch-v" + version + "-oversized-first");
            var limitedPart = limited.responses().get(0).partitions().get(0); int limitedRecords = 0;
            for (Record ignored : FetchResponse.recordsOrFail(limitedPart).records()) limitedRecords++;
            check(limitedRecords == 1 && limitedPart.records().sizeInBytes() > 1, "actual whole first batch exceeds requested max1");
        }
        for (short version = 1; version <= 3; version++) for (long timestamp : new long[]{-2, -1, TIME + 5, TIME + count}) {
            ListOffsetsRequestData data = new ListOffsetsRequestData().setReplicaId(-1).setIsolationLevel((byte) 0);
            data.topics().add(new ListOffsetsRequestData.ListOffsetsTopic().setName(topic).setPartitions(new ArrayList<>(List.of(
                new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(timestamp)))));
            ListOffsetsResponseData response = (ListOffsetsResponseData) call(ApiKeys.LIST_OFFSETS, version, data, "forced-list-offsets-v" + version + "-timestamp" + timestamp);
            var part = response.topics().get(0).partitions().get(0); long expected = timestamp == -2 ? 0 : timestamp == -1 ? count : timestamp == TIME + 5 ? 5 : -1;
            check(part.errorCode() == 0 && part.offset() == expected, "raw ListOffsets earliest/latest/query/no-match");
        }
    }
    private static void checkpoint() throws Exception {
        try (Admin admin = admin()) {
            Properties p = properties(); p.remove("default.api.timeout.ms"); p.setProperty("enable.idempotence", "false");
            p.setProperty("acks", "1"); p.setProperty("retries", "0"); p.setProperty("max.in.flight.requests.per.connection", "1");
            p.setProperty("compression.type", "none"); p.setProperty("buffer.memory", "1048576");
            p.setProperty("max.block.ms", "5000"); p.setProperty("delivery.timeout.ms", "15000");
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer())) {
                for (String ack : List.of("1", "-1", "0")) {
                    String topic = prefix + "-acks" + ack; int partitions = ack.equals("1") ? 2 : 1;
                    check(describe(admin, topic, partitions).equals(Uuid.fromString(Files.readString(state.resolve(topic + ".uuid")))), "restart topic UUID retained");
                    for (int partition = 0; partition < partitions; partition++) {
                        String rid = id(topic, partition, COUNT); long timestamp = TIME + partition * 100 + COUNT;
                        RecordMetadata result = producer.send(new ProducerRecord<>(topic, partition, timestamp, key(rid, COUNT), value(rid, COUNT), headers(rid))).get(15, TimeUnit.SECONDS);
                        check(result.offset() == COUNT && result.partition() == partition, "restart actual Producer recovered SDK durable/noack offset");
                        HISTORY.add("{\"label\":\"restart-Producer-checkpoint\",\"receipt\":" + receipt(topic, partition, COUNT, timestamp, key(rid, COUNT), value(rid, COUNT), headers(rid)) + "}");
                    }
                    Files.writeString(state.resolve(topic + ".checkpoint"), "offset12 after process restart\n");
                }
            }
            String topic = prefix + "-raw"; Uuid id = describe(admin, topic, 1);
            check(id.equals(Uuid.fromString(Files.readString(state.resolve(topic + ".uuid")))), "restart raw UUID retained");
            ProduceResponseData response = (ProduceResponseData) call(ApiKeys.PRODUCE, (short) 13,
                produce(topic, id, (short) 1, ordinary(RAW_COUNT)), "restart-forced-Produce13-checkpoint");
            var part = response.responses().iterator().next().partitionResponses().get(0);
            check(part.errorCode() == 0 && part.baseOffset() == RAW_COUNT, "restart raw acknowledged/noack/corruption-safe offset33 retained");
            Files.writeString(state.resolve(topic + ".checkpoint"), "offset33 after process restart\n");
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("release port append|restart-append|append-all|fetch|restart state-directory");
        release = args[0]; port = Integer.parseInt(args[1]); String phase = args[2]; state = Path.of(args[3]); Files.createDirectories(state);
        prefix = "ordinary-" + release.replace('.', '-'); boolean passed = false;
        expectReadApis = phase.equals("append-all") || phase.equals("fetch") || phase.equals("restart");
        try {
            assertProfile((ApiVersionsResponseData) call(ApiKeys.API_VERSIONS, (short) 4,
                new ApiVersionsRequestData().setClientSoftwareName("ordinary-peer").setClientSoftwareVersion(release), "actual-advertised-profile"));
            if (phase.equals("append") || phase.equals("append-all")) { actualAppend(); rawAppend(); }
            else if (phase.equals("restart-append")) checkpoint();
            else if (phase.equals("fetch") || phase.equals("restart")) { actualFetch(); rawFetch(); }
            else throw new IllegalArgumentException("unknown phase"); passed = true;
            System.out.println("{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"assertions\":" + assertions + ",\"status\":\"pass\"}");
        } finally {
            Files.writeString(state.resolve(release + "-" + phase + "-history.json"), "{\"release\":" + quote(release) + ",\"phase\":" + quote(phase)
                + ",\"passed\":" + passed + ",\"assertions\":" + assertions + ",\"ordinary_configuration\":\"idempotence=false; transactional.id absent; retries0; max.inflight1; compressionnone; manualConsumer/auto.commit=false/no group join\",\"history\":[\n" + String.join(",\n", HISTORY) + "\n]}\n");
        }
    }
}
