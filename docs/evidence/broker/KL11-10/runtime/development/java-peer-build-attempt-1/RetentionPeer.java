/* Actual pinned Apache Admin/Producer/manual Consumer and forced-version peers. */
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class RetentionPeer {
    private static final long[] TIMES = {1000, 1007, 1003, 1007, 1010, 1011, 1012, 1013};
    private static final List<String> HISTORY = new ArrayList<>();
    private static String release;
    private static String topic;
    private static int port;
    private static int correlation = 300;
    private static int checks;
    private RetentionPeer() { }
    private static void check(boolean condition, String label) { checks++; if (!condition) throw new AssertionError(label); }
    private static String quote(String value) { return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""; }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String hex(byte[] value) { return value == null ? null : HexFormat.of().formatHex(value); }
    private static byte[] key(int index) { return index % 3 == 0 ? null : utf8("key:" + topic + ":" + index); }
    private static byte[] value(int index) { return index % 3 == 0 ? null : index % 3 == 1 ? new byte[0] : utf8("value:" + topic + ":" + index); }
    private static Header[] headers(int index) { return new Header[]{new RecordHeader("receipt", utf8(topic + ":" + index)), new RecordHeader("dup", utf8("a")), new RecordHeader("dup", null)}; }
    private static String receipt(long offset, long timestamp, byte[] key, byte[] value, Iterable<Header> headers) throws Exception {
        List<String> fields = new ArrayList<>();
        for (Header header : headers) fields.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":" + quote(hex(header.value())) + "}");
        String json = "{\"topic\":" + quote(topic) + ",\"partition\":0,\"offset\":" + offset + ",\"timestamp\":" + timestamp + ",\"key_hex\":" + quote(hex(key))
            + ",\"value_hex\":" + quote(hex(value)) + ",\"headers\":[" + String.join(",", fields) + "]}";
        return "{\"sha256\":" + quote(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(utf8(json)))) + ",\"record\":" + json + "}";
    }
    private static Properties properties() {
        Properties p = new Properties(); p.setProperty("bootstrap.servers", "127.0.0.1:" + port); p.setProperty("client.id", "retention-java-" + release);
        p.setProperty("request.timeout.ms", "5000"); p.setProperty("default.api.timeout.ms", "15000"); p.setProperty("retry.backoff.ms", "50"); return p;
    }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache(); ByteBuffer out = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(out), cache, version); check(!out.hasRemaining(), "Apache serializer full write"); return out.array();
    }
    private static Message call(ApiKeys key, short version, Message body, String label) throws Exception {
        int cid = correlation++; RequestHeader header = new RequestHeader(key, version, "retention-oracle", cid);
        byte[] head = encode(header.data(), header.headerVersion()); byte[] data = encode(body, version);
        byte[] request = Arrays.copyOf(head, head.length + data.length); System.arraycopy(data, 0, request, head.length, data.length); byte[] response;
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000); DataOutputStream output = new DataOutputStream(socket.getOutputStream()); output.writeInt(request.length); output.write(request); output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream()); int length = input.readInt(); check(length >= 4 && length <= 65536, "bounded response frame");
            response = input.readNBytes(length); check(response.length == length, "complete response frame");
        }
        ByteBuffer buffer = ByteBuffer.wrap(response); short hv = key.responseHeaderVersion(version);
        check(ResponseHeader.parse(buffer, hv).correlationId() == cid, "exact response correlation");
        Message parsed = AbstractResponse.parseResponse(key, new ByteBufferAccessor(buffer), version).data(); check(!buffer.hasRemaining(), "full Apache response consumption");
        HISTORY.add("{\"label\":" + quote(label) + ",\"api_key\":" + key.id + ",\"api_version\":" + version + ",\"correlation_id\":" + cid
            + ",\"request_header_version\":" + header.headerVersion() + ",\"response_header_version\":" + hv + ",\"request_hex\":" + quote(hex(request)) + ",\"response_hex\":" + quote(hex(response)) + "}");
        return parsed;
    }
    private static void profiles() throws Exception {
        Map<Integer, String> expected = Map.of(0, "3:13", 1, "4:6", 2, "1:3", 3, "0:13", 18, "0:4", 19, "2:4", 20, "1:6", 21, "0:2");
        for (short version = 0; version <= 4; version++) {
            ApiVersionsRequestData request = new ApiVersionsRequestData();
            if (version >= 3) request.setClientSoftwareName("retention-peer").setClientSoftwareVersion(release);
            ApiVersionsResponseData response = (ApiVersionsResponseData) call(ApiKeys.API_VERSIONS, version, request, "retention-profile");
            Map<Integer, String> actual = new HashMap<>();
            for (var entry : response.apiKeys()) check(actual.put((int) entry.apiKey(), entry.minVersion() + ":" + entry.maxVersion()) == null, "unique profile key");
            check(response.errorCode() == 0 && response.throttleTimeMs() == 0 && actual.equals(expected), "actual exact eight-entry profile");
        }
    }
    private static Uuid identity(Admin admin, Path state, boolean restart) throws Exception {
        var description = admin.describeTopics(TopicCollection.ofTopicNames(List.of(topic))).allTopicNames().get(15, TimeUnit.SECONDS).get(topic);
        check(description != null && description.partitions().size() == 2 && !description.topicId().equals(Uuid.ZERO_UUID), "actual topic identity/partitions");
        Path file = state.resolve(topic + ".uuid");
        if (restart) check(description.topicId().equals(Uuid.fromString(Files.readString(file))), "UUID retained through durable deletion/restart");
        else Files.writeString(file, description.topicId().toString());
        HISTORY.add("{\"label\":\"topic-identity\",\"topic\":" + quote(topic) + ",\"uuid\":" + quote(description.topicId().toString()) + "}"); return description.topicId();
    }
    private static void produceInitial(String name, Uuid id) throws Exception {
        SimpleRecord[] records = new SimpleRecord[5];
        for (int index = 0; index < records.length; index++) records[index] = new SimpleRecord(TIMES[index], key(index), value(index), headers(index));
        MemoryRecords bytes = MemoryRecords.withRecords(Compression.NONE, records);
        ProduceRequestData request = new ProduceRequestData().setTransactionalId(null).setAcks((short) 1).setTimeoutMs(5000);
        request.topicData().add(new ProduceRequestData.TopicProduceData().setName(name).setTopicId(id).setPartitionData(new ArrayList<>(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(bytes)))));
        var response = (ProduceResponseData) call(ApiKeys.PRODUCE, (short) 3, request, "Apache-built-five-record-containing-batch");
        var result = response.responses().iterator().next().partitionResponses().get(0); check(result.errorCode() == 0 && result.baseOffset() == 0, "ordinary batch fsync acknowledgment");
    }
    private static void actualAppend(String name, int index) throws Exception {
        Properties p = properties(); p.remove("default.api.timeout.ms"); p.setProperty("enable.idempotence", "false"); p.setProperty("acks", "1");
        p.setProperty("retries", "0"); p.setProperty("compression.type", "none"); p.setProperty("max.in.flight.requests.per.connection", "1");
        p.setProperty("buffer.memory", "1048576"); p.setProperty("max.block.ms", "5000"); p.setProperty("delivery.timeout.ms", "15000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer())) {
            var acknowledged = producer.send(new ProducerRecord<>(name, 0, TIMES[index], key(index), value(index), List.of(headers(index)))).get(15, TimeUnit.SECONDS);
            check(acknowledged.offset() == index, "actual Producer preserves exclusive append end after deletion");
            HISTORY.add("{\"label\":\"actual-producer\",\"receipt\":" + receipt(index, TIMES[index], key(index), value(index), List.of(headers(index))) + "}");
        }
    }
    private static void rawDelete(short version, long offset, long expected, short error) throws Exception {
        DeleteRecordsRequestData request = new DeleteRecordsRequestData().setTimeoutMs(5000);
        request.topics().add(new DeleteRecordsRequestData.DeleteRecordsTopic().setName(topic).setPartitions(new ArrayList<>(List.of(new DeleteRecordsRequestData.DeleteRecordsPartition().setPartitionIndex(0).setOffset(offset)))));
        var response = (DeleteRecordsResponseData) call(ApiKeys.DELETE_RECORDS, version, request, "forced-delete-offset" + offset);
        check(response.throttleTimeMs() == 0 && response.topics().size() == 1, "delete response envelope");
        var result = response.topics().iterator().next().partitions().iterator().next();
        check(result.partitionIndex() == 0 && result.errorCode() == error && result.lowWatermark() == expected, "exact delete low watermark/error");
    }
    private static void readWire(long floor, long end) throws Exception {
        for (short version = 4; version <= 6; version++) for (long offset : new long[]{floor - 1, floor}) {
            FetchRequestData request = new FetchRequestData().setReplicaId(-1).setMaxWaitMs(0).setMinBytes(0).setMaxBytes(65536).setIsolationLevel((byte) 0);
            request.topics().add(new FetchRequestData.FetchTopic().setTopic(topic).setPartitions(new ArrayList<>(List.of(new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(offset).setPartitionMaxBytes(65536)))));
            var response = (FetchResponseData) call(ApiKeys.FETCH, version, request, "floor-aware-fetch-offset" + offset);
            var part = response.responses().get(0).partitions().get(0);
            check(part.errorCode() == (offset < floor ? 1 : 0), "below-floor Fetch is offset-out-of-range");
            if (offset >= floor) {
                check(part.highWatermark() == end && part.lastStableOffset() == end, "ordinary confirmed HW/LSO");
                if (version >= 5) check(part.logStartOffset() == floor, "actual Fetch logical start");
                int received = 0; long first = -1;
                for (var batch : FetchResponse.recordsOrFail(part).batches()) { batch.ensureValid(); for (var record : batch) { if (first < 0) first = record.offset(); received++; } }
                check(first == 0 && received == end, "whole containing batch kept; raw reader observes lower offsets");
            }
        }
    }
    private static void actualRead(long floor, int end) throws Exception {
        Properties p = properties(); p.setProperty("enable.auto.commit", "false"); p.setProperty("allow.auto.create.topics", "false"); p.setProperty("auto.offset.reset", "none");
        p.setProperty("fetch.min.bytes", "1"); p.setProperty("fetch.max.wait.ms", "20"); p.setProperty("max.poll.records", "100");
        p.setProperty("fetch.max.bytes", "65536"); p.setProperty("max.partition.fetch.bytes", "4096");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition partition = new TopicPartition(topic, 0); consumer.assign(List.of(partition)); consumer.seek(partition, floor);
            int expected = (int) floor; long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (expected < end && System.nanoTime() < deadline) for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                check(record.offset() == expected, "actual Consumer filters earlier records in containing batch");
                String observed = receipt(record.offset(), record.timestamp(), record.key(), record.value(), record.headers());
                check(observed.equals(receipt(expected, TIMES[expected], key(expected), value(expected), List.of(headers(expected)))), "retained exact record hash/null/empty/time/ordered duplicate headers");
                HISTORY.add("{\"label\":\"retained-consumer-record\",\"receipt\":" + observed + "}"); expected++;
            }
            check(expected == end, "all retained suffix records consumed");
            check(consumer.beginningOffsets(List.of(partition), Duration.ofSeconds(10)).get(partition) == floor, "actual earliest logical offset");
            check(consumer.endOffsets(List.of(partition), Duration.ofSeconds(10)).get(partition) == end, "actual latest durable offset");
            var time = consumer.offsetsForTimes(Map.of(partition, 1004L), Duration.ofSeconds(10)).get(partition);
            check(time != null && time.offset() == 3 && time.timestamp() == 1007, "timestamp query skips retired equal/larger times");
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("release port seed|restart state-directory topic");
        release = args[0]; port = Integer.parseInt(args[1]); String phase = args[2]; Path state = Path.of(args[3]); topic = args[4];
        check(phase.equals("seed") || phase.equals("restart"), "bounded phase"); check(topic.matches("[A-Za-z0-9._-]{1,64}"), "bounded topic"); Files.createDirectories(state);
        boolean passed = false;
        try {
            profiles();
            try (Admin admin = Admin.create(properties())) {
                if (phase.equals("seed")) {
                    admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get(15, TimeUnit.SECONDS);
                    produceInitial(topic, identity(admin, state, false)); actualAppend(topic, 5); actualAppend(topic, 6);
                    for (short version = 0; version <= 2; version++) { rawDelete(version, -2, -1, (short) 1); rawDelete(version, 8, -1, (short) 1); rawDelete(version, 2, 2, (short) 0); }
                    var deleted = admin.deleteRecords(Map.of(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(3))).all().get(15, TimeUnit.SECONDS);
                    check(deleted.get(new TopicPartition(topic, 0)).lowWatermark() == 3, "actual Admin DeleteRecords result");
                    HISTORY.add("{\"label\":\"actual-admin-delete\",\"offset\":3,\"low_watermark\":3}");
                    for (short version = 0; version <= 2; version++) rawDelete(version, 1, 3, (short) 0);
                    readWire(3, 7); actualRead(3, 7);
                } else {
                    identity(admin, state, true); readWire(3, 7); actualRead(3, 7);
                    actualAppend(topic, 7); actualRead(3, 8);
                }
            }
            passed = true; System.out.println("{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"assertions\":" + checks + ",\"passed\":true}");
        } finally {
            Files.writeString(state.resolve(release + "-" + phase + ".json"), "{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"passed\":" + passed
                + ",\"assertions\":" + checks + ",\"ordinary_profile\":\"RF1 local fsync; no idempotence/transaction/group; manual assignment; whole containing batch\",\"history\":[" + String.join(",", HISTORY) + "]}\n");
        }
    }
}
