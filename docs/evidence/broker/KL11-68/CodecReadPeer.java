/* Actual Apache readers verify persisted normalized codec histories via Fetch4–6. */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

public final class CodecReadPeer {
    private static final long TIME = 1_700_000_000_000L;
    private static final List<String> HISTORY = new ArrayList<>();
    private static int checks;
    private static int correlation = 1400;
    private CodecReadPeer() { }
    private static void check(boolean condition, String label) { checks++; if (!condition) throw new AssertionError(label); }
    private static String quote(String text) {
        return text == null ? "null" : "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    private static byte[] utf8(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    private static byte[] bytes(ByteBuffer buffer) {
        if (buffer == null) return null;
        ByteBuffer copy = buffer.duplicate(); byte[] data = new byte[copy.remaining()]; copy.get(data); return data;
    }
    private static String hex(byte[] data) { return data == null ? null : HexFormat.of().formatHex(data); }
    private static String receipt(String topic, long offset, long timestamp, byte[] key, byte[] value, Header[] headers) throws Exception {
        List<String> list = new ArrayList<>();
        for (Header header : headers) list.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":" + quote(hex(header.value())) + "}");
        String content = "{\"topic\":" + quote(topic) + ",\"partition\":0,\"offset\":" + offset + ",\"timestamp\":" + timestamp
            + ",\"key_hex\":" + quote(hex(key)) + ",\"value_hex\":" + quote(hex(value)) + ",\"headers\":[" + String.join(",", list) + "]}";
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(utf8(content)));
        return "{\"sha256\":" + quote(hash) + ",\"record\":" + content + "}";
    }
    private static byte[] encode(Message data, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache(); int size = data.size(cache, version);
        check(size >= 0 && size <= 128 * 1024, "bounded request message"); ByteBuffer buffer = ByteBuffer.allocate(size);
        data.write(new ByteBufferAccessor(buffer), cache, version); check(!buffer.hasRemaining(), "serializer full write"); return buffer.array();
    }
    private static FetchResponseData fetch(String release, int port, String topic, short version, byte isolation) throws Exception {
        FetchRequestData data = new FetchRequestData().setReplicaId(-1).setMaxWaitMs(0).setMinBytes(0).setMaxBytes(65536).setIsolationLevel(isolation);
        data.topics().add(new FetchRequestData.FetchTopic().setTopic(topic).setPartitions(new ArrayList<>(List.of(
            new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(0).setPartitionMaxBytes(65536)))));
        int cid = correlation++; RequestHeader header = new RequestHeader(ApiKeys.FETCH, version, "codec-read-" + release, cid);
        byte[] head = encode(header.data(), header.headerVersion()); byte[] body = encode(data, version);
        byte[] request = Arrays.copyOf(head, head.length + body.length); System.arraycopy(body, 0, request, head.length, body.length);
        byte[] response;
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000); DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(request.length); output.write(request); output.flush(); DataInputStream input = new DataInputStream(socket.getInputStream());
            int size = input.readInt(); check(size >= 4 && size <= 128 * 1024, "bounded response frame");
            response = input.readNBytes(size); check(response.length == size, "complete response read");
        }
        ByteBuffer buffer = ByteBuffer.wrap(response); check(ResponseHeader.parse(buffer, ApiKeys.FETCH.responseHeaderVersion(version)).correlationId() == cid, "response correlation");
        FetchResponseData parsed = FetchResponse.parse(new ByteBufferAccessor(buffer), version).data(); check(!buffer.hasRemaining(), "full response parse");
        HISTORY.add("{\"label\":\"normalized-codec-fetch\",\"api_key\":1,\"api_version\":" + version + ",\"correlation_id\":" + cid
            + ",\"isolation_level\":" + isolation + ",\"request_hex\":" + quote(hex(request)) + ",\"response_hex\":" + quote(hex(response)) + "}");
        return parsed;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("release port state-directory output-json plain-fixture");
        String release = args[0]; int port = Integer.parseInt(args[1]); Path state = Path.of(args[2]); Path output = Path.of(args[3]);
        String prefix = "ordinary-" + release.replace('.', '-'); Path fixture = Path.of(args[4]); boolean passed = false;
        check(Files.size(fixture) <= 4096, "bounded independent plain codec fixture");
        MemoryRecords plain = MemoryRecords.readableRecords(ByteBuffer.wrap(Files.readAllBytes(fixture))); List<Record> rich = new ArrayList<>();
        for (var batch : plain.batches()) { batch.ensureValid(); for (var record : batch) rich.add(record); }
        check(rich.size() == 3, "independent codec source exactly three rich records");
        Properties properties = new Properties(); properties.setProperty("bootstrap.servers", "127.0.0.1:" + port);
        properties.setProperty("client.id", "codec-read-" + release); properties.setProperty("request.timeout.ms", "5000");
        properties.setProperty("default.api.timeout.ms", "15000"); properties.setProperty("retries", "0");
        try (Admin admin = Admin.create(properties)) {
            for (boolean enabled : new boolean[]{false, true}) {
                String topic = prefix + (enabled ? "-codec-enabled" : "-codec-default"); int count = enabled ? 121 : 1;
                var description = admin.describeTopics(TopicCollection.ofTopicNames(List.of(topic))).allTopicNames().get(15, TimeUnit.SECONDS).get(topic);
                check(description != null && description.partitions().size() == 1
                    && description.topicId().equals(Uuid.fromString(Files.readString(state.resolve(topic + ".uuid")))), "normalized-codec topic UUID retained");
                for (short version = 4; version <= 6; version++) for (byte isolation : new byte[]{0, 1}) {
                    FetchResponseData response = fetch(release, port, topic, version, isolation);
                    check(response.responses().size() == 1 && response.responses().get(0).topic().equals(topic)
                        && response.responses().get(0).partitions().size() == 1, "exact normalized topic/partition response");
                    var part = response.responses().get(0).partitions().get(0);
                    check(part.partitionIndex() == 0 && part.errorCode() == 0 && part.highWatermark() == count && part.lastStableOffset() == count, "normalized record HW/LSO");
                    if (version >= 5) check(part.logStartOffset() == 0, "normalized record retained start");
                    check(isolation == 0 ? part.abortedTransactions() == null : part.abortedTransactions() != null && part.abortedTransactions().isEmpty(), "ordinary-only isolation/abort shape");
                    int ordinal = 0;
                    for (var batch : FetchResponse.recordsOrFail(part).batches()) {
                        batch.ensureValid(); check(batch.magic() == 2 && batch.compressionType().id == 0 && !batch.isTransactional() && !batch.isControlBatch(), "valid persisted normalized ordinary batch");
                        for (var record : batch) {
                            check(ordinal < count && record.offset() == ordinal, "normalized exact offset/order/no duplicates");
                            String wanted;
                            if (enabled && ordinal < 120) {
                                Record expected = rich.get(ordinal % 3);
                                wanted = receipt(topic, ordinal, expected.timestamp(), bytes(expected.key()), bytes(expected.value()), expected.headers());
                            } else {
                                wanted = receipt(topic, ordinal, TIME, utf8("raw-key:0"), utf8("raw-value:0"),
                                    new Header[]{new RecordHeader("receipt", utf8(prefix + ":raw:0"))});
                            }
                            String observed = receipt(topic, record.offset(), record.timestamp(), bytes(record.key()), bytes(record.value()), record.headers());
                            check(wanted.equals(observed), "normalized exact hash/key/value/null/empty/binary/timestamp/ordered duplicate headers");
                            HISTORY.add("{\"label\":\"normalized-codec-record\",\"api_version\":" + version + ",\"isolation_level\":" + isolation + ",\"receipt\":" + observed + "}"); ordinal++;
                        }
                    }
                    check(ordinal == count, "all and only persisted codec/marker records returned");
                }
            }
            passed = true; System.out.println("{\"release\":" + quote(release) + ",\"exchanges\":12,\"record_comparisons\":732,\"assertions\":" + checks + ",\"passed\":true}");
        } finally {
            Files.writeString(output, "{\"release\":" + quote(release) + ",\"passed\":" + passed + ",\"assertions\":" + checks
                + ",\"scope\":\"Actual Fetch4–6 read of persisted KL11-67 normalized rich records from accepted c34 Produce runs; both ordinary isolation levels, exact marker offsets and topic UUIDs.\",\"history\":[\n"
                + String.join(",\n", HISTORY) + "\n]}\n");
        }
    }
}
