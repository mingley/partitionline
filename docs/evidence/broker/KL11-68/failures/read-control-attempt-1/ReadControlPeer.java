/* Actual Apache Fetch/ListOffsets goldens executed against a bounded ordinary log. */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.*;

public final class ReadControlPeer {
    private static final int MAX_FRAME = 128 * 1024;
    private static final List<String> HISTORY = new ArrayList<>();
    private static int checks;
    private static int correlation = 900;
    private static int port;
    private static String topic;
    private static String release;
    private static Path fixtures;
    private ReadControlPeer() { }
    private static void check(boolean condition, String label) { checks++; if (!condition) throw new AssertionError(label); }
    private static String quote(String text) {
        return text == null ? "null" : "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\"";
    }
    private static String hex(byte[] data) { return HexFormat.of().formatHex(data); }
    private static byte[] readBounded(Path path, int maximum) throws Exception {
        long size = Files.size(path); check(size >= 0 && size <= maximum, "bounded fixture file");
        byte[] data = Files.readAllBytes(path); check(data.length == size, "stable fixture read"); return data;
    }
    private static byte[] encode(Message data, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache(); int length = data.size(cache, version);
        check(length >= 0 && length <= MAX_FRAME, "bounded serialized message"); ByteBuffer buffer = ByteBuffer.allocate(length);
        data.write(new ByteBufferAccessor(buffer), cache, version); check(!buffer.hasRemaining(), "serializer full write"); return buffer.array();
    }
    private static byte[] concat(byte[] head, byte[] body) {
        check(head.length + body.length <= MAX_FRAME, "bounded serialized frame");
        byte[] result = Arrays.copyOf(head, head.length + body.length); System.arraycopy(body, 0, result, head.length, body.length); return result;
    }
    private static byte[] requestFrame(ApiKeys key, short version, ApiMessage data, int cid) {
        RequestHeader header = new RequestHeader(key, version, "ordinary-read-control-" + release, cid);
        return concat(encode(header.data(), header.headerVersion()), encode(data, version));
    }
    private static byte[] responseFrame(ApiKeys key, short version, ApiMessage data, int cid) {
        short headerVersion = key.responseHeaderVersion(version);
        return concat(encode(new ResponseHeader(cid, headerVersion).data(), headerVersion), encode(data, version));
    }
    private static ApiMessage parseRequest(byte[] raw, ApiKeys key, short version) {
        ByteBuffer buffer = ByteBuffer.wrap(raw); RequestHeader header = RequestHeader.parse(buffer);
        check(header.apiKey() == key && header.apiVersion() == version, "fixture request API identity");
        ApiMessage data = AbstractRequest.parseRequest(key, version, new ByteBufferAccessor(buffer)).request.data();
        check(!buffer.hasRemaining(), "fixture full request parse"); return data;
    }
    private static ApiMessage parseResponse(byte[] raw, ApiKeys key, short version, int cid) {
        ByteBuffer buffer = ByteBuffer.wrap(raw); check(ResponseHeader.parse(buffer, key.responseHeaderVersion(version)).correlationId() == cid, "response correlation");
        ApiMessage data = AbstractResponse.parseResponse(key, new ByteBufferAccessor(buffer), version).data();
        check(!buffer.hasRemaining(), "response full parse");
        if (data instanceof FetchResponseData fetch) for (var responseTopic : fetch.responses()) for (var part : responseTopic.partitions()) {
            if (part.errorCode() == 0) for (var batch : FetchResponse.recordsOrFail(part).batches()) {
                batch.ensureValid(); check(batch.magic() == 2 && !batch.isTransactional() && !batch.isControlBatch(), "valid ordinary returned batch");
                for (var record : batch) record.ensureValid();
            }
        }
        return data;
    }
    private static ApiMessage parseGoldenResponse(byte[] raw, ApiKeys key, short version) {
        return parseResponse(raw, key, version, 7);
    }
    private static void rename(ApiMessage data) {
        if (data instanceof FetchRequestData fetch) for (var item : fetch.topics()) { if (item.topic().equals("alpha")) item.setTopic(topic); }
        else if (data instanceof ListOffsetsRequestData offsets) for (var item : offsets.topics()) { if (item.name().equals("alpha")) item.setName(topic); }
        else if (data instanceof FetchResponseData fetch) for (var item : fetch.responses()) { if (item.topic().equals("alpha")) item.setTopic(topic); }
        else if (data instanceof ListOffsetsResponseData offsets) for (var item : offsets.topics()) { if (item.name().equals("alpha")) item.setName(topic); }
        else throw new IllegalArgumentException("unsupported fixture data");
    }
    private static byte[] exchange(ApiKeys key, short version, int cid, byte[] frame, String label, boolean close) throws Exception {
        byte[] response = null; String failure = null; boolean observedClose = false;
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5000); DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(frame.length); output.write(frame); output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            if (close) { observedClose = input.read() == -1; check(observedClose, "structural failure clean EOF"); return null; }
            int size = input.readInt(); check(size >= 4 && size <= MAX_FRAME, "bounded live response");
            response = input.readNBytes(size); check(response.length == size, "complete live response"); parseResponse(response, key, version, cid); return response;
        } catch (Exception | AssertionError error) { failure = error.toString(); throw error; }
        finally {
            HISTORY.add("{\"name\":" + quote(label) + ",\"api_key\":" + key.id + ",\"api_version\":" + version + ",\"correlation_id\":" + cid
                + ",\"request_header_version\":" + key.requestHeaderVersion(version) + ",\"response_header_version\":" + key.responseHeaderVersion(version)
                + ",\"request_hex\":" + quote(hex(frame)) + ",\"response_hex\":" + (response == null ? "null" : quote(hex(response)))
                + ",\"observed_clean_eof\":" + observedClose + ",\"failure\":" + quote(failure) + "}");
        }
    }
    private static void profile() throws Exception {
        Map<Integer, String> expected = Map.of(0, "3:13", 1, "4:6", 2, "1:3", 3, "0:13", 18, "0:4", 19, "2:4", 20, "1:6");
        for (short version = 0; version <= 4; version++) {
            ApiVersionsRequestData data = new ApiVersionsRequestData(); if (version >= 3) data.setClientSoftwareName("read-control-peer").setClientSoftwareVersion(release);
            int cid = correlation++; byte[] frame = requestFrame(ApiKeys.API_VERSIONS, version, data, cid);
            var parsed = (ApiVersionsResponseData) parseResponse(exchange(ApiKeys.API_VERSIONS, version, cid, frame, "seven-entry-profile-v" + version, false), ApiKeys.API_VERSIONS, version, cid);
            Map<Integer, String> actual = new HashMap<>(); for (var entry : parsed.apiKeys()) check(actual.put((int) entry.apiKey(), entry.minVersion() + ":" + entry.maxVersion()) == null, "unique advertised APIs");
            check(parsed.errorCode() == 0 && actual.equals(expected), "actual seven-entry ordinary data profile");
        }
    }
    private static Admin admin() {
        Properties p = new Properties(); p.setProperty("bootstrap.servers", "127.0.0.1:" + port); p.setProperty("client.id", "read-control-" + release);
        p.setProperty("request.timeout.ms", "5000"); p.setProperty("default.api.timeout.ms", "15000"); p.setProperty("retries", "0"); return Admin.create(p);
    }
    private static Uuid describe(Admin admin) throws Exception {
        var described = admin.describeTopics(TopicCollection.ofTopicNames(List.of(topic))).allTopicNames().get(15, TimeUnit.SECONDS).get(topic);
        check(described != null && described.partitions().size() == 2 && !described.topicId().equals(Uuid.ZERO_UUID), "actual allocated topic UUID/partitions"); return described.topicId();
    }
    private static void seed(Path state) throws Exception {
        Uuid uuid; try (Admin admin = admin()) { admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get(15, TimeUnit.SECONDS); uuid = describe(admin); }
        Files.writeString(state.resolve(topic + ".uuid"), uuid.toString());
        for (int base : new int[]{0, 3}) {
            MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(readBounded(fixtures.resolve("log-batch-" + base + ".bin"), 4096)));
            ProduceRequestData request = new ProduceRequestData().setAcks((short) 1).setTransactionalId(null).setTimeoutMs(5000);
            request.topicData().add(new ProduceRequestData.TopicProduceData().setName(topic).setTopicId(uuid).setPartitionData(new ArrayList<>(List.of(
                new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records)))));
            int cid = correlation++; short version = 13; byte[] frame = requestFrame(ApiKeys.PRODUCE, version, request, cid);
            var parsed = (ProduceResponseData) parseResponse(exchange(ApiKeys.PRODUCE, version, cid, frame, "seed-batch-offset" + base, false), ApiKeys.PRODUCE, version, cid);
            var part = parsed.responses().iterator().next().partitionResponses().get(0); check(part.errorCode() == 0 && part.baseOffset() == base, "actual seed durable offset receipt");
        }
    }
    private static void corpus() throws Exception {
        String index = new String(readBounded(fixtures.resolve("cases.tsv"), 64 * 1024), java.nio.charset.StandardCharsets.UTF_8); int count = 0;
        for (String row : index.split("\n")) {
            if (row.isEmpty()) continue; String[] fields = row.split("\t", -1); check(fields.length == 5 && fields[3].equals("log"), "exact frozen case index shape");
            String name = fields[0]; ApiKeys key = ApiKeys.forId(Integer.parseInt(fields[1])); short version = Short.parseShort(fields[2]);
            check(key == ApiKeys.FETCH || key == ApiKeys.LIST_OFFSETS, "frozen read API identity"); int cid = correlation++; byte[] frame;
            boolean close = fields[4].equals("structural_reject");
            if (close) {
                String canonical = key == ApiKeys.FETCH ? "fetch-v" + version + "-offset0" : "list-offsets-v" + version + "-earliest";
                ApiMessage data = parseRequest(readBounded(fixtures.resolve(canonical + ".request.bin"), MAX_FRAME), key, version); rename(data);
                byte[] complete = requestFrame(key, version, data, cid); boolean trailing = name.endsWith("-trailing-zero");
                check(trailing || name.endsWith("-truncated"), "explicit reviewed structural mutation"); frame = Arrays.copyOf(complete, complete.length + (trailing ? 1 : -1));
                exchange(key, version, cid, frame, name, true);
            } else {
                check(fields[4].equals("response"), "explicit positive read outcome");
                ApiMessage data = parseRequest(readBounded(fixtures.resolve(name + ".request.bin"), MAX_FRAME), key, version); rename(data); frame = requestFrame(key, version, data, cid);
                ApiMessage expected = parseGoldenResponse(readBounded(fixtures.resolve(name + ".response.bin"), MAX_FRAME), key, version); rename(expected);
                byte[] wanted = responseFrame(key, version, expected, cid); byte[] observed = exchange(key, version, cid, frame, name, false);
                check(Arrays.equals(wanted, observed), "actual full response equals independently pinned Apache golden: " + name);
            }
            count++;
        }
        check(count == 122, "all 122 frozen read cases executed");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("release port seed|restart state-directory fixture-directory");
        release = args[0]; port = Integer.parseInt(args[1]); String phase = args[2]; Path state = Path.of(args[3]); fixtures = Path.of(args[4]);
        Files.createDirectories(state); topic = "ordinary-control-" + release.replace('.', '-'); boolean passed = false;
        try {
            profile(); if (phase.equals("seed")) seed(state); else {
                check(phase.equals("restart"), "explicit phase"); try (Admin admin = admin()) {
                    check(describe(admin).equals(Uuid.fromString(Files.readString(state.resolve(topic + ".uuid")))), "restart UUID retained");
                }
            }
            corpus(); passed = true; System.out.println("{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"cases\":122,\"assertions\":" + checks + ",\"passed\":true}");
        } finally {
            Files.writeString(state.resolve(release + "-read-control-" + phase + ".json"), "{\"release\":" + quote(release) + ",\"phase\":" + quote(phase)
                + ",\"passed\":" + passed + ",\"assertions\":" + checks + ",\"topic\":" + quote(topic) + ",\"fixture_mutation\":\"Only topic alpha is renamed and request/response correlation is changed using official serializers; structural cases truncate/add zero after full canonical serialization.\",\"history\":[\n"
                + String.join(",\n", HISTORY) + "\n]}\n");
        }
    }
}
