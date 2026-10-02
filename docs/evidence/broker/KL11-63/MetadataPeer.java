/* Independent official Apache Java client and forced-version TCP peer. */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.requests.*;

public final class MetadataPeer {
    private static int port;
    private static String release;
    private static String prefix;
    private static int correlation = 100;
    private static int assertions;
    private static final List<String> HISTORY = new ArrayList<>();
    private MetadataPeer() { }
    private static String quote(String value) {
        if (value == null) return "null";
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }
    private static void check(boolean condition, String label) {
        assertions++;
        if (!condition) throw new AssertionError(label);
    }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer bytes = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(bytes), cache, version);
        check(!bytes.hasRemaining(), "serializer full write");
        return bytes.array();
    }
    private static ApiMessage call(ApiKeys key, short version, ApiMessage data, String label) throws Exception {
        int cid = correlation++;
        RequestHeader header = new RequestHeader(key, version, "metadata-live-oracle", cid);
        byte[] head = encode(header.data(), header.headerVersion());
        byte[] body = encode(data, version);
        byte[] request = Arrays.copyOf(head, head.length + body.length);
        System.arraycopy(body, 0, request, head.length, body.length);
        byte[] response;
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(10_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(request.length); out.write(request); out.flush();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int length = in.readInt(); check(length >= 4 && length <= 128 * 1024, "response allocation bound");
            response = in.readNBytes(length); check(response.length == length, "response full read");
        }
        ByteBuffer buffer = ByteBuffer.wrap(response);
        ResponseHeader parsedHeader = ResponseHeader.parse(buffer, key.responseHeaderVersion(version));
        check(parsedHeader.correlationId() == cid, "correlation");
        ApiMessage result = AbstractResponse.parseResponse(key, new ByteBufferAccessor(buffer), version).data();
        check(!buffer.hasRemaining(), "response full consumption");
        HISTORY.add("{\"label\":" + quote(label) + ",\"api_key\":" + key.id + ",\"version\":" + version
            + ",\"request_hex\":" + quote(HexFormat.of().formatHex(request))
            + ",\"response_hex\":" + quote(HexFormat.of().formatHex(response))
            + ",\"apache_parsed_response\":" + quote(result.toString()) + "}");
        return result;
    }
    private static void expectClosed(ApiKeys key, short version, ApiMessage data, String label) throws Exception {
        RequestHeader header = new RequestHeader(key, version, "metadata-live-oracle", correlation++);
        byte[] head = encode(header.data(), header.headerVersion());
        byte[] body = encode(data, version);
        byte[] frame = Arrays.copyOf(head, head.length + body.length);
        System.arraycopy(body, 0, frame, head.length, body.length);
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(10_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(frame.length); out.write(frame); out.flush();
            int first = socket.getInputStream().read();
            check(first == -1, "deliberately rejected request closes connection without malformed response");
            HISTORY.add("{\"label\":" + quote(label) + ",\"api_key\":" + key.id + ",\"version\":" + version
                + ",\"request_hex\":" + quote(HexFormat.of().formatHex(frame)) + ",\"outcome\":\"clean EOF before response\"}");
        }
    }
    private static CreateTopicsRequestData createData(String name, int partitions, short replicas, boolean validate) {
        CreateTopicsRequestData data = new CreateTopicsRequestData().setTimeoutMs(30_000).setValidateOnly(validate);
        data.topics().add(new CreateTopicsRequestData.CreatableTopic().setName(name)
            .setNumPartitions(partitions).setReplicationFactor(replicas));
        return data;
    }
    private static void create(short version, String name, int partitions, short replicas, boolean validate) throws Exception {
        CreateTopicsResponseData result = (CreateTopicsResponseData) call(ApiKeys.CREATE_TOPICS, version,
            createData(name, partitions, replicas, validate), "create-" + name);
        check(result.topics().size() == 1 && result.topics().iterator().next().errorCode() == 0, "create success");
    }
    private static MetadataRequestData request(short version, String name, Uuid id) {
        MetadataRequestData data = new MetadataRequestData().setTopics(name == null && id == null
            ? version == 0 ? List.of() : null
            : List.of(new MetadataRequestData.MetadataRequestTopic().setName(name).setTopicId(id == null ? Uuid.ZERO_UUID : id)));
        if (version >= 4) data.setAllowAutoTopicCreation(false);
        return data;
    }
    private static MetadataResponseData metadata(short version, String name, Uuid id, String label) throws Exception {
        return (MetadataResponseData) call(ApiKeys.METADATA, version, request(version, name, id), label);
    }
    private static MetadataResponseData.MetadataResponseTopic only(MetadataResponseData data) {
        check(data.topics().size() == 1, "one metadata topic"); return data.topics().iterator().next();
    }
    private static void delete(short version, String name, Uuid id) throws Exception {
        DeleteTopicsRequestData request = new DeleteTopicsRequestData().setTimeoutMs(30_000);
        if (version < 6) request.setTopicNames(List.of(name));
        else request.setTopics(List.of(new DeleteTopicsRequestData.DeleteTopicState().setName(name).setTopicId(id == null ? Uuid.ZERO_UUID : id)));
        DeleteTopicsResponseData result = (DeleteTopicsResponseData) call(ApiKeys.DELETE_TOPICS, version, request, "delete-v" + version);
        check(result.responses().size() == 1 && result.responses().iterator().next().errorCode() == 0, "delete success");
    }
    private static void forcedVersions() throws Exception {
        for (short version = 0; version <= 4; version++) {
            ApiVersionsRequestData request = new ApiVersionsRequestData();
            if (version >= 3) request.setClientSoftwareName("metadata-live-oracle").setClientSoftwareVersion("1");
            ApiVersionsResponseData result = (ApiVersionsResponseData) call(ApiKeys.API_VERSIONS, version, request, "api-versions-v" + version);
            check(result.errorCode() == 0 && result.apiKeys().size() == 4, "composed API registry");
            for (short[] range : List.of(new short[]{3, 0, 13}, new short[]{18, 0, 4}, new short[]{19, 2, 4}, new short[]{20, 1, 6})) {
                ApiVersionsResponseData.ApiVersion found = result.apiKeys().find(range[0]);
                check(found != null && found.minVersion() == range[1] && found.maxVersion() == range[2], "exact API range");
            }
            check(result.apiKeys().find((short) 0) == null, "Produce unadvertised");
        }
        for (short version : new short[]{5, 6, 7}) {
            expectClosed(ApiKeys.CREATE_TOPICS, version, createData(prefix + "-unsupported", 1, (short) 1, false), "unsupported-create-version");
        }
        for (short version : new short[]{12, 13}) {
            expectClosed(ApiKeys.METADATA, version, request(version, null, Uuid.ZERO_UUID), "reject-neither-identity");
        }
        String base = prefix + "-raw";
        for (short version = 2; version <= 4; version++) create(version, base + version, version == 4 ? -1 : 2, version == 4 ? (short) -1 : 1, false);
        create((short) 3, base + "-validate", 1, (short) 1, true);
        check(only(metadata((short) 13, base + "-validate", null, "validate-no-mutation")).errorCode() == 3, "validate only no mutation");
        Uuid id = only(metadata((short) 13, base + "2", null, "dynamic-id")).topicId();
        check(!id.equals(Uuid.ZERO_UUID), "nonzero dynamic UUID");
        for (short version = 0; version <= 13; version++) {
            MetadataResponseData result = metadata(version, base + "2", null, "metadata-v" + version);
            MetadataResponseData.MetadataResponseTopic topic = only(result);
            check(topic.errorCode() == 0 && topic.partitions().size() == 2, "named metadata");
            check(result.brokers().size() == 1 && result.brokers().iterator().next().port() == port, "dynamic advertised endpoint");
            if (version >= 10) check(topic.topicId().equals(id), "UUID survives version field");
        }
        for (short version : new short[]{10, 11}) {
            MetadataResponseData result = metadata(version, null, id, "pre-v12-id-invalid");
            check(only(result).errorCode() == 42 && result.brokers().isEmpty() && result.controllerId() == -1, "actual semantic error envelope");
        }
        for (short version : new short[]{12, 13}) {
            check(only(metadata(version, null, id, "supported-id-v" + version)).errorCode() == 0, "ID lookup");
            check(only(metadata(version, null, new Uuid(0, 99), "unknown-id-v" + version)).errorCode() == 100, "unknown UUID");
        }
        for (short version = 1; version <= 6; version++) {
            String name = prefix + "-delete" + version;
            create((short) 4, name, 1, (short) 1, false);
            Uuid deleteId = only(metadata((short) 13, name, null, "pre-delete-identity")).topicId();
            delete(version, version == 6 ? null : name, version == 6 ? deleteId : null);
            check(only(metadata((short) 13, name, null, "post-delete-missing")).errorCode() == 3, "delete mutation");
        }
        delete((short) 6, null, id);
        delete((short) 1, base + "3", null);
        delete((short) 1, base + "4", null);
    }
    private static Admin admin() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:" + port);
        props.setProperty("client.id", "metadata-admin-" + release);
        props.setProperty("request.timeout.ms", "5000");
        props.setProperty("default.api.timeout.ms", "15000");
        props.setProperty("retries", "0");
        return Admin.create(props);
    }
    private static Uuid describe(Admin admin, String name) throws Exception {
        TopicDescription topic = admin.describeTopics(TopicCollection.ofTopicNames(List.of(name))).allTopicNames().get(15, TimeUnit.SECONDS).get(name);
        check(topic != null && topic.partitions().size() == 2, "AdminClient describe partitions");
        HISTORY.add("{\"label\":\"actual-AdminClient-describe\",\"name\":" + quote(name) + ",\"uuid\":" + quote(topic.topicId().toString()) + ",\"description\":" + quote(topic.toString()) + "}");
        return topic.topicId();
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("release port create|restart state-directory");
        release = args[0]; port = Integer.parseInt(args[1]); String phase = args[2]; Path state = Path.of(args[3]);
        prefix = "peer-" + release.replace('.', '-'); Files.createDirectories(state);
        String name = prefix + "-persistent";
        try {
            if (phase.equals("create")) {
                forcedVersions();
                try (Admin admin = admin()) {
                    admin.createTopics(List.of(new NewTopic(name, 2, (short) 1))).all().get(15, TimeUnit.SECONDS);
                    admin.createTopics(List.of(new NewTopic(prefix + "-admin-validate", 1, (short) 1)), new CreateTopicsOptions().validateOnly(true)).all().get(15, TimeUnit.SECONDS);
                    check(!admin.listTopics().names().get(15, TimeUnit.SECONDS).contains(prefix + "-admin-validate"), "actual AdminClient validate only");
                    Uuid id = describe(admin, name); check(!id.equals(Uuid.ZERO_UUID), "actual AdminClient UUID");
                    Files.writeString(state.resolve(release + ".uuid"), id.toString());
                }
            } else if (phase.equals("restart")) {
                Uuid expected = Uuid.fromString(Files.readString(state.resolve(release + ".uuid")));
                try (Admin admin = admin()) {
                    Uuid actual = describe(admin, name); check(actual.equals(expected), "UUID retained across process restart");
                    admin.deleteTopics(TopicCollection.ofTopicIds(List.of(expected))).all().get(15, TimeUnit.SECONDS);
                    check(!admin.listTopics().names().get(15, TimeUnit.SECONDS).contains(name), "actual AdminClient UUID deletion");
                    admin.createTopics(List.of(new NewTopic(name, 2, (short) 1))).all().get(15, TimeUnit.SECONDS);
                    Uuid replaced = describe(admin, name); check(!replaced.equals(expected), "delete/recreate gets fresh UUID");
                    admin.deleteTopics(TopicCollection.ofTopicNames(List.of(name))).all().get(15, TimeUnit.SECONDS);
                }
            } else throw new IllegalArgumentException("unknown phase");
            System.out.println("{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"assertions\":" + assertions + ",\"status\":\"pass\"}");
        } finally {
            Files.writeString(state.resolve(release + "-" + phase + "-history.json"), "{\"release\":" + quote(release) + ",\"phase\":" + quote(phase) + ",\"assertions\":" + assertions + ",\"history\":[\n" + String.join(",\n", HISTORY) + "\n]}\n");
        }
    }
}
