/* Apache Kafka protocol serializer/parser oracle. No partitionline code is loaded. */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HexFormat;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.*;

public final class MetadataOracle {
    private static final Uuid ZERO = Uuid.ZERO_UUID;
    private static final Uuid ALPHA = new Uuid(0, 2);
    private static final Uuid INTERNAL = new Uuid(0, 3);
    private static final Uuid MISSING = new Uuid(0, 99);
    private static final List<String> CASES = new ArrayList<>();
    private static final List<String> SCHEMA_ONLY = new ArrayList<>();
    private static final StringBuilder TSV = new StringBuilder();
    private static Path output;
    private MetadataOracle() { }
    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder out = new StringBuilder("\"");
        for (char ch : value.toCharArray()) {
            if (ch == '"' || ch == '\\') out.append('\\').append(ch);
            else if (ch < 32) out.append(String.format("\\u%04x", (int) ch));
            else out.append(ch);
        }
        return out.append('"').toString();
    }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) throw new AssertionError("serializer underwrite");
        return buffer.array();
    }
    private static byte[] concat(byte[] first, byte[] second) {
        byte[] out = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }
    private static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static void emit(String name, ApiKeys key, short version, String seed,
                             ApiMessage request, ApiMessage response, String basis) throws Exception {
        RequestHeader header = new RequestHeader(key, version, "metadata-oracle", 7);
        byte[] requestBody = encode(request, version);
        byte[] requestFrame = concat(encode(header.data(), header.headerVersion()), requestBody);
        short responseHeaderVersion = key.responseHeaderVersion(version);
        byte[] responseBody = encode(response, version);
        byte[] responseFrame = concat(encode(new ResponseHeader(7, responseHeaderVersion).data(),
            responseHeaderVersion), responseBody);
        ByteBuffer rb = ByteBuffer.wrap(requestFrame);
        RequestHeader parsedHeader = RequestHeader.parse(rb);
        AbstractRequest parsedRequest = AbstractRequest.parseRequest(key, version,
            new ByteBufferAccessor(rb)).request;
        if (rb.hasRemaining() || parsedHeader.correlationId() != 7) throw new AssertionError("request trailing bytes");
        ByteBuffer sb = ByteBuffer.wrap(responseFrame);
        ResponseHeader parsedResponseHeader = ResponseHeader.parse(sb, responseHeaderVersion);
        AbstractResponse parsedResponse = AbstractResponse.parseResponse(key, new ByteBufferAccessor(sb), version);
        if (sb.hasRemaining() || parsedResponseHeader.correlationId() != 7) throw new AssertionError("response trailing bytes");
        if (!Arrays.equals(encode(parsedRequest.data(), version), requestBody)
                || !Arrays.equals(encode(parsedResponse.data(), version), responseBody)) {
            throw new AssertionError("parser/serializer round trip changed bytes");
        }
        if (requestFrame.length > 128 * 1024 || responseFrame.length > 128 * 1024) throw new AssertionError("fixture budget");
        Files.write(output.resolve(name + ".request.bin"), requestFrame);
        Files.write(output.resolve(name + ".response.bin"), responseFrame);
        String item = "{\"name\":" + quote(name) + ",\"api_key\":" + key.id
            + ",\"api_version\":" + version + ",\"seed\":" + quote(seed)
            + ",\"request_hex\":" + quote(HexFormat.of().formatHex(requestFrame))
            + ",\"response_hex\":" + quote(HexFormat.of().formatHex(responseFrame))
            + ",\"request_sha256\":" + quote(hash(requestFrame))
            + ",\"response_sha256\":" + quote(hash(responseFrame))
            + ",\"request_header_version\":" + header.headerVersion()
            + ",\"response_header_version\":" + responseHeaderVersion
            + ",\"basis\":" + quote(basis)
            + ",\"apache_parsed_request\":" + quote(parsedRequest.data().toString())
            + ",\"apache_parsed_response\":" + quote(parsedResponse.data().toString()) + "}";
        CASES.add(item);
        TSV.append(name).append('\t').append(key.id).append('\t').append(version).append('\t').append(seed).append('\n');
    }
    private static MetadataRequestData.MetadataRequestTopic target(String name, Uuid id) {
        return new MetadataRequestData.MetadataRequestTopic().setName(name).setTopicId(id);
    }
    private static MetadataRequestData metadataRequest(short version,
            List<MetadataRequestData.MetadataRequestTopic> topics, boolean ops) {
        MetadataRequestData data = new MetadataRequestData().setTopics(topics);
        if (version >= 4) data.setAllowAutoTopicCreation(false);
        if (version >= 8) data.setIncludeTopicAuthorizedOperations(ops);
        if (version >= 8 && version <= 10) data.setIncludeClusterAuthorizedOperations(ops);
        return data;
    }
    private static MetadataResponseData metadataResponse(short version, boolean ops) {
        MetadataResponseData data = new MetadataResponseData().setControllerId(0)
            .setClusterId("partitionline-fixture");
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(0)
            .setHost("127.0.0.1").setPort(19095));
        if (ops && version >= 8 && version <= 10) data.setClusterAuthorizedOperations(8096);
        return data;
    }
    private static MetadataResponseData.MetadataResponseTopic topic(short version, String name,
            Uuid id, short error, boolean ops) {
        MetadataResponseData.MetadataResponseTopic result = new MetadataResponseData.MetadataResponseTopic()
            .setName(name).setTopicId(id).setErrorCode(error)
            .setIsInternal("__consumer_offsets".equals(name));
        if (ops && version >= 8 && error != 100) result.setTopicAuthorizedOperations(3576);
        int count = error != 0 ? 0 : "alpha".equals(name) ? 2 : 1;
        for (int index = 0; index < count; index++) {
            result.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(index).setLeaderId(0).setLeaderEpoch(0)
                .setReplicaNodes(List.of(0)).setIsrNodes(List.of(0)));
        }
        return result;
    }
    private static void metadataCase(String label, short version,
            List<MetadataRequestData.MetadataRequestTopic> targets, boolean ops,
            MetadataResponseData.MetadataResponseTopic... topics) throws Exception {
        MetadataResponseData response = metadataResponse(version, ops);
        response.topics().addAll(List.of(topics));
        emit("metadata-v" + version + "-" + label, ApiKeys.METADATA, version, "fixture",
            metadataRequest(version, targets, ops), response,
            "Apache serializer/parser executed; response values are the declared single-node fixture policy, not an executed Apache controller.");
    }
    private static void metadata() throws Exception {
        for (short version = 0; version <= 13; version++) {
            List<MetadataRequestData.MetadataRequestTopic> all = version == 0 ? List.of() : null;
            metadataCase("all", version, all, false, topic(version, "alpha", ALPHA, (short) 0, false),
                topic(version, "__consumer_offsets", INTERNAL, (short) 0, false));
            if (version > 0) metadataCase("empty", version, List.of(), false);
            metadataCase("named", version, List.of(target("alpha", ZERO)), false,
                topic(version, "alpha", ALPHA, (short) 0, false));
            metadataCase("missing", version, List.of(target("missing", ZERO)), false,
                topic(version, "missing", ZERO, (short) 3, false));
            metadataCase("invalid", version, List.of(target("bad/name", ZERO)), false,
                topic(version, "bad/name", ZERO, (short) 17, false));
            metadataCase("duplicate-names", version, List.of(target("alpha", ZERO), target("alpha", ZERO)), false,
                topic(version, "alpha", ALPHA, (short) 0, false));
            if (version >= 8) metadataCase("authops", version, all, true,
                topic(version, "alpha", ALPHA, (short) 0, true),
                topic(version, "__consumer_offsets", INTERNAL, (short) 0, true));
            if (version == 10 || version == 11) {
                for (String label : List.of("null-name", "id", "duplicate-id")) {
                    List<MetadataRequestData.MetadataRequestTopic> ts = label.equals("null-name")
                        ? List.of(target(null, ZERO)) : label.equals("id")
                        ? List.of(target("alpha", ALPHA)) : List.of(target(null, ALPHA), target(null, ALPHA));
                    MetadataRequestData request = metadataRequest(version, ts, false);
                    ApiMessage response = new MetadataRequest(request, version).getErrorResponse(0,
                        new InvalidRequestException("semantic guard selected from pinned KafkaApis source")).data();
                    emit("metadata-v" + version + "-" + label, ApiKeys.METADATA, version, "fixture", request,
                        response, "Apache MetadataRequest.getErrorResponse executed for INVALID_REQUEST 42; source-inspected KafkaApis version<12 null-name/nonzero-ID guard selected the error.");
                }
            }
            if (version >= 12) {
                metadataCase("id", version, List.of(target(null, ALPHA)), false,
                    topic(version, "alpha", ALPHA, (short) 0, false));
                metadataCase("unknown-id", version, List.of(target(null, MISSING)), true,
                    topic(version, null, MISSING, (short) 100, true));
                metadataCase("mixed-names-ids", version, List.of(target("__consumer_offsets", ZERO), target("ignored-name", ALPHA)), true,
                    topic(version, "alpha", ALPHA, (short) 0, true));
                metadataCase("duplicate-ids", version, List.of(target(null, ALPHA), target(null, ALPHA)), false,
                    topic(version, "alpha", ALPHA, (short) 0, false));
                Path positiveOutput = output;
                output = output.resolve("schema-only");
                Files.createDirectories(output);
                MetadataResponseData malformedResponse = metadataResponse(version, false);
                malformedResponse.topics().add(topic(version, null, ZERO, (short) 3, false));
                emit("metadata-v" + version + "-null-zero", ApiKeys.METADATA, version, "fixture",
                    metadataRequest(version, List.of(target(null, ZERO)), false), malformedResponse,
                    "MALFORMED SCHEMA-ONLY: official serializer/parser accepts null-name plus zero-ID fields, but selector/response identity semantics are invalid. Previously policy-assembled error3 response retained only as superseded diagnostic, never a positive router golden. Actual Apache Topic.validate(null) throws NullPointerException; KafkaApis/controller runtime not executed. Local router deliberately rejects selector before response encoding.");
                String diagnostic = CASES.get(CASES.size() - 1);
                SCHEMA_ONLY.add(diagnostic);
                String rejection = diagnostic.replaceFirst("\"response_hex\":\"[0-9a-f]+\"", "\"response_hex\":null")
                    .replaceFirst("\"response_sha256\":\"[0-9a-f]+\"", "\"response_sha256\":null")
                    .replace("\"apache_parsed_response\":", "\"apache_schema_only_parsed_response\":");
                rejection = rejection.substring(0, rejection.length() - 1)
                    + ",\"handler_policy\":\"reject_neither_identity\"}";
                CASES.set(CASES.size() - 1, rejection);
                Files.copy(output.resolve("metadata-v" + version + "-null-zero.request.bin"),
                    positiveOutput.resolve("metadata-v" + version + "-null-zero.request.bin"),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                output = positiveOutput;
            }
            if (version >= 9) {
                MetadataRequestData request = metadataRequest(version, List.of(target("alpha", ZERO)), false);
                request.unknownTaggedFields().add(new RawTaggedField(71, new byte[]{1, 2, 3}));
                request.topics().get(0).unknownTaggedFields().add(new RawTaggedField(19, new byte[]{4, 5}));
                MetadataResponseData response = metadataResponse(version, false);
                response.topics().add(topic(version, "alpha", ALPHA, (short) 0, false));
                emit("metadata-v" + version + "-unknown-tags", ApiKeys.METADATA, version, "fixture", request,
                    response, "Apache flexible parser preserves unknown request tags; declared router policy skips them and emits no unknown response tags.");
            }
        }
    }
    private static void versions() throws Exception {
        for (short version = 0; version <= 4; version++) {
            ApiVersionsRequestData request = new ApiVersionsRequestData();
            if (version >= 3) request.setClientSoftwareName("metadata-oracle").setClientSoftwareVersion("1");
            ApiVersionsResponseData response = new ApiVersionsResponseData();
            for (short[] range : List.of(new short[]{3, 0, 13}, new short[]{18, 0, 4}, new short[]{19, 2, 4}, new short[]{20, 1, 6})) {
                response.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(range[0])
                    .setMinVersion(range[1]).setMaxVersion(range[2]));
            }
            emit("api-versions-v" + version, ApiKeys.API_VERSIONS, version, "fixture", request, response,
                "Apache serializer/parser executed for the composed local registry; only these four ranges are advertised.");
        }
    }
    private static CreateTopicsRequestData.CreatableTopic createTopic(String name, int partitions, short replicas) {
        return new CreateTopicsRequestData.CreatableTopic().setName(name).setNumPartitions(partitions).setReplicationFactor(replicas);
    }
    private static void createCase(String label, short version, String seed, boolean validate,
            List<CreateTopicsRequestData.CreatableTopic> topics, String name, short code, String message) throws Exception {
        CreateTopicsRequestData request = new CreateTopicsRequestData().setTimeoutMs(60_000).setValidateOnly(validate);
        request.topics().addAll(topics);
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        response.topics().add(new CreateTopicsResponseData.CreatableTopicResult().setName(name).setErrorCode(code).setErrorMessage(message));
        emit("create-v" + version + "-" + label, ApiKeys.CREATE_TOPICS, version, seed, request, response,
            "Apache serializer/parser executed; result is declared local single-node validation policy. Duplicate/reserved/manual-field messages are source-inspected Apache ControllerApis/ReplicationControlManager rules; local RF/config/name messages are not upstream runtime outcomes.");
    }
    private static void creates() throws Exception {
        for (short version = 2; version <= 4; version++) {
            createCase("success", version, "empty", false, List.of(createTopic("created", 2, (short) 1)), "created", (short) 0, null);
            createCase("validate-only", version, "empty", true, List.of(createTopic("validated", 1, (short) 1)), "validated", (short) 0, null);
            createCase("existing", version, "fixture", false, List.of(createTopic("alpha", 1, (short) 1)), "alpha", (short) 36, "Topic already exists.");
            createCase("invalid-name", version, "empty", false, List.of(createTopic("bad/name", 1, (short) 1)), "bad/name", (short) 17, "Invalid topic name.");
            createCase("invalid-partitions", version, "empty", false, List.of(createTopic("badparts", 0, (short) 1)), "badparts", (short) 37, "Number of partitions was set to an invalid non-positive value.");
            createCase("invalid-replication", version, "empty", false, List.of(createTopic("badrf", 1, (short) 2)), "badrf", (short) 38, "Replication factor must be one on this single-node broker.");
            createCase("duplicate", version, "empty", false, List.of(createTopic("duplicate", 1, (short) 1), createTopic("duplicate", 1, (short) 1)), "duplicate", (short) 42, "Duplicate topic name.");
            createCase("reserved", version, "empty", false, List.of(createTopic("__cluster_metadata", 1, (short) 1)), "__cluster_metadata", (short) 42, "Creation of internal topic __cluster_metadata is prohibited.");
            CreateTopicsRequestData.CreatableTopic configured = createTopic("configured", 1, (short) 1);
            configured.configs().add(new CreateTopicsRequestData.CreatableTopicConfig().setName("retention.ms").setValue("1000"));
            createCase("config-unsupported", version, "empty", false, List.of(configured), "configured", (short) 40, "Topic configuration overrides are not supported by this broker.");
            CreateTopicsRequestData.CreatableTopic manual = createTopic("manual", -1, (short) -1);
            manual.assignments().add(new CreateTopicsRequestData.CreatableReplicaAssignment().setPartitionIndex(0).setBrokerIds(List.of(0)));
            manual.assignments().add(new CreateTopicsRequestData.CreatableReplicaAssignment().setPartitionIndex(1).setBrokerIds(List.of(0)));
            createCase("manual-node-zero", version, "empty", false, List.of(manual), "manual", (short) 0, null);
            CreateTopicsRequestData.CreatableTopic badManual = createTopic("badmanual", -1, (short) -1);
            badManual.assignments().add(new CreateTopicsRequestData.CreatableReplicaAssignment().setPartitionIndex(0).setBrokerIds(List.of(1)));
            createCase("manual-other-node", version, "empty", false, List.of(badManual), "badmanual", (short) 39, "Manual assignments must contain only this broker.");
            if (version == 4) createCase("defaults", version, "empty", false, List.of(createTopic("defaults", -1, (short) -1)), "defaults", (short) 0, null);
        }
    }
    private static DeleteTopicsRequestData.DeleteTopicState deletion(String name, Uuid id) {
        return new DeleteTopicsRequestData.DeleteTopicState().setName(name).setTopicId(id);
    }
    private static void deleteCase(String label, short version,
            List<DeleteTopicsRequestData.DeleteTopicState> targets, String name, Uuid id, short code, String message) throws Exception {
        DeleteTopicsRequestData request = new DeleteTopicsRequestData().setTimeoutMs(60_000);
        if (version < 6) request.setTopicNames(targets.stream().map(DeleteTopicsRequestData.DeleteTopicState::name).toList());
        else request.setTopics(targets);
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult().setName(name).setTopicId(id)
            .setErrorCode(code).setErrorMessage(message));
        emit("delete-v" + version + "-" + label, ApiKeys.DELETE_TOPICS, version, "fixture", request, response,
            "Apache serializer/parser executed; source-inspected ControllerApis validation and declared seed catalog resolution. Apache shuffles response order; fixtures use deterministic router order, live probes compare sets.");
    }
    private static void deletes() throws Exception {
        for (short version = 1; version <= 6; version++) {
            deleteCase("name", version, List.of(deletion("alpha", ZERO)), "alpha", ALPHA, (short) 0, null);
            deleteCase("missing", version, List.of(deletion("missing", ZERO)), "missing", ZERO, (short) 3, null);
            deleteCase("duplicate-name", version, List.of(deletion("alpha", ZERO), deletion("alpha", ZERO)), "alpha", ZERO, (short) 42, "Duplicate topic name.");
            if (version == 6) {
                deleteCase("id", version, List.of(deletion(null, ALPHA)), "alpha", ALPHA, (short) 0, null);
                deleteCase("unknown-id", version, List.of(deletion(null, MISSING)), null, MISSING, (short) 100, null);
                deleteCase("duplicate-id", version, List.of(deletion(null, ALPHA), deletion(null, ALPHA)), null, ALPHA, (short) 42, "Duplicate topic id.");
                deleteCase("neither", version, List.of(deletion(null, ZERO)), null, ZERO, (short) 42, "Neither topic name nor id were specified.");
                deleteCase("both", version, List.of(deletion("alpha", ALPHA)), "alpha", ALPHA, (short) 42, "You may not specify both topic name and topic id.");
            }
        }
        String aliasMessage = "The provided topic name maps to an ID that was already supplied.";
        deleteCase("name-id-alias", (short) 6, List.of(deletion("alpha", ZERO), deletion(null, ALPHA)),
            "alpha", ALPHA, (short) 42, aliasMessage);
        DeleteTopicsRequestData request = new DeleteTopicsRequestData().setTimeoutMs(60_000)
            .setTopics(List.of(deletion("alpha", ZERO), deletion("alpha", ZERO), deletion(null, ALPHA)));
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult().setName("alpha")
            .setErrorCode((short) 42).setErrorMessage("Duplicate topic name."));
        response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult().setName("alpha")
            .setTopicId(ALPHA));
        emit("delete-v6-duplicate-name-plus-id", ApiKeys.DELETE_TOPICS, (short) 6, "fixture", request,
            response, "Source-inspected ControllerApis removes duplicate names before ID/name alias resolution; duplicated name errors but unique ID deletes. Deterministic local response order, Apache order unspecified.");
        request = new DeleteTopicsRequestData().setTimeoutMs(60_000)
            .setTopics(List.of(deletion("alpha", ZERO), deletion(null, ALPHA), deletion(null, ALPHA)));
        response = new DeleteTopicsResponseData();
        response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult().setName("alpha")
            .setTopicId(ALPHA).setErrorCode((short) 42).setErrorMessage(aliasMessage));
        response.responses().add(new DeleteTopicsResponseData.DeletableTopicResult().setName(null)
            .setTopicId(ALPHA).setErrorCode((short) 42).setErrorMessage("Duplicate topic id."));
        emit("delete-v6-name-plus-duplicate-id", ApiKeys.DELETE_TOPICS, (short) 6, "fixture", request,
            response, "Source-inspected ControllerApis retains duplicate IDs in providedIds during alias checking: duplicate ID and mapped-name errors, no deletion. Deterministic local response order, Apache order unspecified.");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("release output-directory");
        output = Path.of(args[1]);
        Files.createDirectories(output);
        versions(); metadata(); creates(); deletes();
        String manifest = "{\"release\":" + quote(args[0]) + ",\"wire\":\"Kafka header plus body, without length prefix\",\"seed\":{\"node_id\":0,\"host\":\"127.0.0.1\",\"port\":19095,\"cluster_id\":\"partitionline-fixture\",\"alpha_id\":\"00000000000000000000000000000002\",\"internal_id\":\"00000000000000000000000000000003\"},\"cases\":[\n" + String.join(",\n", CASES) + "\n]}\n";
        Files.writeString(output.resolve("goldens.json"), manifest, StandardCharsets.UTF_8);
        Files.writeString(output.resolve("cases.tsv"), TSV.toString(), StandardCharsets.UTF_8);
        Files.writeString(output.resolve("schema-only/goldens.json"), "{\"release\":" + quote(args[0])
            + ",\"qualification\":\"Excluded from positive router comparisons; historical malformed field combinations only\",\"cases\":[\n"
            + String.join(",\n", SCHEMA_ONLY) + "\n]}\n", StandardCharsets.UTF_8);
        System.out.println("{\"release\":" + quote(args[0]) + ",\"cases\":" + CASES.size() + ",\"manifest_sha256\":" + quote(hash(manifest.getBytes(StandardCharsets.UTF_8))) + "}");
    }
}
