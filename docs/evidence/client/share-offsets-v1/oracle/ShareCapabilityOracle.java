/* Independent official serializers/parsers and public share-offset handler.
 * This source does not read any Rust source or use a Rust-derived expected frame. */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;

public final class ShareCapabilityOracle {
    private static final List<String> CASES = new ArrayList<>();
    private static final StringBuilder TSV = new StringBuilder();
    private static final TopicPartition TARGET = new TopicPartition("t", 0);
    private static Path output;
    private ShareCapabilityOracle() { }
    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder text = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '\\' || c == '"') text.append('\\').append(c);
            else if (c < 32) text.append(String.format("\\u%04x", (int) c));
            else text.append(c);
        }
        return text.append('"').toString();
    }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        if (size < 0 || size > 65536) throw new AssertionError("finite encoded fixture");
        ByteBuffer bytes = ByteBuffer.allocate(size);
        message.write(new ByteBufferAccessor(bytes), cache, version);
        if (bytes.hasRemaining()) throw new AssertionError("serializer underwrite");
        return bytes.array();
    }
    private static byte[] concat(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
    private static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static void scalar(Message data, String setter, Class<?> type, Object value) throws Exception {
        try { data.getClass().getMethod(setter, type).invoke(data, value); }
        catch (NoSuchMethodException absent) {
            // Actual old generated class lacks the newer ignorable field.
            // Old-version serialization omits it; newest-version serialize
            // itself records UnsupportedVersionException below.
        }
    }
    private static DescribeShareGroupOffsetsRequestData shareRequest(String shape) {
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup group =
            new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup().setGroupId("g");
        if (shape.equals("null")) group.setTopics(null);
        if (shape.equals("named")) group.setTopics(new ArrayList<>(List.of(
            new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName("t").setPartitions(new ArrayList<>(List.of(0,1))))));
        return new DescribeShareGroupOffsetsRequestData().setGroups(new ArrayList<>(List.of(group)));
    }
    private static DescribeShareGroupOffsetsResponseData shareResponse(long lag, short code) throws Exception {
        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition partition =
            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                .setPartitionIndex(0).setStartOffset(17).setLeaderEpoch(7).setErrorCode(code);
        scalar(partition, "setLag", long.class, lag);
        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic topic =
            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                .setTopicName("t").setTopicId(new Uuid(0x0101010101010101L,0x0101010101010101L))
                .setPartitions(new ArrayList<>(List.of(partition)));
        return new DescribeShareGroupOffsetsResponseData().setThrottleTimeMs(13)
            .setGroups(new ArrayList<>(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
                .setGroupId("g").setTopics(new ArrayList<>(List.of(topic))))));
    }
    private static String officialOffsetAccessor(Object info) {
        String release = org.apache.kafka.common.utils.AppInfoParser.getVersion();
        String expectedType;
        String accessor;
        switch (release) {
            case "4.1.2":
                expectedType = "org.apache.kafka.clients.consumer.OffsetAndMetadata";
                accessor = "offset";
                break;
            case "4.2.1":
            case "4.3.1":
                expectedType = "org.apache.kafka.clients.admin.SharePartitionOffsetInfo";
                accessor = "startOffset";
                break;
            default:
                throw new AssertionError("unqualified official release");
        }
        if (!info.getClass().getName().equals(expectedType)) {
            throw new AssertionError("official public result type differs from pinned release");
        }
        return accessor;
    }
    private static long officialOffset(Object info) throws Exception {
        Object value = info.getClass().getMethod(officialOffsetAccessor(info)).invoke(info);
        if (!(value instanceof Long)) throw new AssertionError("official long offset accessor");
        return (Long) value;
    }
    private static String officialLeaderEpoch(Object info) throws Exception {
        officialOffsetAccessor(info);
        Object value = info.getClass().getMethod("leaderEpoch").invoke(info);
        if (!(value instanceof Optional<?>)) throw new AssertionError("official Optional leader epoch");
        Optional<?> epoch = (Optional<?>) value;
        if (epoch.isPresent() && !(epoch.get() instanceof Integer)) {
            throw new AssertionError("official integer leader epoch");
        }
        return epoch.isPresent() ? epoch.get().toString() : "null";
    }
    private static String officialLagGetter(Object info) throws Exception {
        officialOffsetAccessor(info);
        if (org.apache.kafka.common.utils.AppInfoParser.getVersion().equals("4.1.2")) {
            try {
                info.getClass().getMethod("lag");
            } catch (NoSuchMethodException expectedAbsent) {
                return "absent-on-this-official-release";
            }
            throw new AssertionError("unexpected lag accessor on pinned older public type");
        }
        return "actual-official-public-Optional";
    }
    private static String officialLag(Object info) throws Exception {
        if (officialLagGetter(info).equals("absent-on-this-official-release")) return "null";
        Object value = info.getClass().getMethod("lag").invoke(info);
        if (!(value instanceof Optional<?>)) throw new AssertionError("official Optional lag");
        Optional<?> lag = (Optional<?>) value;
        if (lag.isPresent() && !(lag.get() instanceof Long)) {
            throw new AssertionError("official long lag");
        }
        return lag.isPresent() ? lag.get().toString() : "null";
    }
    private static String shareOutcome(DescribeShareGroupOffsetsResponseData response) throws Exception {
        CoordinatorKey key=CoordinatorKey.byGroupId("g");
        ListShareGroupOffsetsHandler handler=new ListShareGroupOffsetsHandler(
            Map.of("g",new ListShareGroupOffsetsSpec()),new LogContext());
        AdminApiHandler.ApiResult<CoordinatorKey,? extends Map<TopicPartition,?>> result=
            handler.handleResponse(new Node(2,"127.0.0.1",19095),Set.of(key),
                new DescribeShareGroupOffsetsResponse(response));
        Map<TopicPartition,?> values=result.completedKeys.get(key);
        Object info=values==null ? null : values.get(TARGET);
        String lag=info==null ? "null" : officialLag(info);
        String getter=info==null ? "no-public-result" : officialLagGetter(info);
        return "{\"completed_groups\":"+result.completedKeys.size()+",\"failed_groups\":"+result.failedKeys.size()
            +",\"failure_type\":"+quote(result.failedKeys.isEmpty() ? null : result.failedKeys.get(key).getClass().getName())
            +",\"unmapped_groups\":"+result.unmappedKeys.size()+",\"returned_partition_count\":"+(values==null ? 0 : values.size())
            +",\"start_offset\":"+(info==null ? "null" : Long.toString(officialOffset(info)))
            +",\"public_result_type\":"+quote(info==null ? null : info.getClass().getName())
            +",\"offset_getter\":"+quote(info==null ? null : officialOffsetAccessor(info))
            +",\"leader_epoch\":"+(info==null ? "null" : officialLeaderEpoch(info))
            +",\"lag\":"+lag+",\"lag_getter_basis\":"+quote(getter)+"}";
    }
    private static void emit(String label, ApiKeys api, short version, Message request, Message response) throws Exception {
        String name = "api-"+api.id+"-v"+version+"-"+label;
        try {
            RequestHeader header = new RequestHeader(api,version,"capability-schema",7);
            byte[] requestBody = encode(request,version);
            byte[] requestFrame = concat(encode(header.data(),header.headerVersion()),requestBody);
            ByteBuffer cursor = ByteBuffer.wrap(requestFrame);
            RequestHeader.parse(cursor);
            AbstractRequest parsed = AbstractRequest.parseRequest(api,version,new ByteBufferAccessor(cursor)).request;
            if (cursor.hasRemaining() || !Arrays.equals(encode(parsed.data(),version),requestBody))
                throw new AssertionError("request full parse/byte agreement");
            short hv = api.responseHeaderVersion(version);
            byte[] responseBody = encode(response,version);
            byte[] responseFrame = concat(encode(new ResponseHeader(7,hv).data(),hv),responseBody);
            cursor = ByteBuffer.wrap(responseFrame); ResponseHeader.parse(cursor,hv);
            AbstractResponse decoded = AbstractResponse.parseResponse(api,new ByteBufferAccessor(cursor),version);
            if (cursor.hasRemaining() || !Arrays.equals(encode(decoded.data(),version),responseBody))
                throw new AssertionError("response full parse/byte agreement");
            Files.write(output.resolve(name+".request.bin"),requestFrame);
            Files.write(output.resolve(name+".response.bin"),responseFrame);
            String handler = shareOutcome((DescribeShareGroupOffsetsResponseData)decoded.data());
            CASES.add("{\"name\":"+quote(name)+",\"api_key\":"+api.id+",\"api_version\":"+version
                +",\"status\":\"actual-official-serialized-and-parsed\",\"request_hex\":"+quote(HexFormat.of().formatHex(requestFrame))
                +",\"response_hex\":"+quote(HexFormat.of().formatHex(responseFrame))+",\"request_sha256\":"+quote(hash(requestFrame))
                +",\"response_sha256\":"+quote(hash(responseFrame))+",\"official_handler\":"+handler
                +",\"request_data\":"+quote(parsed.data().toString())+",\"response_data\":"+quote(decoded.data().toString())+"}");
            TSV.append(name).append('\t').append(api.id).append('\t').append(version).append("\tsupported\n");
        } catch (org.apache.kafka.common.errors.UnsupportedVersionException unsupported) {
            CASES.add("{\"name\":"+quote(name)+",\"api_key\":"+api.id+",\"api_version\":"+version
                +",\"status\":\"actual-official-unsupported\",\"exception_type\":"+quote(unsupported.getClass().getName())+"}");
            TSV.append(name).append('\t').append(api.id).append('\t').append(version).append("\tunsupported\n");
        }
    }
    private static void reverse(Path directory) throws Exception {
        int checks = 0;
        for (String line : Files.readAllLines(directory.resolve("rust-emitted.tsv"))) {
            if (line.isBlank()) continue;
            String[] fields = line.split("\t",-1);
            if (fields.length!=5) throw new AssertionError("finite reverse row");
            ApiKeys api = ApiKeys.forId(Integer.parseInt(fields[1]));
            short version = Short.parseShort(fields[2]);
            byte[] request = HexFormat.of().parseHex(fields[3]);
            byte[] response = HexFormat.of().parseHex(fields[4]);
            if (request.length>65536 || response.length>65536) throw new AssertionError("reverse bound");
            ByteBuffer cursor = ByteBuffer.wrap(request); RequestHeader header = RequestHeader.parse(cursor);
            if (header.apiKey()!=api || header.apiVersion()!=version || header.correlationId()!=7)
                throw new AssertionError("reverse request header");
            AbstractRequest parsed = AbstractRequest.parseRequest(api,version,new ByteBufferAccessor(cursor)).request;
            if (cursor.hasRemaining() || !Arrays.equals(concat(encode(header.data(),header.headerVersion()),encode(parsed.data(),version)),request))
                throw new AssertionError("reverse request exact bytes");
            cursor = ByteBuffer.wrap(response); short hv=api.responseHeaderVersion(version);
            ResponseHeader responseHeader=ResponseHeader.parse(cursor,hv);
            AbstractResponse parsedResponse=AbstractResponse.parseResponse(api,new ByteBufferAccessor(cursor),version);
            if (responseHeader.correlationId()!=7 || cursor.hasRemaining()
                || !Arrays.equals(concat(encode(responseHeader.data(),hv),encode(parsedResponse.data(),version)),response))
                throw new AssertionError("reverse response exact bytes");
            checks+=2;
        }
        Files.writeString(output.resolve("reverse-validation.json"),"{\"passed\":true,\"actual_parse_checks\":"+checks+"}\n");
    }
    public static void main(String[] args) throws Exception {
        if (args.length<1 || args.length>2) throw new IllegalArgumentException("output and optional reverse directory");
        output=Path.of(args[0]);Files.createDirectories(output);
        if (args.length==2) {reverse(Path.of(args[1]));return;}
        for (short version : new short[]{0,1}) {
            for (long lag : new long[]{Long.MIN_VALUE,-2,-1,0,1,Long.MAX_VALUE}) emit("lag-"+lag,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                version,shareRequest("null"),shareResponse(lag,(short)0));
            for (String shape : List.of("empty","named")) emit("topics-"+shape,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                version,shareRequest(shape),shareResponse(7,(short)0));
            for (short error : new short[]{3,29,87}) emit("partition-error-"+error,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                version,shareRequest("named"),shareResponse(-1,error));
            DescribeShareGroupOffsetsRequestData taggedRequest=shareRequest("null");
            taggedRequest.unknownTaggedFields().add(new RawTaggedField(2,new byte[]{1,2,3}));
            DescribeShareGroupOffsetsResponseData taggedResponse=shareResponse(7,(short)0);
            taggedResponse.unknownTaggedFields().add(new RawTaggedField(2,new byte[]{1,2,3}));
            emit("unknown-tags",ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,version,taggedRequest,taggedResponse);
            for (short code : new short[]{14,15,16,30,69}) {
                DescribeShareGroupOffsetsResponseData data=shareResponse(-1,(short)0);
                data.groups().get(0).setErrorCode(code);
                emit("group-error-"+code,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,version,shareRequest("null"),data);
            }
            DescribeShareGroupOffsetsResponseData missing=shareResponse(7,(short)0);
            missing.groups().get(0).setGroupId("other");
            emit("missing-group",ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,version,shareRequest("null"),missing);
            DescribeShareGroupOffsetsResponseData duplicate=shareResponse(7,(short)0);
            duplicate.groups().add(shareResponse(33,(short)0).groups().get(0));
            emit("duplicate-group",ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,version,shareRequest("null"),duplicate);
            DescribeShareGroupOffsetsResponseData empty=shareResponse(7,(short)0);
            empty.setGroups(new ArrayList<>());
            emit("empty-groups",ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,version,shareRequest("null"),empty);
        }
        Files.writeString(output.resolve("goldens.json"),"{\"schema_version\":1,\"scope\":\"actual official share codecs and public result handler; no broker runtime\",\"cases\":["+String.join(",",CASES)+"]}\n");
        Files.writeString(output.resolve("cases.tsv"),TSV.toString());
    }
}
