/* Independent official serializers/parsers and genuine Admin handlers for APIs27/90.
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
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.admin.internals.AbortTransactionHandler;
import org.apache.kafka.clients.admin.internals.AdminApiHandler;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
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
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.LogContext;

public final class CapabilityOracle {
    private static final List<String> CASES = new ArrayList<>();
    private static final StringBuilder TSV = new StringBuilder();
    private static final TopicPartition TARGET = new TopicPartition("t", 0);
    private static Path output;
    private CapabilityOracle() { }
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
    private static WriteTxnMarkersRequestData markerRequest(int tv, boolean tagged) throws Exception {
        WriteTxnMarkersRequestData.WritableTxnMarkerTopic topic =
            new WriteTxnMarkersRequestData.WritableTxnMarkerTopic().setName("t")
                .setPartitionIndexes(new ArrayList<>(List.of(0)));
        WriteTxnMarkersRequestData.WritableTxnMarker marker =
            new WriteTxnMarkersRequestData.WritableTxnMarker().setProducerId(1000)
                .setProducerEpoch((short) 2).setTransactionResult(false).setCoordinatorEpoch(7)
                .setTopics(new ArrayList<>(List.of(topic)));
        scalar(marker, "setTransactionVersion", byte.class, (byte) tv);
        WriteTxnMarkersRequestData data = new WriteTxnMarkersRequestData()
            .setMarkers(new ArrayList<>(List.of(marker)));
        if (tagged) {
            topic.unknownTaggedFields().add(new RawTaggedField(5, new byte[]{1,2}));
            marker.unknownTaggedFields().add(new RawTaggedField(7, new byte[]{3}));
            data.unknownTaggedFields().add(new RawTaggedField(9, new byte[]{4,5}));
        }
        return data;
    }
    private static WriteTxnMarkersResponseData markerResponse(String shape, short code) {
        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partition =
            new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                .setPartitionIndex(shape.equals("wrong-partition") ? 1 : 0).setErrorCode(code);
        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topic =
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                .setName(shape.equals("wrong-topic") ? "other" : "t")
                .setPartitions(new ArrayList<>(List.of(partition)));
        WriteTxnMarkersResponseData.WritableTxnMarkerResult marker =
            new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                .setProducerId(shape.equals("wrong-producer") ? 1001 : 1000)
                .setTopics(new ArrayList<>(List.of(topic)));
        if (shape.equals("no-topic")) marker.topics().clear();
        if (shape.equals("no-partition")) topic.partitions().clear();
        if (shape.equals("duplicate-topic")) marker.topics().add(topic.duplicate());
        if (shape.equals("duplicate-partition")) topic.partitions().add(partition.duplicate());
        WriteTxnMarkersResponseData data = new WriteTxnMarkersResponseData()
            .setMarkers(new ArrayList<>(List.of(marker)));
        if (shape.equals("empty")) data.markers().clear();
        if (shape.equals("duplicate-producer")) data.markers().add(marker.duplicate());
        if (shape.equals("unrelated-zero") || shape.equals("unrelated-error")) {
            WriteTxnMarkersResponseData.WritableTxnMarkerResult other = marker.duplicate().setProducerId(1001);
            other.topics().get(0).partitions().get(0).setErrorCode((short) (shape.equals("unrelated-error") ? 31 : 0));
            data.markers().add(other);
        }
        return data;
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
    private static String abortOutcome(WriteTxnMarkersResponseData response) {
        AbortTransactionHandler handler = new AbortTransactionHandler(
            new AbortTransactionSpec(TARGET,1000,(short)2,7),new LogContext());
        AdminApiHandler.ApiResult<TopicPartition,Void> result = handler.handleResponse(
            new Node(2,"127.0.0.1",19095),Set.of(TARGET),new WriteTxnMarkersResponse(response));
        String category = !result.completedKeys.isEmpty() ? "completed"
            : !result.unmappedKeys.isEmpty() ? "unmapped" : "failed";
        String type = result.failedKeys.isEmpty() ? null : result.failedKeys.get(TARGET).getClass().getName();
        return "{\"category\":"+quote(category)+",\"failure_type\":"+quote(type)+"}";
    }
    private static String shareOutcome(DescribeShareGroupOffsetsResponseData response) throws Exception {
        CoordinatorKey key=CoordinatorKey.byGroupId("g");
        ListShareGroupOffsetsHandler handler=new ListShareGroupOffsetsHandler(
            Map.of("g",new ListShareGroupOffsetsSpec()),new LogContext());
        AdminApiHandler.ApiResult<CoordinatorKey,Map<TopicPartition,SharePartitionOffsetInfo>> result=
            handler.handleResponse(new Node(2,"127.0.0.1",19095),Set.of(key),
                new DescribeShareGroupOffsetsResponse(response));
        Map<TopicPartition,SharePartitionOffsetInfo> values=result.completedKeys.get(key);
        SharePartitionOffsetInfo info=values==null ? null : values.get(TARGET);
        String lag="null";
        String getter="absent-on-this-official-release";
        if (info!=null) {
            try {
                Object returned=info.getClass().getMethod("lag").invoke(info);
                if (!(returned instanceof Optional<?>)) throw new AssertionError("official Optional lag");
                Optional<?> actual=(Optional<?>) returned;
                lag=actual.isPresent() ? actual.get().toString() : "null";
                getter="actual-official-public-Optional";
            } catch (NoSuchMethodException absent) { }
        }
        return "{\"completed_groups\":"+result.completedKeys.size()+",\"failed_groups\":"+result.failedKeys.size()
            +",\"unmapped_groups\":"+result.unmappedKeys.size()+",\"returned_partition_count\":"+(values==null ? 0 : values.size())
            +",\"start_offset\":"+(info==null ? "null" : Long.toString(info.startOffset()))
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
            String handler = api == ApiKeys.WRITE_TXN_MARKERS ? abortOutcome((WriteTxnMarkersResponseData)decoded.data())
                : shareOutcome((DescribeShareGroupOffsetsResponseData)decoded.data());
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
        for (short version : new short[]{0,1,2}) {
            for (int tv : new int[]{-128,-1,0,1,2,127}) emit("tv-"+tv,ApiKeys.WRITE_TXN_MARKERS,version,
                markerRequest(tv,false),markerResponse("normal",(short)0));
            emit("unknown-tags",ApiKeys.WRITE_TXN_MARKERS,version,markerRequest(2,true),markerResponse("normal",(short)0));
            for (String shape : List.of("empty","wrong-producer","wrong-topic","wrong-partition","no-topic",
                    "no-partition","duplicate-producer","duplicate-topic","duplicate-partition","unrelated-zero","unrelated-error"))
                emit(shape,ApiKeys.WRITE_TXN_MARKERS,version,markerRequest(0,false),markerResponse(shape,(short)0));
            for (short error : new short[]{3,6,8,9,31,47,52}) emit("error-"+error,ApiKeys.WRITE_TXN_MARKERS,version,
                markerRequest(0,false),markerResponse("normal",error));
        }
        for (short version : new short[]{0,1}) {
            for (long lag : new long[]{Long.MIN_VALUE,-2,-1,0,1,Long.MAX_VALUE}) emit("lag-"+lag,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                version,shareRequest("null"),shareResponse(lag,(short)0));
            for (String shape : List.of("empty","named")) emit("topics-"+shape,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                version,shareRequest(shape),shareResponse(7,(short)0));
            for (short error : new short[]{3,29,87}) emit("partition-error-"+error,ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS,
                version,shareRequest("named"),shareResponse(-1,error));
        }
        Files.writeString(output.resolve("goldens.json"),"{\"schema_version\":1,\"scope\":\"actual official codecs and AbortTransactionHandler component; no broker runtime\",\"cases\":["+String.join(",",CASES)+"]}\n");
        Files.writeString(output.resolve("cases.tsv"),TSV.toString());
    }
}
