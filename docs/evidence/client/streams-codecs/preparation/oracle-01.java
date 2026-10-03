import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

/** Actual pinned generated Kafka serializers/parsers. No Kafka broker/runtime claim. */
public final class StreamsWireOracle {
    private static final short VERSION = 0;
    private static final List<String> INDEX = new ArrayList<>();
    private StreamsWireOracle() { }

    private static Message parse(String type, ByteBuffer bytes) {
        ByteBufferAccessor readable = new ByteBufferAccessor(bytes);
        return switch (type) {
            case "StreamsGroupHeartbeatRequest" -> new StreamsGroupHeartbeatRequestData(readable, VERSION);
            case "StreamsGroupHeartbeatResponse" -> new StreamsGroupHeartbeatResponseData(readable, VERSION);
            case "StreamsGroupDescribeRequest" -> new StreamsGroupDescribeRequestData(readable, VERSION);
            case "StreamsGroupDescribeResponse" -> new StreamsGroupDescribeResponseData(readable, VERSION);
            default -> throw new IllegalArgumentException("Unknown family");
        };
    }

    private static byte[] bytes(Message message) {
        ByteBuffer encoded = MessageUtil.toByteBuffer(message, VERSION);
        byte[] result = new byte[encoded.remaining()];
        encoded.get(result);
        return result;
    }

    private static void record(Path out, String name, int api, String type, byte[] body,
                               String rustPolicy) throws Exception {
        String outcome;
        int remaining = -1;
        try {
            ByteBuffer input = ByteBuffer.wrap(body);
            parse(type, input);
            remaining = input.remaining();
            outcome = "parsed";
        } catch (RuntimeException | OutOfMemoryError failure) {
            outcome = failure.getClass().getName();
        }
        Files.write(out.resolve(name + ".bin"), body);
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        INDEX.add(name + "\t" + api + "\t" + type + "\t0\t" + outcome + "\t" + remaining +
                  "\t" + rustPolicy + "\t" + digest + "\n");
    }

    private static void emit(Path out, String name, int api, String type, Message message) throws Exception {
        byte[] body = bytes(message);
        ByteBuffer input = ByteBuffer.wrap(body);
        Message decoded = parse(type, input);
        if (input.hasRemaining() || !message.equals(decoded) || !Arrays.equals(body, bytes(decoded))) {
            throw new IllegalStateException("Actual Apache serialization/parser mismatch: " + name);
        }
        record(out, name, api, type, body, "accept");
    }

    private static byte[] replaceTags(byte[] canonical, byte[] tags) {
        if (canonical.length == 0 || canonical[canonical.length - 1] != 0) {
            throw new IllegalArgumentException("Expected canonical empty root tags");
        }
        byte[] result = Arrays.copyOf(canonical, canonical.length - 1 + tags.length);
        System.arraycopy(tags, 0, result, canonical.length - 1, tags.length);
        return result;
    }
    private static StreamsGroupHeartbeatRequestData StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest() {
        return new StreamsGroupHeartbeatRequestData()
            .setGroupId("StreamsGroupHeartbeatRequest-GroupId-詳細")
            .setMemberId("StreamsGroupHeartbeatRequest-MemberId-詳細")
            .setMemberEpoch(-1234567)
            .setEndpointInformationEpoch(-1234567)
            .setInstanceId("StreamsGroupHeartbeatRequest-InstanceId-詳細")
            .setRackId("StreamsGroupHeartbeatRequest-RackId-詳細")
            .setRebalanceTimeoutMs(-1234567)
            .setTopology(StreamsGroupHeartbeatRequest_Topology())
            .setActiveTasks(List.of(StreamsGroupHeartbeatRequest_TaskIds()))
            .setStandbyTasks(List.of(StreamsGroupHeartbeatRequest_TaskIds()))
            .setWarmupTasks(List.of(StreamsGroupHeartbeatRequest_TaskIds()))
            .setProcessId("StreamsGroupHeartbeatRequest-ProcessId-詳細")
            .setUserEndpoint(StreamsGroupHeartbeatRequest_Endpoint())
            .setClientTags(List.of(StreamsGroupHeartbeatRequest_KeyValue()))
            .setTaskOffsets(List.of(StreamsGroupHeartbeatRequest_TaskOffset()))
            .setTaskEndOffsets(List.of(StreamsGroupHeartbeatRequest_TaskOffset()))
            .setShutdownApplication(true);
    }

    private static StreamsGroupHeartbeatRequestData.KeyValue StreamsGroupHeartbeatRequest_KeyValue() {
        return new StreamsGroupHeartbeatRequestData.KeyValue()
            .setKey("StreamsGroupHeartbeatRequest-Key-詳細")
            .setValue("StreamsGroupHeartbeatRequest-Value-詳細");
    }

    private static StreamsGroupHeartbeatRequestData.TopicInfo StreamsGroupHeartbeatRequest_TopicInfo() {
        return new StreamsGroupHeartbeatRequestData.TopicInfo()
            .setName("StreamsGroupHeartbeatRequest-Name-詳細")
            .setPartitions(-1234567)
            .setReplicationFactor((short) -123)
            .setTopicConfigs(List.of(StreamsGroupHeartbeatRequest_KeyValue()));
    }

    private static StreamsGroupHeartbeatRequestData.Endpoint StreamsGroupHeartbeatRequest_Endpoint() {
        return new StreamsGroupHeartbeatRequestData.Endpoint()
            .setHost("StreamsGroupHeartbeatRequest-Host-詳細")
            .setPort(65535);
    }

    private static StreamsGroupHeartbeatRequestData.TaskOffset StreamsGroupHeartbeatRequest_TaskOffset() {
        return new StreamsGroupHeartbeatRequestData.TaskOffset()
            .setSubtopologyId("StreamsGroupHeartbeatRequest-SubtopologyId-詳細")
            .setPartition(-1234567)
            .setOffset(Long.MIN_VALUE);
    }

    private static StreamsGroupHeartbeatRequestData.TaskIds StreamsGroupHeartbeatRequest_TaskIds() {
        return new StreamsGroupHeartbeatRequestData.TaskIds()
            .setSubtopologyId("StreamsGroupHeartbeatRequest-SubtopologyId-詳細")
            .setPartitions(List.of(-1234567));
    }

    private static StreamsGroupHeartbeatRequestData.Topology StreamsGroupHeartbeatRequest_Topology() {
        return new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(-1234567)
            .setSubtopologies(List.of(StreamsGroupHeartbeatRequest_Subtopology()));
    }

    private static StreamsGroupHeartbeatRequestData.Subtopology StreamsGroupHeartbeatRequest_Subtopology() {
        return new StreamsGroupHeartbeatRequestData.Subtopology()
            .setSubtopologyId("StreamsGroupHeartbeatRequest-SubtopologyId-詳細")
            .setSourceTopics(List.of("StreamsGroupHeartbeatRequest-SourceTopics-詳細"))
            .setSourceTopicRegex(List.of("StreamsGroupHeartbeatRequest-SourceTopicRegex-詳細"))
            .setStateChangelogTopics(List.of(StreamsGroupHeartbeatRequest_TopicInfo()))
            .setRepartitionSinkTopics(List.of("StreamsGroupHeartbeatRequest-RepartitionSinkTopics-詳細"))
            .setRepartitionSourceTopics(List.of(StreamsGroupHeartbeatRequest_TopicInfo()))
            .setCopartitionGroups(List.of(StreamsGroupHeartbeatRequest_CopartitionGroup()));
    }

    private static StreamsGroupHeartbeatRequestData.CopartitionGroup StreamsGroupHeartbeatRequest_CopartitionGroup() {
        return new StreamsGroupHeartbeatRequestData.CopartitionGroup()
            .setSourceTopics(List.of((short) -123))
            .setSourceTopicRegex(List.of((short) -123))
            .setRepartitionSourceTopics(List.of((short) -123));
    }

    private static StreamsGroupHeartbeatResponseData StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse() {
        return new StreamsGroupHeartbeatResponseData()
            .setThrottleTimeMs(-1234567)
            .setErrorCode((short) -123)
            .setErrorMessage("StreamsGroupHeartbeatResponse-ErrorMessage-詳細")
            .setMemberId("StreamsGroupHeartbeatResponse-MemberId-詳細")
            .setMemberEpoch(-1234567)
            .setHeartbeatIntervalMs(-1234567)
            .setAcceptableRecoveryLag(-1234567)
            .setTaskOffsetIntervalMs(-1234567)
            .setStatus(List.of(StreamsGroupHeartbeatResponse_Status()))
            .setActiveTasks(List.of(StreamsGroupHeartbeatResponse_TaskIds()))
            .setStandbyTasks(List.of(StreamsGroupHeartbeatResponse_TaskIds()))
            .setWarmupTasks(List.of(StreamsGroupHeartbeatResponse_TaskIds()))
            .setEndpointInformationEpoch(-1234567)
            .setPartitionsByUserEndpoint(List.of(StreamsGroupHeartbeatResponse_EndpointToPartitions()));
    }

    private static StreamsGroupHeartbeatResponseData.Status StreamsGroupHeartbeatResponse_Status() {
        return new StreamsGroupHeartbeatResponseData.Status()
            .setStatusCode((byte) -128)
            .setStatusDetail("StreamsGroupHeartbeatResponse-StatusDetail-詳細");
    }

    private static StreamsGroupHeartbeatResponseData.TopicPartition StreamsGroupHeartbeatResponse_TopicPartition() {
        return new StreamsGroupHeartbeatResponseData.TopicPartition()
            .setTopic("StreamsGroupHeartbeatResponse-Topic-詳細")
            .setPartitions(List.of(-1234567));
    }

    private static StreamsGroupHeartbeatResponseData.TaskIds StreamsGroupHeartbeatResponse_TaskIds() {
        return new StreamsGroupHeartbeatResponseData.TaskIds()
            .setSubtopologyId("StreamsGroupHeartbeatResponse-SubtopologyId-詳細")
            .setPartitions(List.of(-1234567));
    }

    private static StreamsGroupHeartbeatResponseData.Endpoint StreamsGroupHeartbeatResponse_Endpoint() {
        return new StreamsGroupHeartbeatResponseData.Endpoint()
            .setHost("StreamsGroupHeartbeatResponse-Host-詳細")
            .setPort(65535);
    }

    private static StreamsGroupHeartbeatResponseData.EndpointToPartitions StreamsGroupHeartbeatResponse_EndpointToPartitions() {
        return new StreamsGroupHeartbeatResponseData.EndpointToPartitions()
            .setUserEndpoint(StreamsGroupHeartbeatResponse_Endpoint())
            .setActivePartitions(List.of(StreamsGroupHeartbeatResponse_TopicPartition()))
            .setStandbyPartitions(List.of(StreamsGroupHeartbeatResponse_TopicPartition()));
    }

    private static StreamsGroupDescribeRequestData StreamsGroupDescribeRequest_StreamsGroupDescribeRequest() {
        return new StreamsGroupDescribeRequestData()
            .setGroupIds(List.of("StreamsGroupDescribeRequest-GroupIds-詳細"))
            .setIncludeAuthorizedOperations(true);
    }

    private static StreamsGroupDescribeResponseData StreamsGroupDescribeResponse_StreamsGroupDescribeResponse() {
        return new StreamsGroupDescribeResponseData()
            .setThrottleTimeMs(-1234567)
            .setGroups(List.of(StreamsGroupDescribeResponse_DescribedGroup()));
    }

    private static StreamsGroupDescribeResponseData.Endpoint StreamsGroupDescribeResponse_Endpoint() {
        return new StreamsGroupDescribeResponseData.Endpoint()
            .setHost("StreamsGroupDescribeResponse-Host-詳細")
            .setPort(65535);
    }

    private static StreamsGroupDescribeResponseData.TaskOffset StreamsGroupDescribeResponse_TaskOffset() {
        return new StreamsGroupDescribeResponseData.TaskOffset()
            .setSubtopologyId("StreamsGroupDescribeResponse-SubtopologyId-詳細")
            .setPartition(-1234567)
            .setOffset(Long.MIN_VALUE);
    }

    private static StreamsGroupDescribeResponseData.Assignment StreamsGroupDescribeResponse_Assignment() {
        return new StreamsGroupDescribeResponseData.Assignment()
            .setActiveTasks(List.of(StreamsGroupDescribeResponse_TaskIds()))
            .setStandbyTasks(List.of(StreamsGroupDescribeResponse_TaskIds()))
            .setWarmupTasks(List.of(StreamsGroupDescribeResponse_TaskIds()));
    }

    private static StreamsGroupDescribeResponseData.TaskIds StreamsGroupDescribeResponse_TaskIds() {
        return new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("StreamsGroupDescribeResponse-SubtopologyId-詳細")
            .setPartitions(List.of(-1234567));
    }

    private static StreamsGroupDescribeResponseData.KeyValue StreamsGroupDescribeResponse_KeyValue() {
        return new StreamsGroupDescribeResponseData.KeyValue()
            .setKey("StreamsGroupDescribeResponse-Key-詳細")
            .setValue("StreamsGroupDescribeResponse-Value-詳細");
    }

    private static StreamsGroupDescribeResponseData.TopicInfo StreamsGroupDescribeResponse_TopicInfo() {
        return new StreamsGroupDescribeResponseData.TopicInfo()
            .setName("StreamsGroupDescribeResponse-Name-詳細")
            .setPartitions(-1234567)
            .setReplicationFactor((short) -123)
            .setTopicConfigs(List.of(StreamsGroupDescribeResponse_KeyValue()));
    }

    private static StreamsGroupDescribeResponseData.DescribedGroup StreamsGroupDescribeResponse_DescribedGroup() {
        return new StreamsGroupDescribeResponseData.DescribedGroup()
            .setErrorCode((short) -123)
            .setErrorMessage("StreamsGroupDescribeResponse-ErrorMessage-詳細")
            .setGroupId("StreamsGroupDescribeResponse-GroupId-詳細")
            .setGroupState("StreamsGroupDescribeResponse-GroupState-詳細")
            .setGroupEpoch(-1234567)
            .setAssignmentEpoch(-1234567)
            .setTopology(StreamsGroupDescribeResponse_Topology())
            .setMembers(List.of(StreamsGroupDescribeResponse_Member()))
            .setAuthorizedOperations(-1234567);
    }

    private static StreamsGroupDescribeResponseData.Topology StreamsGroupDescribeResponse_Topology() {
        return new StreamsGroupDescribeResponseData.Topology()
            .setEpoch(-1234567)
            .setSubtopologies(List.of(StreamsGroupDescribeResponse_Subtopology()));
    }

    private static StreamsGroupDescribeResponseData.Subtopology StreamsGroupDescribeResponse_Subtopology() {
        return new StreamsGroupDescribeResponseData.Subtopology()
            .setSubtopologyId("StreamsGroupDescribeResponse-SubtopologyId-詳細")
            .setSourceTopics(List.of("StreamsGroupDescribeResponse-SourceTopics-詳細"))
            .setRepartitionSinkTopics(List.of("StreamsGroupDescribeResponse-RepartitionSinkTopics-詳細"))
            .setStateChangelogTopics(List.of(StreamsGroupDescribeResponse_TopicInfo()))
            .setRepartitionSourceTopics(List.of(StreamsGroupDescribeResponse_TopicInfo()));
    }

    private static StreamsGroupDescribeResponseData.Member StreamsGroupDescribeResponse_Member() {
        return new StreamsGroupDescribeResponseData.Member()
            .setMemberId("StreamsGroupDescribeResponse-MemberId-詳細")
            .setMemberEpoch(-1234567)
            .setInstanceId("StreamsGroupDescribeResponse-InstanceId-詳細")
            .setRackId("StreamsGroupDescribeResponse-RackId-詳細")
            .setClientId("StreamsGroupDescribeResponse-ClientId-詳細")
            .setClientHost("StreamsGroupDescribeResponse-ClientHost-詳細")
            .setTopologyEpoch(-1234567)
            .setProcessId("StreamsGroupDescribeResponse-ProcessId-詳細")
            .setUserEndpoint(StreamsGroupDescribeResponse_Endpoint())
            .setClientTags(List.of(StreamsGroupDescribeResponse_KeyValue()))
            .setTaskOffsets(List.of(StreamsGroupDescribeResponse_TaskOffset()))
            .setTaskEndOffsets(List.of(StreamsGroupDescribeResponse_TaskOffset()))
            .setAssignment(StreamsGroupDescribeResponse_Assignment())
            .setTargetAssignment(StreamsGroupDescribeResponse_Assignment())
            .setIsClassic(true);
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) { throw new IllegalArgumentException("Output directory required"); }
        Path out = Path.of(arguments[0]);
        Files.createDirectories(out);
        StreamsGroupHeartbeatRequestData StreamsGroupHeartbeatRequestDefault = new StreamsGroupHeartbeatRequestData();
        emit(out, "heartbeat-request-default", 88, "StreamsGroupHeartbeatRequest", StreamsGroupHeartbeatRequestDefault);
        StreamsGroupHeartbeatRequestData StreamsGroupHeartbeatRequestFull = StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest();
        emit(out, "heartbeat-request-full", 88, "StreamsGroupHeartbeatRequest", StreamsGroupHeartbeatRequestFull);
        StreamsGroupHeartbeatRequestFull.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {9, 8, 7}));
        emit(out, "heartbeat-request-unknown-root-tags", 88, "StreamsGroupHeartbeatRequest", StreamsGroupHeartbeatRequestFull);
        byte[] StreamsGroupHeartbeatRequestBase = bytes(StreamsGroupHeartbeatRequestDefault);
        record(out, "heartbeat-request-duplicate-tags", 88, "StreamsGroupHeartbeatRequest", replaceTags(StreamsGroupHeartbeatRequestBase, new byte[] {2, 7, 0, 7, 0}), "reject-local-tag-order");
        record(out, "heartbeat-request-descending-tags", 88, "StreamsGroupHeartbeatRequest", replaceTags(StreamsGroupHeartbeatRequestBase, new byte[] {2, 7, 0, 3, 0}), "reject-local-tag-order");
        record(out, "heartbeat-request-trailing-zero", 88, "StreamsGroupHeartbeatRequest", Arrays.copyOf(StreamsGroupHeartbeatRequestBase, StreamsGroupHeartbeatRequestBase.length + 1), "reject-local-whole-input");
        StreamsGroupHeartbeatResponseData StreamsGroupHeartbeatResponseDefault = new StreamsGroupHeartbeatResponseData();
        emit(out, "heartbeat-response-default", 88, "StreamsGroupHeartbeatResponse", StreamsGroupHeartbeatResponseDefault);
        StreamsGroupHeartbeatResponseData StreamsGroupHeartbeatResponseFull = StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse();
        emit(out, "heartbeat-response-full", 88, "StreamsGroupHeartbeatResponse", StreamsGroupHeartbeatResponseFull);
        StreamsGroupHeartbeatResponseFull.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {9, 8, 7}));
        emit(out, "heartbeat-response-unknown-root-tags", 88, "StreamsGroupHeartbeatResponse", StreamsGroupHeartbeatResponseFull);
        byte[] StreamsGroupHeartbeatResponseBase = bytes(StreamsGroupHeartbeatResponseDefault);
        record(out, "heartbeat-response-duplicate-tags", 88, "StreamsGroupHeartbeatResponse", replaceTags(StreamsGroupHeartbeatResponseBase, new byte[] {2, 7, 0, 7, 0}), "reject-local-tag-order");
        record(out, "heartbeat-response-descending-tags", 88, "StreamsGroupHeartbeatResponse", replaceTags(StreamsGroupHeartbeatResponseBase, new byte[] {2, 7, 0, 3, 0}), "reject-local-tag-order");
        record(out, "heartbeat-response-trailing-zero", 88, "StreamsGroupHeartbeatResponse", Arrays.copyOf(StreamsGroupHeartbeatResponseBase, StreamsGroupHeartbeatResponseBase.length + 1), "reject-local-whole-input");
        StreamsGroupDescribeRequestData StreamsGroupDescribeRequestDefault = new StreamsGroupDescribeRequestData();
        emit(out, "describe-request-default", 89, "StreamsGroupDescribeRequest", StreamsGroupDescribeRequestDefault);
        StreamsGroupDescribeRequestData StreamsGroupDescribeRequestFull = StreamsGroupDescribeRequest_StreamsGroupDescribeRequest();
        emit(out, "describe-request-full", 89, "StreamsGroupDescribeRequest", StreamsGroupDescribeRequestFull);
        StreamsGroupDescribeRequestFull.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {9, 8, 7}));
        emit(out, "describe-request-unknown-root-tags", 89, "StreamsGroupDescribeRequest", StreamsGroupDescribeRequestFull);
        byte[] StreamsGroupDescribeRequestBase = bytes(StreamsGroupDescribeRequestDefault);
        record(out, "describe-request-duplicate-tags", 89, "StreamsGroupDescribeRequest", replaceTags(StreamsGroupDescribeRequestBase, new byte[] {2, 7, 0, 7, 0}), "reject-local-tag-order");
        record(out, "describe-request-descending-tags", 89, "StreamsGroupDescribeRequest", replaceTags(StreamsGroupDescribeRequestBase, new byte[] {2, 7, 0, 3, 0}), "reject-local-tag-order");
        record(out, "describe-request-trailing-zero", 89, "StreamsGroupDescribeRequest", Arrays.copyOf(StreamsGroupDescribeRequestBase, StreamsGroupDescribeRequestBase.length + 1), "reject-local-whole-input");
        StreamsGroupDescribeResponseData StreamsGroupDescribeResponseDefault = new StreamsGroupDescribeResponseData();
        emit(out, "describe-response-default", 89, "StreamsGroupDescribeResponse", StreamsGroupDescribeResponseDefault);
        StreamsGroupDescribeResponseData StreamsGroupDescribeResponseFull = StreamsGroupDescribeResponse_StreamsGroupDescribeResponse();
        emit(out, "describe-response-full", 89, "StreamsGroupDescribeResponse", StreamsGroupDescribeResponseFull);
        StreamsGroupDescribeResponseFull.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {9, 8, 7}));
        emit(out, "describe-response-unknown-root-tags", 89, "StreamsGroupDescribeResponse", StreamsGroupDescribeResponseFull);
        byte[] StreamsGroupDescribeResponseBase = bytes(StreamsGroupDescribeResponseDefault);
        record(out, "describe-response-duplicate-tags", 89, "StreamsGroupDescribeResponse", replaceTags(StreamsGroupDescribeResponseBase, new byte[] {2, 7, 0, 7, 0}), "reject-local-tag-order");
        record(out, "describe-response-descending-tags", 89, "StreamsGroupDescribeResponse", replaceTags(StreamsGroupDescribeResponseBase, new byte[] {2, 7, 0, 3, 0}), "reject-local-tag-order");
        record(out, "describe-response-trailing-zero", 89, "StreamsGroupDescribeResponse", Arrays.copyOf(StreamsGroupDescribeResponseBase, StreamsGroupDescribeResponseBase.length + 1), "reject-local-whole-input");
        emit(out, "heartbeat-request-empty-changes", 88, "StreamsGroupHeartbeatRequest",
             new StreamsGroupHeartbeatRequestData().setActiveTasks(List.of()).setStandbyTasks(List.of())
                 .setWarmupTasks(List.of()).setClientTags(List.of()).setTaskOffsets(List.of()).setTaskEndOffsets(List.of()));
        emit(out, "heartbeat-request-null-changes", 88, "StreamsGroupHeartbeatRequest",
             new StreamsGroupHeartbeatRequestData().setActiveTasks(null).setStandbyTasks(null)
                 .setWarmupTasks(null).setClientTags(null).setTaskOffsets(null).setTaskEndOffsets(null));
        emit(out, "heartbeat-response-empty-changes", 88, "StreamsGroupHeartbeatResponse",
             new StreamsGroupHeartbeatResponseData().setStatus(List.of()).setActiveTasks(List.of())
                 .setStandbyTasks(List.of()).setWarmupTasks(List.of()).setPartitionsByUserEndpoint(List.of()));
        emit(out, "heartbeat-response-null-changes", 88, "StreamsGroupHeartbeatResponse",
             new StreamsGroupHeartbeatResponseData().setStatus(null).setActiveTasks(null)
                 .setStandbyTasks(null).setWarmupTasks(null).setPartitionsByUserEndpoint(null));
        emit(out, "describe-response-null-topology", 89, "StreamsGroupDescribeResponse",
             new StreamsGroupDescribeResponseData().setGroups(List.of(new StreamsGroupDescribeResponseData.DescribedGroup()
                 .setErrorCode((short) 15).setErrorMessage(null).setTopology(null))));
        emit(out, "describe-response-null-subtopologies", 89, "StreamsGroupDescribeResponse",
             new StreamsGroupDescribeResponseData().setGroups(List.of(new StreamsGroupDescribeResponseData.DescribedGroup()
                 .setTopology(new StreamsGroupDescribeResponseData.Topology().setSubtopologies(null)))));
        emit(out, "describe-response-empty-subtopologies", 89, "StreamsGroupDescribeResponse",
             new StreamsGroupDescribeResponseData().setGroups(List.of(new StreamsGroupDescribeResponseData.DescribedGroup()
                 .setTopology(new StreamsGroupDescribeResponseData.Topology().setSubtopologies(List.of())))));
        // These are source-inspected generated-reader leniencies. Actual outcomes are recorded below.
        byte[] heartbeatBoolean = bytes(new StreamsGroupHeartbeatRequestData());
        if (heartbeatBoolean.length != 27) { throw new IllegalStateException("Default heartbeat shape changed"); }
        heartbeatBoolean[25] = 2;
        record(out, "heartbeat-request-noncanonical-boolean", 88, "StreamsGroupHeartbeatRequest", heartbeatBoolean, "accept-normalize-boolean");
        byte[] heartbeatMarkers = bytes(new StreamsGroupHeartbeatRequestData());
        heartbeatMarkers[16] = (byte) -2;
        heartbeatMarkers[21] = (byte) -128;
        record(out, "heartbeat-request-negative-null-markers", 88, "StreamsGroupHeartbeatRequest", heartbeatMarkers, "accept-normalize-null-marker");
        byte[] describeBoolean = bytes(new StreamsGroupDescribeRequestData());
        if (describeBoolean.length != 3) { throw new IllegalStateException("Default describe shape changed"); }
        describeBoolean[1] = (byte) -1;
        record(out, "describe-request-noncanonical-boolean", 89, "StreamsGroupDescribeRequest", describeBoolean, "accept-normalize-boolean");
        record(out, "describe-request-count-bomb", 89, "StreamsGroupDescribeRequest", new byte[] {(byte) 255, (byte) 255, (byte) 255, (byte) 255, 15, 0, 0}, "reject-bounded-count");
        record(out, "describe-request-null-nonnullable-array", 89, "StreamsGroupDescribeRequest", new byte[] {0, 0, 0}, "reject-nonnullable-null");
        record(out, "describe-request-invalid-utf8", 89, "StreamsGroupDescribeRequest", new byte[] {2, 2, (byte) 255, 0, 0}, "reject-local-utf8");
        Files.writeString(out.resolve("cases.tsv"), "name\tapi\ttype\tversion\tapache_outcome\tremaining\trust_policy\tsha256\n" + String.join("", INDEX));
        System.out.println("{\"actual_generated_cases\":" + INDEX.size() + ",\"runtime_broker_claim\":false}");
    }
}
