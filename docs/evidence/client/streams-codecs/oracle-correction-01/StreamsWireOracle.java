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
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
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
        ByteBuffer encoded = MessageUtil.toByteBufferAccessor(message, VERSION).buffer();
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
            .setGroupId("StreamsGroupHeartbeatRequest.GroupId-詳細")
            .setMemberId("StreamsGroupHeartbeatRequest.MemberId-詳細")
            .setMemberEpoch(-1000003)
            .setEndpointInformationEpoch(-1000004)
            .setInstanceId("StreamsGroupHeartbeatRequest.InstanceId-詳細")
            .setRackId("StreamsGroupHeartbeatRequest.RackId-詳細")
            .setRebalanceTimeoutMs(-1000007)
            .setTopology(new StreamsGroupHeartbeatRequestData.Topology()
                .setEpoch(-1000008)
                .setSubtopologies(List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                    .setSubtopologyId("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SubtopologyId-詳細")
                    .setSourceTopics(List.of("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopics[0]-詳細", "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopics[1]-詳細"))
                    .setSourceTopicRegex(List.of("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopicRegex[0]-詳細", "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].SourceTopicRegex[1]-詳細"))
                    .setStateChangelogTopics(List.of(new StreamsGroupHeartbeatRequestData.TopicInfo()
                        .setName("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].StateChangelogTopics[0].Name-詳細")
                        .setPartitions(-1000015)
                        .setReplicationFactor((short) -9984)
                        .setTopicConfigs(List.of(new StreamsGroupHeartbeatRequestData.KeyValue()
                            .setKey("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Key-詳細")
                            .setValue("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Value-詳細")))))
                    .setRepartitionSinkTopics(List.of("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSinkTopics[0]-詳細", "StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSinkTopics[1]-詳細"))
                    .setRepartitionSourceTopics(List.of(new StreamsGroupHeartbeatRequestData.TopicInfo()
                        .setName("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSourceTopics[0].Name-詳細")
                        .setPartitions(-1000022)
                        .setReplicationFactor((short) -9977)
                        .setTopicConfigs(List.of(new StreamsGroupHeartbeatRequestData.KeyValue()
                            .setKey("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Key-詳細")
                            .setValue("StreamsGroupHeartbeatRequest.Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Value-詳細")))))
                    .setCopartitionGroups(List.of(new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                        .setSourceTopics(List.of((short) -9974, (short) -9973))
                        .setSourceTopicRegex(List.of((short) -9972, (short) -9971))
                        .setRepartitionSourceTopics(List.of((short) -9970, (short) -9969)))))))
            .setActiveTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                .setSubtopologyId("StreamsGroupHeartbeatRequest.ActiveTasks[0].SubtopologyId-詳細")
                .setPartitions(List.of(-1000033, -1000034))))
            .setStandbyTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                .setSubtopologyId("StreamsGroupHeartbeatRequest.StandbyTasks[0].SubtopologyId-詳細")
                .setPartitions(List.of(-1000036, -1000037))))
            .setWarmupTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                .setSubtopologyId("StreamsGroupHeartbeatRequest.WarmupTasks[0].SubtopologyId-詳細")
                .setPartitions(List.of(-1000039, -1000040))))
            .setProcessId("StreamsGroupHeartbeatRequest.ProcessId-詳細")
            .setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint()
                .setHost("StreamsGroupHeartbeatRequest.UserEndpoint.Host-詳細")
                .setPort(65492))
            .setClientTags(List.of(new StreamsGroupHeartbeatRequestData.KeyValue()
                .setKey("StreamsGroupHeartbeatRequest.ClientTags[0].Key-詳細")
                .setValue("StreamsGroupHeartbeatRequest.ClientTags[0].Value-詳細")))
            .setTaskOffsets(List.of(new StreamsGroupHeartbeatRequestData.TaskOffset()
                .setSubtopologyId("StreamsGroupHeartbeatRequest.TaskOffsets[0].SubtopologyId-詳細")
                .setPartition(-1000047)
                .setOffset(Long.MIN_VALUE + 48L)))
            .setTaskEndOffsets(List.of(new StreamsGroupHeartbeatRequestData.TaskOffset()
                .setSubtopologyId("StreamsGroupHeartbeatRequest.TaskEndOffsets[0].SubtopologyId-詳細")
                .setPartition(-1000050)
                .setOffset(Long.MIN_VALUE + 51L)))
            .setShutdownApplication(true);
    }

    private static void clear_StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest(StreamsGroupHeartbeatRequestData value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        clear_StreamsGroupHeartbeatRequest_Topology(value.topology());
        if (value.activeTasks() != null) {
            for (StreamsGroupHeartbeatRequestData.TaskIds element : value.activeTasks()) { clear_StreamsGroupHeartbeatRequest_TaskIds(element); }
        }
        if (value.standbyTasks() != null) {
            for (StreamsGroupHeartbeatRequestData.TaskIds element : value.standbyTasks()) { clear_StreamsGroupHeartbeatRequest_TaskIds(element); }
        }
        if (value.warmupTasks() != null) {
            for (StreamsGroupHeartbeatRequestData.TaskIds element : value.warmupTasks()) { clear_StreamsGroupHeartbeatRequest_TaskIds(element); }
        }
        clear_StreamsGroupHeartbeatRequest_Endpoint(value.userEndpoint());
        if (value.clientTags() != null) {
            for (StreamsGroupHeartbeatRequestData.KeyValue element : value.clientTags()) { clear_StreamsGroupHeartbeatRequest_KeyValue(element); }
        }
        if (value.taskOffsets() != null) {
            for (StreamsGroupHeartbeatRequestData.TaskOffset element : value.taskOffsets()) { clear_StreamsGroupHeartbeatRequest_TaskOffset(element); }
        }
        if (value.taskEndOffsets() != null) {
            for (StreamsGroupHeartbeatRequestData.TaskOffset element : value.taskEndOffsets()) { clear_StreamsGroupHeartbeatRequest_TaskOffset(element); }
        }
    }

    private static void clear_StreamsGroupHeartbeatRequest_KeyValue(StreamsGroupHeartbeatRequestData.KeyValue value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatRequest_TopicInfo(StreamsGroupHeartbeatRequestData.TopicInfo value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.topicConfigs() != null) {
            for (StreamsGroupHeartbeatRequestData.KeyValue element : value.topicConfigs()) { clear_StreamsGroupHeartbeatRequest_KeyValue(element); }
        }
    }

    private static void clear_StreamsGroupHeartbeatRequest_Endpoint(StreamsGroupHeartbeatRequestData.Endpoint value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatRequest_TaskOffset(StreamsGroupHeartbeatRequestData.TaskOffset value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatRequest_TaskIds(StreamsGroupHeartbeatRequestData.TaskIds value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatRequest_Topology(StreamsGroupHeartbeatRequestData.Topology value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.subtopologies() != null) {
            for (StreamsGroupHeartbeatRequestData.Subtopology element : value.subtopologies()) { clear_StreamsGroupHeartbeatRequest_Subtopology(element); }
        }
    }

    private static void clear_StreamsGroupHeartbeatRequest_Subtopology(StreamsGroupHeartbeatRequestData.Subtopology value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.stateChangelogTopics() != null) {
            for (StreamsGroupHeartbeatRequestData.TopicInfo element : value.stateChangelogTopics()) { clear_StreamsGroupHeartbeatRequest_TopicInfo(element); }
        }
        if (value.repartitionSourceTopics() != null) {
            for (StreamsGroupHeartbeatRequestData.TopicInfo element : value.repartitionSourceTopics()) { clear_StreamsGroupHeartbeatRequest_TopicInfo(element); }
        }
        if (value.copartitionGroups() != null) {
            for (StreamsGroupHeartbeatRequestData.CopartitionGroup element : value.copartitionGroups()) { clear_StreamsGroupHeartbeatRequest_CopartitionGroup(element); }
        }
    }

    private static void clear_StreamsGroupHeartbeatRequest_CopartitionGroup(StreamsGroupHeartbeatRequestData.CopartitionGroup value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static StreamsGroupHeartbeatResponseData StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse() {
        return new StreamsGroupHeartbeatResponseData()
            .setThrottleTimeMs(-1000053)
            .setErrorCode((short) -9946)
            .setErrorMessage("StreamsGroupHeartbeatResponse.ErrorMessage-詳細")
            .setMemberId("StreamsGroupHeartbeatResponse.MemberId-詳細")
            .setMemberEpoch(-1000057)
            .setHeartbeatIntervalMs(-1000058)
            .setAcceptableRecoveryLag(-1000059)
            .setTaskOffsetIntervalMs(-1000060)
            .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                .setStatusCode((byte) -59)
                .setStatusDetail("StreamsGroupHeartbeatResponse.Status[0].StatusDetail-詳細")))
            .setActiveTasks(List.of(new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId("StreamsGroupHeartbeatResponse.ActiveTasks[0].SubtopologyId-詳細")
                .setPartitions(List.of(-1000064, -1000065))))
            .setStandbyTasks(List.of(new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId("StreamsGroupHeartbeatResponse.StandbyTasks[0].SubtopologyId-詳細")
                .setPartitions(List.of(-1000067, -1000068))))
            .setWarmupTasks(List.of(new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId("StreamsGroupHeartbeatResponse.WarmupTasks[0].SubtopologyId-詳細")
                .setPartitions(List.of(-1000070, -1000071))))
            .setEndpointInformationEpoch(-1000072)
            .setPartitionsByUserEndpoint(List.of(new StreamsGroupHeartbeatResponseData.EndpointToPartitions()
                .setUserEndpoint(new StreamsGroupHeartbeatResponseData.Endpoint()
                    .setHost("StreamsGroupHeartbeatResponse.PartitionsByUserEndpoint[0].UserEndpoint.Host-詳細")
                    .setPort(65461))
                .setActivePartitions(List.of(new StreamsGroupHeartbeatResponseData.TopicPartition()
                    .setTopic("StreamsGroupHeartbeatResponse.PartitionsByUserEndpoint[0].ActivePartitions[0].Topic-詳細")
                    .setPartitions(List.of(-1000076, -1000077))))
                .setStandbyPartitions(List.of(new StreamsGroupHeartbeatResponseData.TopicPartition()
                    .setTopic("StreamsGroupHeartbeatResponse.PartitionsByUserEndpoint[0].StandbyPartitions[0].Topic-詳細")
                    .setPartitions(List.of(-1000079, -1000080))))));
    }

    private static void clear_StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse(StreamsGroupHeartbeatResponseData value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.status() != null) {
            for (StreamsGroupHeartbeatResponseData.Status element : value.status()) { clear_StreamsGroupHeartbeatResponse_Status(element); }
        }
        if (value.activeTasks() != null) {
            for (StreamsGroupHeartbeatResponseData.TaskIds element : value.activeTasks()) { clear_StreamsGroupHeartbeatResponse_TaskIds(element); }
        }
        if (value.standbyTasks() != null) {
            for (StreamsGroupHeartbeatResponseData.TaskIds element : value.standbyTasks()) { clear_StreamsGroupHeartbeatResponse_TaskIds(element); }
        }
        if (value.warmupTasks() != null) {
            for (StreamsGroupHeartbeatResponseData.TaskIds element : value.warmupTasks()) { clear_StreamsGroupHeartbeatResponse_TaskIds(element); }
        }
        if (value.partitionsByUserEndpoint() != null) {
            for (StreamsGroupHeartbeatResponseData.EndpointToPartitions element : value.partitionsByUserEndpoint()) { clear_StreamsGroupHeartbeatResponse_EndpointToPartitions(element); }
        }
    }

    private static void clear_StreamsGroupHeartbeatResponse_Status(StreamsGroupHeartbeatResponseData.Status value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatResponse_TopicPartition(StreamsGroupHeartbeatResponseData.TopicPartition value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatResponse_TaskIds(StreamsGroupHeartbeatResponseData.TaskIds value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatResponse_Endpoint(StreamsGroupHeartbeatResponseData.Endpoint value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupHeartbeatResponse_EndpointToPartitions(StreamsGroupHeartbeatResponseData.EndpointToPartitions value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        clear_StreamsGroupHeartbeatResponse_Endpoint(value.userEndpoint());
        if (value.activePartitions() != null) {
            for (StreamsGroupHeartbeatResponseData.TopicPartition element : value.activePartitions()) { clear_StreamsGroupHeartbeatResponse_TopicPartition(element); }
        }
        if (value.standbyPartitions() != null) {
            for (StreamsGroupHeartbeatResponseData.TopicPartition element : value.standbyPartitions()) { clear_StreamsGroupHeartbeatResponse_TopicPartition(element); }
        }
    }

    private static StreamsGroupDescribeRequestData StreamsGroupDescribeRequest_StreamsGroupDescribeRequest() {
        return new StreamsGroupDescribeRequestData()
            .setGroupIds(List.of("StreamsGroupDescribeRequest.GroupIds[0]-詳細", "StreamsGroupDescribeRequest.GroupIds[1]-詳細"))
            .setIncludeAuthorizedOperations(true);
    }

    private static void clear_StreamsGroupDescribeRequest_StreamsGroupDescribeRequest(StreamsGroupDescribeRequestData value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static StreamsGroupDescribeResponseData StreamsGroupDescribeResponse_StreamsGroupDescribeResponse() {
        return new StreamsGroupDescribeResponseData()
            .setThrottleTimeMs(-1000084)
            .setGroups(List.of(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setErrorCode((short) -9915)
                .setErrorMessage("StreamsGroupDescribeResponse.Groups[0].ErrorMessage-詳細")
                .setGroupId("StreamsGroupDescribeResponse.Groups[0].GroupId-詳細")
                .setGroupState("StreamsGroupDescribeResponse.Groups[0].GroupState-詳細")
                .setGroupEpoch(-1000089)
                .setAssignmentEpoch(-1000090)
                .setTopology(new StreamsGroupDescribeResponseData.Topology()
                    .setEpoch(-1000091)
                    .setSubtopologies(List.of(new StreamsGroupDescribeResponseData.Subtopology()
                        .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].SubtopologyId-詳細")
                        .setSourceTopics(List.of("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].SourceTopics[0]-詳細", "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].SourceTopics[1]-詳細"))
                        .setRepartitionSinkTopics(List.of("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSinkTopics[0]-詳細", "StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSinkTopics[1]-詳細"))
                        .setStateChangelogTopics(List.of(new StreamsGroupDescribeResponseData.TopicInfo()
                            .setName("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].StateChangelogTopics[0].Name-詳細")
                            .setPartitions(-1000098)
                            .setReplicationFactor((short) -9901)
                            .setTopicConfigs(List.of(new StreamsGroupDescribeResponseData.KeyValue()
                                .setKey("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Key-詳細")
                                .setValue("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].StateChangelogTopics[0].TopicConfigs[0].Value-詳細")))))
                        .setRepartitionSourceTopics(List.of(new StreamsGroupDescribeResponseData.TopicInfo()
                            .setName("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSourceTopics[0].Name-詳細")
                            .setPartitions(-1000103)
                            .setReplicationFactor((short) -9896)
                            .setTopicConfigs(List.of(new StreamsGroupDescribeResponseData.KeyValue()
                                .setKey("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Key-詳細")
                                .setValue("StreamsGroupDescribeResponse.Groups[0].Topology.Subtopologies[0].RepartitionSourceTopics[0].TopicConfigs[0].Value-詳細"))))))))
                .setMembers(List.of(new StreamsGroupDescribeResponseData.Member()
                    .setMemberId("StreamsGroupDescribeResponse.Groups[0].Members[0].MemberId-詳細")
                    .setMemberEpoch(-1000108)
                    .setInstanceId("StreamsGroupDescribeResponse.Groups[0].Members[0].InstanceId-詳細")
                    .setRackId("StreamsGroupDescribeResponse.Groups[0].Members[0].RackId-詳細")
                    .setClientId("StreamsGroupDescribeResponse.Groups[0].Members[0].ClientId-詳細")
                    .setClientHost("StreamsGroupDescribeResponse.Groups[0].Members[0].ClientHost-詳細")
                    .setTopologyEpoch(-1000113)
                    .setProcessId("StreamsGroupDescribeResponse.Groups[0].Members[0].ProcessId-詳細")
                    .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint()
                        .setHost("StreamsGroupDescribeResponse.Groups[0].Members[0].UserEndpoint.Host-詳細")
                        .setPort(65419))
                    .setClientTags(List.of(new StreamsGroupDescribeResponseData.KeyValue()
                        .setKey("StreamsGroupDescribeResponse.Groups[0].Members[0].ClientTags[0].Key-詳細")
                        .setValue("StreamsGroupDescribeResponse.Groups[0].Members[0].ClientTags[0].Value-詳細")))
                    .setTaskOffsets(List.of(new StreamsGroupDescribeResponseData.TaskOffset()
                        .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].TaskOffsets[0].SubtopologyId-詳細")
                        .setPartition(-1000120)
                        .setOffset(Long.MIN_VALUE + 121L)))
                    .setTaskEndOffsets(List.of(new StreamsGroupDescribeResponseData.TaskOffset()
                        .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].TaskEndOffsets[0].SubtopologyId-詳細")
                        .setPartition(-1000123)
                        .setOffset(Long.MIN_VALUE + 124L)))
                    .setAssignment(new StreamsGroupDescribeResponseData.Assignment()
                        .setActiveTasks(List.of(new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].Assignment.ActiveTasks[0].SubtopologyId-詳細")
                            .setPartitions(List.of(-1000126, -1000127))))
                        .setStandbyTasks(List.of(new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].Assignment.StandbyTasks[0].SubtopologyId-詳細")
                            .setPartitions(List.of(-1000129, -1000130))))
                        .setWarmupTasks(List.of(new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].Assignment.WarmupTasks[0].SubtopologyId-詳細")
                            .setPartitions(List.of(-1000132, -1000133)))))
                    .setTargetAssignment(new StreamsGroupDescribeResponseData.Assignment()
                        .setActiveTasks(List.of(new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].TargetAssignment.ActiveTasks[0].SubtopologyId-詳細")
                            .setPartitions(List.of(-1000135, -1000136))))
                        .setStandbyTasks(List.of(new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].TargetAssignment.StandbyTasks[0].SubtopologyId-詳細")
                            .setPartitions(List.of(-1000138, -1000139))))
                        .setWarmupTasks(List.of(new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId("StreamsGroupDescribeResponse.Groups[0].Members[0].TargetAssignment.WarmupTasks[0].SubtopologyId-詳細")
                            .setPartitions(List.of(-1000141, -1000142)))))
                    .setIsClassic(true)))
                .setAuthorizedOperations(-1000144)));
    }

    private static void clear_StreamsGroupDescribeResponse_StreamsGroupDescribeResponse(StreamsGroupDescribeResponseData value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.groups() != null) {
            for (StreamsGroupDescribeResponseData.DescribedGroup element : value.groups()) { clear_StreamsGroupDescribeResponse_DescribedGroup(element); }
        }
    }

    private static void clear_StreamsGroupDescribeResponse_Endpoint(StreamsGroupDescribeResponseData.Endpoint value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupDescribeResponse_TaskOffset(StreamsGroupDescribeResponseData.TaskOffset value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupDescribeResponse_Assignment(StreamsGroupDescribeResponseData.Assignment value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.activeTasks() != null) {
            for (StreamsGroupDescribeResponseData.TaskIds element : value.activeTasks()) { clear_StreamsGroupDescribeResponse_TaskIds(element); }
        }
        if (value.standbyTasks() != null) {
            for (StreamsGroupDescribeResponseData.TaskIds element : value.standbyTasks()) { clear_StreamsGroupDescribeResponse_TaskIds(element); }
        }
        if (value.warmupTasks() != null) {
            for (StreamsGroupDescribeResponseData.TaskIds element : value.warmupTasks()) { clear_StreamsGroupDescribeResponse_TaskIds(element); }
        }
    }

    private static void clear_StreamsGroupDescribeResponse_TaskIds(StreamsGroupDescribeResponseData.TaskIds value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupDescribeResponse_KeyValue(StreamsGroupDescribeResponseData.KeyValue value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
    }

    private static void clear_StreamsGroupDescribeResponse_TopicInfo(StreamsGroupDescribeResponseData.TopicInfo value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.topicConfigs() != null) {
            for (StreamsGroupDescribeResponseData.KeyValue element : value.topicConfigs()) { clear_StreamsGroupDescribeResponse_KeyValue(element); }
        }
    }

    private static void clear_StreamsGroupDescribeResponse_DescribedGroup(StreamsGroupDescribeResponseData.DescribedGroup value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        clear_StreamsGroupDescribeResponse_Topology(value.topology());
        if (value.members() != null) {
            for (StreamsGroupDescribeResponseData.Member element : value.members()) { clear_StreamsGroupDescribeResponse_Member(element); }
        }
    }

    private static void clear_StreamsGroupDescribeResponse_Topology(StreamsGroupDescribeResponseData.Topology value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.subtopologies() != null) {
            for (StreamsGroupDescribeResponseData.Subtopology element : value.subtopologies()) { clear_StreamsGroupDescribeResponse_Subtopology(element); }
        }
    }

    private static void clear_StreamsGroupDescribeResponse_Subtopology(StreamsGroupDescribeResponseData.Subtopology value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        if (value.stateChangelogTopics() != null) {
            for (StreamsGroupDescribeResponseData.TopicInfo element : value.stateChangelogTopics()) { clear_StreamsGroupDescribeResponse_TopicInfo(element); }
        }
        if (value.repartitionSourceTopics() != null) {
            for (StreamsGroupDescribeResponseData.TopicInfo element : value.repartitionSourceTopics()) { clear_StreamsGroupDescribeResponse_TopicInfo(element); }
        }
    }

    private static void clear_StreamsGroupDescribeResponse_Member(StreamsGroupDescribeResponseData.Member value) {
        if (value == null) { return; }
        value.unknownTaggedFields().clear();
        clear_StreamsGroupDescribeResponse_Endpoint(value.userEndpoint());
        if (value.clientTags() != null) {
            for (StreamsGroupDescribeResponseData.KeyValue element : value.clientTags()) { clear_StreamsGroupDescribeResponse_KeyValue(element); }
        }
        if (value.taskOffsets() != null) {
            for (StreamsGroupDescribeResponseData.TaskOffset element : value.taskOffsets()) { clear_StreamsGroupDescribeResponse_TaskOffset(element); }
        }
        if (value.taskEndOffsets() != null) {
            for (StreamsGroupDescribeResponseData.TaskOffset element : value.taskEndOffsets()) { clear_StreamsGroupDescribeResponse_TaskOffset(element); }
        }
        clear_StreamsGroupDescribeResponse_Assignment(value.assignment());
        clear_StreamsGroupDescribeResponse_Assignment(value.targetAssignment());
    }

    private static void clearTags(Message message) {
        switch (message) {
            case StreamsGroupHeartbeatRequestData value -> clear_StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest(value);
            case StreamsGroupHeartbeatResponseData value -> clear_StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse(value);
            case StreamsGroupDescribeRequestData value -> clear_StreamsGroupDescribeRequest_StreamsGroupDescribeRequest(value);
            case StreamsGroupDescribeResponseData value -> clear_StreamsGroupDescribeResponse_StreamsGroupDescribeResponse(value);
            default -> throw new IllegalArgumentException("Unknown message");
        }
    }

    private static void reverse(Path index) throws Exception {
        int count = 0;
        for (String row : Files.readAllLines(index)) {
            if (row.isEmpty() || row.startsWith("name\t")) { continue; }
            String[] cells = row.split("\t", -1);
            if (cells.length != 4) { throw new IllegalArgumentException("Invalid reverse index"); }
            byte[] original = Files.readAllBytes(index.getParent().resolve(cells[2]));
            byte[] rust = Files.readAllBytes(index.getParent().resolve(cells[3]));
            ByteBuffer expectedBody = ByteBuffer.wrap(original);
            ByteBuffer actualBody = ByteBuffer.wrap(rust);
            Message expected = parse(cells[1], expectedBody);
            Message actual = parse(cells[1], actualBody);
            clearTags(expected);
            if (expectedBody.hasRemaining() || actualBody.hasRemaining() || !expected.equals(actual) ||
                !Arrays.equals(bytes(expected), rust)) {
                throw new IllegalStateException("Actual Apache reverse parse mismatch: " + cells[0]);
            }
            String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(rust));
            System.out.println(cells[0] + "\t" + rust.length + "\tconsumed\tsemantic-equal\t" + hash);
            count++;
        }
        System.out.println("{\"actual_reverse_cases\":" + count + ",\"runtime_broker_claim\":false}");
    }

    private static final List<String> HEADER_INDEX = new ArrayList<>();
    private static byte[] serialize(Message message, short version) {
        ByteBuffer encoded = MessageUtil.toByteBufferAccessor(message, version).buffer();
        byte[] body = new byte[encoded.remaining()];
        encoded.get(body);
        return body;
    }

    private static void emitHeader(Path out, String name, int api, String type, boolean request,
                                   String client, boolean tags, Message message) throws Exception {
        int correlation = 0x11223300 + api;
        Message header;
        short headerVersion;
        if (request) {
            RequestHeaderData data = new RequestHeaderData().setRequestApiKey((short) api)
                .setRequestApiVersion(VERSION).setCorrelationId(correlation).setClientId(client);
            if (tags) { data.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {2, 3})); }
            header = data;
            headerVersion = 2;
        } else {
            ResponseHeaderData data = new ResponseHeaderData().setCorrelationId(correlation);
            if (tags) { data.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {4})); }
            header = data;
            headerVersion = 1;
        }
        byte[] encodedHeader = serialize(header, headerVersion);
        byte[] body = bytes(message);
        byte[] full = Arrays.copyOf(encodedHeader, encodedHeader.length + body.length);
        System.arraycopy(body, 0, full, encodedHeader.length, body.length);
        ByteBuffer input = ByteBuffer.wrap(full);
        Message decodedHeader = request ? new RequestHeaderData(new ByteBufferAccessor(input), headerVersion) :
            new ResponseHeaderData(new ByteBufferAccessor(input), headerVersion);
        if (input.position() != encodedHeader.length || !header.equals(decodedHeader)) {
            throw new IllegalStateException("Actual header parse mismatch");
        }
        Message decodedBody = parse(type, input);
        if (input.hasRemaining() || !message.equals(decodedBody)) {
            throw new IllegalStateException("Actual header/body parse mismatch");
        }
        Files.write(out.resolve(name + ".bin"), full);
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(full));
        String clientLabel = client == null ? "null" : client.isEmpty() ? "empty" : "utf8";
        HEADER_INDEX.add(name + "\t" + api + "\t" + type + "\t" + request + "\t" + clientLabel +
                         "\t" + correlation + "\t" + headerVersion + "\t" + encodedHeader.length +
                         "\t" + body.length + "\t" + hash + "\n");
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length == 2 && arguments[0].equals("--reverse")) { reverse(Path.of(arguments[1])); return; }
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
        {
            StreamsGroupHeartbeatRequestData message = StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest();
            message.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().subtopologies().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().subtopologies().get(0).stateChangelogTopics().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().subtopologies().get(0).stateChangelogTopics().get(0).topicConfigs().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().subtopologies().get(0).repartitionSourceTopics().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().subtopologies().get(0).repartitionSourceTopics().get(0).topicConfigs().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.topology().subtopologies().get(0).copartitionGroups().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.activeTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.standbyTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.warmupTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.userEndpoint().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.clientTags().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.taskOffsets().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.taskEndOffsets().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            emit(out, "heartbeat-request-unknown-all-structures", 88, "StreamsGroupHeartbeatRequest", message);
        }
        emitHeader(out, "heartbeat-request-header-null", 88, "StreamsGroupHeartbeatRequest", true, null, false, StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest());
        emitHeader(out, "heartbeat-request-header-empty", 88, "StreamsGroupHeartbeatRequest", true, "", false, StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest());
        emitHeader(out, "heartbeat-request-header-utf8", 88, "StreamsGroupHeartbeatRequest", true, "streams-client-詳細", false, StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest());
        emitHeader(out, "heartbeat-request-header-tags", 88, "StreamsGroupHeartbeatRequest", true, "streams-client-詳細", true, StreamsGroupHeartbeatRequest_StreamsGroupHeartbeatRequest());
        {
            StreamsGroupHeartbeatResponseData message = StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse();
            message.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.status().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.activeTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.standbyTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.warmupTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.partitionsByUserEndpoint().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.partitionsByUserEndpoint().get(0).userEndpoint().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.partitionsByUserEndpoint().get(0).activePartitions().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.partitionsByUserEndpoint().get(0).standbyPartitions().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            emit(out, "heartbeat-response-unknown-all-structures", 88, "StreamsGroupHeartbeatResponse", message);
        }
        emitHeader(out, "heartbeat-response-header-empty", 88, "StreamsGroupHeartbeatResponse", false, null, false, StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse());
        emitHeader(out, "heartbeat-response-header-tags", 88, "StreamsGroupHeartbeatResponse", false, null, true, StreamsGroupHeartbeatResponse_StreamsGroupHeartbeatResponse());
        {
            StreamsGroupDescribeRequestData message = StreamsGroupDescribeRequest_StreamsGroupDescribeRequest();
            message.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            emit(out, "describe-request-unknown-all-structures", 89, "StreamsGroupDescribeRequest", message);
        }
        emitHeader(out, "describe-request-header-null", 89, "StreamsGroupDescribeRequest", true, null, false, StreamsGroupDescribeRequest_StreamsGroupDescribeRequest());
        emitHeader(out, "describe-request-header-empty", 89, "StreamsGroupDescribeRequest", true, "", false, StreamsGroupDescribeRequest_StreamsGroupDescribeRequest());
        emitHeader(out, "describe-request-header-utf8", 89, "StreamsGroupDescribeRequest", true, "streams-client-詳細", false, StreamsGroupDescribeRequest_StreamsGroupDescribeRequest());
        emitHeader(out, "describe-request-header-tags", 89, "StreamsGroupDescribeRequest", true, "streams-client-詳細", true, StreamsGroupDescribeRequest_StreamsGroupDescribeRequest());
        {
            StreamsGroupDescribeResponseData message = StreamsGroupDescribeResponse_StreamsGroupDescribeResponse();
            message.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).topology().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).topology().subtopologies().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).topology().subtopologies().get(0).stateChangelogTopics().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).topology().subtopologies().get(0).stateChangelogTopics().get(0).topicConfigs().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).topology().subtopologies().get(0).repartitionSourceTopics().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).topology().subtopologies().get(0).repartitionSourceTopics().get(0).topicConfigs().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).userEndpoint().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).clientTags().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).taskOffsets().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).taskEndOffsets().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).assignment().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).assignment().activeTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).assignment().standbyTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).assignment().warmupTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).targetAssignment().unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).targetAssignment().activeTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).targetAssignment().standbyTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            message.groups().get(0).members().get(0).targetAssignment().warmupTasks().get(0).unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            emit(out, "describe-response-unknown-all-structures", 89, "StreamsGroupDescribeResponse", message);
        }
        emitHeader(out, "describe-response-header-empty", 89, "StreamsGroupDescribeResponse", false, null, false, StreamsGroupDescribeResponse_StreamsGroupDescribeResponse());
        emitHeader(out, "describe-response-header-tags", 89, "StreamsGroupDescribeResponse", false, null, true, StreamsGroupDescribeResponse_StreamsGroupDescribeResponse());
        Files.writeString(out.resolve("headers.tsv"), "name\tapi\ttype\trequest\tclient_id\tcorrelation\theader_version\theader_bytes\tbody_bytes\tsha256\n" + String.join("", HEADER_INDEX));
        Files.writeString(out.resolve("cases.tsv"), "name\tapi\ttype\tversion\tapache_outcome\tremaining\trust_policy\tsha256\n" + String.join("", INDEX));
        System.out.println("{\"actual_generated_cases\":" + INDEX.size() + ",\"runtime_broker_claim\":false}");
    }
}
