/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.internal.UnalignedMemoryRecords;

/**
 * Body bytes from Apache kafka-clients 4.3.1 only.
 * These versions stay off the controller advertisement.
 */
public final class KraftLayoutApache {
    static final String JAR_SHA = "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36";

    static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) throw new AssertionError("size/write mismatch");
        return buffer.array();
    }

    static Uuid filled(int value) {
        long bits = 0;
        for (int i = 0; i < 8; i++) bits = (bits << 8) | (value & 0xff);
        return new Uuid(bits, bits);
    }

    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    static void output(Path path, byte[] bytes, boolean verify) throws Exception {
        if (verify) {
            if (!Arrays.equals(Files.readAllBytes(path), bytes))
                throw new AssertionError("fixture differs: " + path);
        } else {
            Files.write(path, bytes);
        }
        System.out.println(path.getFileName() + " " + sha(bytes));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3)
            throw new IllegalArgumentException("<kafka-clients-4.3.1.jar> <out-dir> [--verify]");
        boolean verify = args.length == 3 && args[2].equals("--verify");
        if (args.length == 3 && !verify) throw new IllegalArgumentException("unknown option");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(VoteRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(JAR_SHA))
            throw new AssertionError("use kafka-clients 4.3.1 sha256 " + JAR_SHA);
        Path dir = Path.of(args[1]);
        Files.createDirectories(dir);
        output(dir.resolve("vote-v0.bin"), encode(vote((short) 0), (short) 0), verify);
        output(dir.resolve("vote-v2.bin"), encode(vote((short) 2), (short) 2), verify);
        output(dir.resolve("fetch-snapshot-v1.bin"), encode(fetchReq(), (short) 1), verify);
        output(dir.resolve("begin-v1.bin"), encode(begin(), (short) 1), verify);
        output(dir.resolve("end-v1.bin"), encode(end(), (short) 1), verify);
        output(dir.resolve("update-voter.bin"), encode(update(), (short) 0), verify);
        output(dir.resolve("voters-record.bin"), encode(voters(), (short) 0), verify);
        output(dir.resolve("fetch-snapshot-response-v1.bin"), encode(fetchResp(), (short) 1), verify);
        output(dir.resolve("describe-quorum-v2.bin"), encode(describe(true), (short) 2), verify);
        output(dir.resolve("describe-quorum-v2-empty.bin"), encode(describe(false), (short) 2), verify);
        output(dir.resolve("add-voter-v0.bin"), encode(add(false), (short) 0), verify);
        output(dir.resolve("add-voter-v1.bin"), encode(add(true), (short) 1), verify);
        output(dir.resolve("remove-voter.bin"), encode(remove(), (short) 0), verify);
        output(dir.resolve("vote-response-v0.bin"), encode(voteResponse(false), (short) 0), verify);
        output(dir.resolve("vote-response-v1.bin"), encode(voteResponse(true), (short) 1), verify);
        output(dir.resolve("begin-response-v0.bin"), encode(beginResponse(false), (short) 0), verify);
        output(dir.resolve("begin-response-v1.bin"), encode(beginResponse(true), (short) 1), verify);
        output(dir.resolve("end-response-v0.bin"), encode(endResponse(false), (short) 0), verify);
        output(dir.resolve("end-response-v1.bin"), encode(endResponse(true), (short) 1), verify);
        output(dir.resolve("add-voter-response-v0.bin"), encode(addResponse(null), (short) 0), verify);
        output(dir.resolve("add-voter-response-v1-empty.bin"), encode(addResponse(""), (short) 1), verify);
        output(dir.resolve("remove-voter-response.bin"), encode(removeResponse(), (short) 0), verify);
        output(dir.resolve("update-voter-response.bin"), encode(updateResponse(), (short) 0), verify);
        output(dir.resolve("describe-quorum-request.bin"), encode(describeRequest(), (short) 2), verify);
        output(dir.resolve("describe-quorum-request-two.bin"), encode(describeRequestTwo(), (short) 2), verify);
    }

    static VoteRequestData vote(short version) {
        var part = new VoteRequestData.PartitionData()
            .setPartitionIndex(0)
            .setReplicaEpoch(version == 2 ? 4 : 1)
            .setReplicaId(2)
            .setLastOffsetEpoch(version == 2 ? 1 : 0)
            .setLastOffset(version == 2 ? 3 : 0);
        if (version >= 1) part.setReplicaDirectoryId(filled(7)).setVoterDirectoryId(filled(8));
        if (version >= 2) part.setPreVote(true);
        var topic = new VoteRequestData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var req = new VoteRequestData().setClusterId("cluster-1");
        if (version >= 1) req.setVoterId(1);
        req.topics().add(topic);
        return req;
    }

    static FetchSnapshotRequestData fetchReq() {
        var part = new FetchSnapshotRequestData.PartitionSnapshot()
            .setPartition(0)
            .setCurrentLeaderEpoch(2)
            .setSnapshotId(new FetchSnapshotRequestData.SnapshotId().setEndOffset(10).setEpoch(2))
            .setPosition(0)
            .setReplicaDirectoryId(filled(7));
        var topic = new FetchSnapshotRequestData.TopicSnapshot().setName("__cluster_metadata");
        topic.partitions().add(part);
        var req = new FetchSnapshotRequestData().setReplicaId(3).setMaxBytes(1024).setClusterId("cluster-1");
        req.topics().add(topic);
        return req;
    }

    static BeginQuorumEpochRequestData begin() {
        var part = new BeginQuorumEpochRequestData.PartitionData()
            .setPartitionIndex(0)
            .setVoterDirectoryId(filled(6))
            .setLeaderId(2)
            .setLeaderEpoch(3);
        var topic = new BeginQuorumEpochRequestData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var endpoints = new BeginQuorumEpochRequestData.LeaderEndpointCollection();
        endpoints.add(new BeginQuorumEpochRequestData.LeaderEndpoint()
            .setName("CONTROLLER").setHost("localhost").setPort(9093));
        var req = new BeginQuorumEpochRequestData().setClusterId("cluster-1").setVoterId(1);
        req.topics().add(topic);
        req.setLeaderEndpoints(endpoints);
        return req;
    }

    static EndQuorumEpochRequestData end() {
        var part = new EndQuorumEpochRequestData.PartitionData()
            .setPartitionIndex(0)
            .setLeaderId(2)
            .setLeaderEpoch(3);
        part.preferredCandidates().add(new EndQuorumEpochRequestData.ReplicaInfo()
            .setCandidateId(1).setCandidateDirectoryId(filled(6)));
        var topic = new EndQuorumEpochRequestData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var endpoints = new EndQuorumEpochRequestData.LeaderEndpointCollection();
        endpoints.add(new EndQuorumEpochRequestData.LeaderEndpoint()
            .setName("CONTROLLER").setHost("localhost").setPort(9093));
        var req = new EndQuorumEpochRequestData().setClusterId("cluster-1");
        req.topics().add(topic);
        req.setLeaderEndpoints(endpoints);
        return req;
    }

    static UpdateRaftVoterRequestData update() {
        var listeners = new UpdateRaftVoterRequestData.ListenerCollection();
        listeners.add(new UpdateRaftVoterRequestData.Listener()
            .setName("CONTROLLER").setHost("localhost").setPort(9093));
        return new UpdateRaftVoterRequestData()
            .setClusterId("cluster-1")
            .setCurrentLeaderEpoch(3)
            .setVoterId(2)
            .setVoterDirectoryId(filled(7))
            .setListeners(listeners)
            .setKRaftVersionFeature(new UpdateRaftVoterRequestData.KRaftVersionFeature()
                .setMinSupportedVersion((short) 0)
                .setMaxSupportedVersion((short) 1));
    }

    static VotersRecord voters() {
        var endpoints = new VotersRecord.EndpointCollection();
        endpoints.add(new VotersRecord.Endpoint().setName("CONTROLLER").setHost("localhost").setPort(9093));
        var voter = new VotersRecord.Voter()
            .setVoterId(2)
            .setVoterDirectoryId(filled(7))
            .setEndpoints(endpoints)
            .setKRaftVersionFeature(new VotersRecord.KRaftVersionFeature()
                .setMinSupportedVersion((short) 0)
                .setMaxSupportedVersion((short) 1));
        var record = new VotersRecord().setVersion((short) 0);
        record.voters().add(voter);
        return record;
    }

    static FetchSnapshotResponseData fetchResp() {
        var part = new FetchSnapshotResponseData.PartitionSnapshot()
            .setIndex(0)
            .setErrorCode((short) 0)
            .setSnapshotId(new FetchSnapshotResponseData.SnapshotId().setEndOffset(4).setEpoch(1))
            .setCurrentLeader(new FetchSnapshotResponseData.LeaderIdAndEpoch().setLeaderId(2).setLeaderEpoch(1))
            .setSize(4)
            .setPosition(0)
            .setUnalignedRecords(new UnalignedMemoryRecords(ByteBuffer.wrap("snap".getBytes())));
        var topic = new FetchSnapshotResponseData.TopicSnapshot().setName("__cluster_metadata");
        topic.partitions().add(part);
        var endpoints = new FetchSnapshotResponseData.NodeEndpointCollection();
        endpoints.add(new FetchSnapshotResponseData.NodeEndpoint().setNodeId(2).setHost("localhost").setPort(9093));
        var resp = new FetchSnapshotResponseData().setThrottleTimeMs(0).setErrorCode((short) 0);
        resp.topics().add(topic);
        resp.setNodeEndpoints(endpoints);
        return resp;
    }

    static AddRaftVoterRequestData add(boolean ackFalse) {
        var listeners = new AddRaftVoterRequestData.ListenerCollection();
        listeners.add(new AddRaftVoterRequestData.Listener()
            .setName("CONTROLLER").setHost("localhost").setPort(9093));
        var req = new AddRaftVoterRequestData()
            .setClusterId("cluster-1")
            .setTimeoutMs(1500)
            .setVoterId(4)
            .setVoterDirectoryId(filled(9))
            .setListeners(listeners);
        if (ackFalse) req.setAckWhenCommitted(false);
        return req;
    }

    static RemoveRaftVoterRequestData remove() {
        return new RemoveRaftVoterRequestData()
            .setClusterId("cluster-1")
            .setVoterId(2)
            .setVoterDirectoryId(filled(7));
    }

    static DescribeQuorumResponseData describe(boolean nullMessages) {
        var replica = new DescribeQuorumResponseData.ReplicaState()
            .setReplicaId(2)
            .setReplicaDirectoryId(filled(8))
            .setLogEndOffset(10)
            .setLastFetchTimestamp(1)
            .setLastCaughtUpTimestamp(2);
        var part = new DescribeQuorumResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode((short) 0)
            .setLeaderId(2)
            .setLeaderEpoch(3)
            .setHighWatermark(9);
        part.setErrorMessage(nullMessages ? null : "");
        part.currentVoters().add(replica);
        var topic = new DescribeQuorumResponseData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var listeners = new DescribeQuorumResponseData.ListenerCollection();
        listeners.add(new DescribeQuorumResponseData.Listener()
            .setName("plaintext").setHost("127.0.0.1").setPort(9093));
        var nodes = new DescribeQuorumResponseData.NodeCollection();
        nodes.add(new DescribeQuorumResponseData.Node().setNodeId(2).setListeners(listeners));
        var resp = new DescribeQuorumResponseData().setErrorCode((short) 0).setErrorMessage(nullMessages ? null : "");
        resp.topics().add(topic);
        resp.setNodes(nodes);
        return resp;
    }

    static VoteResponseData voteResponse(boolean endpoints) {
        var part = new VoteResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode((short) 0)
            .setLeaderId(2)
            .setLeaderEpoch(3)
            .setVoteGranted(true);
        var topic = new VoteResponseData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var resp = new VoteResponseData().setErrorCode((short) 0);
        resp.topics().add(topic);
        if (endpoints) {
            var nodes = new VoteResponseData.NodeEndpointCollection();
            nodes.add(new VoteResponseData.NodeEndpoint().setNodeId(2).setHost("localhost").setPort(9093));
            resp.setNodeEndpoints(nodes);
        }
        return resp;
    }

    static BeginQuorumEpochResponseData beginResponse(boolean endpoints) {
        var part = new BeginQuorumEpochResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode((short) 0)
            .setLeaderId(2)
            .setLeaderEpoch(3);
        var topic = new BeginQuorumEpochResponseData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var resp = new BeginQuorumEpochResponseData().setErrorCode((short) 0);
        resp.topics().add(topic);
        if (endpoints) {
            var nodes = new BeginQuorumEpochResponseData.NodeEndpointCollection();
            nodes.add(new BeginQuorumEpochResponseData.NodeEndpoint().setNodeId(2).setHost("localhost").setPort(9093));
            resp.setNodeEndpoints(nodes);
        }
        return resp;
    }

    static EndQuorumEpochResponseData endResponse(boolean endpoints) {
        var part = new EndQuorumEpochResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode((short) 0)
            .setLeaderId(2)
            .setLeaderEpoch(3);
        var topic = new EndQuorumEpochResponseData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var resp = new EndQuorumEpochResponseData().setErrorCode((short) 0);
        resp.topics().add(topic);
        if (endpoints) {
            var nodes = new EndQuorumEpochResponseData.NodeEndpointCollection();
            nodes.add(new EndQuorumEpochResponseData.NodeEndpoint().setNodeId(2).setHost("localhost").setPort(9093));
            resp.setNodeEndpoints(nodes);
        }
        return resp;
    }

    static AddRaftVoterResponseData addResponse(String message) {
        return new AddRaftVoterResponseData().setThrottleTimeMs(0).setErrorCode((short) 0).setErrorMessage(message);
    }

    static RemoveRaftVoterResponseData removeResponse() {
        return new RemoveRaftVoterResponseData().setThrottleTimeMs(4).setErrorCode((short) 0).setErrorMessage(null);
    }

    static UpdateRaftVoterResponseData updateResponse() {
        return new UpdateRaftVoterResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode((short) 0)
            .setCurrentLeader(new UpdateRaftVoterResponseData.CurrentLeader()
                .setLeaderId(2)
                .setLeaderEpoch(3)
                .setHost("localhost")
                .setPort(9093));
    }

    static DescribeQuorumRequestData describeRequest() {
        var part = new DescribeQuorumRequestData.PartitionData().setPartitionIndex(0);
        var topic = new DescribeQuorumRequestData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(part);
        var req = new DescribeQuorumRequestData();
        req.topics().add(topic);
        return req;
    }

    static DescribeQuorumRequestData describeRequestTwo() {
        var topic = new DescribeQuorumRequestData.TopicData().setTopicName("__cluster_metadata");
        topic.partitions().add(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(0));
        topic.partitions().add(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(3));
        var req = new DescribeQuorumRequestData();
        req.topics().add(topic);
        return req;
    }
}
