/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache Vote/Begin/End directory and pre-vote request generator
 * for KL11-70. Compiles against each pinned kafka-clients 4.1.2+ jar.
 */
public final class DirectoryFixtures {
    static final Map<String, String> PINS = Map.of(
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final Uuid DIR1 = new Uuid(0x0101010101010101L, 0x0101010101010101L);
    static final Uuid DIR2 = new Uuid(0x0202020202020202L, 0x0202020202020202L);
    static final Uuid DIR3 = new Uuid(0x0303030303030303L, 0x0303030303030303L);
    static final Uuid STALE = new Uuid(0x0909090909090909L, 0x0909090909090909L);

    private DirectoryFixtures() {}

    static String sha(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    static byte[] encode(Message data, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(data.size(cache, version));
        data.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) {
            throw new AssertionError("size/write mismatch version " + version);
        }
        return buffer.array();
    }

    static byte[] frame(short apiKey, short version, byte[] body) {
        RequestHeaderData header = new RequestHeaderData()
                .setRequestApiKey(apiKey)
                .setRequestApiVersion(version)
                .setCorrelationId(77)
                .setClientId(null);
        short headerVersion = ApiKeys.forId(apiKey).requestHeaderVersion(version);
        byte[] prefix = encode(header, headerVersion);
        byte[] out = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        System.arraycopy(body, 0, out, prefix.length, body.length);
        return out;
    }

    static VoteRequestData vote(int voterId, int replicaId, Uuid replicaDir, Uuid voterDir, boolean preVote) {
        VoteRequestData.PartitionData partition = new VoteRequestData.PartitionData()
                .setPartitionIndex(0)
                .setReplicaEpoch(1)
                .setReplicaId(replicaId)
                .setReplicaDirectoryId(replicaDir)
                .setVoterDirectoryId(voterDir)
                .setLastOffsetEpoch(0)
                .setLastOffset(0L)
                .setPreVote(preVote);
        VoteRequestData.TopicData topic = new VoteRequestData.TopicData()
                .setTopicName("__cluster_metadata")
                .setPartitions(List.of(partition));
        return new VoteRequestData()
                .setClusterId("cluster-fixed")
                .setVoterId(voterId)
                .setTopics(List.of(topic));
    }

    static BeginQuorumEpochRequestData begin(int voterId, Uuid voterDir, int leaderId) {
        BeginQuorumEpochRequestData.PartitionData partition =
                new BeginQuorumEpochRequestData.PartitionData()
                        .setPartitionIndex(0)
                        .setVoterDirectoryId(voterDir)
                        .setLeaderId(leaderId)
                        .setLeaderEpoch(1);
        BeginQuorumEpochRequestData.TopicData topic = new BeginQuorumEpochRequestData.TopicData()
                .setTopicName("__cluster_metadata")
                .setPartitions(List.of(partition));
        return new BeginQuorumEpochRequestData()
                .setClusterId("cluster-fixed")
                .setVoterId(voterId)
                .setTopics(List.of(topic));
    }

    static EndQuorumEpochRequestData end(Uuid candidateDir) {
        EndQuorumEpochRequestData.ReplicaInfo candidate = new EndQuorumEpochRequestData.ReplicaInfo()
                .setCandidateId(3)
                .setCandidateDirectoryId(candidateDir);
        EndQuorumEpochRequestData.PartitionData partition =
                new EndQuorumEpochRequestData.PartitionData()
                        .setPartitionIndex(0)
                        .setLeaderId(2)
                        .setLeaderEpoch(1)
                        .setPreferredCandidates(List.of(candidate));
        EndQuorumEpochRequestData.TopicData topic = new EndQuorumEpochRequestData.TopicData()
                .setTopicName("__cluster_metadata")
                .setPartitions(List.of(partition));
        return new EndQuorumEpochRequestData()
                .setClusterId("cluster-fixed")
                .setTopics(List.of(topic));
    }

    static void writeCase(Path dest, StringBuilder table, String name, short key, short version,
            String disposition, byte[] request) throws Exception {
        String file = name + ".request.bin";
        Files.write(dest.resolve(file), request);
        table.append(name).append('\t').append(key).append('\t').append(version)
                .append('\t').append(file).append('\t').append(disposition)
                .append("\t-\n");
    }

    static void generate(Path jar, Path dest) throws Exception {
        Files.createDirectories(dest);
        byte[] jarBytes = Files.readAllBytes(jar);
        String digest = sha(jarBytes);
        String release = PINS.get(digest);
        if (release == null) {
            throw new IllegalArgumentException("unpinned kafka-clients jar");
        }
        if (ApiKeys.VOTE.latestVersion() < 2 || ApiKeys.BEGIN_QUORUM_EPOCH.latestVersion() < 1
                || ApiKeys.END_QUORUM_EPOCH.latestVersion() < 1) {
            throw new AssertionError(release + " does not speak directory Vote/Begin/End");
        }
        StringBuilder table = new StringBuilder(
                "name\tkey\tversion\trequest_file\tdisposition\tresponse_file\n");

        VoteRequestData voteV1 = vote(1, 2, DIR2, DIR1, false);
        byte[] voteV1Body = encode(voteV1, (short) 1);
        VoteRequestData parsedV1 = new VoteRequestData(new ByteBufferAccessor(ByteBuffer.wrap(voteV1Body)), (short) 1);
        if (!voteV1.equals(parsedV1)) {
            throw new AssertionError("Vote v1 self-roundtrip mismatch");
        }
        writeCase(dest, table, "vote-v1-positive", (short) 52, (short) 1, "response",
                frame((short) 52, (short) 1, voteV1Body));
        writeCase(dest, table, "vote-v1-stale-replica", (short) 52, (short) 1, "deny",
                frame((short) 52, (short) 1, encode(vote(1, 2, STALE, DIR1, false), (short) 1)));
        writeCase(dest, table, "vote-v1-zero-replica", (short) 52, (short) 1, "deny",
                frame((short) 52, (short) 1, encode(vote(1, 2, Uuid.ZERO_UUID, DIR1, false), (short) 1)));
        writeCase(dest, table, "vote-v1-wrong-voter", (short) 52, (short) 1, "deny",
                frame((short) 52, (short) 1, encode(vote(2, 3, DIR3, DIR2, false), (short) 1)));

        VoteRequestData preVote = vote(1, 2, DIR2, DIR1, true);
        byte[] preVoteBody = encode(preVote, (short) 2);
        VoteRequestData parsedPre = new VoteRequestData(new ByteBufferAccessor(ByteBuffer.wrap(preVoteBody)), (short) 2);
        if (!preVote.equals(parsedPre) || !parsedPre.topics().get(0).partitions().get(0).preVote()) {
            throw new AssertionError("Vote v2 pre-vote self-roundtrip mismatch");
        }
        writeCase(dest, table, "vote-v2-prevote", (short) 52, (short) 2, "prevote",
                frame((short) 52, (short) 2, preVoteBody));
        writeCase(dest, table, "vote-v2-prevote-stale", (short) 52, (short) 2, "deny",
                frame((short) 52, (short) 2, encode(vote(1, 2, STALE, DIR1, true), (short) 2)));
        writeCase(dest, table, "vote-v2-binding", (short) 52, (short) 2, "response",
                frame((short) 52, (short) 2, encode(vote(1, 2, DIR2, DIR1, false), (short) 2)));

        BeginQuorumEpochRequestData begin = begin(1, DIR1, 2);
        byte[] beginBody = encode(begin, (short) 1);
        if (!begin.equals(new BeginQuorumEpochRequestData(
                new ByteBufferAccessor(ByteBuffer.wrap(beginBody)), (short) 1))) {
            throw new AssertionError("Begin v1 self-roundtrip mismatch");
        }
        writeCase(dest, table, "begin-v1-positive", (short) 53, (short) 1, "response",
                frame((short) 53, (short) 1, beginBody));
        writeCase(dest, table, "begin-v1-stale-voter", (short) 53, (short) 1, "unchanged",
                frame((short) 53, (short) 1, encode(begin(1, STALE, 2), (short) 1)));

        EndQuorumEpochRequestData end = end(DIR3);
        byte[] endBody = encode(end, (short) 1);
        if (!end.equals(new EndQuorumEpochRequestData(
                new ByteBufferAccessor(ByteBuffer.wrap(endBody)), (short) 1))) {
            throw new AssertionError("End v1 self-roundtrip mismatch");
        }
        writeCase(dest, table, "end-v1-positive", (short) 54, (short) 1, "response",
                frame((short) 54, (short) 1, endBody));
        writeCase(dest, table, "end-v1-stale-candidate", (short) 54, (short) 1, "unchanged",
                frame((short) 54, (short) 1, encode(end(STALE), (short) 1)));

        byte[] truncated = frame((short) 52, (short) 1, voteV1Body);
        writeCase(dest, table, "vote-v1-truncated-uuid", (short) 52, (short) 1, "close",
                java.util.Arrays.copyOf(truncated, truncated.length - 12));

        Files.writeString(dest.resolve("cases.tsv"), table.toString());
        Files.writeString(
                dest.resolve("manifest.json"),
                "{\n  \"release\": \""
                        + release
                        + "\",\n  \"jar_sha256\": \""
                        + digest
                        + "\",\n  \"vote_latest\": "
                        + ApiKeys.VOTE.latestVersion()
                        + ",\n  \"begin_latest\": "
                        + ApiKeys.BEGIN_QUORUM_EPOCH.latestVersion()
                        + ",\n  \"end_latest\": "
                        + ApiKeys.END_QUORUM_EPOCH.latestVersion()
                        + "\n}\n");
        System.out.println(release + " wrote 12 directory/pre-vote request fixtures");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("<pinned-kafka-clients-jar> <out-dir>");
        }
        generate(Path.of(args[0]), Path.of(args[1]));
    }
}
