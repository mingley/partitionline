/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/** Independent Apache serializer for the v11 pending-upload timestamp selector. */
public final class ListOffsetsV11Fixtures {
    static final String JAR_SHA = "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e";
    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    static byte[] encode(Message message, short version) {
        var cache = new ObjectSerializationCache();
        var buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) throw new AssertionError("size/write mismatch");
        return buffer.array();
    }
    static void output(Path path, byte[] bytes, boolean verify) throws Exception {
        if (verify) {
            if (!Arrays.equals(Files.readAllBytes(path), bytes)) throw new AssertionError("fixture differs: " + path);
        } else Files.write(path, bytes);
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3 || args.length == 3 && !args[2].equals("--verify"))
            throw new IllegalArgumentException("<pinned-distribution-jar> <out-dir> [--verify]");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(ListOffsetsResponseData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(JAR_SHA))
            throw new AssertionError("use recorded Apache 4.3.1 distribution jar");
        Path dir = Path.of(args[1]); Files.createDirectories(dir);
        boolean verify = args.length == 3;
        for (String cell : new String[] {"defaults", "empty", "uncommitted", "committed", "errors", "tagged"}) {
            short version = 11;
            var request = new ListOffsetsRequestData();
            var response = new ListOffsetsResponseData();
            if (!cell.equals("defaults")) request.setReplicaId(-1).setTimeoutMs(1500);
            if (cell.equals("committed") || cell.equals("tagged")) request.setIsolationLevel((byte) 1);
            if (!cell.equals("defaults") && !cell.equals("empty")) {
                var topic = new ListOffsetsRequestData.ListOffsetsTopic().setName("béta");
                var results = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("béta");
                long[] timestamps = {-6, -5, -4, -3, -2, -1, 0, Long.MAX_VALUE};
                for (int index = 0; index < timestamps.length; index++) {
                    topic.partitions().add(new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(index).setCurrentLeaderEpoch(17).setTimestamp(timestamps[index]));
                    short error = cell.equals("errors") ? (short) (index == 0 ? 31 : 78) : 0;
                    results.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(index).setErrorCode(error).setTimestamp(error == 0 ? timestamps[index] : -1)
                        .setOffset(error == 0 ? 100 + index : -1).setLeaderEpoch(error == 0 ? 17 : -1));
                }
                request.topics().add(topic);
                response.setThrottleTimeMs(37).topics().add(results);
            }
            if (cell.equals("tagged")) {
                request.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {1, 2}));
                request.topics().get(0).unknownTaggedFields().add(new RawTaggedField(11, new byte[] {3}));
                request.topics().get(0).partitions().get(0).unknownTaggedFields().add(new RawTaggedField(12, new byte[] {4}));
                response.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {5}));
                response.topics().get(0).unknownTaggedFields().add(new RawTaggedField(8, new byte[] {6}));
                response.topics().get(0).partitions().get(0).unknownTaggedFields().add(new RawTaggedField(13, new byte[] {7}));
            }
            byte[] req = encode(request, version), resp = encode(response, version);
            var reqBuffer = ByteBuffer.wrap(req); var respBuffer = ByteBuffer.wrap(resp);
            var decodedRequest = new ListOffsetsRequestData(new ByteBufferAccessor(reqBuffer), version);
            var decodedResponse = new ListOffsetsResponseData(new ByteBufferAccessor(respBuffer), version);
            if (!request.equals(decodedRequest) || !response.equals(decodedResponse)
                || reqBuffer.hasRemaining() || respBuffer.hasRemaining()) throw new AssertionError("self-roundtrip " + cell);
            // No field layout delta: v10 serializers write identical syntax.
            // -6 on v10 is deliberately a wire-layout comparison, NOT a valid
            // pending-upload operation on an older server.
            if (!Arrays.equals(req, encode(request, (short) 10)) || !Arrays.equals(resp, encode(response, (short) 10)))
                throw new AssertionError("unexpected v10/v11 field-layout delta");
            String prefix = "list_offsets_v11_" + cell;
            String metadata = "{\n  \"api\": \"ListOffsets\", \"api_key\": 2, \"version\": 11,\n"
                + "  \"apache_version\": \"4.3.1\", \"upstream_commit\": \"26b251a451ce941d3d7a55e6487bcb7f16b5ad48\",\n"
                + "  \"jar_origin\": \"apache/kafka:4.3.1 distribution\", \"jar_sha256\": \"" + JAR_SHA + "\",\n"
                + "  \"generator\": \"tests/conformance/java/ListOffsetsV11Fixtures.java\",\n"
                + "  \"request_sha256\": \"" + sha(req) + "\", \"response_sha256\": \"" + sha(resp) + "\"\n}\n";
            output(dir.resolve(prefix + "_request.bin"), req, verify);
            output(dir.resolve(prefix + "_response.bin"), resp, verify);
            output(dir.resolve(prefix + ".json"), metadata.getBytes(StandardCharsets.UTF_8), verify);
            System.out.println(prefix + " request=" + sha(req) + " response=" + sha(resp));
        }
    }
}
