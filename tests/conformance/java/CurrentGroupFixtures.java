/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/** Apache-generated nullable struct fixtures, independent of the Rust codec. */
public final class CurrentGroupFixtures {
    static final String JAR_SHA = "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e";
    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
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
        Path loaded = Path.of(ConsumerGroupHeartbeatResponseData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(JAR_SHA))
            throw new AssertionError("use recorded Apache 4.3.1 distribution jar");
        Path dir = Path.of(args[1]); Files.createDirectories(dir);
        boolean verify = args.length == 3;
        for (short version : new short[] {0, 1}) {
            for (String cell : new String[] {"null", "leave", "error", "empty", "populated", "tagged"}) {
                var response = new ConsumerGroupHeartbeatResponseData()
                    .setThrottleTimeMs(37).setMemberId("m1").setMemberEpoch(3).setHeartbeatIntervalMs(5000);
                if (cell.equals("leave")) response.setMemberEpoch(-1).setHeartbeatIntervalMs(0);
                if (cell.equals("error")) response.setErrorCode((short) 42).setErrorMessage("invalid").setMemberId(null);
                if (cell.equals("empty") || cell.equals("populated") || cell.equals("tagged")) {
                    var assignment = new ConsumerGroupHeartbeatResponseData.Assignment();
                    response.setAssignment(assignment);
                    if (!cell.equals("empty")) assignment.topicPartitions().add(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(new Uuid(0x0102030405060708L, 0x090a0b0c0d0e0f10L))
                            .setPartitions(List.of(0, 2)));
                    if (cell.equals("tagged")) {
                        assignment.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
                        assignment.topicPartitions().get(0).unknownTaggedFields().add(new RawTaggedField(8, new byte[] {3}));
                    }
                }
                if (cell.equals("tagged")) response.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {4, 5}));
                var cache = new ObjectSerializationCache();
                var buffer = ByteBuffer.allocate(response.size(cache, version));
                response.write(new ByteBufferAccessor(buffer), cache, version);
                if (buffer.hasRemaining()) throw new AssertionError("size/write mismatch");
                byte[] bytes = buffer.array();
                var input = ByteBuffer.wrap(bytes);
                var decoded = new ConsumerGroupHeartbeatResponseData(new ByteBufferAccessor(input), version);
                if (!response.equals(decoded) || input.hasRemaining()) throw new AssertionError("self-roundtrip");
                String prefix = "cgheartbeat_v" + version + "_" + cell;
                String metadata = "{\n  \"api\": \"ConsumerGroupHeartbeat\", \"api_key\": 68, \"version\": " + version + ",\n"
                    + "  \"apache_version\": \"4.3.1\", \"upstream_commit\": \"26b251a451ce941d3d7a55e6487bcb7f16b5ad48\",\n"
                    + "  \"jar_sha256\": \"" + JAR_SHA + "\", \"generator\": \"tests/conformance/java/CurrentGroupFixtures.java\",\n"
                    + "  \"response_sha256\": \"" + sha(bytes) + "\"\n}\n";
                output(dir.resolve(prefix + "_response.bin"), bytes, verify);
                output(dir.resolve(prefix + ".json"), metadata.getBytes(StandardCharsets.UTF_8), verify);
                System.out.println(prefix + " response=" + sha(bytes));
            }
        }
    }
}
