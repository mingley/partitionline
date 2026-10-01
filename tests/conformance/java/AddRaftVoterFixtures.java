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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/** Offline v0 oracle. Uses Apache message serializers only, never Rust bytes. */
public final class AddRaftVoterFixtures {
    static final String JAR_SHA = "180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb";
    static final String COMMIT = "13f70256db3c994c590e5d262a7cc50b9e973204";
    static byte[] encode(Message message) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, (short) 0));
        message.write(new ByteBufferAccessor(buffer), cache, (short) 0);
        if (buffer.hasRemaining()) throw new AssertionError("size/write mismatch");
        return buffer.array();
    }
    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    static void output(Path path, byte[] bytes, boolean verify) throws Exception {
        if (verify) {
            if (!Arrays.equals(Files.readAllBytes(path), bytes))
                throw new AssertionError("fixture differs: " + path);
        } else Files.write(path, bytes);
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3)
            throw new IllegalArgumentException("<pinned-distribution-jar> <out-dir> [--verify]");
        if (args.length == 3 && !args[2].equals("--verify"))
            throw new IllegalArgumentException("unknown option");
        boolean verify = args.length == 3 && args[2].equals("--verify");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(AddRaftVoterRequestData.class.getProtectionDomain()
            .getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(JAR_SHA))
            throw new AssertionError("use the recorded Apache 4.1.0 distribution jar");
        Path dir = Path.of(args[1]); Files.createDirectories(dir);
        for (String cell : new String[] {"defaults", "nullable", "populated", "tagged"}) {
            AddRaftVoterRequestData request = new AddRaftVoterRequestData();
            AddRaftVoterResponseData response = new AddRaftVoterResponseData();
            if (cell.equals("nullable")) {
                request.setClusterId(null);
                response.setErrorMessage(null);
            }
            if (cell.equals("populated") || cell.equals("tagged")) {
                request.setClusterId("cluster-410").setTimeoutMs(1234).setVoterId(42)
                    .setVoterDirectoryId(new Uuid(7, 9));
                request.listeners().add(new AddRaftVoterRequestData.Listener()
                    .setName("CONTROLLER").setHost("localhost").setPort(65535));
                request.listeners().add(new AddRaftVoterRequestData.Listener()
                    .setName("INTERNAL").setHost("::1").setPort(9093));
                response.setThrottleTimeMs(42).setErrorCode((short) 41)
                    .setErrorMessage("controller moved");
            }
            if (cell.equals("tagged")) {
                request.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {1, 2, 3}));
                request.listeners().iterator().next().unknownTaggedFields()
                    .add(new RawTaggedField(11, new byte[] {4, 5}));
                response.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {6, 7}));
            }
            byte[] req = encode(request), resp = encode(response);
            AddRaftVoterRequestData decodedReq = new AddRaftVoterRequestData(
                new ByteBufferAccessor(ByteBuffer.wrap(req)), (short) 0);
            AddRaftVoterResponseData decodedResp = new AddRaftVoterResponseData(
                new ByteBufferAccessor(ByteBuffer.wrap(resp)), (short) 0);
            if (!request.equals(decodedReq) || !response.equals(decodedResp))
                throw new AssertionError("Apache self-roundtrip mismatch");
            String prefix = "add_raft_voter_v0_" + cell;
            String metadata = "{\n  \"api\": \"AddRaftVoter\", \"api_key\": 80, \"version\": 0,\n"
                + "  \"apache_version\": \"4.1.0\", \"upstream_commit\": \"" + COMMIT + "\",\n"
                + "  \"jar_origin\": \"apache/kafka:4.1.0 distribution\", \"jar_sha256\": \"" + JAR_SHA + "\",\n"
                + "  \"generator\": \"tests/conformance/java/AddRaftVoterFixtures.java\",\n"
                + "  \"request_sha256\": \"" + sha(req) + "\", \"response_sha256\": \"" + sha(resp) + "\"\n}\n";
            output(dir.resolve(prefix + "_request.bin"), req, verify);
            output(dir.resolve(prefix + "_response.bin"), resp, verify);
            output(dir.resolve(prefix + ".json"), metadata.getBytes(java.nio.charset.StandardCharsets.UTF_8), verify);
            System.out.println(prefix + " request=" + sha(req) + " response=" + sha(resp));
        }
    }
}
