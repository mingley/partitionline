/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/** Independent Apache v1-v5 serializer, including the v5 IsCordoned delta. */
public final class DescribeLogDirsFixtures {
    static final String JAR_SHA = "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e";
    static final String COMMIT = "26b251a451ce941d3d7a55e6487bcb7f16b5ad48";
    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, version));
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
        Path loaded = Path.of(DescribeLogDirsResponseData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(JAR_SHA))
            throw new AssertionError("use recorded Apache 4.3.1 Docker distribution jar");
        Path dir = Path.of(args[1]); Files.createDirectories(dir);
        boolean verify = args.length == 3;
        for (short version = 1; version <= 5; version++) {
            for (String cell : new String[] {"defaults", "nullable", "empty", "populated", "tagged"}) {
                if (version == 1 && cell.equals("tagged")) continue;
                DescribeLogDirsRequestData request = new DescribeLogDirsRequestData();
                DescribeLogDirsResponseData response = new DescribeLogDirsResponseData();
                if (!cell.equals("defaults")) request.setTopics(new DescribeLogDirsRequestData.DescribableLogDirTopicCollection());
                if (cell.equals("nullable")) request.setTopics(null);
                if (cell.equals("empty")) response.setThrottleTimeMs(37).setErrorCode((short) 31);
                if (cell.equals("populated") || cell.equals("tagged")) {
                    request.topics().add(new DescribeLogDirsRequestData.DescribableLogDirTopic()
                        .setTopic("alpha").setPartitions(List.of(0, 2)));
                    request.topics().add(new DescribeLogDirsRequestData.DescribableLogDirTopic()
                        .setTopic("béta").setPartitions(List.of()));
                    response.setThrottleTimeMs(37).setErrorCode((short) 31);
                    DescribeLogDirsResponseData.DescribeLogDirsPartition partition = new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                        .setPartitionIndex(2).setPartitionSize(123456).setOffsetLag(-1).setIsFutureKey(true);
                    DescribeLogDirsResponseData.DescribeLogDirsTopic topic = new DescribeLogDirsResponseData.DescribeLogDirsTopic()
                        .setName("alpha").setPartitions(List.of(partition));
                    // Deliberately true even for older versions: the ignorable
                    // v5 field is omitted and the older reader returns false.
                    response.results().add(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode((short) 0).setLogDir("/logs/a").setTopics(List.of(topic))
                        .setTotalBytes(1000000).setUsableBytes(456789).setIsCordoned(true));
                    response.results().add(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode((short) 56).setLogDir("/logs/b").setTopics(List.of())
                        .setTotalBytes(-1).setUsableBytes(-1).setIsCordoned(false));
                }
                if (cell.equals("tagged")) {
                    request.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {1, 2}));
                    request.topics().iterator().next().unknownTaggedFields().add(new RawTaggedField(11, new byte[] {3}));
                    response.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {4}));
                    var result = response.results().get(0);
                    result.unknownTaggedFields().add(new RawTaggedField(8, new byte[] {5}));
                    result.topics().get(0).unknownTaggedFields().add(new RawTaggedField(12, new byte[] {6}));
                    result.topics().get(0).partitions().get(0).unknownTaggedFields().add(new RawTaggedField(13, new byte[] {7}));
                }
                byte[] req = encode(request, version), resp = encode(response, version);
                ByteBuffer reqBuffer = ByteBuffer.wrap(req), respBuffer = ByteBuffer.wrap(resp);
                var decodedRequest = new DescribeLogDirsRequestData(new ByteBufferAccessor(reqBuffer), version);
                var decodedResponse = new DescribeLogDirsResponseData(new ByteBufferAccessor(respBuffer), version);
                // Project only after serialization, retaining the original
                // unknown-tag values (duplicate() is not our oracle).
                var projected = response;
                if (version < 3) projected.setErrorCode((short) 0);
                for (var result : projected.results()) {
                    if (version < 4) result.setTotalBytes(-1).setUsableBytes(-1);
                    if (version < 5) result.setIsCordoned(false);
                }
                if (!request.equals(decodedRequest) || !projected.equals(decodedResponse)
                    || reqBuffer.hasRemaining() || respBuffer.hasRemaining())
                    throw new AssertionError("Apache self-roundtrip/projection mismatch v" + version + " " + cell
                        + " request=" + request.equals(decodedRequest) + " response=" + projected.equals(decodedResponse)
                        + " remaining=" + reqBuffer.remaining() + "/" + respBuffer.remaining()
                        + " expected=" + projected + " got=" + decodedResponse);
                String prefix = "describe_log_dirs_v" + version + "_" + cell;
                String metadata = "{\n  \"api\": \"DescribeLogDirs\", \"api_key\": 35, \"version\": " + version + ",\n"
                    + "  \"apache_version\": \"4.3.1\", \"upstream_commit\": \"" + COMMIT + "\",\n"
                    + "  \"jar_origin\": \"apache/kafka:4.3.1 distribution\", \"jar_sha256\": \"" + JAR_SHA + "\",\n"
                    + "  \"generator\": \"tests/conformance/java/DescribeLogDirsFixtures.java\",\n"
                    + "  \"request_sha256\": \"" + sha(req) + "\", \"response_sha256\": \"" + sha(resp) + "\"\n}\n";
                output(dir.resolve(prefix + "_request.bin"), req, verify);
                output(dir.resolve(prefix + "_response.bin"), resp, verify);
                output(dir.resolve(prefix + ".json"), metadata.getBytes(StandardCharsets.UTF_8), verify);
                System.out.println(prefix + " request=" + sha(req) + " response=" + sha(resp));
            }
        }
    }
}
