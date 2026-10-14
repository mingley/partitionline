/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.JoinGroupRequest;

/**
 * Independent Apache JoinGroup v0/v1 body generator for KL05-42.
 *
 * Compiles against each pinned kafka-clients jar. Current 3.9.1 and
 * 4.1.0+ jars report ApiKeys.JOIN_GROUP 0-9. Builder.build still
 * refuses a present group.instance.id below v5.
 */
public final class LegacyJoinGroupFixtures {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");

    private LegacyJoinGroupFixtures() {}

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

    static JoinGroupRequestData requestData(boolean withInstance) {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
                new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(new byte[] {1, 2, 3}));
        JoinGroupRequestData data = new JoinGroupRequestData()
                .setGroupId("g")
                .setSessionTimeoutMs(10_000)
                .setRebalanceTimeoutMs(10_000)
                .setMemberId("m1")
                .setProtocolType("consumer")
                .setProtocols(protocols);
        if (withInstance) {
            data.setGroupInstanceId("static");
        }
        return data;
    }

    static JoinGroupResponseData responseData(boolean error) {
        JoinGroupResponseData data = new JoinGroupResponseData();
        if (error) {
            data.setErrorCode((short) 16);
            data.setGenerationId(-1);
            data.setProtocolName("");
            data.setLeader("");
            data.setMemberId("");
            return data;
        }
        data.setErrorCode((short) 0);
        data.setGenerationId(7);
        data.setProtocolName("range");
        data.setLeader("l");
        data.setMemberId("m1");
        data.members().add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("m1")
                .setMetadata(new byte[] {1, 2, 3}));
        return data;
    }

    static void writeJson(Path path, String body) throws Exception {
        Files.writeString(path, body);
    }

    static void writeUnsupported(Path dir, String name, short version, String reason)
            throws Exception {
        writeJson(
                dir.resolve(name),
                "{\n"
                        + "  \"disposition\": \"unsupported\",\n"
                        + "  \"version\": "
                        + version
                        + ",\n"
                        + "  \"reason\": "
                        + jsonString(reason)
                        + "\n}\n");
    }

    static String jsonString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    static boolean supported(short version, short oldest, short latest) {
        return version >= oldest && version <= latest;
    }

    static void generate(Path jar, Path dest) throws Exception {
        Files.createDirectories(dest);
        byte[] jarBytes = Files.readAllBytes(jar);
        String digest = sha(jarBytes);
        String release = PINS.get(digest);
        if (release == null) {
            throw new IllegalArgumentException("unpinned kafka-clients jar");
        }
        short oldest = ApiKeys.JOIN_GROUP.oldestVersion();
        short latest = ApiKeys.JOIN_GROUP.latestVersion();
        int bodies = 0;
        int receipts = 0;
        for (short version : new short[] {0, 1}) {
            if (!supported(version, oldest, latest)) {
                writeUnsupported(
                        dest,
                        "v" + version + "-unsupported.json",
                        version,
                        "ApiKeys.JOIN_GROUP oldestVersion=" + oldest + " latestVersion=" + latest);
                receipts++;
                writeUnsupported(
                        dest,
                        "v" + version + "-static-unsupported.json",
                        version,
                        "group.instance.id requires JoinGroup v5+; pin oldestVersion=" + oldest);
                receipts++;
                continue;
            }
            try {
                JoinGroupRequest built = new JoinGroupRequest.Builder(requestData(false)).build(version);
                byte[] request = encode(built.data(), version);
                Files.write(dest.resolve("v" + version + "-request-canonical.bin"), request);
                bodies++;
                JoinGroupRequestData parsed = new JoinGroupRequestData();
                parsed.read(new ByteBufferAccessor(ByteBuffer.wrap(request)), version);
                if (!"g".equals(parsed.groupId()) || !"m1".equals(parsed.memberId())) {
                    throw new AssertionError("parsed request identity differs v" + version);
                }
            } catch (UnsupportedVersionException e) {
                writeUnsupported(
                        dest,
                        "v" + version + "-unsupported.json",
                        version,
                        e.toString());
                receipts++;
            }

            try {
                new JoinGroupRequest.Builder(requestData(true)).build(version);
                throw new AssertionError("static membership must fail on v" + version);
            } catch (UnsupportedVersionException e) {
                writeUnsupported(
                        dest,
                        "v" + version + "-static-unsupported.json",
                        version,
                        e.toString());
                receipts++;
            }

            try {
                byte[] response = encode(responseData(false), version);
                Files.write(dest.resolve("v" + version + "-response-canonical.bin"), response);
                bodies++;
                JoinGroupResponseData parsed = new JoinGroupResponseData();
                parsed.read(new ByteBufferAccessor(ByteBuffer.wrap(response)), version);
                if (parsed.errorCode() != 0 || parsed.generationId() != 7) {
                    throw new AssertionError("parsed response identity differs v" + version);
                }
            } catch (UnsupportedVersionException | IllegalArgumentException e) {
                writeUnsupported(
                        dest,
                        "v" + version + "-response-unsupported.json",
                        version,
                        e.toString());
                receipts++;
            }

            try {
                byte[] error = encode(responseData(true), version);
                Files.write(dest.resolve("v" + version + "-response-error.bin"), error);
                bodies++;
            } catch (UnsupportedVersionException | IllegalArgumentException e) {
                writeUnsupported(
                        dest,
                        "v" + version + "-response-error-unsupported.json",
                        version,
                        e.toString());
                receipts++;
            }
        }
        String identity =
                "{\n"
                        + "  \"release\": "
                        + jsonString(release)
                        + ",\n"
                        + "  \"jar_sha256\": "
                        + jsonString(digest)
                        + ",\n"
                        + "  \"lowest_supported\": "
                        + oldest
                        + ",\n"
                        + "  \"highest_supported\": "
                        + latest
                        + ",\n"
                        + "  \"generated_bodies\": "
                        + bodies
                        + ",\n"
                        + "  \"unsupported_receipts\": "
                        + receipts
                        + "\n}\n";
        writeJson(dest.resolve("identity.json"), identity);
        System.out.println(identity.replace('\n', ' ').trim());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !"generate".equals(args[0])) {
            throw new IllegalArgumentException("usage: generate <jar> <dest>");
        }
        generate(Path.of(args[1]), Path.of(args[2]));
    }
}
