/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache AlterShareGroupOffsets / DeleteShareGroupOffsets v0
 * bodies for KL01-29.
 *
 * Finite cells: alter start-offset 7, alter INVALID_REQUEST, delete
 * topic-name-only, and delete GROUP_ID_NOT_FOUND. 3.9.1 does not ship
 * these message classes; the Python runner records unsupported receipts.
 */
public final class ConformanceShareOffsetMutations {
    static final Map<String, String> PINS = Map.of(
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short ALTER_VERSION = 0;
    static final short DELETE_VERSION = 0;
    static final String GROUP = "pl-share";
    static final String TOPIC = "pl-share";
    static final long START_OFFSET = 7L;
    static final short INVALID_REQUEST = 42;
    static final short GROUP_ID_NOT_FOUND = 69;
    static final Uuid TOPIC_ID = new Uuid(0x0101010101010101L, 0x0101010101010101L);

    private ConformanceShareOffsetMutations() {}

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

    static void roundtrip(Message original, short version, String cell) {
        byte[] encoded = encode(original, version);
        ByteBufferAccessor accessor = new ByteBufferAccessor(ByteBuffer.wrap(encoded));
        Message back;
        if (original instanceof AlterShareGroupOffsetsRequestData) {
            back = new AlterShareGroupOffsetsRequestData(accessor, version);
        } else if (original instanceof AlterShareGroupOffsetsResponseData) {
            back = new AlterShareGroupOffsetsResponseData(accessor, version);
        } else if (original instanceof DeleteShareGroupOffsetsRequestData) {
            back = new DeleteShareGroupOffsetsRequestData(accessor, version);
        } else if (original instanceof DeleteShareGroupOffsetsResponseData) {
            back = new DeleteShareGroupOffsetsResponseData(accessor, version);
        } else {
            throw new AssertionError("unknown message " + original.getClass().getName());
        }
        if (!Arrays.equals(encoded, encode(back, version))) {
            throw new AssertionError("Apache self-roundtrip failed " + cell);
        }
    }

    static AlterShareGroupOffsetsRequestData alterRequest() {
        AlterShareGroupOffsetsRequestData req = new AlterShareGroupOffsetsRequestData().setGroupId(GROUP);
        AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic topic =
                new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic().setTopicName(TOPIC);
        topic.partitions()
                .add(new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
                        .setPartitionIndex(0)
                        .setStartOffset(START_OFFSET));
        req.topics().add(topic);
        return req;
    }

    static AlterShareGroupOffsetsResponseData alterOk() {
        AlterShareGroupOffsetsResponseData resp = new AlterShareGroupOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0)
                .setErrorMessage(null);
        AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic topic =
                new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
                        .setTopicName(TOPIC)
                        .setTopicId(TOPIC_ID);
        topic.partitions()
                .add(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setErrorMessage(null));
        resp.responses().add(topic);
        return resp;
    }

    static AlterShareGroupOffsetsResponseData alterInvalid() {
        return new AlterShareGroupOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(INVALID_REQUEST)
                .setErrorMessage(null);
    }

    static DeleteShareGroupOffsetsRequestData deleteRequest() {
        DeleteShareGroupOffsetsRequestData req = new DeleteShareGroupOffsetsRequestData().setGroupId(GROUP);
        req.topics()
                .add(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                        .setTopicName(TOPIC));
        return req;
    }

    static DeleteShareGroupOffsetsResponseData deleteOk() {
        DeleteShareGroupOffsetsResponseData resp = new DeleteShareGroupOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode((short) 0)
                .setErrorMessage(null);
        resp.responses()
                .add(new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicName(TOPIC)
                        .setTopicId(TOPIC_ID)
                        .setErrorCode((short) 0)
                        .setErrorMessage(null));
        return resp;
    }

    static DeleteShareGroupOffsetsResponseData deleteUnknown() {
        return new DeleteShareGroupOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(GROUP_ID_NOT_FOUND)
                .setErrorMessage(null);
    }

    static void writePair(Path out, String stem, byte[] req, byte[] resp, List<String> rows) throws Exception {
        Files.write(out.resolve(stem + "-request.bin"), req);
        Files.write(out.resolve(stem + "-response.bin"), resp);
        rows.add("{\"cell\":\"" + stem + "\",\"request_sha256\":\"" + sha(req)
                + "\",\"response_sha256\":\"" + sha(resp) + "\",\"request_bytes\":" + req.length
                + ",\"response_bytes\":" + resp.length + "}");
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(
                AlterShareGroupOffsetsRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        if (AlterShareGroupOffsetsRequestData.LOWEST_SUPPORTED_VERSION != 0
                || AlterShareGroupOffsetsRequestData.HIGHEST_SUPPORTED_VERSION != 0
                || DeleteShareGroupOffsetsRequestData.LOWEST_SUPPORTED_VERSION != 0
                || DeleteShareGroupOffsetsRequestData.HIGHEST_SUPPORTED_VERSION != 0) {
            throw new AssertionError("unexpected spoken versions");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;

        AlterShareGroupOffsetsRequestData alterReq = alterRequest();
        AlterShareGroupOffsetsResponseData alterOk = alterOk();
        roundtrip(alterReq, ALTER_VERSION, "alter-ok-request");
        roundtrip(alterOk, ALTER_VERSION, "alter-ok-response");
        writePair(out, "alter-ok", encode(alterReq, ALTER_VERSION), encode(alterOk, ALTER_VERSION), rows);
        bodies += 2;

        AlterShareGroupOffsetsResponseData alterInvalid = alterInvalid();
        roundtrip(alterReq, ALTER_VERSION, "alter-invalid-request");
        roundtrip(alterInvalid, ALTER_VERSION, "alter-invalid-response");
        writePair(out, "alter-invalid", encode(alterReq, ALTER_VERSION), encode(alterInvalid, ALTER_VERSION), rows);
        bodies += 2;

        DeleteShareGroupOffsetsRequestData deleteReq = deleteRequest();
        DeleteShareGroupOffsetsResponseData deleteOk = deleteOk();
        roundtrip(deleteReq, DELETE_VERSION, "delete-ok-request");
        roundtrip(deleteOk, DELETE_VERSION, "delete-ok-response");
        writePair(out, "delete-ok", encode(deleteReq, DELETE_VERSION), encode(deleteOk, DELETE_VERSION), rows);
        bodies += 2;

        DeleteShareGroupOffsetsResponseData deleteUnknown = deleteUnknown();
        roundtrip(deleteReq, DELETE_VERSION, "delete-unknown-request");
        roundtrip(deleteUnknown, DELETE_VERSION, "delete-unknown-response");
        writePair(out, "delete-unknown", encode(deleteReq, DELETE_VERSION), encode(deleteUnknown, DELETE_VERSION), rows);
        bodies += 2;

        String expected = "{\"group_id\":\"" + GROUP + "\",\"topic\":\"" + TOPIC + "\""
                + ",\"alter_start_offset\":7,\"alter_ok_error\":0,\"alter_invalid_error\":42"
                + ",\"delete_ok_error\":0,\"delete_unknown_error\":69"
                + ",\"delete_topics_are_names_only\":true,\"coordinator_key_type\":0}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"alter_version\":" + ALTER_VERSION + ","
                + "\"delete_version\":" + DELETE_VERSION + ","
                + "\"group_id\":\"" + GROUP + "\","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"generated_bodies\":" + bodies + ","
                + "\"unsupported_receipts\":0,"
                + "\"cells\":[" + String.join(",", rows) + "]}\n";
        Files.writeString(out.resolve("identity.json"), identity);
        System.out.println("{\"generated_bodies\":" + bodies + ",\"unsupported_receipts\":0}");
    }

    static <T> T first(Iterable<T> items) {
        for (T item : items) {
            return item;
        }
        throw new AssertionError("empty collection");
    }

    static void decodeRust(String kind, short version, String hex) throws Exception {
        byte[] bytes = HexFormat.of().parseHex(hex);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        if (kind.equals("alter-request")) {
            AlterShareGroupOffsetsRequestData req = new AlterShareGroupOffsetsRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed AlterShareGroupOffsets request");
            }
            long start = first(first(req.topics()).partitions()).startOffset();
            System.out.println("{\"ok\":true,\"kind\":\"alter-request\",\"group_id\":\"" + req.groupId()
                    + "\",\"start_offset\":" + start + "}");
            return;
        }
        if (kind.equals("alter-response")) {
            AlterShareGroupOffsetsResponseData resp = new AlterShareGroupOffsetsResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed AlterShareGroupOffsets response");
            }
            int parts = resp.responses().isEmpty() ? 0 : first(resp.responses()).partitions().size();
            System.out.println("{\"ok\":true,\"kind\":\"alter-response\",\"error_code\":" + resp.errorCode()
                    + ",\"partitions\":" + parts + "}");
            return;
        }
        if (kind.equals("delete-request")) {
            DeleteShareGroupOffsetsRequestData req = new DeleteShareGroupOffsetsRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed DeleteShareGroupOffsets request");
            }
            System.out.println("{\"ok\":true,\"kind\":\"delete-request\",\"group_id\":\"" + req.groupId()
                    + "\",\"topics\":" + req.topics().size() + "}");
            return;
        }
        if (kind.equals("delete-response")) {
            DeleteShareGroupOffsetsResponseData resp = new DeleteShareGroupOffsetsResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed DeleteShareGroupOffsets response");
            }
            System.out.println("{\"ok\":true,\"kind\":\"delete-response\",\"error_code\":" + resp.errorCode()
                    + ",\"topics\":" + resp.responses().size() + "}");
            return;
        }
        throw new IllegalArgumentException("unknown kind " + kind);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("generate")) {
            generate(Path.of(args[1]), Path.of(args[2]));
            return;
        }
        if (args.length == 5 && args[0].equals("decode-rust")) {
            Path jar = Path.of(args[1]);
            Path loaded = Path.of(
                    AlterShareGroupOffsetsRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
                throw new AssertionError("wrong pinned Apache jar");
            }
            decodeRust(args[2], Short.parseShort(args[3]), args[4]);
            return;
        }
        throw new IllegalArgumentException(
                "generate <jar> <outdir> | decode-rust <jar> <kind> <version> <hex>");
    }
}
