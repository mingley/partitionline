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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/**
 * Independent Apache ListOffsets v1–v11 body generator for KL01-21.
 *
 * Compiles against each pinned kafka-clients jar. TimeoutMs (v10,
 * KIP-1075) is applied through reflection so the same source builds on
 * 3.9.1 (validVersions 0-9) and 4.1.0+ (1-10 or 1-11).
 */
public final class ConformanceListOffsetsWire {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short CRATE_MIN = 1;
    static final short CRATE_MAX = 11;
    static final int CONSUMER_REPLICA_ID = -1;
    static final long LATEST_TIMESTAMP = -1L;
    static final long EARLIEST_TIMESTAMP = -2L;

    private ConformanceListOffsetsWire() {}

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

    static void setTimeoutMs(Object req, int value) {
        try {
            req.getClass().getMethod("setTimeoutMs", int.class).invoke(req, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("ListOffsets v10 TimeoutMs accessor missing", e);
        }
    }

    static int timeoutMs(Object req) {
        try {
            return (Integer) req.getClass().getMethod("timeoutMs").invoke(req);
        } catch (ReflectiveOperationException e) {
            return 0;
        }
    }

    static ListOffsetsRequestData request(short version, String cell) {
        ListOffsetsRequestData req = new ListOffsetsRequestData().setReplicaId(CONSUMER_REPLICA_ID);
        if (cell.equals("empty")) {
            if (version >= 2) {
                req.setIsolationLevel((byte) 0);
            }
            if (version >= 10) {
                setTimeoutMs(req, 0);
            }
            return req;
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            if (version >= 2) {
                req.setIsolationLevel((byte) 0);
            }
            if (version >= 10) {
                setTimeoutMs(req, 1500);
            }
            ListOffsetsRequestData.ListOffsetsTopic topic = new ListOffsetsRequestData.ListOffsetsTopic()
                    .setName("pl-list-offsets");
            ListOffsetsRequestData.ListOffsetsPartition part = new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(0)
                    .setTimestamp(LATEST_TIMESTAMP);
            if (version >= 4) {
                part.setCurrentLeaderEpoch(7);
            }
            topic.partitions().add(part);
            req.topics().add(topic);
            if (cell.equals("tagged")) {
                req.unknownTaggedFields().add(new RawTaggedField(125, new byte[] {4}));
                topic.unknownTaggedFields().add(new RawTaggedField(126, new byte[] {3}));
                part.unknownTaggedFields().add(new RawTaggedField(127, new byte[] {1, 2}));
            }
            return req;
        }
        if (cell.equals("errors")) {
            if (version >= 2) {
                req.setIsolationLevel((byte) 1);
            }
            if (version >= 10) {
                setTimeoutMs(req, 60);
            }
            ListOffsetsRequestData.ListOffsetsTopic topic = new ListOffsetsRequestData.ListOffsetsTopic()
                    .setName("pl-missing");
            ListOffsetsRequestData.ListOffsetsPartition part = new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(0)
                    .setTimestamp(EARLIEST_TIMESTAMP);
            if (version >= 4) {
                part.setCurrentLeaderEpoch(-1);
            }
            topic.partitions().add(part);
            req.topics().add(topic);
            return req;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static ListOffsetsResponseData response(short version, String cell) {
        ListOffsetsResponseData resp = new ListOffsetsResponseData();
        if (cell.equals("empty")) {
            return resp.setThrottleTimeMs(37);
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            resp.setThrottleTimeMs(42);
            ListOffsetsResponseData.ListOffsetsTopicResponse topic =
                    new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("pl-list-offsets");
            ListOffsetsResponseData.ListOffsetsPartitionResponse part =
                    new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 0)
                            .setTimestamp(LATEST_TIMESTAMP)
                            .setOffset(12L);
            if (version >= 4) {
                part.setLeaderEpoch(7);
            }
            topic.partitions().add(part);
            resp.topics().add(topic);
            if (cell.equals("tagged")) {
                resp.unknownTaggedFields().add(new RawTaggedField(124, new byte[] {5}));
                topic.unknownTaggedFields().add(new RawTaggedField(123, new byte[] {6}));
                part.unknownTaggedFields().add(new RawTaggedField(122, new byte[] {7}));
            }
            return resp;
        }
        if (cell.equals("errors")) {
            resp.setThrottleTimeMs(60);
            ListOffsetsResponseData.ListOffsetsTopicResponse topic =
                    new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("pl-missing");
            ListOffsetsResponseData.ListOffsetsPartitionResponse part =
                    new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(0)
                            .setErrorCode((short) 3)
                            .setTimestamp(-1L)
                            .setOffset(-1L);
            if (version >= 4) {
                part.setLeaderEpoch(-1);
            }
            topic.partitions().add(part);
            resp.topics().add(topic);
            return resp;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static List<String> cellsFor(short version) {
        List<String> cells = new ArrayList<>(List.of("empty", "canonical", "errors"));
        if (version >= 6) {
            cells.add("tagged");
        }
        return cells;
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(ListOffsetsRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        short lowest = ListOffsetsRequestData.LOWEST_SUPPORTED_VERSION;
        short highest = ListOffsetsRequestData.HIGHEST_SUPPORTED_VERSION;
        int bodies = 0;
        int unsupported = 0;
        List<String> rows = new ArrayList<>();
        for (short version = CRATE_MIN; version <= CRATE_MAX; version++) {
            if (version < lowest || version > highest) {
                String reason = "ListOffsetsRequestData validVersions " + lowest + "-" + highest
                        + " rejects crate-spoken v" + version;
                try {
                    short construct = version > highest ? highest : version;
                    if (construct < lowest) {
                        construct = lowest;
                    }
                    encode(request(construct, "canonical"), version);
                    throw new AssertionError("expected unsupported version " + version);
                } catch (UnsupportedVersionException expected) {
                    Path receipt = out.resolve(String.format("unsupported-v%02d.json", version));
                    Files.writeString(receipt, "{\"version\":" + version + ",\"disposition\":\"unsupported\","
                            + "\"exception\":\"" + expected.getClass().getName() + "\","
                            + "\"message\":" + jsonString(expected.getMessage()) + ","
                            + "\"reason\":" + jsonString(reason) + "}\n");
                    rows.add("{\"version\":" + version + ",\"cell\":\"unsupported\",\"exception\":"
                            + jsonString(expected.getClass().getName()) + "}");
                    unsupported++;
                }
                continue;
            }
            for (String cell : cellsFor(version)) {
                ListOffsetsRequestData req = request(version, cell);
                ListOffsetsResponseData resp = response(version, cell);
                byte[] reqBytes = encode(req, version);
                byte[] respBytes = encode(resp, version);
                ListOffsetsRequestData reqBack =
                        new ListOffsetsRequestData(new ByteBufferAccessor(ByteBuffer.wrap(reqBytes)), version);
                ListOffsetsResponseData respBack =
                        new ListOffsetsResponseData(new ByteBufferAccessor(ByteBuffer.wrap(respBytes)), version);
                if (!Arrays.equals(reqBytes, encode(reqBack, version))
                        || !Arrays.equals(respBytes, encode(respBack, version))) {
                    throw new AssertionError("Apache self-roundtrip failed v" + version + " " + cell);
                }
                String stem = String.format("v%02d-%s", version, cell);
                Files.write(out.resolve(stem + "-request.bin"), reqBytes);
                Files.write(out.resolve(stem + "-response.bin"), respBytes);
                rows.add("{\"version\":" + version + ",\"cell\":\"" + cell + "\","
                        + "\"request_sha256\":\"" + sha(reqBytes) + "\","
                        + "\"response_sha256\":\"" + sha(respBytes) + "\","
                        + "\"request_bytes\":" + reqBytes.length + ","
                        + "\"response_bytes\":" + respBytes.length + "}");
                bodies += 2;
            }
        }
        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"lowest_supported\":" + lowest + ","
                + "\"highest_supported\":" + highest + ","
                + "\"generated_bodies\":" + bodies + ","
                + "\"unsupported_receipts\":" + unsupported + ","
                + "\"cells\":[" + String.join(",", rows) + "]}\n";
        Files.writeString(out.resolve("identity.json"), identity);
        System.out.println("{\"generated_bodies\":" + bodies + ",\"unsupported_receipts\":" + unsupported
                + ",\"lowest_supported\":" + lowest + ",\"highest_supported\":" + highest + "}");
    }

    static void decodeRust(short version, String reqHex, String respHex) {
        byte[] reqBytes = HexFormat.of().parseHex(reqHex);
        byte[] respBytes = HexFormat.of().parseHex(respHex);
        ByteBuffer reqBuf = ByteBuffer.wrap(reqBytes);
        ByteBuffer respBuf = ByteBuffer.wrap(respBytes);
        ListOffsetsRequestData req = new ListOffsetsRequestData(new ByteBufferAccessor(reqBuf), version);
        ListOffsetsResponseData resp = new ListOffsetsResponseData(new ByteBufferAccessor(respBuf), version);
        if (reqBuf.hasRemaining() || respBuf.hasRemaining()) {
            throw new AssertionError("unconsumed Rust body bytes");
        }
        int firstError = resp.topics().isEmpty() || resp.topics().get(0).partitions().isEmpty()
                ? -1
                : resp.topics().get(0).partitions().get(0).errorCode();
        System.out.println("{\"ok\":true,\"version\":" + version
                + ",\"replica_id\":" + req.replicaId()
                + ",\"isolation_level\":" + req.isolationLevel()
                + ",\"request_topics\":" + req.topics().size()
                + ",\"timeout_ms\":" + timeoutMs(req)
                + ",\"throttle_time_ms\":" + resp.throttleTimeMs()
                + ",\"response_topics\":" + resp.topics().size()
                + ",\"first_error_code\":" + firstError + "}");
    }

    static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' || c == '"') {
                out.append('\\').append(c);
            } else if (c < 0x20) {
                out.append(String.format("\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("generate")) {
            generate(Path.of(args[1]), Path.of(args[2]));
            return;
        }
        if (args.length == 5 && args[0].equals("decode-rust")) {
            Path jar = Path.of(args[1]);
            Path loaded = Path.of(ListOffsetsRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
                throw new AssertionError("wrong pinned Apache jar");
            }
            decodeRust(Short.parseShort(args[2]), args[3], args[4]);
            return;
        }
        throw new IllegalArgumentException("generate <jar> <outdir> | decode-rust <jar> <version> <req-hex> <resp-hex>");
    }
}
