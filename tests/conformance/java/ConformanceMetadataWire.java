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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/**
 * Independent Apache Metadata v1–v13 body generator for KL01-20.
 *
 * Compiles against each pinned kafka-clients jar. The v13 response
 * ErrorCode is applied through reflection so the same source builds on
 * 3.9.1 (validVersions 0-12) and 4.1.0+ (0-13).
 */
public final class ConformanceMetadataWire {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final Uuid CANONICAL_ID = new Uuid(1L, 17L);
    static final short CRATE_MIN = 1;
    static final short CRATE_MAX = 13;

    private ConformanceMetadataWire() {}

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

    static void setErrorCode(Object resp, short value) {
        try {
            resp.getClass().getMethod("setErrorCode", short.class).invoke(resp, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Metadata v13 ErrorCode accessor missing", e);
        }
    }

    static MetadataRequestData request(short version, String cell) {
        MetadataRequestData req = new MetadataRequestData();
        if (cell.equals("empty")) {
            req.setTopics(null);
            return req;
        }
        MetadataRequestData.MetadataRequestTopic topic = new MetadataRequestData.MetadataRequestTopic();
        if (cell.equals("canonical") || cell.equals("tagged")) {
            topic.setName("pl-metadata");
            req.topics().add(topic);
            if (cell.equals("tagged")) {
                req.unknownTaggedFields().add(new RawTaggedField(125, new byte[] {4}));
                topic.unknownTaggedFields().add(new RawTaggedField(126, new byte[] {3}));
            }
            return req;
        }
        if (cell.equals("errors")) {
            topic.setName("pl-missing");
            req.topics().add(topic);
            if (version >= 4) {
                req.setAllowAutoTopicCreation(false);
            }
            if (version >= 8) {
                req.setIncludeTopicAuthorizedOperations(true);
            }
            return req;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static MetadataResponseData response(short version, String cell) {
        MetadataResponseData resp = new MetadataResponseData();
        if (cell.equals("empty")) {
            return resp.setThrottleTimeMs(37);
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            resp.setThrottleTimeMs(42);
            if (version >= 2) {
                resp.setClusterId("pl-cluster");
            }
            if (version >= 1) {
                resp.setControllerId(1);
            }
            MetadataResponseData.MetadataResponseBroker broker = new MetadataResponseData.MetadataResponseBroker()
                    .setNodeId(1)
                    .setHost("broker")
                    .setPort(9092)
                    .setRack(null);
            resp.brokers().add(broker);
            MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode((short) 0)
                    .setName("pl-metadata")
                    .setIsInternal(false);
            if (version >= 10) {
                topic.setTopicId(CANONICAL_ID);
            }
            MetadataResponseData.MetadataResponsePartition part = new MetadataResponseData.MetadataResponsePartition()
                    .setErrorCode((short) 0)
                    .setPartitionIndex(0)
                    .setLeaderId(1);
            part.replicaNodes().add(1);
            part.isrNodes().add(1);
            if (version >= 7) {
                part.setLeaderEpoch(7);
            }
            topic.partitions().add(part);
            resp.topics().add(topic);
            if (cell.equals("tagged")) {
                resp.unknownTaggedFields().add(new RawTaggedField(124, new byte[] {5}));
            }
            return resp;
        }
        if (cell.equals("errors")) {
            resp.setThrottleTimeMs(60);
            if (version >= 13) {
                setErrorCode(resp, (short) 0);
            }
            MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode((short) 3)
                    .setName("pl-missing")
                    .setIsInternal(false);
            resp.topics().add(topic);
            return resp;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static List<String> cellsFor(short version) {
        List<String> cells = new ArrayList<>(List.of("empty", "canonical", "errors"));
        if (version >= 9) {
            cells.add("tagged");
        }
        return cells;
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(MetadataRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        short lowest = MetadataRequestData.LOWEST_SUPPORTED_VERSION;
        short highest = MetadataRequestData.HIGHEST_SUPPORTED_VERSION;
        int bodies = 0;
        int unsupported = 0;
        List<String> rows = new ArrayList<>();
        for (short version = CRATE_MIN; version <= CRATE_MAX; version++) {
            if (version < lowest || version > highest) {
                String reason = "MetadataRequestData validVersions " + lowest + "-" + highest
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
                MetadataRequestData req = request(version, cell);
                MetadataResponseData resp = response(version, cell);
                byte[] reqBytes = encode(req, version);
                byte[] respBytes = encode(resp, version);
                MetadataRequestData reqBack = new MetadataRequestData(new ByteBufferAccessor(ByteBuffer.wrap(reqBytes)), version);
                MetadataResponseData respBack = new MetadataResponseData(new ByteBufferAccessor(ByteBuffer.wrap(respBytes)), version);
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
        MetadataRequestData req = new MetadataRequestData(new ByteBufferAccessor(reqBuf), version);
        MetadataResponseData resp = new MetadataResponseData(new ByteBufferAccessor(respBuf), version);
        if (reqBuf.hasRemaining() || respBuf.hasRemaining()) {
            throw new AssertionError("unconsumed Rust body bytes");
        }
        System.out.println("{\"ok\":true,\"version\":" + version
                + ",\"request_topics\":" + (req.topics() == null ? -1 : req.topics().size())
                + ",\"allow_auto_topic_creation\":" + req.allowAutoTopicCreation()
                + ",\"throttle_time_ms\":" + resp.throttleTimeMs()
                + ",\"brokers\":" + resp.brokers().size()
                + ",\"response_topics\":" + resp.topics().size()
                + ",\"controller_id\":" + resp.controllerId() + "}");
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
            Path loaded = Path.of(MetadataRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
                throw new AssertionError("wrong pinned Apache jar");
            }
            decodeRust(Short.parseShort(args[2]), args[3], args[4]);
            return;
        }
        throw new IllegalArgumentException("generate <jar> <outdir> | decode-rust <jar> <version> <req-hex> <resp-hex>");
    }
}
