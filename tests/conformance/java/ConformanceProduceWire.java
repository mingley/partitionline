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
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/**
 * Independent Apache Produce v3–v13 body generator for KL01-18.
 *
 * Compiles against each pinned kafka-clients jar. Topic IDs are applied
 * through reflection so the same source builds on 3.9.1 (validVersions
 * 0-11, no TopicId field) and 4.1.0+ (validVersions 3-13).
 */
public final class ConformanceProduceWire {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final Uuid CANONICAL_ID = new Uuid(1L, 17L);
    static final Uuid TXN_TOPIC_ID = new Uuid(2L, 19L);
    static final short CRATE_MIN = 3;
    static final short CRATE_MAX = 13;

    private ConformanceProduceWire() {}

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

    static void setTopicIdentity(Object topic, short version, String name, Uuid id) {
        try {
            if (version >= 13) {
                topic.getClass().getMethod("setTopicId", Uuid.class).invoke(topic, id);
            } else {
                topic.getClass().getMethod("setName", String.class).invoke(topic, name);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("topic identity helpers missing for v" + version, e);
        }
    }

    static String topicName(Object topic) {
        try {
            Object value = topic.getClass().getMethod("name").invoke(topic);
            return value == null ? "" : value.toString();
        } catch (ReflectiveOperationException e) {
            return "";
        }
    }

    static String topicId(Object topic) {
        try {
            Object value = topic.getClass().getMethod("topicId").invoke(topic);
            return value == null ? "" : value.toString();
        } catch (ReflectiveOperationException e) {
            return "";
        }
    }

    static ProduceRequestData request(short version, String cell) {
        ProduceRequestData req = new ProduceRequestData();
        if (cell.equals("empty")) {
            req.setAcks((short) -1).setTimeoutMs(1234).setTransactionalId(null);
            return req;
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            req.setAcks((short) -1).setTimeoutMs(1234).setTransactionalId(null);
            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData();
            setTopicIdentity(topic, version, "pl-produce", CANONICAL_ID);
            ProduceRequestData.PartitionProduceData part = new ProduceRequestData.PartitionProduceData()
                    .setIndex(0).setRecords(null);
            topic.partitionData().add(part);
            req.topicData().add(topic);
            if (cell.equals("tagged")) {
                req.unknownTaggedFields().add(new RawTaggedField(125, new byte[] {4}));
                topic.unknownTaggedFields().add(new RawTaggedField(126, new byte[] {3}));
                part.unknownTaggedFields().add(new RawTaggedField(127, new byte[] {1, 2}));
            }
            return req;
        }
        if (cell.equals("txn_errors")) {
            req.setAcks((short) 1).setTimeoutMs(3000).setTransactionalId("txn-pl");
            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData();
            setTopicIdentity(topic, version, "pl-txn", TXN_TOPIC_ID);
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(null));
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(3).setRecords(null));
            req.topicData().add(topic);
            return req;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static ProduceResponseData response(short version, String cell) {
        ProduceResponseData resp = new ProduceResponseData();
        if (cell.equals("empty")) {
            return resp.setThrottleTimeMs(37);
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            resp.setThrottleTimeMs(42);
            ProduceResponseData.TopicProduceResponse topic = new ProduceResponseData.TopicProduceResponse();
            setTopicIdentity(topic, version, "pl-produce", CANONICAL_ID);
            ProduceResponseData.PartitionProduceResponse part = new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(0)
                    .setErrorCode((short) 0)
                    .setBaseOffset(100L)
                    .setLogAppendTimeMs(1_710_000_000_000L);
            if (version >= 5) {
                part.setLogStartOffset(0L);
            }
            if (cell.equals("tagged") && version >= 10) {
                part.setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderId(7).setLeaderEpoch(9));
                resp.nodeEndpoints().add(new ProduceResponseData.NodeEndpoint()
                        .setNodeId(7).setHost("broker").setPort(9092).setRack(null));
            }
            topic.partitionResponses().add(part);
            resp.responses().add(topic);
            if (cell.equals("tagged")) {
                resp.unknownTaggedFields().add(new RawTaggedField(124, new byte[] {5}));
            }
            return resp;
        }
        if (cell.equals("txn_errors")) {
            resp.setThrottleTimeMs(60);
            ProduceResponseData.TopicProduceResponse topic = new ProduceResponseData.TopicProduceResponse();
            setTopicIdentity(topic, version, "pl-txn", TXN_TOPIC_ID);
            ProduceResponseData.PartitionProduceResponse ok = new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(0)
                    .setErrorCode((short) 0)
                    .setBaseOffset(200L)
                    .setLogAppendTimeMs(1_710_000_001_000L);
            if (version >= 5) {
                ok.setLogStartOffset(50L);
            }
            topic.partitionResponses().add(ok);
            ProduceResponseData.PartitionProduceResponse failed = new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(3)
                    .setErrorCode((short) 6)
                    .setBaseOffset(-1L)
                    .setLogAppendTimeMs(-1L);
            if (version >= 5) {
                failed.setLogStartOffset(-1L);
            }
            if (version >= 8) {
                failed.setErrorMessage("not leader");
                failed.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage()
                        .setBatchIndex(0)
                        .setBatchIndexErrorMessage("invalid"));
            }
            if (version >= 10) {
                failed.setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderId(7).setLeaderEpoch(9));
                resp.nodeEndpoints().add(new ProduceResponseData.NodeEndpoint()
                        .setNodeId(7).setHost("broker").setPort(9092).setRack(null));
            }
            topic.partitionResponses().add(failed);
            resp.responses().add(topic);
            return resp;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static List<String> cellsFor(short version) {
        List<String> cells = new ArrayList<>(List.of("empty", "canonical", "txn_errors"));
        if (version >= 9) {
            cells.add("tagged");
        }
        return cells;
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        short lowest = ProduceRequestData.LOWEST_SUPPORTED_VERSION;
        short highest = ProduceRequestData.HIGHEST_SUPPORTED_VERSION;
        int bodies = 0;
        int unsupported = 0;
        List<String> rows = new ArrayList<>();
        for (short version = CRATE_MIN; version <= CRATE_MAX; version++) {
            if (version < lowest || version > highest) {
                String reason = "ProduceRequestData validVersions " + lowest + "-" + highest
                        + " rejects crate-spoken v" + version;
                try {
                    // Construct with a name-based spoken version so 3.9.1 can
                    // build the object, then serialize at the rejected version.
                    short construct = version > 12 ? 12 : version;
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
                ProduceRequestData req = request(version, cell);
                ProduceResponseData resp = response(version, cell);
                byte[] reqBytes = encode(req, version);
                byte[] respBytes = encode(resp, version);
                ProduceRequestData reqBack = new ProduceRequestData(new ByteBufferAccessor(ByteBuffer.wrap(reqBytes)), version);
                ProduceResponseData respBack = new ProduceResponseData(new ByteBufferAccessor(ByteBuffer.wrap(respBytes)), version);
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
        ProduceRequestData req = new ProduceRequestData(new ByteBufferAccessor(reqBuf), version);
        ProduceResponseData resp = new ProduceResponseData(new ByteBufferAccessor(respBuf), version);
        if (reqBuf.hasRemaining() || respBuf.hasRemaining()) {
            throw new AssertionError("unconsumed Rust body bytes");
        }
        StringBuilder topics = new StringBuilder("[");
        int i = 0;
        for (ProduceRequestData.TopicProduceData topic : req.topicData()) {
            if (i++ > 0) {
                topics.append(',');
            }
            topics.append("{\"name\":").append(jsonString(topicName(topic)))
                    .append(",\"topic_id\":").append(jsonString(topicId(topic)))
                    .append(",\"partitions\":").append(topic.partitionData().size()).append('}');
        }
        topics.append(']');
        StringBuilder results = new StringBuilder("[");
        i = 0;
        for (ProduceResponseData.TopicProduceResponse topic : resp.responses()) {
            if (i++ > 0) {
                results.append(',');
            }
            results.append("{\"name\":").append(jsonString(topicName(topic)))
                    .append(",\"topic_id\":").append(jsonString(topicId(topic)))
                    .append(",\"partitions\":").append(topic.partitionResponses().size()).append('}');
        }
        results.append(']');
        System.out.println("{\"ok\":true,\"version\":" + version
                + ",\"acks\":" + req.acks()
                + ",\"timeout_ms\":" + req.timeoutMs()
                + ",\"transactional_id\":" + jsonString(req.transactionalId())
                + ",\"request_topics\":" + topics
                + ",\"throttle_time_ms\":" + resp.throttleTimeMs()
                + ",\"response_topics\":" + results + "}");
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
            Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
                throw new AssertionError("wrong pinned Apache jar");
            }
            decodeRust(Short.parseShort(args[2]), args[3], args[4]);
            return;
        }
        throw new IllegalArgumentException("generate <jar> <outdir> | decode-rust <jar> <version> <req-hex> <resp-hex>");
    }
}
