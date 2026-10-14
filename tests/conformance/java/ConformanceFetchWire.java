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
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/**
 * Independent Apache Fetch v4–v18 body generator for KL01-19.
 *
 * Compiles against each pinned kafka-clients jar. Topic IDs and the v18
 * request HighWatermark tag are applied through reflection so the same
 * source builds on 3.9.1 (validVersions 0-17) and 4.1.0+ (4-18).
 */
public final class ConformanceFetchWire {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final Uuid CANONICAL_ID = new Uuid(1L, 17L);
    static final Uuid SESSION_ID = new Uuid(2L, 19L);
    static final Uuid FORGOTTEN_ID = new Uuid(3L, 23L);
    static final short CRATE_MIN = 4;
    static final short CRATE_MAX = 18;

    private ConformanceFetchWire() {}

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
                try {
                    topic.getClass().getMethod("setTopic", String.class).invoke(topic, name);
                } catch (NoSuchMethodException missing) {
                    topic.getClass().getMethod("setName", String.class).invoke(topic, name);
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("topic identity helpers missing for v" + version, e);
        }
    }

    static String topicName(Object topic) {
        for (String method : new String[] {"topic", "name"}) {
            try {
                Object value = topic.getClass().getMethod(method).invoke(topic);
                return value == null ? "" : value.toString();
            } catch (ReflectiveOperationException ignored) {
                // try the next Fetch/Produce-shaped accessor
            }
        }
        return "";
    }

    static String topicId(Object topic) {
        try {
            Object value = topic.getClass().getMethod("topicId").invoke(topic);
            return value == null ? "" : value.toString();
        } catch (ReflectiveOperationException e) {
            return "";
        }
    }

    static void setHighWatermark(Object partition, long value) {
        try {
            partition.getClass().getMethod("setHighWatermark", long.class).invoke(partition, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Fetch v18 HighWatermark accessor missing", e);
        }
    }

    static FetchRequestData request(short version, String cell) {
        FetchRequestData req = new FetchRequestData()
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10_485_760)
                .setIsolationLevel((byte) 0);
        if (version <= 14) {
            req.setReplicaId(-1);
        }
        if (cell.equals("empty")) {
            return req;
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic();
            setTopicIdentity(topic, version, "pl-fetch", CANONICAL_ID);
            FetchRequestData.FetchPartition part = new FetchRequestData.FetchPartition()
                    .setPartition(0)
                    .setFetchOffset(0L)
                    .setPartitionMaxBytes(1_048_576);
            if (version >= 5) {
                part.setLogStartOffset(-1L);
            }
            if (version >= 9) {
                part.setCurrentLeaderEpoch(-1);
            }
            if (version >= 12) {
                part.setLastFetchedEpoch(-1);
            }
            if (cell.equals("tagged") && version >= 18) {
                setHighWatermark(part, 99L);
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
        if (cell.equals("session_errors")) {
            req.setMaxWaitMs(5000).setIsolationLevel((byte) 1);
            if (version >= 7) {
                req.setSessionId(42).setSessionEpoch(3);
                FetchRequestData.ForgottenTopic forgotten = new FetchRequestData.ForgottenTopic();
                setTopicIdentity(forgotten, version, "pl-forgotten", FORGOTTEN_ID);
                forgotten.partitions().add(1);
                req.forgottenTopicsData().add(forgotten);
            }
            if (version >= 11) {
                req.setRackId("rack-a");
            }
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic();
            setTopicIdentity(topic, version, "pl-fetch-session", SESSION_ID);
            FetchRequestData.FetchPartition ok = new FetchRequestData.FetchPartition()
                    .setPartition(0)
                    .setFetchOffset(100L)
                    .setPartitionMaxBytes(1_048_576);
            FetchRequestData.FetchPartition failed = new FetchRequestData.FetchPartition()
                    .setPartition(3)
                    .setFetchOffset(200L)
                    .setPartitionMaxBytes(1_048_576);
            if (version >= 5) {
                ok.setLogStartOffset(50L);
                failed.setLogStartOffset(-1L);
            }
            if (version >= 9) {
                ok.setCurrentLeaderEpoch(7);
                failed.setCurrentLeaderEpoch(-1);
            }
            if (version >= 12) {
                ok.setLastFetchedEpoch(6);
                failed.setLastFetchedEpoch(-1);
            }
            topic.partitions().add(ok);
            topic.partitions().add(failed);
            req.topics().add(topic);
            return req;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static FetchResponseData response(short version, String cell) {
        FetchResponseData resp = new FetchResponseData();
        if (cell.equals("empty")) {
            return resp.setThrottleTimeMs(37);
        }
        if (cell.equals("canonical") || cell.equals("tagged")) {
            resp.setThrottleTimeMs(42);
            FetchResponseData.FetchableTopicResponse topic = new FetchResponseData.FetchableTopicResponse();
            setTopicIdentity(topic, version, "pl-fetch", CANONICAL_ID);
            FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                    .setPartitionIndex(0)
                    .setErrorCode((short) 0)
                    .setHighWatermark(50L)
                    .setLastStableOffset(45L)
                    .setRecords(null);
            if (version >= 5) {
                part.setLogStartOffset(0L);
            }
            if (cell.equals("tagged") && version >= 12) {
                part.setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(7).setLeaderEpoch(9));
            }
            if (cell.equals("tagged") && version >= 16) {
                resp.nodeEndpoints().add(new FetchResponseData.NodeEndpoint()
                        .setNodeId(7).setHost("broker").setPort(9092).setRack(null));
            }
            topic.partitions().add(part);
            resp.responses().add(topic);
            if (cell.equals("tagged")) {
                resp.unknownTaggedFields().add(new RawTaggedField(124, new byte[] {5}));
            }
            return resp;
        }
        if (cell.equals("session_errors")) {
            resp.setThrottleTimeMs(60);
            if (version >= 7) {
                resp.setErrorCode((short) 0).setSessionId(42);
            }
            FetchResponseData.FetchableTopicResponse topic = new FetchResponseData.FetchableTopicResponse();
            setTopicIdentity(topic, version, "pl-fetch-session", SESSION_ID);
            FetchResponseData.PartitionData ok = new FetchResponseData.PartitionData()
                    .setPartitionIndex(0)
                    .setErrorCode((short) 0)
                    .setHighWatermark(250L)
                    .setLastStableOffset(240L)
                    .setRecords(null);
            if (version >= 5) {
                ok.setLogStartOffset(100L);
            }
            if (version >= 11) {
                ok.setPreferredReadReplica(7);
            }
            FetchResponseData.PartitionData failed = new FetchResponseData.PartitionData()
                    .setPartitionIndex(3)
                    .setErrorCode((short) 3)
                    .setHighWatermark(-1L)
                    .setLastStableOffset(-1L)
                    .setRecords(null);
            if (version >= 5) {
                failed.setLogStartOffset(-1L);
            }
            failed.abortedTransactions().add(new FetchResponseData.AbortedTransaction()
                    .setProducerId(7L)
                    .setFirstOffset(10L));
            if (version >= 12) {
                failed.setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(7).setLeaderEpoch(9));
            }
            if (version >= 16) {
                resp.nodeEndpoints().add(new FetchResponseData.NodeEndpoint()
                        .setNodeId(7).setHost("broker").setPort(9092).setRack(null));
            }
            topic.partitions().add(ok);
            topic.partitions().add(failed);
            resp.responses().add(topic);
            return resp;
        }
        throw new IllegalArgumentException("unknown cell " + cell);
    }

    static List<String> cellsFor(short version) {
        List<String> cells = new ArrayList<>(List.of("empty", "canonical", "session_errors"));
        if (version >= 12) {
            cells.add("tagged");
        }
        return cells;
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        short lowest = FetchRequestData.LOWEST_SUPPORTED_VERSION;
        short highest = FetchRequestData.HIGHEST_SUPPORTED_VERSION;
        int bodies = 0;
        int unsupported = 0;
        List<String> rows = new ArrayList<>();
        for (short version = CRATE_MIN; version <= CRATE_MAX; version++) {
            if (version < lowest || version > highest) {
                String reason = "FetchRequestData validVersions " + lowest + "-" + highest
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
                FetchRequestData req = request(version, cell);
                FetchResponseData resp = response(version, cell);
                byte[] reqBytes = encode(req, version);
                byte[] respBytes = encode(resp, version);
                FetchRequestData reqBack = new FetchRequestData(new ByteBufferAccessor(ByteBuffer.wrap(reqBytes)), version);
                FetchResponseData respBack = new FetchResponseData(new ByteBufferAccessor(ByteBuffer.wrap(respBytes)), version);
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
        FetchRequestData req = new FetchRequestData(new ByteBufferAccessor(reqBuf), version);
        FetchResponseData resp = new FetchResponseData(new ByteBufferAccessor(respBuf), version);
        if (reqBuf.hasRemaining() || respBuf.hasRemaining()) {
            throw new AssertionError("unconsumed Rust body bytes");
        }
        System.out.println("{\"ok\":true,\"version\":" + version
                + ",\"max_wait_ms\":" + req.maxWaitMs()
                + ",\"min_bytes\":" + req.minBytes()
                + ",\"max_bytes\":" + req.maxBytes()
                + ",\"isolation_level\":" + req.isolationLevel()
                + ",\"request_topics\":" + req.topics().size()
                + ",\"throttle_time_ms\":" + resp.throttleTimeMs()
                + ",\"response_topics\":" + resp.responses().size() + "}");
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
            Path loaded = Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
                throw new AssertionError("wrong pinned Apache jar");
            }
            decodeRust(Short.parseShort(args[2]), args[3], args[4]);
            return;
        }
        throw new IllegalArgumentException("generate <jar> <outdir> | decode-rust <jar> <version> <req-hex> <resp-hex>");
    }
}
