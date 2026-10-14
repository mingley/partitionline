/* SPDX-License-Identifier: Apache-2.0 */
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache Fetch v12 session-runtime bodies for KL01-24.
 *
 * Cells follow hashed FetchSessionHandler / FetchMetadata: full open,
 * incremental empty, forgotten partition, FETCH_SESSION_ID_NOT_FOUND
 * reset to INITIAL, and INVALID_FETCH_SESSION_EPOCH close-existing-attempt-new.
 *
 * Record batches are built through reflection so the same source compiles
 * on 3.9.1–4.2.1 (public MemoryRecords) and 4.3.1 (internal package).
 */
public final class ConformanceFetchSessionRuntime {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short FETCH_VERSION = 12;
    static final String TOPIC = "pl-session";
    static final int SESSION_ID = 91;
    static final short FETCH_SESSION_ID_NOT_FOUND = 70;
    static final short INVALID_FETCH_SESSION_EPOCH = 71;

    private ConformanceFetchSessionRuntime() {}

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

    static Class<?> firstClass(String... names) {
        for (String name : names) {
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException ignored) {
                // try the next public/internal record class
            }
        }
        throw new AssertionError("missing " + Arrays.toString(names));
    }

    static Object compressionNone() throws Exception {
        return Class.forName("org.apache.kafka.common.compress.Compression").getField("NONE").get(null);
    }

    static Class<?> memoryRecordsClass() {
        return firstClass(
                "org.apache.kafka.common.record.MemoryRecords",
                "org.apache.kafka.common.record.internal.MemoryRecords");
    }

    static Class<?> simpleRecordClass() {
        return firstClass(
                "org.apache.kafka.common.record.SimpleRecord",
                "org.apache.kafka.common.record.internal.SimpleRecord");
    }

    static Object simpleRecord(byte[] value) throws Exception {
        return simpleRecordClass().getConstructor(long.class, byte[].class).newInstance(0L, value);
    }

    static Object dataRecords(long baseOffset, byte[]... values) throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> simple = simpleRecordClass();
        Object array = Array.newInstance(simple, values.length);
        for (int i = 0; i < values.length; i++) {
            Array.set(array, i, simpleRecord(values[i]));
        }
        Class<?> compression = Class.forName("org.apache.kafka.common.compress.Compression");
        Method method = recordsClass.getMethod("withRecords", long.class, compression, array.getClass());
        return method.invoke(null, baseOffset, compressionNone(), array);
    }

    static void setRecords(FetchResponseData.PartitionData part, Object records) throws Exception {
        for (Method method : part.getClass().getMethods()) {
            if (method.getName().equals("setRecords") && method.getParameterCount() == 1) {
                method.invoke(part, records);
                return;
            }
        }
        throw new AssertionError("PartitionData.setRecords missing");
    }

    static FetchRequestData.FetchPartition fetchPartition(int index) {
        return new FetchRequestData.FetchPartition()
                .setPartition(index)
                .setFetchOffset(0L)
                .setLogStartOffset(-1L)
                .setPartitionMaxBytes(1_048_576)
                .setCurrentLeaderEpoch(-1);
    }

    static FetchRequestData fetchRequest(int sessionId, int epoch, int[] partitions, int[] forgotten) {
        FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10_485_760)
                .setIsolationLevel((byte) 0)
                .setSessionId(sessionId)
                .setSessionEpoch(epoch)
                .setRackId("");
        if (partitions.length > 0) {
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic(TOPIC);
            for (int partition : partitions) {
                topic.partitions().add(fetchPartition(partition));
            }
            req.topics().add(topic);
        }
        if (forgotten.length > 0) {
            FetchRequestData.ForgottenTopic topic = new FetchRequestData.ForgottenTopic().setTopic(TOPIC);
            for (int partition : forgotten) {
                topic.partitions().add(partition);
            }
            req.forgottenTopicsData().add(topic);
        }
        return req;
    }

    static FetchResponseData.PartitionData okPartition(int index, Object records) throws Exception {
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                .setPartitionIndex(index)
                .setErrorCode((short) 0)
                .setHighWatermark(2L)
                .setLastStableOffset(2L)
                .setLogStartOffset(0L);
        setRecords(part, records);
        return part;
    }

    static FetchResponseData fetchResponse(int sessionId, short error, FetchResponseData.PartitionData... partitions) {
        FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(error)
                .setSessionId(sessionId);
        if (partitions.length > 0) {
            FetchResponseData.FetchableTopicResponse topic =
                    new FetchResponseData.FetchableTopicResponse().setTopic(TOPIC);
            topic.partitions().addAll(Arrays.asList(partitions));
            resp.responses().add(topic);
        }
        return resp;
    }

    static void writeBytes(Path out, String name, byte[] bytes, List<String> rows) throws Exception {
        Files.write(out.resolve(name), bytes);
        rows.add("{\"file\":\"" + name + "\",\"sha256\":\"" + sha(bytes) + "\",\"bytes\":" + bytes.length + "}");
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;
        Object rec0 = dataRecords(0L, "a".getBytes(StandardCharsets.UTF_8));
        Object rec1 = dataRecords(0L, "b".getBytes(StandardCharsets.UTF_8));

        byte[] s01Req = encode(fetchRequest(0, 0, new int[] {0, 1}, new int[0]), FETCH_VERSION);
        byte[] s01Resp = encode(
                fetchResponse(SESSION_ID, (short) 0, okPartition(0, rec0), okPartition(1, rec1)),
                FETCH_VERSION);
        writeBytes(out, "s01-full-request.bin", s01Req, rows);
        writeBytes(out, "s01-full-response.bin", s01Resp, rows);
        bodies += 2;

        byte[] s02Req = encode(fetchRequest(SESSION_ID, 1, new int[0], new int[0]), FETCH_VERSION);
        byte[] s02Resp = encode(fetchResponse(SESSION_ID, (short) 0), FETCH_VERSION);
        writeBytes(out, "s02-incremental-request.bin", s02Req, rows);
        writeBytes(out, "s02-incremental-response.bin", s02Resp, rows);
        bodies += 2;

        byte[] s03Req = encode(fetchRequest(SESSION_ID, 2, new int[0], new int[] {1}), FETCH_VERSION);
        byte[] s03Resp = encode(fetchResponse(SESSION_ID, (short) 0), FETCH_VERSION);
        writeBytes(out, "s03-forgotten-request.bin", s03Req, rows);
        writeBytes(out, "s03-forgotten-response.bin", s03Resp, rows);
        bodies += 2;

        byte[] s04Resp = encode(fetchResponse(0, FETCH_SESSION_ID_NOT_FOUND), FETCH_VERSION);
        writeBytes(out, "s04-not-found-response.bin", s04Resp, rows);
        bodies++;

        byte[] s05Req = encode(fetchRequest(0, 0, new int[] {0, 1}, new int[0]), FETCH_VERSION);
        writeBytes(out, "s05-after-not-found-request.bin", s05Req, rows);
        bodies++;

        byte[] s06Resp = encode(fetchResponse(0, INVALID_FETCH_SESSION_EPOCH), FETCH_VERSION);
        writeBytes(out, "s06-invalid-epoch-response.bin", s06Resp, rows);
        bodies++;

        byte[] s07Req = encode(fetchRequest(SESSION_ID, 0, new int[] {0, 1}, new int[0]), FETCH_VERSION);
        writeBytes(out, "s07-after-invalid-epoch-request.bin", s07Req, rows);
        bodies++;

        String expected = "{\"topic\":\"" + TOPIC + "\",\"session_id\":" + SESSION_ID
                + ",\"s01_session\":[0,0],\"s01_partitions\":[0,1],\"s02_session\":[91,1],"
                + "\"s02_partitions\":[],\"s03_session\":[91,2],\"s03_forgotten\":[1],"
                + "\"s04_error\":70,\"s05_session\":[0,0],\"s06_error\":71,\"s07_session\":[91,0]}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"fetch_version\":" + FETCH_VERSION + ","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"session_id\":" + SESSION_ID + ","
                + "\"generated_bodies\":" + bodies + ","
                + "\"unsupported_receipts\":0,"
                + "\"cells\":[" + String.join(",", rows) + "]}\n";
        Files.writeString(out.resolve("identity.json"), identity);
        System.out.println("{\"generated_bodies\":" + bodies + ",\"unsupported_receipts\":0}");
    }

    static void decodeRust(String kind, short version, String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        if (kind.equals("fetch-request")) {
            FetchRequestData req = new FetchRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Fetch request");
            }
            int partitions = req.topics().isEmpty() ? 0 : req.topics().get(0).partitions().size();
            int forgotten = req.forgottenTopicsData().isEmpty()
                    ? 0
                    : req.forgottenTopicsData().get(0).partitions().size();
            System.out.println("{\"ok\":true,\"kind\":\"fetch-request\",\"session_id\":" + req.sessionId()
                    + ",\"epoch\":" + req.sessionEpoch() + ",\"partitions\":" + partitions
                    + ",\"forgotten\":" + forgotten + "}");
            return;
        }
        if (kind.equals("fetch-response")) {
            FetchResponseData resp = new FetchResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Fetch response");
            }
            System.out.println("{\"ok\":true,\"kind\":\"fetch-response\",\"error\":" + resp.errorCode()
                    + ",\"session_id\":" + resp.sessionId() + ",\"topics\":" + resp.responses().size() + "}");
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
            Path loaded = Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
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
