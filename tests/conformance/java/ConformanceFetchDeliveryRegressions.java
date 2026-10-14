/* SPDX-License-Identifier: Apache-2.0 */
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache Fetch / OffsetCommit / OffsetFetch bodies for the
 * five KL01-22 delivered-position regression histories.
 *
 * Record batches are built through reflection so the same source compiles
 * on 3.9.1–4.2.1 (public MemoryRecords) and 4.3.1 (internal package).
 */
public final class ConformanceFetchDeliveryRegressions {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short FETCH_VERSION = 12;
    static final short OFFSET_COMMIT_VERSION = 8;
    static final short OFFSET_FETCH_VERSION = 8;
    static final short NOT_LEADER_OR_FOLLOWER = 6;
    static final short OFFSET_OUT_OF_RANGE = 1;

    private ConformanceFetchDeliveryRegressions() {}

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

    static Object timestampCreateTime() throws Exception {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Class type = Class.forName("org.apache.kafka.common.record.TimestampType");
        @SuppressWarnings("unchecked")
        Object value = Enum.valueOf(type, "CREATE_TIME");
        return value;
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

    static Object dataRecords(long baseOffset, boolean transactional, int sequence, byte[]... values)
            throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> simple = simpleRecordClass();
        Object array = Array.newInstance(simple, values.length);
        for (int i = 0; i < values.length; i++) {
            Array.set(array, i, simpleRecord(values[i]));
        }
        Class<?> compression = Class.forName("org.apache.kafka.common.compress.Compression");
        if (transactional) {
            Method method = recordsClass.getMethod(
                    "withTransactionalRecords",
                    long.class,
                    compression,
                    long.class,
                    short.class,
                    int.class,
                    int.class,
                    array.getClass());
            return method.invoke(null, baseOffset, compressionNone(), 7L, (short) 0, sequence, 0, array);
        }
        Method method = recordsClass.getMethod("withRecords", long.class, compression, array.getClass());
        return method.invoke(null, baseOffset, compressionNone(), array);
    }

    static Object markerRecords(long offset, String control) throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> markerClass = firstClass(
                "org.apache.kafka.common.record.EndTransactionMarker",
                "org.apache.kafka.common.record.internal.EndTransactionMarker");
        @SuppressWarnings({"rawtypes", "unchecked"})
        Class type = firstClass(
                "org.apache.kafka.common.record.ControlRecordType",
                "org.apache.kafka.common.record.internal.ControlRecordType");
        @SuppressWarnings("unchecked")
        Object controlType = Enum.valueOf(type, control);
        Object marker = markerClass.getConstructor(type, int.class).newInstance(controlType, 0);
        Method builderMethod = null;
        for (Method method : recordsClass.getMethods()) {
            if (!method.getName().equals("builder") || method.getParameterCount() != 12) {
                continue;
            }
            Class<?>[] params = method.getParameterTypes();
            if (params[0] == ByteBuffer.class
                    && params[1] == byte.class
                    && params[9] == boolean.class
                    && params[10] == boolean.class) {
                builderMethod = method;
                break;
            }
        }
        if (builderMethod == null) {
            throw new AssertionError("MemoryRecords.builder(12-arg control) missing");
        }
        Object builder = builderMethod.invoke(
                null,
                ByteBuffer.allocate(512),
                (byte) 2,
                compressionNone(),
                timestampCreateTime(),
                offset,
                0L,
                7L,
                (short) 0,
                0,
                true,
                true,
                0);
        builder.getClass().getMethod("appendEndTxnMarker", long.class, markerClass).invoke(builder, offset, marker);
        return builder.getClass().getMethod("build").invoke(builder);
    }

    static Object concatRecords(Object... batches) throws Exception {
        int total = 0;
        byte[][] copies = new byte[batches.length][];
        for (int i = 0; i < batches.length; i++) {
            ByteBuffer buffer = (ByteBuffer) batches[i].getClass().getMethod("buffer").invoke(batches[i]);
            ByteBuffer dup = buffer.duplicate();
            copies[i] = new byte[dup.remaining()];
            dup.get(copies[i]);
            total += copies[i].length;
        }
        ByteBuffer out = ByteBuffer.allocate(total);
        for (byte[] copy : copies) {
            out.put(copy);
        }
        out.flip();
        return memoryRecordsClass().getMethod("readableRecords", ByteBuffer.class).invoke(null, out);
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

    static FetchRequestData.FetchPartition fetchPartition(int index, long fetchOffset) {
        return new FetchRequestData.FetchPartition()
                .setPartition(index)
                .setFetchOffset(fetchOffset)
                .setLogStartOffset(-1L)
                .setPartitionMaxBytes(1_048_576)
                .setCurrentLeaderEpoch(-1);
    }

    static FetchRequestData fetchRequest(long fetchOffset, int partitions, byte isolation) {
        FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10_485_760)
                .setIsolationLevel(isolation)
                .setSessionId(0)
                .setSessionEpoch(-1)
                .setRackId("");
        FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic("t");
        for (int i = 0; i < partitions; i++) {
            topic.partitions().add(fetchPartition(i, fetchOffset));
        }
        req.topics().add(topic);
        return req;
    }

    static FetchResponseData.PartitionData okPartition(int index, Object records) throws Exception {
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                .setPartitionIndex(index)
                .setErrorCode((short) 0)
                .setHighWatermark(4L)
                .setLastStableOffset(4L)
                .setLogStartOffset(0L);
        setRecords(part, records);
        return part;
    }

    static FetchResponseData fetchResponse(FetchResponseData.PartitionData... partitions) {
        FetchResponseData resp = new FetchResponseData().setThrottleTimeMs(0);
        FetchResponseData.FetchableTopicResponse topic =
                new FetchResponseData.FetchableTopicResponse().setTopic("t");
        topic.partitions().addAll(Arrays.asList(partitions));
        resp.responses().add(topic);
        return resp;
    }

    static FetchResponseData a01() throws Exception {
        return fetchResponse(okPartition(0, dataRecords(0L, false, 0, "a".getBytes(), "b".getBytes(), "c".getBytes())));
    }

    static FetchResponseData a02() throws Exception {
        Object records = concatRecords(
                dataRecords(0L, true, 0, "aborted".getBytes()),
                markerRecords(1L, "ABORT"),
                dataRecords(2L, true, 1, "committed".getBytes()),
                markerRecords(3L, "COMMIT"));
        FetchResponseData.PartitionData part = okPartition(0, records);
        FetchResponseData.AbortedTransaction aborted = new FetchResponseData.AbortedTransaction()
                .setProducerId(7L)
                .setFirstOffset(0L);
        part.abortedTransactions().add(aborted);
        return fetchResponse(part);
    }

    static FetchResponseData a03(int attempt) throws Exception {
        FetchResponseData.PartitionData p0 =
                okPartition(0, dataRecords(0L, false, 0, "keep-me".getBytes()));
        FetchResponseData.PartitionData p1;
        if (attempt == 0) {
            p1 = new FetchResponseData.PartitionData()
                    .setPartitionIndex(1)
                    .setErrorCode(NOT_LEADER_OR_FOLLOWER)
                    .setHighWatermark(4L)
                    .setLastStableOffset(4L)
                    .setLogStartOffset(0L);
        } else {
            p1 = okPartition(1, dataRecords(0L, false, 0, "keep-me".getBytes()));
        }
        return fetchResponse(p0, p1);
    }

    static FetchResponseData a04() {
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode(OFFSET_OUT_OF_RANGE)
                .setHighWatermark(20L)
                .setLastStableOffset(20L)
                .setLogStartOffset(10L);
        return fetchResponse(part);
    }

    static FetchResponseData a05() throws Exception {
        return fetchResponse(okPartition(0, dataRecords(0L, false, 0, "a".getBytes(), "b".getBytes(), "c".getBytes())));
    }

    static OffsetCommitRequestData offsetCommitRequest() {
        OffsetCommitRequestData req = new OffsetCommitRequestData()
                .setGroupId("pl-delivery")
                .setGenerationIdOrMemberEpoch(1)
                .setMemberId("member-a");
        OffsetCommitRequestData.OffsetCommitRequestTopic topic =
                new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("t");
        topic.partitions()
                .add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                        .setPartitionIndex(0)
                        .setCommittedOffset(1L)
                        .setCommittedLeaderEpoch(-1)
                        .setCommittedMetadata(""));
        req.topics().add(topic);
        return req;
    }

    static OffsetCommitResponseData offsetCommitResponse() {
        OffsetCommitResponseData resp = new OffsetCommitResponseData().setThrottleTimeMs(0);
        OffsetCommitResponseData.OffsetCommitResponseTopic topic =
                new OffsetCommitResponseData.OffsetCommitResponseTopic().setName("t");
        topic.partitions()
                .add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0));
        resp.topics().add(topic);
        return resp;
    }

    static OffsetFetchRequestData offsetFetchRequest() {
        OffsetFetchRequestData.OffsetFetchRequestTopics topic =
                new OffsetFetchRequestData.OffsetFetchRequestTopics().setName("t");
        topic.partitionIndexes().add(0);
        OffsetFetchRequestData.OffsetFetchRequestGroup group =
                new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("pl-delivery");
        group.topics().add(topic);
        return new OffsetFetchRequestData().setRequireStable(false).setGroups(List.of(group));
    }

    static OffsetFetchResponseData offsetFetchResponse() {
        OffsetFetchResponseData.OffsetFetchResponseTopics topic =
                new OffsetFetchResponseData.OffsetFetchResponseTopics().setName("t");
        topic.partitions()
                .add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(0)
                        .setCommittedOffset(1L)
                        .setCommittedLeaderEpoch(-1)
                        .setMetadata("")
                        .setErrorCode((short) 0));
        OffsetFetchResponseData.OffsetFetchResponseGroup group =
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId("pl-delivery")
                        .setErrorCode((short) 0);
        group.topics().add(topic);
        return new OffsetFetchResponseData().setThrottleTimeMs(0).setGroups(List.of(group));
    }

    static void writePair(Path out, String stem, byte[] req, byte[] resp, List<String> rows) throws Exception {
        Files.write(out.resolve(stem + "-request.bin"), req);
        Files.write(out.resolve(stem + "-response.bin"), resp);
        rows.add("{\"cell\":\"" + stem + "\",\"request_sha256\":\"" + sha(req)
                + "\",\"response_sha256\":\"" + sha(resp) + "\",\"request_bytes\":" + req.length
                + ",\"response_bytes\":" + resp.length + "}");
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;

        byte[] a01Req = encode(fetchRequest(2L, 1, (byte) 0), FETCH_VERSION);
        byte[] a01Resp = encode(a01(), FETCH_VERSION);
        writePair(out, "a01-offset-filter", a01Req, a01Resp, rows);
        bodies += 2;

        byte[] a02Req = encode(fetchRequest(0L, 1, (byte) 1), FETCH_VERSION);
        byte[] a02Resp = encode(a02(), FETCH_VERSION);
        writePair(out, "a02-abort-interval", a02Req, a02Resp, rows);
        bodies += 2;

        byte[] a03Req = encode(fetchRequest(0L, 2, (byte) 0), FETCH_VERSION);
        byte[] a03a = encode(a03(0), FETCH_VERSION);
        byte[] a03b = encode(a03(1), FETCH_VERSION);
        Files.write(out.resolve("a03-partial-failure-request.bin"), a03Req);
        Files.write(out.resolve("a03-partial-failure-attempt0-response.bin"), a03a);
        Files.write(out.resolve("a03-partial-failure-attempt1-response.bin"), a03b);
        rows.add("{\"cell\":\"a03-partial-failure\",\"request_sha256\":\"" + sha(a03Req)
                + "\",\"attempt0_sha256\":\"" + sha(a03a) + "\",\"attempt1_sha256\":\"" + sha(a03b) + "\"}");
        bodies += 3;

        byte[] a04Req = encode(fetchRequest(0L, 1, (byte) 0), FETCH_VERSION);
        byte[] a04Resp = encode(a04(), FETCH_VERSION);
        writePair(out, "a04-offset-out-of-range", a04Req, a04Resp, rows);
        bodies += 2;

        byte[] a05Req = encode(fetchRequest(0L, 1, (byte) 0), FETCH_VERSION);
        byte[] a05Resp = encode(a05(), FETCH_VERSION);
        writePair(out, "a05-cursor-commit", a05Req, a05Resp, rows);
        bodies += 2;

        byte[] commitReq = encode(offsetCommitRequest(), OFFSET_COMMIT_VERSION);
        byte[] commitResp = encode(offsetCommitResponse(), OFFSET_COMMIT_VERSION);
        writePair(out, "a05-offset-commit", commitReq, commitResp, rows);
        bodies += 2;

        byte[] fetchOffReq = encode(offsetFetchRequest(), OFFSET_FETCH_VERSION);
        byte[] fetchOffResp = encode(offsetFetchResponse(), OFFSET_FETCH_VERSION);
        writePair(out, "a05-offset-fetch", fetchOffReq, fetchOffResp, rows);
        bodies += 2;

        String expected = "{\"a01_delivered_offsets\":[2],\"a02_delivered_offsets\":[2],"
                + "\"a03_delivered_partitions\":[[0,0],[1,0]],\"a04_error\":1,\"a04_position\":0,"
                + "\"a05_first_poll\":[0],\"a05_position\":1,\"a05_fetch_cursor\":3,"
                + "\"a05_commit_offset\":1}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"fetch_version\":" + FETCH_VERSION + ","
                + "\"offset_commit_version\":" + OFFSET_COMMIT_VERSION + ","
                + "\"offset_fetch_version\":" + OFFSET_FETCH_VERSION + ","
                + "\"generated_bodies\":" + bodies + ","
                + "\"cells\":[" + String.join(",", rows) + "]}\n";
        Files.writeString(out.resolve("identity.json"), identity);
        System.out.println("{\"generated_bodies\":" + bodies + ",\"unsupported_receipts\":0}");
    }

    static void decodeRust(String kind, short version, String hex) throws Exception {
        byte[] bytes = HexFormat.of().parseHex(hex);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        if (kind.equals("fetch-response")) {
            FetchResponseData resp = new FetchResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed Fetch response");
            }
            int topics = resp.responses().size();
            int parts = topics == 0 ? 0 : resp.responses().get(0).partitions().size();
            int error = parts == 0 ? -1 : resp.responses().get(0).partitions().get(0).errorCode();
            System.out.println("{\"ok\":true,\"kind\":\"fetch-response\",\"topics\":" + topics
                    + ",\"partitions\":" + parts + ",\"first_error\":" + error + "}");
            return;
        }
        if (kind.equals("offset-commit")) {
            OffsetCommitRequestData req = new OffsetCommitRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed OffsetCommit");
            }
            long offset = req.topics().get(0).partitions().get(0).committedOffset();
            System.out.println("{\"ok\":true,\"kind\":\"offset-commit\",\"committed_offset\":" + offset + "}");
            return;
        }
        if (kind.equals("offset-fetch")) {
            OffsetFetchResponseData resp = new OffsetFetchResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed OffsetFetch");
            }
            long offset = resp.groups().get(0).topics().get(0).partitions().get(0).committedOffset();
            System.out.println("{\"ok\":true,\"kind\":\"offset-fetch\",\"committed_offset\":" + offset + "}");
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
