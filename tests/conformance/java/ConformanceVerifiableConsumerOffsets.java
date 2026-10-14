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
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

/**
 * Independent Apache Fetch / ListOffsets / OffsetCommit / OffsetFetch
 * bodies for the KL01-27 verifiable Consumer reset and autocommit
 * profiles.
 *
 * Cells follow hashed VerifiableConsumer --reset-policy / --enable-autocommit
 * and @JsonPropertyOrder({"timestamp","name"}): latest delivers only the
 * new offset and stores the next offset; none without a stored offset
 * has OffsetFetch -1; none with a stored offset resumes; autocommit
 * interval stores the first next-offset and emits no offsets_committed.
 *
 * Record batches are built through reflection so the same source compiles
 * on 3.9.1–4.2.1 (public MemoryRecords) and 4.3.1 (internal package).
 */
public final class ConformanceVerifiableConsumerOffsets {
    static final Map<String, String> PINS = Map.of(
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9", "3.9.1",
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627", "4.1.0",
            "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431", "4.1.2",
            "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8", "4.2.1",
            "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36", "4.3.1");
    static final short FETCH_VERSION = 12;
    static final short LIST_OFFSETS_VERSION = 5;
    static final short OFFSET_COMMIT_VERSION = 8;
    static final short OFFSET_FETCH_VERSION = 8;
    static final String TOPIC = "pl-offsets";
    static final String GROUP = "pl-offsets";
    static final String MEMBER = "member-a";
    static final long LATEST_TIMESTAMP = -1L;
    static final long INVALID_OFFSET = -1L;
    static final long EVENT_TIMESTAMP_BASE = 1_700_000_100_000L;

    private ConformanceVerifiableConsumerOffsets() {}

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

    static Object dataRecords(long baseOffset, String value) throws Exception {
        Class<?> recordsClass = memoryRecordsClass();
        Class<?> simple = simpleRecordClass();
        Object array = Array.newInstance(simple, 1);
        Array.set(array, 0, simpleRecord(value.getBytes(StandardCharsets.UTF_8)));
        Class<?> compression = Class.forName("org.apache.kafka.common.compress.Compression");
        Method method = recordsClass.getMethod("withRecords", long.class, compression, array.getClass());
        return method.invoke(null, baseOffset, compressionNone(), array);
    }

    static void setRecords(Object holder, Object records) throws Exception {
        for (Method method : holder.getClass().getMethods()) {
            if (method.getName().equals("setRecords") && method.getParameterCount() == 1) {
                method.invoke(holder, records);
                return;
            }
        }
        throw new AssertionError(holder.getClass().getName() + ".setRecords missing");
    }

    static FetchRequestData fetchRequest(long fetchOffset) {
        FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10_485_760)
                .setIsolationLevel((byte) 0)
                .setSessionId(0)
                .setSessionEpoch(0)
                .setRackId("");
        FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic(TOPIC);
        topic.partitions()
                .add(new FetchRequestData.FetchPartition()
                        .setPartition(0)
                        .setFetchOffset(fetchOffset)
                        .setLogStartOffset(-1L)
                        .setPartitionMaxBytes(1_048_576)
                        .setCurrentLeaderEpoch(-1));
        req.topics().add(topic);
        return req;
    }

    static FetchResponseData fetchResponse(long baseOffset, long highWatermark, String value) throws Exception {
        FetchResponseData resp = new FetchResponseData().setThrottleTimeMs(0);
        FetchResponseData.FetchableTopicResponse topic =
                new FetchResponseData.FetchableTopicResponse().setTopic(TOPIC);
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(highWatermark)
                .setLastStableOffset(highWatermark)
                .setLogStartOffset(0L);
        setRecords(part, dataRecords(baseOffset, value));
        topic.partitions().add(part);
        resp.responses().add(topic);
        return resp;
    }

    static ListOffsetsRequestData listOffsetsLatestRequest() {
        ListOffsetsRequestData req = new ListOffsetsRequestData()
                .setReplicaId(-1)
                .setIsolationLevel((byte) 0);
        ListOffsetsRequestData.ListOffsetsTopic topic = new ListOffsetsRequestData.ListOffsetsTopic().setName(TOPIC);
        topic.partitions()
                .add(new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(LATEST_TIMESTAMP)
                        .setCurrentLeaderEpoch(-1));
        req.topics().add(topic);
        return req;
    }

    static ListOffsetsResponseData listOffsetsLatestResponse() {
        ListOffsetsResponseData resp = new ListOffsetsResponseData().setThrottleTimeMs(0);
        ListOffsetsResponseData.ListOffsetsTopicResponse topic =
                new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(TOPIC);
        topic.partitions()
                .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0)
                        .setTimestamp(LATEST_TIMESTAMP)
                        .setOffset(1L)
                        .setLeaderEpoch(0));
        resp.topics().add(topic);
        return resp;
    }

    static OffsetFetchRequestData offsetFetchRequest() {
        OffsetFetchRequestData.OffsetFetchRequestTopics topic =
                new OffsetFetchRequestData.OffsetFetchRequestTopics().setName(TOPIC);
        topic.partitionIndexes().add(0);
        OffsetFetchRequestData.OffsetFetchRequestGroup group =
                new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId(GROUP);
        group.topics().add(topic);
        return new OffsetFetchRequestData().setRequireStable(false).setGroups(List.of(group));
    }

    static OffsetFetchResponseData offsetFetchResponse(long committed) {
        OffsetFetchResponseData.OffsetFetchResponseTopics topic =
                new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(TOPIC);
        topic.partitions()
                .add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(0)
                        .setCommittedOffset(committed)
                        .setCommittedLeaderEpoch(-1)
                        .setMetadata("")
                        .setErrorCode((short) 0));
        OffsetFetchResponseData.OffsetFetchResponseGroup group =
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(GROUP)
                        .setErrorCode((short) 0);
        group.topics().add(topic);
        return new OffsetFetchResponseData().setThrottleTimeMs(0).setGroups(List.of(group));
    }

    static OffsetCommitRequestData offsetCommitRequest(long committed) {
        OffsetCommitRequestData req = new OffsetCommitRequestData()
                .setGroupId(GROUP)
                .setGenerationIdOrMemberEpoch(1)
                .setMemberId(MEMBER);
        OffsetCommitRequestData.OffsetCommitRequestTopic topic =
                new OffsetCommitRequestData.OffsetCommitRequestTopic().setName(TOPIC);
        topic.partitions()
                .add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                        .setPartitionIndex(0)
                        .setCommittedOffset(committed)
                        .setCommittedLeaderEpoch(-1)
                        .setCommittedMetadata(""));
        req.topics().add(topic);
        return req;
    }

    static OffsetCommitResponseData offsetCommitResponse() {
        OffsetCommitResponseData resp = new OffsetCommitResponseData().setThrottleTimeMs(0);
        OffsetCommitResponseData.OffsetCommitResponseTopic topic =
                new OffsetCommitResponseData.OffsetCommitResponseTopic().setName(TOPIC);
        topic.partitions()
                .add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode((short) 0));
        resp.topics().add(topic);
        return resp;
    }

    static void writePair(Path out, String stem, byte[] req, byte[] resp, List<String> rows) throws Exception {
        Files.write(out.resolve(stem + "-request.bin"), req);
        Files.write(out.resolve(stem + "-response.bin"), resp);
        rows.add("{\"cell\":\"" + stem + "\",\"request_sha256\":\"" + sha(req)
                + "\",\"response_sha256\":\"" + sha(resp) + "\",\"request_bytes\":" + req.length
                + ",\"response_bytes\":" + resp.length + "}");
    }

    static void roundtrip(Message original, short version, String cell) {
        byte[] encoded = encode(original, version);
        ByteBufferAccessor accessor = new ByteBufferAccessor(ByteBuffer.wrap(encoded));
        Message back;
        if (original instanceof FetchRequestData) {
            back = new FetchRequestData(accessor, version);
        } else if (original instanceof FetchResponseData) {
            back = new FetchResponseData(accessor, version);
        } else if (original instanceof ListOffsetsRequestData) {
            back = new ListOffsetsRequestData(accessor, version);
        } else if (original instanceof ListOffsetsResponseData) {
            back = new ListOffsetsResponseData(accessor, version);
        } else if (original instanceof OffsetCommitRequestData) {
            back = new OffsetCommitRequestData(accessor, version);
        } else if (original instanceof OffsetCommitResponseData) {
            back = new OffsetCommitResponseData(accessor, version);
        } else if (original instanceof OffsetFetchRequestData) {
            back = new OffsetFetchRequestData(accessor, version);
        } else if (original instanceof OffsetFetchResponseData) {
            back = new OffsetFetchResponseData(accessor, version);
        } else {
            throw new AssertionError("unknown message " + original.getClass().getName());
        }
        if (!Arrays.equals(encoded, encode(back, version))) {
            throw new AssertionError("Apache self-roundtrip failed " + cell);
        }
    }

    static String event(long timestamp, String name, String fields) {
        return "{\"timestamp\":" + timestamp + ",\"name\":\"" + name + '"' + fields + '}';
    }

    static String assigned(long timestamp) {
        return event(
                timestamp,
                "partitions_assigned",
                ",\"partitions\":[{\"topic\":\"" + TOPIC + "\",\"partition\":0}]");
    }

    static String recordData(long timestamp, String value, long offset) {
        return event(
                timestamp,
                "record_data",
                ",\"key\":null,\"value\":\"" + value + "\",\"topic\":\"" + TOPIC
                        + "\",\"partition\":0,\"offset\":" + offset);
    }

    static String recordsConsumed(long timestamp, long offset) {
        return event(
                timestamp,
                "records_consumed",
                ",\"count\":1,\"partitions\":[{\"topic\":\"" + TOPIC
                        + "\",\"partition\":0,\"count\":1,\"minOffset\":" + offset
                        + ",\"maxOffset\":" + offset + "}]");
    }

    static String offsetsCommitted(long timestamp, long offset) {
        return event(
                timestamp,
                "offsets_committed",
                ",\"offsets\":[{\"topic\":\"" + TOPIC + "\",\"partition\":0,\"offset\":" + offset
                        + "}],\"success\":true");
    }

    static void writeEvents(Path out) throws Exception {
        long t = EVENT_TIMESTAMP_BASE;
        List<String> latest = new ArrayList<>();
        latest.add(event(t, "startup_complete", ""));
        latest.add(assigned(t + 1));
        latest.add(recordData(t + 2, "new", 1));
        latest.add(recordsConsumed(t + 3, 1));
        latest.add(offsetsCommitted(t + 4, 2));
        latest.add(event(t + 5, "shutdown_complete", ""));

        List<String> noneMissing = new ArrayList<>();
        noneMissing.add(event(t + 10, "startup_complete", ""));
        noneMissing.add(assigned(t + 11));
        noneMissing.add(event(t + 12, "shutdown_complete", ""));

        List<String> noneResume = new ArrayList<>();
        noneResume.add(event(t + 20, "startup_complete", ""));
        noneResume.add(assigned(t + 21));
        noneResume.add(recordData(t + 22, "after-commit", 2));
        noneResume.add(recordsConsumed(t + 23, 2));
        noneResume.add(offsetsCommitted(t + 24, 3));
        noneResume.add(event(t + 25, "shutdown_complete", ""));

        List<String> autocommit = new ArrayList<>();
        autocommit.add(event(t + 30, "startup_complete", ""));
        autocommit.add(assigned(t + 31));
        autocommit.add(recordData(t + 32, "first", 0));
        autocommit.add(recordsConsumed(t + 33, 0));
        autocommit.add(recordData(t + 34, "second", 1));
        autocommit.add(recordsConsumed(t + 35, 1));
        autocommit.add(event(t + 36, "shutdown_complete", ""));

        StringBuilder json = new StringBuilder();
        json.append("{\"json_property_order\":[\"timestamp\",\"name\"],")
                .append("\"record_data_property_order\":[\"timestamp\",\"name\",\"key\",\"value\",\"topic\",\"partition\",\"offset\"],")
                .append("\"topic\":\"")
                .append(TOPIC)
                .append("\",\"latest\":[")
                .append(String.join(",", latest))
                .append("],\"none_missing\":[")
                .append(String.join(",", noneMissing))
                .append("],\"none_resume\":[")
                .append(String.join(",", noneResume))
                .append("],\"autocommit\":[")
                .append(String.join(",", autocommit))
                .append("]}\n");
        Files.writeString(out.resolve("events.json"), json.toString());
    }

    static void writeProfiles(Path out) throws Exception {
        String profiles = "{\"topic\":\"" + TOPIC + "\",\"group_id\":\"" + GROUP + "\""
                + ",\"reset_policies\":[\"earliest\",\"latest\",\"none\"]"
                + ",\"default_reset_policy\":\"earliest\""
                + ",\"enable_autocommit_default\":false"
                + ",\"verbose_record_data\":true"
                + ",\"manual_commit_next_offset\":true"
                + ",\"autocommit_omits_offsets_committed\":true"
                + ",\"close_does_not_autocommit_final_poll\":true"
                + ",\"named_options\":[\"topic\",\"group-id\",\"bootstrap-server\",\"broker-list\","
                + "\"max-messages\",\"session-timeout\",\"verbose\",\"enable-autocommit\",\"reset-policy\","
                + "\"assignment-strategy\",\"consumer.config\"]"
                + ",\"defaults\":{\"reset-policy\":\"earliest\",\"max-messages\":-1,\"session-timeout\":30000,"
                + "\"enable-autocommit\":false}}\n";
        Files.writeString(out.resolve("profiles.json"), profiles);
    }

    static void generate(Path jar, Path out) throws Exception {
        Path loaded = Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !PINS.containsKey(sha(Files.readAllBytes(jar)))) {
            throw new AssertionError("wrong pinned Apache jar");
        }
        Files.createDirectories(out);
        List<String> rows = new ArrayList<>();
        int bodies = 0;

        ListOffsetsRequestData listReq = listOffsetsLatestRequest();
        ListOffsetsResponseData listResp = listOffsetsLatestResponse();
        roundtrip(listReq, LIST_OFFSETS_VERSION, "list-offsets-latest-request");
        roundtrip(listResp, LIST_OFFSETS_VERSION, "list-offsets-latest-response");
        writePair(out, "list-offsets-latest", encode(listReq, LIST_OFFSETS_VERSION),
                encode(listResp, LIST_OFFSETS_VERSION), rows);
        bodies += 2;

        OffsetFetchRequestData fetchEmptyReq = offsetFetchRequest();
        OffsetFetchResponseData fetchEmptyResp = offsetFetchResponse(INVALID_OFFSET);
        roundtrip(fetchEmptyReq, OFFSET_FETCH_VERSION, "offset-fetch-empty-request");
        roundtrip(fetchEmptyResp, OFFSET_FETCH_VERSION, "offset-fetch-empty-response");
        writePair(out, "offset-fetch-empty", encode(fetchEmptyReq, OFFSET_FETCH_VERSION),
                encode(fetchEmptyResp, OFFSET_FETCH_VERSION), rows);
        bodies += 2;

        OffsetFetchRequestData fetchStoredReq = offsetFetchRequest();
        OffsetFetchResponseData fetchStoredResp = offsetFetchResponse(2L);
        roundtrip(fetchStoredReq, OFFSET_FETCH_VERSION, "offset-fetch-stored-request");
        roundtrip(fetchStoredResp, OFFSET_FETCH_VERSION, "offset-fetch-stored-response");
        writePair(out, "offset-fetch-stored", encode(fetchStoredReq, OFFSET_FETCH_VERSION),
                encode(fetchStoredResp, OFFSET_FETCH_VERSION), rows);
        bodies += 2;

        FetchRequestData newReq = fetchRequest(1L);
        FetchResponseData newResp = fetchResponse(1L, 2L, "new");
        roundtrip(newReq, FETCH_VERSION, "fetch-new-request");
        roundtrip(newResp, FETCH_VERSION, "fetch-new-response");
        writePair(out, "fetch-new", encode(newReq, FETCH_VERSION), encode(newResp, FETCH_VERSION), rows);
        bodies += 2;

        FetchRequestData resumeReq = fetchRequest(2L);
        FetchResponseData resumeResp = fetchResponse(2L, 3L, "after-commit");
        roundtrip(resumeReq, FETCH_VERSION, "fetch-resume-request");
        roundtrip(resumeResp, FETCH_VERSION, "fetch-resume-response");
        writePair(out, "fetch-resume", encode(resumeReq, FETCH_VERSION), encode(resumeResp, FETCH_VERSION), rows);
        bodies += 2;

        FetchRequestData firstReq = fetchRequest(0L);
        FetchResponseData firstResp = fetchResponse(0L, 2L, "first");
        roundtrip(firstReq, FETCH_VERSION, "fetch-first-request");
        roundtrip(firstResp, FETCH_VERSION, "fetch-first-response");
        writePair(out, "fetch-first", encode(firstReq, FETCH_VERSION), encode(firstResp, FETCH_VERSION), rows);
        bodies += 2;

        OffsetCommitRequestData commit2 = offsetCommitRequest(2L);
        OffsetCommitResponseData commit2Resp = offsetCommitResponse();
        roundtrip(commit2, OFFSET_COMMIT_VERSION, "offset-commit-next-2-request");
        roundtrip(commit2Resp, OFFSET_COMMIT_VERSION, "offset-commit-next-2-response");
        writePair(out, "offset-commit-next-2", encode(commit2, OFFSET_COMMIT_VERSION),
                encode(commit2Resp, OFFSET_COMMIT_VERSION), rows);
        bodies += 2;

        OffsetCommitRequestData commit1 = offsetCommitRequest(1L);
        OffsetCommitResponseData commit1Resp = offsetCommitResponse();
        roundtrip(commit1, OFFSET_COMMIT_VERSION, "offset-commit-interval-1-request");
        roundtrip(commit1Resp, OFFSET_COMMIT_VERSION, "offset-commit-interval-1-response");
        writePair(out, "offset-commit-interval-1", encode(commit1, OFFSET_COMMIT_VERSION),
                encode(commit1Resp, OFFSET_COMMIT_VERSION), rows);
        bodies += 2;

        writeEvents(out);
        writeProfiles(out);

        String expected = "{\"topic\":\"" + TOPIC + "\",\"group_id\":\"" + GROUP + "\""
                + ",\"latest\":{\"list_offsets_timestamp\":-1,\"list_offsets_offset\":1,"
                + "\"offset_fetch\":-1,\"fetch_offset\":1,\"delivered_value\":\"new\","
                + "\"delivered_offset\":1,\"commit_offset\":2}"
                + ",\"none_missing\":{\"offset_fetch\":-1,\"delivered\":false,\"committed\":false}"
                + ",\"none_resume\":{\"offset_fetch\":2,\"fetch_offset\":2,"
                + "\"delivered_value\":\"after-commit\",\"delivered_offset\":2,\"commit_offset\":3}"
                + ",\"autocommit\":{\"fetch_offset\":0,\"delivered_values\":[\"first\",\"second\"],"
                + "\"delivered_offsets\":[0,1],\"interval_commit_offset\":1,"
                + "\"close_commit\":false,\"offsets_committed_events\":0}}\n";
        Files.writeString(out.resolve("expected.json"), expected);

        String identity = "{\"apache_client_version\":\"" + PINS.get(sha(Files.readAllBytes(jar))) + "\","
                + "\"jar_sha256\":\"" + sha(Files.readAllBytes(jar)) + "\","
                + "\"fetch_version\":" + FETCH_VERSION + ","
                + "\"list_offsets_version\":" + LIST_OFFSETS_VERSION + ","
                + "\"offset_commit_version\":" + OFFSET_COMMIT_VERSION + ","
                + "\"offset_fetch_version\":" + OFFSET_FETCH_VERSION + ","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"group_id\":\"" + GROUP + "\","
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

    static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    static String firstValue(Object records) throws Exception {
        Iterable<?> it = (Iterable<?>) records.getClass().getMethod("records").invoke(records);
        for (Object rec : it) {
            ByteBuffer val = (ByteBuffer) rec.getClass().getMethod("value").invoke(rec);
            if (val == null) {
                return null;
            }
            byte[] bytes = new byte[val.remaining()];
            val.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
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
            FetchResponseData.PartitionData part = first(first(resp.responses()).partitions());
            System.out.println("{\"ok\":true,\"kind\":\"fetch-response\",\"high_watermark\":"
                    + part.highWatermark() + ",\"first_value\":" + jsonString(firstValue(part.records())) + "}");
            return;
        }
        if (kind.equals("list-offsets-response")) {
            ListOffsetsResponseData resp = new ListOffsetsResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed ListOffsets response");
            }
            long offset = first(first(resp.topics()).partitions()).offset();
            System.out.println("{\"ok\":true,\"kind\":\"list-offsets-response\",\"offset\":" + offset + "}");
            return;
        }
        if (kind.equals("offset-commit")) {
            OffsetCommitRequestData req = new OffsetCommitRequestData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed OffsetCommit");
            }
            long offset = first(first(req.topics()).partitions()).committedOffset();
            System.out.println("{\"ok\":true,\"kind\":\"offset-commit\",\"committed_offset\":" + offset + "}");
            return;
        }
        if (kind.equals("offset-fetch")) {
            OffsetFetchResponseData resp = new OffsetFetchResponseData(accessor, version);
            if (buf.hasRemaining()) {
                throw new AssertionError("unconsumed OffsetFetch");
            }
            long offset = first(first(first(resp.groups()).topics()).partitions()).committedOffset();
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
