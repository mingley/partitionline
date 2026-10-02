/* Independent Apache Fetch4–6/ListOffsets1–3 wire builders, parsers and error methods. */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.*;

public final class FetchOracle {
    private static final List<String> CASES = new ArrayList<>();
    private static final StringBuilder TSV = new StringBuilder();
    private static Path output;
    private FetchOracle() { }
    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder result = new StringBuilder("\"");
        for (char ch : value.toCharArray()) {
            if (ch == '"' || ch == '\\') result.append('\\').append(ch);
            else if (ch < 32) result.append(String.format("\\u%04x", (int) ch));
            else result.append(ch);
        }
        return result.append('"').toString();
    }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static MemoryRecords first() {
        return MemoryRecords.withRecords(0L, Compression.NONE,
            new SimpleRecord(1000L, (byte[]) null, (byte[]) null),
            new SimpleRecord(1007L, new byte[0], utf8("payload"), new Header[]{new RecordHeader("dup", utf8("a")), new RecordHeader("dup", null)}),
            new SimpleRecord(1003L, utf8("key2"), new byte[0]));
    }
    private static MemoryRecords second() {
        return MemoryRecords.withRecords(3L, Compression.NONE, new SimpleRecord(1010L, utf8("key3"), utf8("value3")));
    }
    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate(); byte[] data = new byte[buffer.remaining()]; buffer.get(data); return data;
    }
    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length); return result;
    }
    private static MemoryRecords all() { return MemoryRecords.readableRecords(ByteBuffer.wrap(concat(bytes(first()), bytes(second())))); }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.hasRemaining()) throw new AssertionError("serializer underwrite"); return buffer.array();
    }
    private static String hash(byte[] data) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data)); }
    private static FetchRequestData fetch(String name, int partition, long offset, int maxBytes, int partBytes, byte isolation) {
        FetchRequestData data = new FetchRequestData().setReplicaId(-1).setMaxWaitMs(0).setMinBytes(0)
            .setMaxBytes(maxBytes).setIsolationLevel(isolation);
        data.topics().add(new FetchRequestData.FetchTopic().setTopic(name).setPartitions(new ArrayList<>(List.of(
            new FetchRequestData.FetchPartition().setPartition(partition).setFetchOffset(offset).setPartitionMaxBytes(partBytes)))));
        return data;
    }
    private static FetchResponseData fetchSuccess(FetchRequestData request, short version, MemoryRecords records, long end) {
        FetchResponseData.PartitionData part = new FetchResponseData.PartitionData()
            .setPartitionIndex(request.topics().get(0).partitions().get(0).partition()).setErrorCode((short) 0)
            .setHighWatermark(end).setLastStableOffset(end).setAbortedTransactions(request.isolationLevel() == 1 ? new ArrayList<>() : null)
            .setRecords(records);
        if (version >= 5) part.setLogStartOffset(0);
        return new FetchResponseData().setResponses(new ArrayList<>(List.of(
            new FetchResponseData.FetchableTopicResponse().setTopic(request.topics().get(0).topic()).setPartitions(new ArrayList<>(List.of(part))))));
    }
    private static ListOffsetsRequestData list(String name, int partition, long timestamp, byte isolation) {
        ListOffsetsRequestData data = new ListOffsetsRequestData().setReplicaId(-1).setIsolationLevel(isolation);
        data.topics().add(new ListOffsetsRequestData.ListOffsetsTopic().setName(name).setPartitions(new ArrayList<>(List.of(
            new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(partition).setTimestamp(timestamp)))));
        return data;
    }
    private static ListOffsetsResponseData listSuccess(ListOffsetsRequestData request, long timestamp, long offset) {
        return new ListOffsetsResponseData().setTopics(new ArrayList<>(List.of(
            new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(request.topics().get(0).name()).setPartitions(new ArrayList<>(List.of(
                new ListOffsetsResponseData.ListOffsetsPartitionResponse().setPartitionIndex(request.topics().get(0).partitions().get(0).partitionIndex())
                    .setErrorCode((short) 0).setTimestamp(timestamp).setOffset(offset)))))));
    }
    private static void emit(String label, ApiKeys api, short version, Message data, Message response, Errors error, String basis) throws Exception {
        String name = (api == ApiKeys.FETCH ? "fetch-v" : "list-offsets-v") + version + "-" + label;
        RequestHeader header = new RequestHeader(api, version, "fetch-oracle", 7);
        byte[] body = encode(data, version); byte[] frame = concat(encode(header.data(), header.headerVersion()), body);
        ByteBuffer buffer = ByteBuffer.wrap(frame); RequestHeader.parse(buffer);
        AbstractRequest parsed = AbstractRequest.parseRequest(api, version, new ByteBufferAccessor(buffer)).request;
        if (buffer.hasRemaining() || !Arrays.equals(encode(parsed.data(), version), body)) throw new AssertionError("request round trip");
        String errorMethod = "{\"scope\":\"Declared ordinary-log outcome encoded/parsed by Apache; storage component results/source dispatch are separately pinned. No Apache broker runtime.\"}";
        if (error != Errors.NONE) {
            AbstractResponse upstream = parsed.getErrorResponse(0, error.exception());
            response = upstream.data();
            errorMethod = "{\"status\":\"executed\",\"exception_code\":" + error.code() + ",\"apache_data\":" + quote(response.toString()) + "}";
        }
        short hv = api.responseHeaderVersion(version); byte[] responseBody = encode(response, version);
        byte[] responseFrame = concat(encode(new ResponseHeader(7, hv).data(), hv), responseBody);
        ByteBuffer responseBuffer = ByteBuffer.wrap(responseFrame); ResponseHeader.parse(responseBuffer, hv);
        AbstractResponse roundTrip = AbstractResponse.parseResponse(api, new ByteBufferAccessor(responseBuffer), version);
        if (responseBuffer.hasRemaining() || !Arrays.equals(encode(roundTrip.data(), version), responseBody)) throw new AssertionError("response round trip");
        if (frame.length > 128 * 1024 || responseFrame.length > 128 * 1024) throw new AssertionError("fixture frame bound");
        Files.write(output.resolve(name + ".request.bin"), frame); Files.write(output.resolve(name + ".response.bin"), responseFrame);
        CASES.add("{\"name\":" + quote(name) + ",\"api_key\":" + api.id + ",\"api_version\":" + version
            + ",\"seed\":\"log\",\"expected_outcome\":\"response\",\"request_hex\":" + quote(HexFormat.of().formatHex(frame))
            + ",\"response_hex\":" + quote(HexFormat.of().formatHex(responseFrame)) + ",\"request_sha256\":" + quote(hash(frame))
            + ",\"response_sha256\":" + quote(hash(responseFrame)) + ",\"selected_error\":" + error.code()
            + ",\"basis\":" + quote(basis) + ",\"apache_get_error_response\":" + errorMethod
            + ",\"apache_parsed_request\":" + quote(parsed.data().toString()) + "}");
        TSV.append(name).append('\t').append(api.id).append('\t').append(version).append("\tlog\tresponse\n");
    }
    private static void structural(String label, ApiKeys api, short version, Message data, boolean trailing) throws Exception {
        String name = (api == ApiKeys.FETCH ? "fetch-v" : "list-offsets-v") + version + "-" + label;
        RequestHeader header = new RequestHeader(api, version, "fetch-oracle", 7);
        byte[] canonical = concat(encode(header.data(), header.headerVersion()), encode(data, version));
        byte[] frame = Arrays.copyOf(canonical, canonical.length + (trailing ? 1 : -1));
        ByteBuffer buffer = ByteBuffer.wrap(frame); String upstream;
        try {
            RequestHeader.parse(buffer); AbstractRequest.parseRequest(api, version, new ByteBufferAccessor(buffer));
            upstream = "{\"status\":\"accepted\",\"consumed\":" + buffer.position() + ",\"remaining\":" + buffer.remaining() + "}";
        } catch (RuntimeException failure) {
            upstream = "{\"status\":\"rejected\",\"consumed\":" + buffer.position() + ",\"remaining\":" + buffer.remaining()
                + ",\"exception\":" + quote(failure.getClass().getName()) + ",\"message\":" + quote(failure.getMessage()) + "}";
        }
        Files.write(output.resolve(name + ".request.bin"), frame);
        CASES.add("{\"name\":" + quote(name) + ",\"api_key\":" + api.id + ",\"api_version\":" + version
            + ",\"seed\":\"log\",\"expected_outcome\":\"structural_reject\",\"request_hex\":" + quote(HexFormat.of().formatHex(frame))
            + ",\"response_hex\":null,\"request_sha256\":" + quote(hash(frame)) + ",\"response_sha256\":null,\"basis\":\"Explicit bounded full-frame parser rejects truncation/trailing data; actual Apache codec is recorded independently.\",\"apache_parser\":" + upstream + "}");
        TSV.append(name).append('\t').append(api.id).append('\t').append(version).append("\tlog\tstructural_reject\n");
    }
    private static void generateFetch() throws Exception {
        for (short version = 4; version <= 6; version++) {
            for (long offset : new long[]{0, 1, 2, 3, 4}) {
                MemoryRecords selected = offset < 3 ? all() : offset == 3 ? second() : MemoryRecords.EMPTY;
                FetchRequestData data = fetch("alpha", 0, offset, 4096, 4096, (byte) 0);
                emit("offset" + offset, ApiKeys.FETCH, version, data, fetchSuccess(data, version, selected, 4), Errors.NONE,
                    "Declared ordinary durable log. Actual Apache LogSegment offset search/read pins: inside-batch offsets1/2 return the whole containing batch0 plus subsequent batch3; equal log-end4 is empty; HW/LSO4, start0.");
            }
            for (long offset : new long[]{-1, 5, Long.MAX_VALUE})
                emit("out-of-range" + offset, ApiKeys.FETCH, version, fetch("alpha", 0, offset, 4096, 4096, (byte) 0), null, Errors.OFFSET_OUT_OF_RANGE,
                    "Pinned LocalLog rejects negative/outside retained segment range and offsets beyond log end; actual Apache getErrorResponse1 executed. LogSegment alone does not enforce these broker bounds.");
            FetchRequestData empty = fetch("alpha", 1, 0, 4096, 4096, (byte) 0);
            emit("empty-partition", ApiKeys.FETCH, version, empty, fetchSuccess(empty, version, MemoryRecords.EMPTY, 0), Errors.NONE, "Declared alpha1 empty; HW/LSO/start0, no records.");
            emit("unknown-topic", ApiKeys.FETCH, version, fetch("missing", 0, 0, 4096, 4096, (byte) 0), null, Errors.UNKNOWN_TOPIC_OR_PARTITION, "Actual Apache getErrorResponse3 executed for unknown named topic.");
            emit("unknown-partition", ApiKeys.FETCH, version, fetch("alpha", 9, 0, 4096, 4096, (byte) 0), null, Errors.UNKNOWN_TOPIC_OR_PARTITION, "Actual Apache getErrorResponse3 executed; alpha has partitions0/1.");
            FetchRequestData oversized = fetch("alpha", 0, 0, 1, 1, (byte) 0);
            emit("oversized-first-batch", ApiKeys.FETCH, version, oversized, fetchSuccess(oversized, version, first(), 4), Errors.NONE,
                "KIP74 first nonempty batch exception: actual Apache LogSegment.read(maxBytes1,minOneMessagetrue) returns whole first batch larger than both request limits; bounded local configured output maximum still applies.");
            FetchRequestData oversizedSecond = fetch("alpha", 0, 3, 1, 1, (byte) 0);
            emit("oversized-second-batch", ApiKeys.FETCH, version, oversizedSecond, fetchSuccess(oversizedSecond, version, second(), 4), Errors.NONE, "First available batch is batch3; actual Apache LogSegment read pins whole oversized batch despite request limits1.");
            FetchRequestData committed = fetch("alpha", 0, 0, 4096, 4096, (byte) 1);
            emit("read-committed-ordinary", ApiKeys.FETCH, version, committed, fetchSuccess(committed, version, all(), 4), Errors.NONE, "Only declared ordinary nontransactional writes are admitted: read_committed has HW=LSO4 and empty aborted-transactions list. No transactional implementation claimed.");
            FetchRequestData query = new FetchRequestData().setReplicaId(-1).setMaxWaitMs(0).setMinBytes(0).setMaxBytes(4096).setIsolationLevel((byte) 0);
            emit("empty-query", ApiKeys.FETCH, version, query, new FetchResponseData(), Errors.NONE, "Actual Apache serializer/parser for empty topic request/response; no partition output.");
            structural("truncated", ApiKeys.FETCH, version, fetch("alpha", 0, 0, 4096, 4096, (byte) 0), false);
            structural("trailing-zero", ApiKeys.FETCH, version, fetch("alpha", 0, 0, 4096, 4096, (byte) 0), true);
        }
    }
    private static void generateList() throws Exception {
        for (short version = 1; version <= 3; version++) {
            for (long sentinel : new long[]{-2, -1}) {
                ListOffsetsRequestData data = list("alpha", 0, sentinel, (byte) 0);
                emit(sentinel == -2 ? "earliest" : "latest", ApiKeys.LIST_OFFSETS, version, data,
                    listSuccess(data, -1, sentinel == -2 ? 0 : 4), Errors.NONE, "Pinned ListOffsets dispatch: earliest-2 returns retained start0; latest-1 returns ordinary HW/end4; timestamp-1 is sentinel response, not a timestamp search.");
            }
            for (long timestamp : new long[]{0, 999, 1000, 1001, 1003, 1007, 1008, 1010, 1011, Long.MAX_VALUE}) {
                long foundTimestamp = timestamp <= 1000 ? 1000 : timestamp <= 1007 ? 1007 : timestamp <= 1010 ? 1010 : -1;
                long foundOffset = timestamp <= 1000 ? 0 : timestamp <= 1007 ? 1 : timestamp <= 1010 ? 3 : -1;
                ListOffsetsRequestData data = list("alpha", 0, timestamp, (byte) 0);
                emit("timestamp" + timestamp, ApiKeys.LIST_OFFSETS, version, data, listSuccess(data, foundTimestamp, foundOffset), Errors.NONE,
                    "Actual Apache LogSegment.findOffsetByTimestamp across non-monotonic times1000,1007,1003,1010 pins first offset with timestamp>=query; no match is success offset/timestamp-1, not log-end or error.");
            }
            for (long timestamp : new long[]{-10, -6, -5, -4, -3})
                emit("unsupported-timestamp" + timestamp, ApiKeys.LIST_OFFSETS, version, list("alpha", 0, timestamp, (byte) 0), null, Errors.UNSUPPORTED_VERSION,
                    "Pinned ReplicaManager.isListOffsetsTimestampUnsupported rejects unknown negative sentinels and later-version -3/-4/-5/-6 under ListOffsets1–3 as35. Actual Apache getErrorResponse executed; storage component timestamp search alone accepts negatives and is not broker dispatch.");
            for (long sentinel : new long[]{-2, -1}) {
                ListOffsetsRequestData data = list("alpha", 1, sentinel, (byte) 0);
                emit(sentinel == -2 ? "empty-earliest" : "empty-latest", ApiKeys.LIST_OFFSETS, version, data, listSuccess(data, -1, 0), Errors.NONE, "Declared empty partition start/end0; sentinel timestamp remains-1.");
            }
            emit("unknown-topic", ApiKeys.LIST_OFFSETS, version, list("missing", 0, -2, (byte) 0), null, Errors.UNKNOWN_TOPIC_OR_PARTITION, "Actual Apache getErrorResponse3 executed for unknown topic.");
            emit("unknown-partition", ApiKeys.LIST_OFFSETS, version, list("alpha", 9, -2, (byte) 0), null, Errors.UNKNOWN_TOPIC_OR_PARTITION, "Actual Apache getErrorResponse3 executed for unknown partition.");
            if (version >= 2) {
                ListOffsetsRequestData data = list("alpha", 0, -1, (byte) 1);
                emit("read-committed-latest", ApiKeys.LIST_OFFSETS, version, data, listSuccess(data, -1, 4), Errors.NONE, "Ordinary-only admitted log has LSO=HW=end4; no transaction implementation claimed.");
            }
            structural("truncated", ApiKeys.LIST_OFFSETS, version, list("alpha", 0, -2, (byte) 0), false);
            structural("trailing-zero", ApiKeys.LIST_OFFSETS, version, list("alpha", 0, -2, (byte) 0), true);
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("release output-directory");
        output = Path.of(args[1]); Files.createDirectories(output); generateFetch(); generateList();
        Files.write(output.resolve("log-batch-0.bin"), bytes(first())); Files.write(output.resolve("log-batch-3.bin"), bytes(second()));
        Files.writeString(output.resolve("goldens.json"), "{\"release\":" + quote(args[0])
            + ",\"wire\":\"Kafka header/body, no length prefix\",\"seed\":\"Canonical catalog; alpha0 offset0/count3 times1000,1007,1003 plus offset3/count1 time1010; alpha1 empty; HW/LSO4,start0\",\"cases\":[\n"
            + String.join(",\n", CASES) + "\n]}\n");
        Files.writeString(output.resolve("cases.tsv"), TSV.toString());
        System.out.println("{\"release\":" + quote(args[0]) + ",\"cases\":" + CASES.size() + "}");
    }
}
