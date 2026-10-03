import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

/** Official serializers/parsers plus an explicit independently declared RF1 seed policy. */
public final class DeleteRecordsGoldens {
    private static final List<Map<String, Object>> CASES = new ArrayList<>();
    private static final List<Map<String, Object>> HELPERS = new ArrayList<>();
    private static final StringBuilder TABLE = new StringBuilder();
    private static Path directory;
    private static int checks;

    private DeleteRecordsGoldens() { }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
        return result;
    }

    private static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof String string) {
            return "\"" + string.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof List<?> list) return "[" + String.join(",", list.stream().map(DeleteRecordsGoldens::json).toList()) + "]";
        if (value instanceof Map<?, ?> map) {
            List<String> members = new ArrayList<>();
            for (var item : map.entrySet()) members.add(json(item.getKey()) + ":" + json(item.getValue()));
            return "{" + String.join(",", members) + "}";
        }
        throw new IllegalArgumentException("Unknown JSON object");
    }

    private static byte[] encode(Message value, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(value.size(cache, version));
        value.write(new ByteBufferAccessor(buffer), cache, version);
        return buffer.array();
    }

    private static byte[] response(DeleteRecordsResponseData data, short version) {
        ResponseHeader header = new ResponseHeader(7, ApiKeys.DELETE_RECORDS.responseHeaderVersion(version));
        byte[] first = encode(header.data(), header.headerVersion()), second = encode(data, version);
        byte[] full = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, full, first.length, second.length);
        return full;
    }

    private static String sha(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static DeleteRecordsTopic topic(String name, long... indexOffsetPairs) {
        List<DeleteRecordsPartition> parts = new ArrayList<>();
        for (int i = 0; i < indexOffsetPairs.length; i += 2) {
            parts.add(new DeleteRecordsPartition().setPartitionIndex(Math.toIntExact(indexOffsetPairs[i]))
                .setOffset(indexOffsetPairs[i + 1]));
        }
        return new DeleteRecordsTopic().setName(name).setPartitions(parts);
    }

    private static DeleteRecordsRequest request(short version, List<DeleteRecordsTopic> topics, int timeout) {
        return new DeleteRecordsRequest.Builder(new DeleteRecordsRequestData().setTimeoutMs(timeout).setTopics(topics)).build(version);
    }

    private static byte[] frame(DeleteRecordsRequest request) {
        ByteBuffer buffer = request.serializeWithHeader(new RequestHeader(ApiKeys.DELETE_RECORDS,
            request.version(), "retention-oracle", 7));
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static Map<String, Object> parseRequest(byte[] bytes, short version) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            RequestHeader header = RequestHeader.parse(buffer);
            DeleteRecordsRequest parsed = DeleteRecordsRequest.parse(new ByteBufferAccessor(buffer), version);
            return row("accepted", true, "correlation_id", header.correlationId(), "request_header_version", header.headerVersion(),
                "remaining_bytes", buffer.remaining(), "parsed_data", parsed.data().toString());
        } catch (RuntimeException failure) {
            return row("accepted", false, "exception_class", failure.getClass().getName(), "remaining_bytes", buffer.remaining());
        }
    }

    private static DeleteRecordsResponseData declaredResponse(DeleteRecordsRequest request, String seed) {
        // KafkaApis builds a TopicPartition map: repeated identity uses the final offset.
        // Sorting is an explicit local deterministic order, not an upstream order guarantee.
        TreeMap<String, TreeMap<Integer, Long>> grouped = new TreeMap<>();
        for (DeleteRecordsTopic topic : request.data().topics()) {
            for (DeleteRecordsPartition partition : topic.partitions()) {
                grouped.computeIfAbsent(topic.name(), ignored -> new TreeMap<>())
                    .put(partition.partitionIndex(), partition.offset());
            }
        }
        DeleteRecordsTopicResultCollection topics = new DeleteRecordsTopicResultCollection();
        for (var topic : grouped.entrySet()) {
            DeleteRecordsPartitionResultCollection partitions = new DeleteRecordsPartitionResultCollection();
            for (var part : topic.getValue().entrySet()) {
                long hw = !seed.equals("fixture_empty") && topic.getKey().equals("alpha") && part.getKey() == 0 ? 4 : 0;
                long oldFloor = seed.equals("fixture_floor_2") && topic.getKey().equals("alpha") && part.getKey() == 0 ? 2 : 0;
                long offset = part.getValue() == -1 ? hw : part.getValue();
                short error;
                long low;
                if ((!topic.getKey().equals("alpha") && !topic.getKey().equals("__consumer_offsets"))
                    || part.getKey() < 0 || part.getKey() >= (topic.getKey().equals("alpha") ? 2 : 1)) {
                    error = Errors.UNKNOWN_TOPIC_OR_PARTITION.code();
                    low = -1;
                } else if (topic.getKey().equals("__consumer_offsets")) {
                    error = Errors.INVALID_TOPIC_EXCEPTION.code();
                    low = -1;
                } else if (offset < 0 || offset > hw) {
                    error = Errors.OFFSET_OUT_OF_RANGE.code();
                    low = -1;
                } else {
                    error = Errors.NONE.code();
                    low = Math.max(oldFloor, offset);
                }
                partitions.add(new DeleteRecordsPartitionResult().setPartitionIndex(part.getKey())
                    .setLowWatermark(low).setErrorCode(error));
            }
            topics.add(new DeleteRecordsTopicResult().setName(topic.getKey()).setPartitions(partitions));
        }
        return new DeleteRecordsResponseData().setThrottleTimeMs(0).setTopics(topics);
    }

    private static List<Map<String, Object>> results(DeleteRecordsResponseData response) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (DeleteRecordsTopicResult topic : response.topics()) {
            for (DeleteRecordsPartitionResult part : topic.partitions()) {
                list.add(row("topic", topic.name(), "partition", part.partitionIndex(),
                    "low_watermark", part.lowWatermark(), "error_code", part.errorCode()));
            }
        }
        return list;
    }

    private static void writeCase(String suffix, short version, String seed, DeleteRecordsRequest request,
                                  byte[] requestBytes, String outcome) throws IOException {
        String name = "delete-records-v" + version + "-" + suffix;
        byte[] responseBytes = null;
        DeleteRecordsResponseData data = null;
        Map<String, Object> requestParse = parseRequest(requestBytes, version);
        if (outcome.equals("response")) {
            if (!requestParse.get("accepted").equals(true) || !requestParse.get("remaining_bytes").equals(0)) {
                throw new AssertionError("Official request did not parse exactly: " + name);
            }
            data = declaredResponse(request, seed);
            responseBytes = response(data, version);
            ByteBuffer parsed = ByteBuffer.wrap(responseBytes);
            ResponseHeader header = ResponseHeader.parse(parsed, ApiKeys.DELETE_RECORDS.responseHeaderVersion(version));
            DeleteRecordsResponse response = DeleteRecordsResponse.parse(new ByteBufferAccessor(parsed), version);
            if (header.correlationId() != 7 || parsed.hasRemaining() || !response.data().equals(data)) {
                throw new AssertionError("Official response round trip failed: " + name);
            }
            checks += 2;
        } else {
            checks++;
        }
        if (requestBytes.length > 128 * 1024 || responseBytes != null && responseBytes.length > 128 * 1024) {
            throw new AssertionError("Fixture budget");
        }
        Files.write(directory.resolve(name + ".request.bin"), requestBytes);
        if (responseBytes != null) Files.write(directory.resolve(name + ".response.bin"), responseBytes);
        TABLE.append(name).append('\t').append(21).append('\t').append(version).append('\t')
            .append(seed).append('\t').append(outcome).append('\n');
        CASES.add(row("name", name, "api_key", 21, "api_version", version, "seed", seed,
            "expected_outcome", outcome, "request_hex", HexFormat.of().formatHex(requestBytes),
            "response_hex", responseBytes == null ? null : HexFormat.of().formatHex(responseBytes),
            "request_sha256", sha(requestBytes), "response_sha256", responseBytes == null ? null : sha(responseBytes),
            "request_header_version", ApiKeys.DELETE_RECORDS.requestHeaderVersion(version),
            "response_header_version", ApiKeys.DELETE_RECORDS.responseHeaderVersion(version),
            "apache_request_parse", requestParse, "response_results", data == null ? null : results(data),
            "basis", outcome.equals("response") ? "Independent declared ordinary RF1 floor/HW policy encoded and parsed by actual Apache serializers; no Apache broker execution. Duplicate map behavior is source-pinned, sorted response order is local." :
                "Explicit local fail-closed structural request policy; actual Apache parser acceptance/remainder is retained separately."));
    }

    private static void normal(String name, short version, String seed, DeleteRecordsTopic... topics) throws IOException {
        DeleteRecordsRequest request = request(version, Arrays.asList(topics), 60_000);
        writeCase(name, version, seed, request, frame(request), "response");
    }

    private static void helper(short version) {
        DeleteRecordsRequest request = request(version, List.of(topic("alpha", 0, 2, 0, 4), topic("alpha"),
            topic("missing", 7, 0)), 60_000);
        for (Errors error : List.of(Errors.OFFSET_OUT_OF_RANGE, Errors.UNKNOWN_TOPIC_OR_PARTITION,
            Errors.INVALID_TOPIC_EXCEPTION, Errors.INVALID_REQUEST)) {
            DeleteRecordsResponse actual = (DeleteRecordsResponse) request.getErrorResponse(42, error.exception());
            byte[] bytes = response(actual.data(), version);
            HELPERS.add(row("api_version", version, "error_code", error.code(), "throttle_time_ms", 42,
                "request_hex", HexFormat.of().formatHex(frame(request)), "response_hex", HexFormat.of().formatHex(bytes),
                "actual_data", actual.data().toString(), "results", results(actual.data()),
                "scope", "Actual getErrorResponse execution preserves original duplicate/empty topic shape; supplemental global failure envelope, not the normal mapped Router response."));
            if (actual.data().topics().size() != 3 || actual.data().topics().find("alpha").partitions().size() != 2) {
                throw new AssertionError("Global error helper did not preserve duplicate/empty shape");
            }
            checks++;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IllegalArgumentException("output release");
        directory = Path.of(args[0]);
        Files.createDirectories(directory);
        for (short version = 0; version <= 2; version++) {
            for (long offset : List.of(-1L, 0L, 1L, 2L, 3L, 4L, -2L, Long.MIN_VALUE, 5L, Long.MAX_VALUE)) {
                normal("offset-" + offset, version, "fixture", topic("alpha", 0, offset));
            }
            normal("unknown-topic", version, "fixture", topic("missing", 0, 2));
            normal("unknown-partition", version, "fixture", topic("alpha", 2, 2));
            normal("negative-partition", version, "fixture", topic("alpha", -1, 2));
            normal("internal", version, "fixture", topic("__consumer_offsets", 0, -1));
            normal("empty-query", version, "fixture");
            normal("empty-topic-parts", version, "fixture", topic("alpha"));
            normal("empty-partition-hw", version, "fixture", topic("alpha", 1, -1));
            normal("empty-partition-zero", version, "fixture", topic("alpha", 1, 0));
            normal("empty-partition-above-hw", version, "fixture", topic("alpha", 1, 1));
            normal("catalog-only-hw", version, "fixture_empty", topic("alpha", 0, -1));
            normal("catalog-only-above-hw", version, "fixture_empty", topic("alpha", 0, 1));
            normal("floor2-old0", version, "fixture_floor_2", topic("alpha", 0, 0));
            normal("floor2-old1", version, "fixture_floor_2", topic("alpha", 0, 1));
            normal("floor2-equal2", version, "fixture_floor_2", topic("alpha", 0, 2));
            normal("floor2-advance3", version, "fixture_floor_2", topic("alpha", 0, 3));
            normal("floor2-hw", version, "fixture_floor_2", topic("alpha", 0, -1));
            normal("duplicate-last2", version, "fixture", topic("alpha", 0, 4, 0, 2));
            normal("duplicate-last4", version, "fixture", topic("alpha", 0, 2, 0, 4));
            normal("duplicate-topic-last2", version, "fixture", topic("alpha", 0, 4), topic("alpha", 0, 2));
            normal("duplicate-last-error", version, "fixture", topic("alpha", 0, 2, 0, 5));
            normal("duplicate-last-success", version, "fixture", topic("alpha", 0, 5, 0, 2));
            normal("mixed-sorted", version, "fixture", topic("missing", 2, -1), topic("alpha", 1, 0, 0, 2),
                topic("__consumer_offsets", 0, -1));
            normal("empty-topic-omitted", version, "fixture", topic("missing"), topic("alpha", 0, 2));
            normal("unknown-empty-name", version, "fixture", topic("", 0, 2));
            normal("unknown-path-name", version, "fixture", topic("../alpha", 0, 2));
            for (int timeout : List.of(0, -1, Integer.MAX_VALUE)) {
                DeleteRecordsRequest request = request(version, List.of(topic("alpha", 0, 2)), timeout);
                writeCase("timeout-" + timeout, version, "fixture", request, frame(request), "response");
            }
            if (version == 2) {
                DeleteRecordsRequest tagged = request(version, List.of(topic("alpha", 0, 2)), 60_000);
                tagged.data().unknownTaggedFields().add(new RawTaggedField(91, new byte[] {1, 2}));
                tagged.data().topics().get(0).unknownTaggedFields().add(new RawTaggedField(92, new byte[] {3}));
                tagged.data().topics().get(0).partitions().get(0).unknownTaggedFields().add(new RawTaggedField(93, new byte[] {4}));
                writeCase("unknown-tags", version, "fixture", tagged, frame(tagged), "response");
            }
            DeleteRecordsRequest plain = request(version, List.of(topic("alpha", 0, 2)), 60_000);
            byte[] bytes = frame(plain);
            writeCase("truncated", version, "fixture", plain, Arrays.copyOf(bytes, bytes.length - 1), "reject");
            writeCase("trailing-zero", version, "fixture", plain, Arrays.copyOf(bytes, bytes.length + 1), "reject");
            helper(version);
        }
        Files.writeString(directory.resolve("cases.tsv"), TABLE.toString());
        Files.writeString(directory.resolve("goldens.json"), json(row("schema_version", 1, "release", args[1],
            "wire", "Full Kafka request/response header and body; no length prefix; correlation7/client retention-oracle",
            "seed", "alpha UUID2/two partitions; __consumer_offsets UUID3/one; alpha0 104-byte batch0/count3 +78-byte batch3/count1, HW4/floor0; fixture_floor_2 advances durable floor2; fixture_empty has catalog only",
            "cases", CASES, "apache_global_error_helpers", HELPERS, "actual_serializer_parser_checks", checks)) + "\n");
        System.out.println(json(row("passed", true, "cases", CASES.size(), "checks", checks, "global_error_helpers", HELPERS.size())));
    }
}
