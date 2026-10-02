/* Independent official Apache Produce builders, parser and executed error/helper oracles. */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.*;

public final class ProduceOracle {
    private static final Uuid ALPHA = new Uuid(0, 2);
    private static final Uuid UNKNOWN = new Uuid(0, 99);
    private static final List<String> CASES = new ArrayList<>();
    private static final StringBuilder TSV = new StringBuilder();
    private static Path output;
    private ProduceOracle() { }
    private static String quote(String value) {
        if (value == null) return "null";
        StringBuilder text = new StringBuilder("\"");
        for (char ch : value.toCharArray()) {
            if (ch == '"' || ch == '\\') text.append('\\').append(ch);
            else if (ch < 32) text.append(String.format("\\u%04x", (int) ch));
            else text.append(ch);
        }
        return text.append('"').toString();
    }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer source = records.buffer().duplicate();
        byte[] result = new byte[source.remaining()]; source.get(result); return result;
    }
    private static MemoryRecords basic() {
        return MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(1000L, utf8("key"), utf8("value"), new Header[]{new RecordHeader("trace", utf8("receipt-0"))}));
    }
    private static MemoryRecords rich() {
        return MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(1000L, (byte[]) null, (byte[]) null),
            new SimpleRecord(1007L, new byte[0], utf8("payload"), new Header[]{new RecordHeader("dup", utf8("a")), new RecordHeader("dup", null)}),
            new SimpleRecord(1003L, utf8("key2"), new byte[0]));
    }
    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length); return result;
    }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer result = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(result), cache, version);
        if (result.hasRemaining()) throw new AssertionError("serializer underwrite"); return result.array();
    }
    private static String hash(byte[] value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    }
    private static ProduceRequestData request(String name, Uuid id, int partition, short acks, BaseRecords records) {
        ProduceRequestData data = new ProduceRequestData().setAcks(acks).setTimeoutMs(60_000).setTransactionalId(null);
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName(name).setTopicId(id);
        topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(partition).setRecords(records));
        data.topicData().add(topic); return data;
    }
    private static String helperProbe(short version, BaseRecords records) {
        try {
            ProduceRequest.validateRecords(version, records);
            return "{\"status\":\"accepted\"}";
        } catch (RuntimeException failure) {
            return "{\"status\":\"rejected\",\"code\":" + Errors.forException(failure).code()
                + ",\"exception\":" + quote(failure.getClass().getName()) + ",\"message\":" + quote(failure.getMessage()) + "}";
        }
    }
    private static String batchProbe(BaseRecords records) {
        if (records == null) return "{\"status\":\"null\"}";
        try {
            int count = 0;
            for (MutableRecordBatch batch : ((MemoryRecords) records).batches()) {
                if (++count > 2) throw new AssertionError("fixture work bound");
                batch.ensureValid();
            }
            return "{\"status\":\"accepted\",\"batch_count\":" + count + "}";
        } catch (RuntimeException failure) {
            return "{\"status\":\"rejected\",\"code\":" + Errors.forException(failure).code()
                + ",\"exception\":" + quote(failure.getClass().getName()) + ",\"message\":" + quote(failure.getMessage()) + "}";
        }
    }
    private static ProduceResponseData success(ProduceRequestData request, short version) {
        ProduceResponseData data = new ProduceResponseData();
        for (ProduceRequestData.TopicProduceData topic : request.topicData()) {
            ProduceResponseData.TopicProduceResponse result = new ProduceResponseData.TopicProduceResponse()
                .setName(topic.name()).setTopicId(topic.topicId());
            for (ProduceRequestData.PartitionProduceData part : topic.partitionData()) {
                ProduceResponseData.PartitionProduceResponse response = new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(part.index()).setErrorCode((short) 0).setBaseOffset(0).setLogAppendTimeMs(-1);
                if (version >= 5) response.setLogStartOffset(0);
                result.partitionResponses().add(response);
            }
            data.responses().add(result);
        }
        return data;
    }
    private static void emit(String label, short version, ProduceRequestData data, Errors error,
            String outcome, String basis, boolean localNull) throws Exception {
        String name = "produce-v" + version + "-" + label;
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "produce-oracle", 7);
        byte[] body = encode(data, version);
        byte[] frame = concat(encode(header.data(), header.headerVersion()), body);
        ByteBuffer requestBuffer = ByteBuffer.wrap(frame);
        RequestHeader.parse(requestBuffer);
        ProduceRequest parsed = (ProduceRequest) AbstractRequest.parseRequest(ApiKeys.PRODUCE, version,
            new ByteBufferAccessor(requestBuffer)).request;
        if (requestBuffer.hasRemaining() || !Arrays.equals(encode(parsed.data(), version), body)) throw new AssertionError("request round trip");
        BaseRecords records = parsed.data().topicData().iterator().next().partitionData().get(0).records();
        String helper = helperProbe(version, records);
        String batch = batchProbe(records);
        ProduceResponseData response = null;
        String errorMethod;
        if (error == Errors.NONE) {
            response = data.acks() == 0 ? null : success(parsed.data(), version);
            errorMethod = "{\"scope\":\"declared single-node durable success; not an Apache controller runtime response\"}";
        } else {
            try {
                ProduceResponse upstream = parsed.getErrorResponse(0, error.exception());
                response = upstream == null ? null : upstream.data();
                errorMethod = "{\"status\":\"executed\",\"exception_code\":" + error.code()
                    + ",\"returns_null\":" + (upstream == null) + ",\"apache_data\":" + quote(upstream == null ? null : upstream.data().toString()) + "}";
            } catch (RuntimeException failure) {
                errorMethod = "{\"status\":\"throws\",\"exception\":" + quote(failure.getClass().getName())
                    + ",\"message\":" + quote(failure.getMessage()) + "}";
                if (!localNull) throw failure;
                response = success(parsed.data(), version);
                for (ProduceResponseData.TopicProduceResponse topic : response.responses()) {
                    for (ProduceResponseData.PartitionProduceResponse part : topic.partitionResponses()) {
                        part.setErrorCode(error.code()).setBaseOffset(-1).setLogStartOffset(-1);
                    }
                }
            }
        }
        byte[] responseFrame = null;
        if (outcome.equals("response")) {
            if (response == null) throw new AssertionError("missing declared response");
            short hv = ApiKeys.PRODUCE.responseHeaderVersion(version);
            byte[] responseBody = encode(response, version);
            responseFrame = concat(encode(new ResponseHeader(7, hv).data(), hv), responseBody);
            ByteBuffer responseBuffer = ByteBuffer.wrap(responseFrame); ResponseHeader.parse(responseBuffer, hv);
            AbstractResponse roundTrip = AbstractResponse.parseResponse(ApiKeys.PRODUCE, new ByteBufferAccessor(responseBuffer), version);
            if (responseBuffer.hasRemaining() || !Arrays.equals(encode(roundTrip.data(), version), responseBody)) throw new AssertionError("response round trip");
        }
        if (frame.length > 128 * 1024 || responseFrame != null && responseFrame.length > 128 * 1024) throw new AssertionError("fixture frame limit");
        Files.write(output.resolve(name + ".request.bin"), frame);
        if (responseFrame != null) Files.write(output.resolve(name + ".response.bin"), responseFrame);
        CASES.add("{\"name\":" + quote(name) + ",\"api_key\":0,\"api_version\":" + version
            + ",\"seed\":\"fixture\",\"expected_outcome\":" + quote(outcome)
            + ",\"request_hex\":" + quote(HexFormat.of().formatHex(frame))
            + ",\"response_hex\":" + (responseFrame == null ? "null" : quote(HexFormat.of().formatHex(responseFrame)))
            + ",\"request_sha256\":" + quote(hash(frame))
            + ",\"response_sha256\":" + (responseFrame == null ? "null" : quote(hash(responseFrame)))
            + ",\"selected_error\":" + error.code() + ",\"basis\":" + quote(basis)
            + ",\"apache_validate_records\":" + helper + ",\"apache_batch_ensure_valid\":" + batch
            + ",\"apache_get_error_response\":" + errorMethod
            + ",\"apache_parsed_request\":" + quote(parsed.data().toString()) + "}");
        TSV.append(name).append("\t0\t").append(version).append("\tfixture\t").append(outcome).append('\n');
    }
    private static void generate() throws Exception {
        for (short version = 3; version <= 13; version++) {
            for (short acks : new short[]{1, -1, 0}) {
                emit("acks" + acks, version, request("alpha", ALPHA, 0, acks, basic()), Errors.NONE,
                    acks == 0 ? "no_response_keep_open" : "response",
                    "Apache builds/parses ordinary magic2, null transaction, producerID-1. Declared local sync-before-success; acks-1 covers only ISR[node0], not replication. acks0 success leaves channel open with no response (pinned KafkaApis).", false);
            }
            emit("rich-records", version, request("alpha", ALPHA, 0, (short) 1, rich()), Errors.NONE,
                "response", "Three Apache-built records, null/empty key/value fields and duplicate headers; one batch, baseoffset0, nextoffset3.", false);
            emit("unknown-topic", version, request("missing", UNKNOWN, 0, (short) 1, basic()),
                version == 13 ? Errors.UNKNOWN_TOPIC_ID : Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "response", "Actual Apache getErrorResponse executed for source-inspected unknown-name/ID mapping; no Apache broker/controller runtime.", false);
            emit("unknown-partition", version, request("alpha", ALPHA, 9, (short) 1, basic()), Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "response", "Actual Apache getErrorResponse executed for unknown partition3; declared alpha has two partitions.", false);
            emit("invalid-acks", version, request("alpha", ALPHA, 0, (short) 2, basic()), Errors.INVALID_REQUIRED_ACKS,
                "response", "Actual Apache getErrorResponse executed for invalidrequiredacks21; error selected from replica append validation source, not controller execution.", false);
            byte[] damaged = bytes(basic()); damaged[damaged.length - 1] ^= 1;
            emit("bad-crc", version, request("alpha", ALPHA, 0, (short) 1, MemoryRecords.readableRecords(ByteBuffer.wrap(damaged))),
                Errors.CORRUPT_MESSAGE, "response", "Actual Apache batch.ensureValid rejects changed protected payload as CORRUPT_MESSAGE2; getErrorResponse executed.", false);
            emit("empty-records", version, request("alpha", ALPHA, 0, (short) 1, MemoryRecords.readableRecords(ByteBuffer.allocate(0))),
                Errors.INVALID_RECORD, "response", "Actual Apache ProduceRequest.validateRecords rejects zero batches as INVALID_RECORD87; error mapping/getErrorResponse executed.", false);
            emit("multiple-batches", version, request("alpha", ALPHA, 0, (short) 1,
                MemoryRecords.readableRecords(ByteBuffer.wrap(concat(bytes(basic()), bytes(basic()))))), Errors.INVALID_RECORD,
                "response", "Actual Apache ProduceRequest.validateRecords requires exactly one batch per partition and rejects two as87, even though lower storage/record foundations may admit multiple batches.", false);
            emit("legacy-magic1", version, request("alpha", ALPHA, 0, (short) 1,
                MemoryRecords.withRecords((byte) 1, Compression.NONE, new SimpleRecord(1000L, utf8("key"), utf8("legacy")))), Errors.INVALID_RECORD,
                "response", "Actual Apache builder produces legacy magic1; ProduceRequest.validateRecords requires magic2 and returns87.", false);
            emit("null-records", version, request("alpha", ALPHA, 0, (short) 1, null), Errors.INVALID_RECORD,
                "response", "Explicit stricter local null-records rejection87. Actual upstream helper accepts null without checking; getErrorResponse may throw on null partitionSizes. Policy-assembled response is only encoded/parsed by Apache.", true);
            emit("acks0-error", version, request("missing", UNKNOWN, 0, (short) 0, basic()),
                version == 13 ? Errors.UNKNOWN_TOPIC_ID : Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "close", "Actual Apache getErrorResponse returns null foracks0. Pinned KafkaApis closes channel on any partition error rather than sending a response.", false);
            ProduceRequestData transactionalId = request("alpha", ALPHA, 0, (short) 1, basic()).setTransactionalId("transaction");
            emit("unsupported-transaction-id", version, transactionalId, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT,
                "response", "Explicit stricter local ordinary-profile rejection43 for a non-null transaction ID. Apache builds/parses this supported transactional request and is not claimed to reject it.", false);
            emit("unsupported-idempotent", version, request("alpha", ALPHA, 0, (short) 1,
                MemoryRecords.withIdempotentRecords(Compression.NONE, 5L, (short) 0, 0, new SimpleRecord(1000L, utf8("idempotent")))),
                Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, "response", "Apache-built valid idempotent batch. Explicit stricter local ordinary-profile rejection43; not an Apache rejection.", false);
            emit("unsupported-transactional", version, request("alpha", ALPHA, 0, (short) 1,
                MemoryRecords.withTransactionalRecords(Compression.NONE, 5L, (short) 0, 0, new SimpleRecord(1000L, utf8("transactional")))),
                Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, "response", "Apache-built valid transactional batch. Explicit stricter local ordinary-profile rejection43; not an Apache rejection.", false);
            emit("unsupported-control", version, request("alpha", ALPHA, 0, (short) 1,
                MemoryRecords.withEndTransactionMarker(5L, (short) 0, new EndTransactionMarker(ControlRecordType.COMMIT, 0))),
                Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, "response", "Apache-built valid COMMIT control batch. Explicit stricter local ordinary-profile rejection43; no transaction/coordinator semantics claimed.", false);
            emit("unsupported-logappendtime", version, request("alpha", ALPHA, 0, (short) 1,
                MemoryRecords.withRecords((byte) 2, 0L, Compression.NONE, TimestampType.LOG_APPEND_TIME, new SimpleRecord(1000L, utf8("logappendtime")))),
                Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, "response", "Apache-built LogAppendTime batch. Explicit stricter local CreateTime-only policy43; Apache producer log append validation normally rejects producer LogAppendTime with different semantics.", false);
            structural("truncated", version, false);
            structural("trailing-zero", version, true);
            if (version >= 9) {
                ProduceRequestData tagged = request("alpha", ALPHA, 0, (short) 1, basic());
                tagged.unknownTaggedFields().add(new RawTaggedField(77, new byte[]{1, 2}));
                tagged.topicData().iterator().next().unknownTaggedFields().add(new RawTaggedField(91, new byte[]{3}));
                tagged.topicData().iterator().next().partitionData().get(0).unknownTaggedFields().add(new RawTaggedField(17, new byte[]{4, 5}));
                emit("unknown-tags", version, tagged, Errors.NONE, "response", "Actual Apache flexible nested tags preserved by parser; declared router skips unknown request tags and emits no unknown response tags.", false);
            }
        }
    }
    private static void structural(String label, short version, boolean trailing) throws Exception {
        String name = "produce-v" + version + "-" + label;
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "produce-oracle", 7);
        byte[] canonical = concat(encode(header.data(), header.headerVersion()),
            encode(request("alpha", ALPHA, 0, (short) 1, basic()), version));
        byte[] frame = Arrays.copyOf(canonical, canonical.length + (trailing ? 1 : -1));
        ByteBuffer buffer = ByteBuffer.wrap(frame);
        String upstream;
        try {
            RequestHeader.parse(buffer);
            AbstractRequest.parseRequest(ApiKeys.PRODUCE, version, new ByteBufferAccessor(buffer));
            upstream = "{\"status\":\"accepted\",\"consumed\":" + buffer.position() + ",\"remaining\":" + buffer.remaining() + "}";
        } catch (RuntimeException failure) {
            upstream = "{\"status\":\"rejected\",\"consumed\":" + buffer.position() + ",\"remaining\":" + buffer.remaining()
                + ",\"exception\":" + quote(failure.getClass().getName()) + ",\"message\":" + quote(failure.getMessage()) + "}";
        }
        Files.write(output.resolve(name + ".request.bin"), frame);
        CASES.add("{\"name\":" + quote(name) + ",\"api_key\":0,\"api_version\":" + version
            + ",\"seed\":\"fixture\",\"expected_outcome\":\"structural_reject\",\"request_hex\":" + quote(HexFormat.of().formatHex(frame))
            + ",\"response_hex\":null,\"request_sha256\":" + quote(hash(frame)) + ",\"response_sha256\":null"
            + ",\"basis\":\"Local bounded full-frame parser rejects truncation or trailing data; actual Apache codec outcome is retained separately.\",\"apache_parser\":" + upstream + "}");
        TSV.append(name).append("\t0\t").append(version).append("\tfixture\tstructural_reject\n");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("release output-directory");
        output = Path.of(args[1]); Files.createDirectories(output); generate();
        Files.writeString(output.resolve("goldens.json"), "{\"release\":" + quote(args[0])
            + ",\"wire\":\"Kafka header and body, no length prefix\",\"seed\":\"node0; alpha UUID2/two partitions; fresh empty partition journals\",\"cases\":[\n"
            + String.join(",\n", CASES) + "\n]}\n");
        Files.writeString(output.resolve("cases.tsv"), TSV.toString());
        System.out.println("{\"release\":" + quote(args[0]) + ",\"cases\":" + CASES.size() + "}");
    }
}
