/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

/** Apache SDK body fixtures across the supported Produce version range. */
public final class ConformanceProduceWire {
    private static final short SUPPORTED_MIN = 3;
    private static final short SUPPORTED_MAX = 13;
    private static final String TOPIC = "produce-fixture";
    private static final Uuid TOPIC_ID = new Uuid(1, 17);
    private static final Uuid OTHER_TOPIC_ID = new Uuid(2, 18);
    private static final String TRANSACTIONAL_ID = "produce-fixture-txn";
    private static final int SUCCESS_PARTITION = 2;
    private static final int ERROR_PARTITION = 5;
    private static final int RECORD_ERROR_PARTITION = 9;
    private static final long TIMESTAMP = 1_700_000_000_000L;
    private static final HexFormat HEX = HexFormat.of();

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static byte[] encode(Message body, short version) {
        var cache = new ObjectSerializationCache();
        var bytes = ByteBuffer.allocate(body.size(cache, version));
        body.write(new ByteBufferAccessor(bytes), cache, version);
        check(!bytes.hasRemaining(), "Apache body size did not match bytes written");
        return bytes.array();
    }

    private static boolean transactional(short version) {
        return version == 3 || version == 11;
    }

    private static boolean recordErrors(short version) {
        return version == 8 || version == 9 || version >= 12;
    }

    private static boolean leaderTags(short version) {
        return version == 10 || version >= 12;
    }

    private static ProduceRequestData.PartitionProduceData requestPartition(int index, short version) {
        long time = TIMESTAMP + index;
        var key = new byte[] {'k', 'e', 'y'};
        var value = new byte[] {'v', 'a', 'l', 'u', 'e'};
        var first = new SimpleRecord(time, key, value,
            new Header[] {new RecordHeader("fixture", new byte[] {1})});
        var tombstone = new SimpleRecord(time + 1, key, null,
            new Header[] {new RecordHeader("fixture", null)});
        var records = transactional(version)
            ? MemoryRecords.withTransactionalRecords(Compression.NONE, 71, (short) 2, 21 + index,
                first, tombstone)
            : MemoryRecords.withRecords(Compression.NONE, first, tombstone);
        return new ProduceRequestData.PartitionProduceData().setIndex(index).setRecords(records);
    }

    private static ProduceRequestData request(short version) {
        var topic = new ProduceRequestData.TopicProduceData();
        if (version == 13) {
            topic.setTopicId(TOPIC_ID);
        } else {
            topic.setName(TOPIC);
        }
        topic.partitionData().add(requestPartition(SUCCESS_PARTITION, version));
        topic.partitionData().add(requestPartition(ERROR_PARTITION, version));
        if (recordErrors(version)) {
            topic.partitionData().add(requestPartition(RECORD_ERROR_PARTITION, version));
        }
        var request = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(1230 + version);
        if (transactional(version)) {
            request.setTransactionalId(TRANSACTIONAL_ID);
        }
        request.topicData().add(topic);
        if (version == 13) {
            var other = new ProduceRequestData.TopicProduceData().setTopicId(OTHER_TOPIC_ID);
            other.partitionData().add(requestPartition(SUCCESS_PARTITION, version));
            request.topicData().add(other);
        }
        return request;
    }

    private static ProduceResponseData response(short version) {
        var topic = new ProduceResponseData.TopicProduceResponse();
        if (version == 13) {
            topic.setTopicId(TOPIC_ID);
        } else {
            topic.setName(TOPIC);
        }
        topic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
            .setIndex(SUCCESS_PARTITION)
            .setBaseOffset(43)
            .setLogAppendTimeMs(TIMESTAMP + 1000)
            .setLogStartOffset(version >= 5 ? 17 : -1));
        short error = switch (version) {
            case 4 -> 56;  // KAFKA_STORAGE_ERROR
            case 11 -> 120; // TRANSACTION_ABORTABLE
            default -> 6; // NOT_LEADER_OR_FOLLOWER
        };
        var failed = new ProduceResponseData.PartitionProduceResponse()
            .setIndex(ERROR_PARTITION).setErrorCode(error).setBaseOffset(-1);
        topic.partitionResponses().add(failed);
        var response = new ProduceResponseData()
            .setThrottleTimeMs(version == 10 || version == 12 ? 0 : 37);
        response.responses().add(topic);
        if (leaderTags(version)) {
            failed.setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch()
                .setLeaderId(3).setLeaderEpoch(7));
            response.nodeEndpoints().add(new ProduceResponseData.NodeEndpoint().setNodeId(3)
                .setHost("broker.example").setPort(9092).setRack(version == 10 ? null : "rack-a"));
        }
        if (recordErrors(version)) {
            var invalid = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(RECORD_ERROR_PARTITION).setErrorCode((short) 87).setBaseOffset(-1)
                .setErrorMessage("invalid batch");
            invalid.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage()
                .setBatchIndex(0).setBatchIndexErrorMessage("invalid timestamp"));
            invalid.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage()
                .setBatchIndex(1));
            topic.partitionResponses().add(invalid);
        }
        if (version == 13) {
            // The duplicated partition index belongs to a different topic ID.
            var other = new ProduceResponseData.TopicProduceResponse().setTopicId(OTHER_TOPIC_ID);
            other.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(SUCCESS_PARTITION).setErrorCode((short) 100).setBaseOffset(-1));
            response.responses().add(other);
        }
        return response;
    }

    private static void verify(short version, String[] fields) {
        check(fields.length == 3 && fields[0].equals(Short.toString(version)),
            "wrong order or Produce body row");
        var requestBytes = ByteBuffer.wrap(HEX.parseHex(fields[1]));
        var responseBytes = ByteBuffer.wrap(HEX.parseHex(fields[2]));
        var actualRequest = new ProduceRequestData(new ByteBufferAccessor(requestBytes), version);
        var actualResponse = new ProduceResponseData(new ByteBufferAccessor(responseBytes), version);
        check(!requestBytes.hasRemaining() && !responseBytes.hasRemaining(),
            "unconsumed Produce body bytes");
        check(actualRequest.equals(request(version)), "Produce request fields or record bytes differ");
        check(actualResponse.equals(response(version)), "Produce response fields differ");
        for (var topic : actualRequest.topicData()) {
            for (var part : topic.partitionData()) {
                var records = (MemoryRecords) part.records();
                int batches = 0;
                int count = 0;
                for (var batch : records.batches()) {
                    batches++;
                    batch.ensureValid();
                    check(batch.isTransactional() == transactional(version),
                        "Produce transactional flag differs");
                    for (var record : batch) {
                        check(record.offset() == count, "Produce record offset differs");
                        count++;
                    }
                }
                check(batches == 1 && count == 2, "Produce batch or record count differs");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        check(args.length == 5, "<sdk-jar> <expected-min> <expected-max> <generate|verify> <file>");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain()
            .getCodeSource().getLocation().toURI());
        check(Files.isSameFile(jar, loaded), "wrong Apache SDK loaded");

        short declaredMin = Short.parseShort(args[1]);
        short declaredMax = Short.parseShort(args[2]);
        var request = new ProduceRequestData();
        var response = new ProduceResponseData();
        check(request.lowestSupportedVersion() == declaredMin
            && response.lowestSupportedVersion() == declaredMin
            && request.highestSupportedVersion() == declaredMax
            && response.highestSupportedVersion() == declaredMax,
            "Apache SDK Produce version range changed");
        short first = (short) Math.max(SUPPORTED_MIN, declaredMin);
        short last = (short) Math.min(SUPPORTED_MAX, declaredMax);
        check(first <= last, "empty shared Produce version range");
        System.out.println("declared_produce_versions=" + declaredMin + "-" + declaredMax);
        Path file = Path.of(args[4]);
        if (args[3].equals("generate")) {
            var rows = new ArrayList<String>();
            for (short version = first; version <= last; version++) {
                var row = version + " " + HEX.formatHex(encode(request(version), version))
                    + " " + HEX.formatHex(encode(response(version), version));
                verify(version, row.split(" "));
                rows.add(row);
            }
            Files.writeString(file, String.join("\n", rows) + "\n");
            System.out.println("generated_pairs=" + rows.size());
        } else if (args[3].equals("verify")) {
            check(Files.size(file) <= 16_384, "Produce corpus exceeded fixture budget");
            var rows = Files.readAllLines(file);
            int row = 0;
            for (short version = first; version <= last; version++) {
                check(row < rows.size(), "missing Produce body row");
                verify(version, rows.get(row).split(" "));
                row++;
            }
            check(row == rows.size(), "extra Produce body row");
            System.out.println("independently_parsed_pairs=" + row);
        } else {
            throw new IllegalArgumentException("expected generate or verify");
        }
    }
}
