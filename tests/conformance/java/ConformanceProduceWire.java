/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
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

/** Apache SDK body fixtures for previously uncovered Produce versions. */
public final class ConformanceProduceWire {
    private static final short[] VERSIONS = {4, 6, 7, 12};
    private static final String TOPIC = "produce-fixture";
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

    private static ProduceRequestData.PartitionProduceData requestPartition(int index) {
        long time = TIMESTAMP + index;
        var key = new byte[] {'k', 'e', 'y'};
        var value = new byte[] {'v', 'a', 'l', 'u', 'e'};
        var first = new SimpleRecord(time, key, value,
            new Header[] {new RecordHeader("fixture", new byte[] {1})});
        var tombstone = new SimpleRecord(time + 1, key, null,
            new Header[] {new RecordHeader("fixture", null)});
        return new ProduceRequestData.PartitionProduceData().setIndex(index)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, first, tombstone));
    }

    private static ProduceRequestData request(short version) {
        var topic = new ProduceRequestData.TopicProduceData().setName(TOPIC);
        topic.partitionData().add(requestPartition(SUCCESS_PARTITION));
        topic.partitionData().add(requestPartition(ERROR_PARTITION));
        if (version == 12) {
            topic.partitionData().add(requestPartition(RECORD_ERROR_PARTITION));
        }
        var request = new ProduceRequestData().setAcks((short) -1).setTimeoutMs(1230 + version);
        request.topicData().add(topic);
        return request;
    }

    private static ProduceResponseData response(short version) {
        var topic = new ProduceResponseData.TopicProduceResponse().setName(TOPIC);
        topic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
            .setIndex(SUCCESS_PARTITION)
            .setBaseOffset(43)
            .setLogAppendTimeMs(TIMESTAMP + 1000)
            .setLogStartOffset(version >= 5 ? 17 : -1));
        var failed = new ProduceResponseData.PartitionProduceResponse()
            .setIndex(ERROR_PARTITION)
            // KAFKA_STORAGE_ERROR begins at v4. The other cells report NOT_LEADER_OR_FOLLOWER.
            .setErrorCode((short) (version == 4 ? 56 : 6)).setBaseOffset(-1);
        topic.partitionResponses().add(failed);
        var response = new ProduceResponseData().setThrottleTimeMs(version == 12 ? 0 : 37);
        response.responses().add(topic);
        if (version == 12) {
            failed.setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch()
                .setLeaderId(3).setLeaderEpoch(7));
            response.nodeEndpoints().add(new ProduceResponseData.NodeEndpoint().setNodeId(3)
                .setHost("broker.example").setPort(9092).setRack("rack-a"));
            var invalid = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(RECORD_ERROR_PARTITION).setErrorCode((short) 87).setBaseOffset(-1)
                .setErrorMessage("invalid batch");
            invalid.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage()
                .setBatchIndex(0).setBatchIndexErrorMessage("invalid timestamp"));
            invalid.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage()
                .setBatchIndex(1));
            topic.partitionResponses().add(invalid);
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
        for (var part : actualRequest.topicData().iterator().next().partitionData()) {
            var records = (MemoryRecords) part.records();
            int count = 0;
            for (var batch : records.batches()) {
                batch.ensureValid();
                for (var record : batch) {
                    check(record.offset() == count, "Produce record offset differs");
                    count++;
                }
            }
            check(count == 2, "Produce record count differs");
        }
    }

    public static void main(String[] args) throws Exception {
        check(args.length == 4, "<sdk-jar> <expected-max-version> <generate|verify> <file>");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(ProduceRequestData.class.getProtectionDomain()
            .getCodeSource().getLocation().toURI());
        check(Files.isSameFile(jar, loaded), "wrong Apache SDK loaded");

        short declaredMax = Short.parseShort(args[1]);
        check(new ProduceRequestData().highestSupportedVersion() == declaredMax
            && new ProduceResponseData().highestSupportedVersion() == declaredMax,
            "Apache SDK Produce version range changed");
        System.out.println("declared_max_produce_version=" + declaredMax);
        Path file = Path.of(args[3]);
        if (args[2].equals("generate")) {
            var rows = new ArrayList<String>();
            for (short version : VERSIONS) {
                if (version <= declaredMax) {
                    var row = version + " " + HEX.formatHex(encode(request(version), version))
                        + " " + HEX.formatHex(encode(response(version), version));
                    verify(version, row.split(" "));
                    rows.add(row);
                }
            }
            Files.writeString(file, String.join("\n", rows) + "\n");
            System.out.println("generated_pairs=" + rows.size());
        } else if (args[2].equals("verify")) {
            check(Files.size(file) <= 16_384, "Produce corpus exceeded fixture budget");
            var rows = Files.readAllLines(file);
            int row = 0;
            for (short version : VERSIONS) {
                if (version <= declaredMax) {
                    check(row < rows.size(), "missing Produce body row");
                    verify(version, rows.get(row).split(" "));
                    row++;
                }
            }
            check(row == rows.size(), "extra Produce body row");
            System.out.println("independently_parsed_pairs=" + row);
        } else {
            throw new IllegalArgumentException("expected generate or verify");
        }
    }
}
