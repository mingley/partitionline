import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

public final class ObserveProduceTime {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("request response");
        ByteBuffer requestBuffer = ByteBuffer.wrap(Files.readAllBytes(Path.of(args[0])));
        RequestHeader header = RequestHeader.parse(requestBuffer);
        if (header.apiKey() != ApiKeys.PRODUCE) throw new IllegalArgumentException("not Produce");
        ProduceRequest request = ProduceRequest.parse(new ByteBufferAccessor(requestBuffer), header.apiVersion());
        long timestamp = Long.MIN_VALUE;
        long baseTimestamp = Long.MIN_VALUE;
        String marker = null;
        int recordsSeen = 0;
        for (var topic : request.data().topicData()) {
            for (var partition : topic.partitionData()) {
                if (!(partition.records() instanceof Records)) throw new IllegalStateException("no records");
                Records records = (Records) partition.records();
                for (RecordBatch batch : records.batches()) {
                    for (org.apache.kafka.common.record.Record record : batch) {
                        ByteBuffer value = record.value();
                        if (value == null) throw new IllegalStateException("missing value");
                        byte[] bytes = new byte[value.remaining()];
                        value.get(bytes);
                        marker = new String(bytes, StandardCharsets.UTF_8);
                        timestamp = record.timestamp();
                        if (!(batch instanceof DefaultRecordBatch)) {
                            throw new IllegalStateException("expected a magic-v2 batch");
                        }
                        baseTimestamp = ((DefaultRecordBatch) batch).baseTimestamp();
                        recordsSeen++;
                    }
                }
            }
        }
        if (recordsSeen != 1) throw new IllegalStateException("expected one record, got " + recordsSeen);
        ByteBuffer responseBuffer = ByteBuffer.wrap(Files.readAllBytes(Path.of(args[1])));
        ResponseHeader responseHeader = ResponseHeader.parse(
            responseBuffer, ApiKeys.PRODUCE.responseHeaderVersion(header.apiVersion())
        );
        if (responseHeader.correlationId() != header.correlationId()) {
            throw new IllegalStateException("response correlation mismatch");
        }
        ProduceResponse response = ProduceResponse.parse(
            new ByteBufferAccessor(responseBuffer), header.apiVersion()
        );
        int partitionsSeen = 0;
        for (var topic : response.data().responses()) {
            for (var partition : topic.partitionResponses()) {
                System.out.printf(
                    "KAFKA_WIRE marker=%s version=%d correlation=%d timestamp=%d base_timestamp=%d error=%d offset=%d log_append=%d%n",
                    marker, header.apiVersion(), header.correlationId(), timestamp, baseTimestamp,
                    partition.errorCode(), partition.baseOffset(), partition.logAppendTimeMs()
                );
                partitionsSeen++;
            }
        }
        if (partitionsSeen != 1) throw new IllegalStateException("expected one response partition");
    }
}
