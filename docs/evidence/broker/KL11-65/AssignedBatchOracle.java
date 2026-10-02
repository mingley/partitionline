import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

/** Decode actual Rust-written payloads with the pinned Apache implementation. */
public final class AssignedBatchOracle {
    private static String hex(ByteBuffer value) {
        if (value == null) return "null";
        ByteBuffer copy = value.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return "\"" + HexFormat.of().formatHex(bytes) + "\"";
    }
    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            byte[] input = Files.readAllBytes(Path.of(arg));
            MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(input));
            if (records.validBytes() != input.length) throw new AssertionError("trailing bytes");
            for (RecordBatch batch : records.batches()) {
                batch.ensureValid();
                System.out.println("{\"kind\":\"batch\",\"base\":" + batch.baseOffset()
                    + ",\"last\":" + batch.lastOffset() + "}");
                for (Record record : batch) {
                    record.ensureValid();
                    System.out.println("{\"kind\":\"record\",\"offset\":" + record.offset()
                        + ",\"timestamp\":" + record.timestamp() + ",\"key\":" + hex(record.key())
                        + ",\"value\":" + hex(record.value()) + "}");
                }
            }
        }
    }
}
