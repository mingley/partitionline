import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;

/** Retain actual official bounded parsing/CRC/iteration outcomes, including permissive cases. */
public final class InspectCompacted {
    private InspectCompacted() { }
    private static String quote(String value) {
        return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r") + "\"";
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("input-file output-json");
        Path input = Path.of(args[0]);
        if (!Files.isRegularFile(input) || Files.size(input) > 16384) throw new AssertionError("file work bound");
        byte[] bytes = Files.readAllBytes(input); List<Long> offsets = new ArrayList<>();
        int validBytes = -1, batches = 0; String error = null, message = null;
        try {
            MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)); validBytes = records.validBytes();
            for (MutableRecordBatch batch : records.batches()) {
                if (++batches > 8) throw new AssertionError("batch work bound");
                batch.ensureValid();
                for (Record record : batch) {
                    if (offsets.size() >= 128) throw new AssertionError("record work bound");
                    record.ensureValid(); offsets.add(record.offset());
                    // Force every exposed bounded field through the actual parser.
                    record.timestamp(); record.key(); record.value(); record.headers();
                }
            }
        } catch (RuntimeException failure) { error = failure.getClass().getName(); message = failure.getMessage(); }
        String result = "{\"bytes\":" + bytes.length + ",\"valid_bytes\":" + validBytes + ",\"batches\":" + batches
            + ",\"iterated_offsets\":" + offsets + ",\"exception\":" + quote(error) + ",\"message\":" + quote(message) + "}\n";
        Files.writeString(Path.of(args[1]), result); System.out.print(result);
    }
}
