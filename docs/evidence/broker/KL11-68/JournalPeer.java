/* Decode actual Rust-emitted normalized Kafka batches using independent pinned Apache classes. */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;

public final class JournalPeer {
    private JournalPeer() { }
    private static String quote(String value) {
        return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }
    private static String hex(ByteBuffer value) {
        if (value == null) return null;
        ByteBuffer copy = value.duplicate(); byte[] bytes = new byte[copy.remaining()]; copy.get(bytes); return HexFormat.of().formatHex(bytes);
    }
    private static String receipt(String topic, int partition, Record record) throws Exception {
        List<String> headers = new ArrayList<>();
        for (Header header : record.headers()) headers.add("{\"key\":" + quote(header.key()) + ",\"value_hex\":"
            + quote(header.value() == null ? null : HexFormat.of().formatHex(header.value())) + "}");
        String content = "{\"topic\":" + quote(topic) + ",\"partition\":" + partition + ",\"offset\":" + record.offset() + ",\"timestamp\":" + record.timestamp()
            + ",\"key_hex\":" + quote(hex(record.key())) + ",\"value_hex\":" + quote(hex(record.value())) + ",\"headers\":[" + String.join(",", headers) + "]}";
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)));
        return "{\"sha256\":\"" + hash + "\",\"record\":" + content + "}";
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("topic partition payload-directory output-json");
        int partition = Integer.parseInt(args[1]);
        if (partition < 0 || partition > 1) throw new AssertionError("bounded probe partition");
        Path directory = Path.of(args[2]); List<Path> payloads;
        try (Stream<Path> files = Files.list(directory)) { payloads = files.filter(path -> path.getFileName().toString().endsWith(".payload.bin")).sorted().limit(129).toList(); }
        if (payloads.isEmpty() || payloads.size() > 128) throw new AssertionError("bounded nonempty payload list");
        List<String> receipts = new ArrayList<>(); int batches = 0; long next = 0;
        for (Path path : payloads) {
            if (Files.size(path) > 128 * 1024) throw new AssertionError("payload allocation bound");
            byte[] data = Files.readAllBytes(path); MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(data));
            if (records.validBytes() != data.length) throw new AssertionError("full Kafka batch consumption");
            for (MutableRecordBatch batch : records.batches()) {
                batches++; batch.ensureValid();
                if (batch.magic() != 2 || batch.compressionType() != CompressionType.NONE || batch.producerId() != -1
                    || batch.isTransactional() || batch.isControlBatch() || batch.timestampType() != TimestampType.CREATE_TIME)
                    throw new AssertionError("normalized ordinary batch metadata");
                for (Record record : batch) {
                    record.ensureValid(); if (record.offset() != next++ || receipts.size() >= 512) throw new AssertionError("offset/record work bound");
                    receipts.add(receipt(args[0], partition, record));
                }
            }
        }
        Files.writeString(Path.of(args[3]), "{\"scope\":\"Actual Apache decoding of Rust journal payload bytes; no Fetch or broker replication claim\",\"batches\":"
            + batches + ",\"records\":" + receipts.size() + ",\"next_offset\":" + next + ",\"receipts\":[" + String.join(",", receipts) + "]}\n");
        System.out.println("{\"batches\":" + batches + ",\"records\":" + receipts.size() + ",\"next_offset\":" + next + "}");
    }
}
