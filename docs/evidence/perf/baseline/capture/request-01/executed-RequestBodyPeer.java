import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;

/** Genuine SDK parse, field checks, batch CRC validation and body re-serialization. */
public final class RequestBodyPeer {
    private static byte[] payload(long seed, int bytes) {
        byte[] out = new byte[bytes];
        int cursor = 0;
        while (cursor < bytes) {
            seed += 0x9E3779B97F4A7C15L;
            long z = seed;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            z ^= z >>> 31;
            for (int shift = 56; shift >= 0 && cursor < bytes; shift -= 8) {
                out[cursor++] = (byte) (z >>> shift);
            }
        }
        return out;
    }
    private static byte[] bytes(ByteBuffer b) {
        byte[] out = new byte[b.remaining()];
        b.duplicate().get(out);
        return out;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("request body path");
        byte[] raw = Files.readAllBytes(Path.of(args[0]));
        ByteBuffer input = ByteBuffer.wrap(raw);
        var request = new ProduceRequestData(new ByteBufferAccessor(input), (short) 9);
        if (input.hasRemaining() || request.transactionalId() != null || request.acks() != 1
                || request.timeoutMs() != 30000 || request.topicData().size() != 1) {
            throw new AssertionError("request envelope differs");
        }
        var topic = request.topicData().iterator().next();
        if (!topic.name().equals("t") || topic.partitionData().size() != 1) {
            throw new AssertionError("topic shape differs");
        }
        var partition = topic.partitionData().get(0);
        if (partition.index() != 0 || !(partition.records() instanceof MemoryRecords records)) {
            throw new AssertionError("partition or record container differs");
        }
        int batches = 0, count = 0;
        for (var batch : records.batches()) {
            batch.ensureValid();
            batches++;
            for (var record : batch) {
                if (record.keySize() != 16 || record.valueSize() != 100 || record.headers().length != 0) {
                    throw new AssertionError("record shape differs");
                }
                if (record.offset() != count || record.timestamp() != 1700000000000L+count
                        || !Arrays.equals(bytes(record.key()), payload(0xC0DECL ^ count*0x9E3779B97F4A7C15L,16))
                        || !Arrays.equals(bytes(record.value()), payload(0xC0DECL ^ count ^ 0xD1B54A32243F6A88L,100))) {
                    throw new AssertionError("complete seeded record differs");
                }
                count++;
            }
        }
        if (batches != 1 || count != 100) throw new AssertionError("batch/record counts differ");
        var cache = new ObjectSerializationCache();
        var output = ByteBuffer.allocate(request.size(cache, (short) 9));
        request.write(new ByteBufferAccessor(output), cache, (short) 9);
        if (output.hasRemaining() || !Arrays.equals(raw, output.array())) {
            throw new AssertionError("SDK body re-serialization differs");
        }
        System.out.printf("{\"status\":\"pass\",\"version\":9,\"batches\":%d,\"records\":%d,\"full_seeded_records_verified\":true,\"crc_validated\":true,\"body_roundtrip_identical\":true}%n", batches, count);
    }
}
