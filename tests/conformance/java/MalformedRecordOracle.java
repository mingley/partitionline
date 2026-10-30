/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

/** Kafka 4.3.1 oracle for malformed magic-v2 record error ordering. */
public class MalformedRecordOracle {
    private static final String KAFKA_SHA256 =
        "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36";

    private static void pin() throws Exception {
        Path jar = Path.of(MemoryRecords.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)));
        if (!digest.equals(KAFKA_SHA256)) throw new AssertionError("unexpected Kafka jar: " + digest);
    }

    private static byte[] bytes(ByteBuffer b) {
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    private static byte[] withBody(byte[] template, byte[] body) {
        if (body.length > 63) throw new IllegalArgumentException("single-byte varint required");
        byte[] result = java.util.Arrays.copyOf(template, 62 + body.length);
        result[61] = (byte) (body.length << 1);
        System.arraycopy(body, 0, result, 62, body.length);
        ByteBuffer.wrap(result).putInt(8, result.length - 12).putInt(57, 1);
        updateCrc(result);
        return result;
    }

    private static void updateCrc(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 21, bytes.length - 21);
        ByteBuffer.wrap(bytes).putInt(17, (int) crc.getValue());
    }

    private static void expect(String label, byte[] batch, String expected, Path out) throws Exception {
        Files.write(out.resolve(label + ".batch"), batch);
        try {
            var records = MemoryRecords.readableRecords(ByteBuffer.wrap(batch));
            for (var recordBatch : records.batches()) {
                recordBatch.ensureValid();
                for (var record : recordBatch) {
                    record.offset();
                }
            }
            throw new AssertionError(label + " unexpectedly decoded");
        } catch (RuntimeException error) {
            System.out.println(label + " | " + error.getClass().getName() + " | " + error.getMessage());
            if (error.getMessage() == null || !error.getMessage().contains(expected)) {
                throw new AssertionError(label + " unexpected error", error);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        pin();
        Path output = Path.of(args[0]);
        Files.createDirectories(output);
        byte[] one = bytes(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1L, new byte[]{'x'})).buffer());
        byte[] twoDeclared = one.clone();
        ByteBuffer.wrap(twoDeclared).putInt(57, 2);
        updateCrc(twoDeclared);
        expect("premature-eof", twoDeclared, "Incorrect declared batch size, premature EOF reached", output);
        byte[] hugeDeclared = one.clone();
        ByteBuffer.wrap(hugeDeclared).putInt(57, Integer.MAX_VALUE);
        updateCrc(hugeDeclared);
        expect("huge-count", hugeDeclared, "Incorrect declared batch size, premature EOF reached", output);
        // Attributes, timestamp delta, offset delta, null key, null value,
        // one header, then a negative header-key length.
        byte[] negativeKey = withBody(one, new byte[]{0, 0, 0, 1, 1, 2, 1});
        expect("negative-header-key", negativeKey, "Invalid negative header key size -1", output);
        byte[] firstMalformed = negativeKey.clone();
        ByteBuffer.wrap(firstMalformed).putInt(57, 2);
        updateCrc(firstMalformed);
        expect("first-malformed-before-eof", firstMalformed, "Invalid negative header key size -1", output);
        byte[] excessHeaders = withBody(one, new byte[]{0, 0, 0, 1, 1, 20});
        expect("excess-headers", excessHeaders, "Found invalid number of record headers", output);
    }
}
