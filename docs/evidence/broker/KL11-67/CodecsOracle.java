import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/** Independent official Apache encoders and executed record-history parsers. */
public final class CodecsOracle {
    private record Fixture(byte[] bytes, String expected, String origin) { }
    private static final Map<String, Fixture> FIXTURES = new LinkedHashMap<>();
    private static byte[] text(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static byte[] output(MemoryRecords records) {
        ByteBuffer b = records.buffer().duplicate(); byte[] out = new byte[b.remaining()]; b.get(out); return out;
    }
    private static byte[] concat(byte[]... arrays) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] a : arrays) out.writeBytes(a); return out.toByteArray();
    }
    private static byte[] crc(byte[] b) {
        CRC32C c = new CRC32C(); c.update(b, 21, b.length - 21);
        ByteBuffer.wrap(b).putInt(17, (int)c.getValue()); return b;
    }
    private static byte[] payload(byte[] source, byte[] body) {
        byte[] b = concat(Arrays.copyOf(source, 61), body);
        ByteBuffer.wrap(b).putInt(8, b.length - 12); return crc(b);
    }
    private static void put(String id, byte[] bytes, String expected, String origin) {
        if (FIXTURES.put(id, new Fixture(bytes, expected, origin)) != null) throw new AssertionError(id);
    }
    private static byte[] packed(String codec, byte[] body) throws Exception {
        ByteBufferOutputStream buffer = new ByteBufferOutputStream(256);
        try (OutputStream compressed = Compression.of(codec).build().wrapForOutput(buffer, (byte)2)) {
            compressed.write(body);
        }
        ByteBuffer b = buffer.buffer().duplicate(); b.flip();
        byte[] out = new byte[b.remaining()]; b.get(out); return out;
    }
    private static String hex(ByteBuffer b) {
        if (b == null) return "null";
        ByteBuffer copy = b.duplicate(); byte[] out = new byte[copy.remaining()]; copy.get(out);
        return "\"" + HexFormat.of().formatHex(out) + "\"";
    }
    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\"";
    }
    private static String probe(byte[] bytes) {
        int count = 0; StringBuilder history = new StringBuilder();
        try {
            MemoryRecords m = MemoryRecords.readableRecords(ByteBuffer.wrap(bytes));
            int valid = m.validBytes(); int batches = 0;
            for (MutableRecordBatch batch : m.batches()) {
                batches++; batch.ensureValid();
                for (Record r : batch) {
                    r.ensureValid();
                    if (count++ > 32) throw new AssertionError("oracle record bound");
                    if (history.length() != 0) history.append(',');
                    history.append("{\"offset\":").append(r.offset()).append(",\"timestamp\":").append(r.timestamp())
                        .append(",\"key\":").append(hex(r.key())).append(",\"value\":").append(hex(r.value())).append(",\"headers\":[");
                    int i = 0;
                    for (Header h : r.headers()) {
                        if (i++ != 0) history.append(',');
                        history.append("{\"key\":").append(quote(h.key())).append(",\"value\":")
                            .append(h.value() == null ? "null" : quote(HexFormat.of().formatHex(h.value()))).append('}');
                    }
                    history.append("]}");
                }
            }
            return "\"status\":\"accepted\",\"full_input_consumed\":" + (valid == bytes.length)
                + ",\"batches\":" + batches + ",\"records\":" + count + ",\"history\":[" + history + "]";
        } catch (Throwable e) {
            return "\"status\":\"rejected\",\"exception\":" + quote(e.getClass().getName()) + ",\"message\":" + quote(String.valueOf(e.getMessage()));
        }
    }
    private static void generate() throws Exception {
        SimpleRecord[] records = {
            new SimpleRecord(1000L, (byte[])null, (byte[])null),
            new SimpleRecord(1007L, new byte[0], text("payload"), new Header[]{new RecordHeader("trace", text("a")), new RecordHeader("trace", null), new RecordHeader("", new byte[0])}),
            new SimpleRecord(900L, new byte[]{-1,0,16}, new byte[]{0,1,-1}, new Header[]{new RecordHeader("π", text("x"))})
        };
        byte[] plain = output(MemoryRecords.withRecords(Compression.NONE, records));
        put("plain", plain, "accepted", "Apache MemoryRecords ordinary rich history");
        byte[] body = Arrays.copyOfRange(plain, 61, plain.length);
        ByteArrayOutputStream multiple = new ByteArrayOutputStream();
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        multiple.writeBytes(plain); expected.writeBytes(plain);
        for (String codec : new String[]{"gzip", "snappy", "lz4", "zstd"}) {
            byte[] batch = output(MemoryRecords.withRecords(Compression.of(codec).build(), records));
            put(codec, batch, "accepted", "Apache MemoryRecords actual " + codec + " encoder");
            multiple.writeBytes(batch); expected.writeBytes(plain);
            put(codec + "-trailing", payload(batch, concat(Arrays.copyOfRange(batch, 61, batch.length), new byte[]{0})), "rejected", "Trailing protected compressed byte");
            put(codec + "-truncated", payload(batch, Arrays.copyOfRange(batch, 61, batch.length - 1)), "rejected", "Truncated protected stream, valid outer length/CRC");
            byte[] damaged = batch.clone(); damaged[damaged.length - 1] ^= 1;
            put(codec + "-bad-crc", damaged, "rejected", "Encoded payload damaged without CRC repair");
            byte[] count = batch.clone(); ByteBuffer.wrap(count).putInt(57, Integer.MAX_VALUE); crc(count);
            put(codec + "-count-bomb", count, "rejected", "Protected count bomb before decode");
            byte[] idem = output(MemoryRecords.withIdempotentRecords(Compression.of(codec).build(), 9L, (short)0, 3, records));
            put(codec + "-idempotent", idem, "rejected", "Actual Apache idempotent compressed batch excluded");
            byte[] txn = output(MemoryRecords.withTransactionalRecords(Compression.of(codec).build(), 9L, (short)0, 3, records));
            put(codec + "-transactional", txn, "rejected", "Actual Apache transactional compressed batch excluded");
            byte[] malformed = plain.clone(); malformed[61] = 0; // inconsistent inner record length
            put(codec + "-bad-record", payload(batch, packed(codec, Arrays.copyOfRange(malformed, 61, malformed.length))), "rejected", "Actual Apache compression of malformed decoded record body");
            if (!codec.equals("snappy")) {
                int split = body.length / 2;
                byte[] frames = concat(packed(codec, Arrays.copyOfRange(body, 0, split)), packed(codec, Arrays.copyOfRange(body, split, body.length)));
                put(codec + "-concatenated", payload(batch, frames), "accepted", "Two actual Apache codec streams split across one record sequence");
            }
        }
        put("multiple", multiple.toByteArray(), "accepted", "Plain plus four independently compressed batches");
        put("multiple-plain", expected.toByteArray(), "accepted", "Independent uncompressed comparison for complete multi-batch normalization");
        byte[] snappy = FIXTURES.get("snappy").bytes();
        byte[] framed = Arrays.copyOfRange(snappy, 61, snappy.length);
        int chunk = ByteBuffer.wrap(framed).getInt(16);
        if (20 + chunk != framed.length) throw new AssertionError("bounded one-chunk fixture");
        put("snappy-raw", payload(snappy, Arrays.copyOfRange(framed, 20, framed.length)), "accepted", "Actual Snappy JNI block extracted from Apache Xerial envelope");
        byte[] repeated = new byte[32768];
        byte[] pattern = text("bounded-independent-compression-record-π-0123456789");
        for (int i = 0; i < repeated.length; i++) repeated[i] = pattern[i % pattern.length];
        SimpleRecord expanded = new SimpleRecord(1000L, text("expanded"), repeated);
        put("expanded-plain", output(MemoryRecords.withRecords(Compression.NONE, expanded)), "accepted", "Independent expanded ordinary record reference");
        for (String codec : new String[]{"gzip", "snappy", "lz4", "zstd"}) {
            put(codec + "-expanded", output(MemoryRecords.withRecords(Compression.of(codec).build(), expanded)), "accepted", "Actual Apache encoded 32 KiB expansion exercising compressed blocks and output budgets");
        }
    }
    public static void main(String[] args) throws Exception {
        if (!org.apache.kafka.common.utils.AppInfoParser.getVersion().equals("4.3.1")) throw new AssertionError("version");
        Path out = Path.of(args[0]); Files.createDirectories(out); generate();
        for (Map.Entry<String, Fixture> entry : FIXTURES.entrySet()) {
            Fixture f = entry.getValue(); Files.write(out.resolve(entry.getKey() + ".bin"), f.bytes());
            System.out.println("{\"fixture\":" + quote(entry.getKey()) + ",\"expected_rust\":" + quote(f.expected())
                + ",\"bytes\":" + f.bytes().length + ",\"origin\":" + quote(f.origin()) + ",\"upstream\":{" + probe(f.bytes()) + "}}");
        }
    }
}
