import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;

/** Official Apache builders and executed parsers; hostile inputs mutate builder bytes. */
public final class RecordsOracle {
    private record Fixture(byte[] bytes, String expected, String origin) { }
    private static final Map<String, Fixture> FIXTURES = new LinkedHashMap<>();
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static byte[] output(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] result = new byte[buffer.remaining()]; buffer.get(result); return result;
    }
    private static void put(String id, byte[] value, String expected, String origin) {
        if (FIXTURES.put(id, new Fixture(value, expected, origin)) != null) throw new AssertionError(id);
    }
    private static byte[] concat(byte[]... arrays) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] array : arrays) out.writeBytes(array);
        return out.toByteArray();
    }
    private static byte[] crc(byte[] value) {
        CRC32C checksum = new CRC32C(); checksum.update(value, 21, value.length - 21);
        ByteBuffer.wrap(value).putInt(17, (int) checksum.getValue()); return value;
    }
    private static byte[] integer(byte[] source, int at, int value) {
        byte[] changed = source.clone(); ByteBuffer.wrap(changed).putInt(at, value); return crc(changed);
    }
    private static byte[] number(byte[] source, int at, long value) {
        byte[] changed = source.clone(); ByteBuffer.wrap(changed).putLong(at, value); return crc(changed);
    }
    private static byte[] attribute(byte[] source, int value) {
        byte[] changed = source.clone(); ByteBuffer.wrap(changed).putShort(21, (short) value); return crc(changed);
    }
    private static byte[] varint(int value) {
        int unsigned = (value << 1) ^ (value >> 31);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((unsigned & ~127) != 0) { out.write((unsigned & 127) | 128); unsigned >>>= 7; }
        out.write(unsigned); return out.toByteArray();
    }
    private static int readVar(byte[] source, int[] position) {
        int value = 0, shift = 0, part;
        do { part = source[position[0]++] & 255; value |= (part & 127) << shift; shift += 7; } while ((part & 128) != 0);
        return (value >>> 1) ^ -(value & 1);
    }
    // Replaces a field in the one-record basic fixture and adjusts both lengths/CRC.
    private static byte[] field(byte[] source, int start, int end, byte[] replacement) {
        int[] p = {61}; int length = readVar(source, p);
        byte[] body = concat(Arrays.copyOfRange(source, p[0], start), replacement, Arrays.copyOfRange(source, end, source.length));
        if (body.length != length + replacement.length - (end - start)) throw new AssertionError("body length");
        byte[] changed = concat(Arrays.copyOf(source, 61), varint(body.length), body);
        ByteBuffer.wrap(changed).putInt(8, changed.length - 12); return crc(changed);
    }
    private static byte[] append(byte[] source, boolean insideRecord) {
        byte[] changed = concat(source, new byte[]{0});
        ByteBuffer.wrap(changed).putInt(8, changed.length - 12);
        if (insideRecord) changed[61] = varint((source[61] & 255) / 2 + 1)[0];
        return crc(changed);
    }
    private static String escaped(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
    private static String probe(byte[] source) {
        String phase = "readableRecords";
        int validBytes = -1, batches = 0, records = 0, headers = 0;
        try {
            MemoryRecords parsed = MemoryRecords.readableRecords(ByteBuffer.wrap(source));
            phase = "validBytes"; validBytes = parsed.validBytes();
            phase = "batchIterator";
            for (MutableRecordBatch batch : parsed.batches()) {
                batches++;
                phase = "batch.ensureValid"; batch.ensureValid();
                phase = "record.iterator";
                for (Record record : batch) {
                    records++;
                    phase = "record.ensureValid"; record.ensureValid();
                    record.key(); record.value(); record.timestamp(); record.offset();
                    headers += record.headers().length;
                    if (records > 16) throw new AssertionError("fixture exceeded oracle work ceiling");
                    phase = "record.iterator";
                }
                phase = "batchIterator";
            }
            return "\"status\":\"accepted\",\"valid_bytes\":" + validBytes + ",\"full_input_consumed\":" + (validBytes == source.length)
                + ",\"batches\":" + batches + ",\"records\":" + records + ",\"headers\":" + headers;
        } catch (Throwable failure) {
            return "\"status\":\"rejected\",\"phase\":\"" + phase + "\",\"exception\":\"" + escaped(failure.getClass().getName())
                + "\",\"message\":\"" + escaped(String.valueOf(failure.getMessage())) + "\",\"valid_bytes\":" + validBytes
                + ",\"batches\":" + batches + ",\"records\":" + records;
        }
    }
    private static void generate() {
        SimpleRecord basic = new SimpleRecord(1000L, bytes("k"), bytes("v"), new Header[]{new RecordHeader("h", bytes("x"))});
        byte[] one = output(MemoryRecords.withRecords(Compression.NONE, basic));
        put("valid-basic", one, "accepted", "Apache MemoryRecords.withRecords");
        SimpleRecord[] rich = {
            new SimpleRecord(1000L, (byte[]) null, (byte[]) null),
            new SimpleRecord(1007L, new byte[0], bytes("payload"), new Header[]{new RecordHeader("trace", bytes("a")), new RecordHeader("trace", null), new RecordHeader("", new byte[0])}),
            new SimpleRecord(1003L, new byte[]{-1,0,16}, new byte[0], new Header[]{new RecordHeader("π", bytes("x"))})
        };
        byte[] multiple = output(MemoryRecords.withRecords(Compression.NONE, rich));
        put("valid-nulls-empty", multiple, "accepted", "Apache builder: null/empty fields, duplicate/Unicode headers, unordered timestamps");
        put("valid-multiple-batches", concat(one, multiple), "accepted", "Concatenated two independently Apache-built batches, both base offset zero");
        put("valid-negative-timestamp-delta", output(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(1000L, bytes("a")), new SimpleRecord(900L, bytes("b")))), "accepted", "Apache builder with negative timestamp delta");
        put("valid-offset-boundary", output(MemoryRecords.withRecords(Long.MAX_VALUE - 2, Compression.NONE, new SimpleRecord(1000L, bytes("a")), new SimpleRecord(1000L, bytes("b")))), "accepted", "Apache builder reaching signed next offset Long.MAX_VALUE");
        put("valid-no-timestamp", output(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(-1L, bytes("none")))), "accepted", "Apache builder with NO_TIMESTAMP sentinel");
        put("feature-legacy-v0", output(MemoryRecords.withRecords((byte) 0, Compression.NONE, new SimpleRecord(bytes("legacy")))), "legacy0", "Apache legacy magic0 builder");
        put("feature-legacy-v1", output(MemoryRecords.withRecords((byte) 1, Compression.NONE, new SimpleRecord(1000L, bytes("k"), bytes("v")))), "legacy1", "Apache legacy magic1 builder");
        for (String codec : new String[]{"gzip", "snappy", "lz4", "zstd"}) {
            int id = switch(codec) { case "gzip" -> 1; case "snappy" -> 2; case "lz4" -> 3; default -> 4; };
            put("feature-" + codec, output(MemoryRecords.withRecords(Compression.of(codec).build(), basic)), "compression" + id, "Apache builder with actual " + codec + " compression");
        }
        put("feature-idempotent", output(MemoryRecords.withIdempotentRecords(Compression.NONE, 9L, (short)0, 3, basic)), "idempotent", "Apache idempotent builder");
        put("feature-transactional", output(MemoryRecords.withTransactionalRecords(Compression.NONE, 9L, (short)0, 3, basic)), "transactional", "Apache transactional builder");
        put("feature-control", output(MemoryRecords.withEndTransactionMarker(0L, 1000L, 0, 9L, (short)0, new EndTransactionMarker(ControlRecordType.COMMIT, 12))), "control", "Apache transaction control marker builder");
        MemoryRecordsBuilder logTime = MemoryRecords.builder(ByteBuffer.allocate(1024), (byte)2, Compression.NONE, TimestampType.LOG_APPEND_TIME, 0L, 1000L);
        logTime.append(basic); put("feature-log-append-time", output(logTime.build()), "logappend", "Apache LOG_APPEND_TIME builder");
        put("feature-delete-horizon", attribute(one, 64), "deletehorizon", "Protected delete-horizon bit mutation of Apache batch");
        byte[] damaged = one.clone(); damaged[damaged.length - 1] ^= 64;
        put("bad-crc", damaged, "checksum", "Payload mutation without CRC repair");
        damaged = one.clone(); damaged[16] = 3; put("bad-magic", damaged, "magic", "Unprotected magic mutation");
        damaged = one.clone(); ByteBuffer.wrap(damaged).putInt(8,-1); put("bad-negative-batch-length", damaged, "length", "Unprotected batch-length mutation");
        damaged = one.clone(); ByteBuffer.wrap(damaged).putInt(8,Integer.MAX_VALUE); put("bad-huge-batch-length", damaged, "batchbudget", "Unprotected batch-length bomb");
        put("bad-truncated", Arrays.copyOf(one, one.length - 1), "truncated", "Truncated Apache-produced batch");
        put("bad-trailing-input", concat(one, new byte[]{1}), "truncated", "Trailing incomplete batch prefix");
        put("bad-negative-count", integer(one,57,-1), "empty", "Protected negative record count");
        put("bad-zero-count", integer(one,57,0), "empty", "Protected empty declared record count with retained payload");
        put("bad-huge-count", integer(one,57,Integer.MAX_VALUE), "recordbudget", "Protected record-count bomb");
        put("bad-count-mismatch", integer(integer(one,57,2),23,1), "length", "Protected coherent count/delta but missing second record");
        put("bad-last-delta", integer(one,23,10), "offset", "Protected noncontiguous last offset delta");
        put("bad-negative-last-delta", integer(one,23,-1), "offset", "Protected negative last offset delta");
        put("bad-offset-overflow", number(one,0,Long.MAX_VALUE), "offset", "Unprotected offset addition overflow");
        put("bad-negative-base-offset", number(one,0,-1), "offset", "Unprotected negative base offset");
        put("bad-leader-epoch", integer(one,12,-2), "metadata", "Unprotected invalid leader epoch");
        damaged=one.clone(); ByteBuffer.wrap(damaged).putShort(51,(short)0); put("bad-producer-sentinel",crc(damaged),"metadata","Protected incoherent producer metadata");
        put("bad-max-timestamp", number(one,35,1001L), "timestamp", "Protected maximum timestamp mismatch");
        put("bad-reserved-attributes", attribute(one,128), "attributes", "Protected reserved batch bit");
        put("bad-unknown-codec", attribute(one,7), "attributes", "Protected unknown codec ID");
        int[] position = {61}; int recordLength = readVar(one,position); int attributes=position[0]++;
        int timestamp=position[0]; readVar(one,position); int offset=position[0]; readVar(one,position);
        int keyLength=position[0]; int keyBytes=readVar(one,position); position[0]+=keyBytes;
        int valueLength=position[0]; int valueBytes=readVar(one,position); position[0]+=valueBytes;
        int headerCount=position[0]; readVar(one,position); int headerKeyLength=position[0]; int headerKeyBytes=readVar(one,position); int headerKey=position[0]; position[0]+=headerKeyBytes;
        int headerValueLength=position[0]; readVar(one,position);
        damaged=one.clone(); damaged[attributes]=1; put("bad-record-attributes",crc(damaged),"recordattributes","Protected reserved record bit");
        damaged=one.clone(); damaged[offset]=4; put("bad-record-offset",crc(damaged),"offset","Protected record offset delta2 for record0");
        damaged=one.clone(); damaged[61]=varint(1)[0]; put("bad-record-short-length",crc(damaged),"length","Protected undersized record body");
        damaged=one.clone(); damaged[61]=varint(recordLength+20)[0]; put("bad-record-long-length",crc(damaged),"truncated","Protected record body extends beyond batch");
        put("bad-record-trailing",append(one,true),"trailing","Protected unconsumed record byte");
        put("bad-batch-trailing",append(one,false),"trailing","Protected unconsumed batch byte");
        put("bad-key-negative",field(one,keyLength,keyLength+1,varint(-2)),"length","Protected invalid nullable-key sentinel");
        put("bad-value-negative",field(one,valueLength,valueLength+1,varint(-2)),"length","Protected invalid nullable-value sentinel");
        put("bad-key-length-bomb",field(one,keyLength,keyLength+1,varint(Integer.MAX_VALUE)),"fieldbudget","Protected key-length bomb");
        put("bad-value-length-bomb",field(one,valueLength,valueLength+1,varint(Integer.MAX_VALUE)),"fieldbudget","Protected value-length bomb");
        put("bad-header-negative-count",field(one,headerCount,headerCount+1,varint(-1)),"length","Protected negative header count");
        put("bad-header-count-bomb",field(one,headerCount,headerCount+1,varint(Integer.MAX_VALUE)),"headerbudget","Protected header-count bomb");
        put("bad-header-key-null",field(one,headerKeyLength,headerKeyLength+1,varint(-1)),"length","Protected nullable header key");
        put("bad-header-key-bomb",field(one,headerKeyLength,headerKeyLength+1,varint(Integer.MAX_VALUE)),"fieldbudget","Protected header-key length bomb");
        put("bad-header-value-negative",field(one,headerValueLength,headerValueLength+1,varint(-2)),"length","Protected invalid nullable-header-value sentinel");
        damaged=one.clone(); damaged[headerKey]=(byte)255; put("bad-header-utf8",crc(damaged),"headerkey","Protected non-UTF8 header key");
        put("bad-varint-overflow",concat(Arrays.copyOf(one,61),new byte[]{-1,-1,-1,-1,16},Arrays.copyOfRange(one,62,one.length)),"varintoverflow","Protected 32-bit record-length varint overflow");
        damaged=FIXTURES.get("bad-varint-overflow").bytes(); ByteBuffer.wrap(damaged).putInt(8,damaged.length-12); crc(damaged);
        put("bad-varlong-overflow",field(one,timestamp,timestamp+1,new byte[]{-1,-1,-1,-1,-1,-1,-1,-1,-1,2}),"varintoverflow","Protected 64-bit timestamp varint overflow");
        damaged=concat(Arrays.copyOf(one,61),new byte[]{(byte)(one[61]|128),0},Arrays.copyOfRange(one,62,one.length)); ByteBuffer.wrap(damaged).putInt(8,damaged.length-12);
        put("strict-noncanonical-record-length",crc(damaged),"noncanonical","Protected overlong but numerically valid record-length varint");
        put("strict-noncanonical-timestamp",field(one,timestamp,timestamp+1,new byte[]{-128,0}),"noncanonical","Protected overlong zero timestamp delta");
        put("strict-noncanonical-offset",field(one,offset,offset+1,new byte[]{-128,0}),"noncanonical","Protected overlong zero offset delta");
        damaged=field(one,timestamp,timestamp+1,varint(1)); ByteBuffer.wrap(damaged).putLong(27,Long.MAX_VALUE); ByteBuffer.wrap(damaged).putLong(35,Long.MAX_VALUE);
        put("bad-timestamp-overflow",crc(damaged),"timestamp","Protected timestamp addition overflow");
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=1) throw new IllegalArgumentException("output directory required");
        Path directory=Path.of(args[0]); Files.createDirectories(directory);
        generate();
        for(Map.Entry<String,Fixture> entry:FIXTURES.entrySet()) {
            Fixture f=entry.getValue(); Files.write(directory.resolve(entry.getKey()+".bin"),f.bytes());
            System.out.println("{\"fixture\":\""+entry.getKey()+"\",\"bytes\":"+f.bytes().length+",\"expected_rust\":\""+f.expected()+"\",\"origin\":\""+escaped(f.origin())+"\",\"upstream\":{"+probe(f.bytes())+"}}");
        }
    }
}
