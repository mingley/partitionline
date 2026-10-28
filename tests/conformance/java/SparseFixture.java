import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;

public class SparseFixture {
  static byte[] bytes(ByteBuffer source) {
    byte[] bytes = new byte[source.remaining()]; source.get(bytes); return bytes;
  }
  static void crc(byte[] bytes) {
    CRC32C crc = new CRC32C(); crc.update(bytes, 21, bytes.length - 21);
    ByteBuffer.wrap(bytes).putInt(17, (int) crc.getValue());
  }
  static void check(byte[] bytes, boolean wrapped) {
    var batches = MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)).batches().iterator();
    if (!batches.hasNext()) throw new AssertionError("missing batch");
    var batch = batches.next(); batch.ensureValid();
    long first = wrapped ? Long.MAX_VALUE : 10L;
    long second = wrapped ? Long.MIN_VALUE + 1 : 12L;
    long last = wrapped ? Long.MIN_VALUE + 3 : 14L;
    long baseTimestamp = wrapped ? Long.MAX_VALUE : 1000L;
    if (batch.baseOffset() != first || batch.lastOffset() != last) throw new AssertionError("batch range mismatch");
    var records = batch.iterator();
    if (!records.hasNext()) throw new AssertionError("missing first record");
    var a = records.next();
    if (a.offset() != first || a.timestamp() != baseTimestamp || a.hasKey() || !Arrays.equals(bytes(a.value()), new byte[]{1})) throw new AssertionError("first record mismatch");
    if (!records.hasNext()) throw new AssertionError("missing second record");
    var b = records.next();
    if (b.offset() != second || b.timestamp() != baseTimestamp + 2 || !Arrays.equals(bytes(b.key()),new byte[]{2}) || !Arrays.equals(bytes(b.value()),new byte[]{3})) throw new AssertionError("second record mismatch");
    if (records.hasNext() || batches.hasNext()) throw new AssertionError("unexpected extra data");
  }
  public static void main(String[] args) throws Exception {
    Path out = Path.of(args[1]); Files.createDirectories(out);
    if (args[0].equals("generate")) {
      var builder = MemoryRecords.builder(ByteBuffer.allocate(4096), Compression.NONE, TimestampType.CREATE_TIME, 10L);
      builder.appendWithOffset(10L, 1000L, null, new byte[]{1});
      builder.appendWithOffset(12L, 1002L, new byte[]{2}, new byte[]{3});
      byte[] sparse = bytes(builder.build().buffer());
      ByteBuffer.wrap(sparse).putInt(23, 4); crc(sparse); check(sparse, false);
      Files.write(out.resolve("java-sparse.batch"), sparse);
      byte[] wrapped = sparse.clone(); ByteBuffer header = ByteBuffer.wrap(wrapped);
      header.putLong(0, Long.MAX_VALUE); header.putLong(27, Long.MAX_VALUE); header.putLong(35, Long.MAX_VALUE); crc(wrapped); check(wrapped, true);
      Files.write(out.resolve("java-wrapped-sparse.batch"), wrapped);
      System.out.println("{\"status\":\"pass\",\"mode\":\"generate\",\"last_offset_delta\":4}");
    } else if (args[0].equals("verify")) {
      for (String codec : List.of("none", "gzip", "snappy", "lz4", "zstd")) {
        check(Files.readAllBytes(out.resolve("rust-sparse-"+codec+".batch")), false);
        check(Files.readAllBytes(out.resolve("rust-wrapped-sparse-"+codec+".batch")), true);
      }
      System.out.println("{\"status\":\"pass\",\"mode\":\"verify\",\"cases\":10}");
    } else throw new IllegalArgumentException("expected generate or verify");
  }
}
