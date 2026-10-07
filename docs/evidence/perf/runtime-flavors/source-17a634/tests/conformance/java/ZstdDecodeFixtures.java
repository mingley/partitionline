/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.apache.kafka.common.compress.Compression;
import com.github.luben.zstd.Zstd;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

/** Apache record writer and native decoder; no partitionline encoding. */
public class ZstdDecodeFixtures {
    static byte[] copy(ByteBuffer buffer) { byte[] bytes=new byte[buffer.remaining()];buffer.get(bytes);return bytes; }
    static List<String> values(byte[] bytes) {
        var result=new ArrayList<String>();var records=MemoryRecords.readableRecords(ByteBuffer.wrap(bytes));
        for(var batch:records.batches()) {
            batch.ensureValid();
            for(var record:batch) {
                String key=record.hasKey()?HexFormat.of().formatHex(copy(record.key())):"null";
                String value=record.hasValue()?HexFormat.of().formatHex(copy(record.value())):"null";
                StringBuilder row=new StringBuilder(record.timestamp()+":"+key+":"+value);
                for(var header:record.headers()) row.append(":"+header.key()+"="+(header.value()==null?"null":HexFormat.of().formatHex(header.value())));
                result.add(row.toString());
            }
        }
        return result;
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=3 && args.length!=4) throw new IllegalArgumentException("pinned jar output-dir generate|verify");
        var jar=Path.of(args[0]);var loaded=Path.of(MemoryRecords.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String hash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(loaded)));
        if(!Files.isSameFile(jar,loaded) || !hash.equals("52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36")) throw new AssertionError("loaded jar pin");
        var nativeJar=Path.of(Zstd.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String nativeHash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(nativeJar)));
        if(!nativeHash.equals("b6c3237e24b8252a5a9c3a0e1ae30ef8f323412e74bd358548b7a9e527d676df")) throw new AssertionError("zstd-jni 1.5.6-10 pin");
        var out=Path.of(args[1]);Files.createDirectories(out);
        if(args[2].equals("generate")) {
            byte[] random=new byte[65536],runs=new byte[200000];new Random(1592590337L).nextBytes(random);Arrays.fill(runs,(byte)'x');
            var records=new SimpleRecord[] {
                new SimpleRecord(1,null,(byte[])null),new SimpleRecord(2,new byte[0],new byte[0]),
                new SimpleRecord(3,"key".getBytes(java.nio.charset.StandardCharsets.UTF_8),"世界".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new SimpleRecord(4,null,random),new SimpleRecord(5,null,runs),
                new SimpleRecord(6,"headers".getBytes(java.nio.charset.StandardCharsets.UTF_8),new byte[]{0,1,2},new RecordHeader[]{new RecordHeader("a",new byte[]{0,(byte)255}),new RecordHeader("nullable",null)})
            };
            for(var item:List.of(Map.entry("java-none.batch",Compression.NONE),Map.entry("java-zstd.batch",Compression.zstd().build()))) {
                Files.write(out.resolve(item.getKey()),copy(MemoryRecords.withRecords(item.getValue(),records).buffer()));
            }
        }
        var expected=values(Files.readAllBytes(out.resolve("java-none.batch")));int checked=0;
        try(var paths=Files.list(out)) {
            for(var path:paths.filter(p->p.getFileName().toString().endsWith(".batch")).sorted().toList()) {
                if(!expected.equals(values(Files.readAllBytes(path)))) throw new AssertionError("record values differ: "+path);
                checked++;
            }
        }
        if(args.length==4) {
            var rust=Path.of(args[3]);int outputs=0;
            try(var paths=Files.list(rust)) {
                for(var path:paths.filter(p->p.getFileName().toString().endsWith(".batch")).sorted().toList()) {
                    var reference=rust.resolve(path.getFileName().toString().replace(".batch",".expected"));
                    if(!values(Files.readAllBytes(reference)).equals(values(Files.readAllBytes(path)))) throw new AssertionError("Rust frame differs: "+path);
                    outputs++;
                }
            }
            if(outputs<19) throw new AssertionError("missing Rust level cells");
            System.out.println("{\"rust_encoded_batches\":"+outputs+",\"status\":\"pass\"}");
        }
        System.out.println("{\"native_jar_sha256\":\""+nativeHash+"\",\"status\":\"pass\",\"checked_batches\":"+checked+",\"records_per_batch\":"+expected.size()+",\"loaded_jar_sha256\":\""+hash+"\"}");
    }
}
