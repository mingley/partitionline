/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.*;
import java.nio.file.*;
import java.security.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
/** Independent Apache codec and native broker peer. */
public class CodecMatrix {
 static byte[] copy(ByteBuffer b){byte[] out=new byte[b.remaining()];b.get(out);return out;}
 static void pin() throws Exception {
  Map<String,String> pins=Map.of(
   MemoryRecords.class.getName(),"52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36",
   "org.xerial.snappy.Snappy","4c766cb3f855415ee734b2392949a0b6f12a60879334a74518deaf6270d32e36",
   "net.jpountz.lz4.LZ4Factory","252dabd586d22149a31661a8afc1a50747714eb1a298bd4a6e780c8c91a13866",
   "com.github.luben.zstd.Zstd","b6c3237e24b8252a5a9c3a0e1ae30ef8f323412e74bd358548b7a9e527d676df");
  for(var pin:pins.entrySet()) {
   Path jar=Path.of(Class.forName(pin.getKey()).getProtectionDomain().getCodeSource().getLocation().toURI());
   String sha=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)));
   if(!sha.equals(pin.getValue()))throw new AssertionError("peer jar pin: "+pin.getKey());
  }
 }
 static byte[] entropy(int size){byte[] out=new byte[size];long state=0x5eed0001L;for(int at=0;at<size;at+=8){state^=state<<13;state^=state>>>7;state^=state<<17;for(int j=0;j<8&&at+j<size;j++)out[at+j]=(byte)(state>>>(j*8));}return out;}
 static byte[] key(int id){return switch(id){case 0,3,4,6,7,8->null;case 1->new byte[0];case 2->new byte[]{'k','e','y'};default->new byte[]{'h','e','a','d','e','r','s'};};}
 static byte[] value(int id){return switch(id){case 0->null;case 1->new byte[0];case 2->"世界".getBytes(java.nio.charset.StandardCharsets.UTF_8);case 3->entropy(65536);case 4->{byte[] b=new byte[200000];Arrays.fill(b,(byte)'x');yield b;}case 5->new byte[]{0,1,2};default->entropy(131071+id-6);};}
 static Header[] headers(int id){return new Header[]{new RecordHeader("id",ByteBuffer.allocate(8).putLong(id).array()),new RecordHeader("a",new byte[]{0,(byte)255}),new RecordHeader("nullable",null)};}
 static List<String> rows(byte[] b){var out=new ArrayList<String>();for(var batch:MemoryRecords.readableRecords(ByteBuffer.wrap(b)).batches()){batch.ensureValid();for(var r:batch){var row=new StringBuilder(r.offset()+":"+r.timestamp()+":"+(r.hasKey()?HexFormat.of().formatHex(copy(r.key())):"null")+":"+(r.hasValue()?HexFormat.of().formatHex(copy(r.value())):"null"));for(var h:r.headers())row.append(":"+h.key()+"="+(h.value()==null?"null":HexFormat.of().formatHex(h.value())));out.add(row.toString());}}return out;}
 static Compression compression(String c){return switch(c){case "none"->Compression.NONE;case "gzip"->Compression.gzip().build();case "snappy"->Compression.snappy().build();case "lz4"->Compression.lz4().build();case "zstd"->Compression.zstd().build();default->throw new IllegalArgumentException(c);};}
 static void fixtures(Path dir) throws Exception {
  Files.createDirectories(dir);var records=new SimpleRecord[9];for(int i=0;i<9;i++)records[i]=new SimpleRecord(i+1,key(i),value(i),headers(i));
  for(String c:List.of("none","gzip","snappy","lz4","zstd"))Files.write(dir.resolve("java-"+c+".batch"),copy(MemoryRecords.withRecords(compression(c),records).buffer()));
  var baseline=rows(Files.readAllBytes(dir.resolve("java-none.batch")));for(String c:List.of("none","gzip","snappy","lz4","zstd"))if(!rows(Files.readAllBytes(dir.resolve("java-"+c+".batch"))).equals(baseline))throw new AssertionError("fixture differs "+c);
  // Signed field arithmetic in the Java wire helper deliberately wraps.
  byte[] edge=Files.readAllBytes(dir.resolve("java-none.batch"));ByteBuffer header=ByteBuffer.wrap(edge);header.putLong(0,Long.MAX_VALUE);header.putLong(27,Long.MAX_VALUE);header.putLong(35,Long.MAX_VALUE);var crc=new java.util.zip.CRC32C();crc.update(edge,21,edge.length-21);header.putInt(17,(int)crc.getValue());Files.write(dir.resolve("java-signed-boundary.batch"),edge);
  var edgeRecords=MemoryRecords.readableRecords(ByteBuffer.wrap(edge)).records().iterator();var first=edgeRecords.next();var second=edgeRecords.next();if(first.offset()!=Long.MAX_VALUE||second.offset()!=Long.MIN_VALUE||first.timestamp()!=Long.MAX_VALUE||second.timestamp()!=Long.MIN_VALUE)throw new AssertionError("Java arithmetic contract");
  System.out.println("{\"status\":\"pass\",\"independent_codecs\":5,\"records\":9,\"signed_delta_wrap\":true}");
 }
 static Properties props(String bootstrap){var p=new Properties();p.put("bootstrap.servers",bootstrap);p.put("request.timeout.ms","10000");return p;}
 public static void main(String[] args) throws Exception {
  pin();if(args[0].equals("fixtures")){fixtures(Path.of(args[1]));return;}
  if(args[0].equals("verify-fixtures")) {
   Path dir=Path.of(args[1]),rust=Path.of(args[2]);var expected=rows(Files.readAllBytes(dir.resolve("java-none.batch")));int count=0;
   var signed=rows(Files.readAllBytes(dir.resolve("java-signed-boundary.batch")));
   for(String c:List.of("none","gzip","snappy","lz4","zstd")) {
    if(!rows(Files.readAllBytes(rust.resolve("rust-"+c+".batch"))).equals(expected))throw new AssertionError("Rust codec differs "+c);
    if(!rows(Files.readAllBytes(rust.resolve("rust-signed-"+c+".batch"))).equals(signed))throw new AssertionError("Rust signed deltas differ "+c);count++;
   }
   byte[] raw=Files.readAllBytes(dir.resolve("native-raw-snappy.batch")),plain=Files.readAllBytes(dir.resolve("java-none.batch"));
   if(!Arrays.equals(org.xerial.snappy.Snappy.uncompress(Arrays.copyOfRange(raw,61,raw.length)),Arrays.copyOfRange(plain,61,plain.length)))throw new AssertionError("independent raw Snappy");
   System.out.println("{\"status\":\"pass\",\"rust_codecs\":"+count+",\"signed_rust_codecs\":"+count+",\"raw_snappy_native_crosscheck\":true}");return;
  }
  String mode=args[0],bootstrap=args[1],topic=args[2],codec=args[3];long timestamp=Long.parseLong(args[4]);
  if(mode.equals("produce")) {
   var p=props(bootstrap);p.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");p.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");p.put("acks","all");p.put("enable.idempotence","false");p.put("compression.type",codec);p.put("delivery.timeout.ms","30000");p.put("max.block.ms","10000");
   try(var producer=new KafkaProducer<byte[],byte[]>(p)){for(int i=0;i<9;i++){var m=producer.send(new ProducerRecord<byte[],byte[]>(topic,0,timestamp+i,key(i),value(i),Arrays.asList(headers(i)))).get(30,java.util.concurrent.TimeUnit.SECONDS);if(m.offset()!=i)throw new AssertionError("producer offset");}producer.flush();}
   System.out.println("{\"status\":\"pass\",\"mode\":\"produce\",\"records\":9,\"producer_closed\":true}");
  } else if(mode.equals("consume")) {
   var p=props(bootstrap);p.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");p.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");p.put("enable.auto.commit","false");p.put("group.id","codec-matrix-java");var partition=new TopicPartition(topic,0);int count=0;long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();
   try(var consumer=new KafkaConsumer<byte[],byte[]>(p)){consumer.assign(List.of(partition));consumer.seek(partition,0);while(count<9&&System.nanoTime()<deadline){for(var r:consumer.poll(Duration.ofMillis(100))){int id=(int)r.offset();if(id!=count||id>=9||r.timestamp()!=timestamp+id||!Arrays.equals(r.key(),key(id))||!Arrays.equals(r.value(),value(id)))throw new AssertionError("record bytes/offset/timestamp");var hs=r.headers().toArray();var expected=headers(id);if(hs.length!=expected.length)throw new AssertionError("header count");for(int j=0;j<hs.length;j++)if(!hs[j].key().equals(expected[j].key())||!Arrays.equals(hs[j].value(),expected[j].value()))throw new AssertionError("headers");count++;}}if(count!=9||consumer.endOffsets(List.of(partition)).get(partition)!=9)throw new AssertionError("record count/HW");}
   System.out.println("{\"status\":\"pass\",\"mode\":\"consume\",\"verified\":9,\"consumer_closed\":true}");
  } else throw new IllegalArgumentException(mode);
 }
}
