// Standalone Kafka 3.9.1 correctness fixture: three data records + one commit marker.
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.*;
import java.security.MessageDigest;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class ControlRecords {
  static long mix(long x) {
    x += 0x9e3779b97f4a7c15L;
    x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
    x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
    return x ^ (x >>> 31);
  }
  static String hex(byte[] value) {
    StringBuilder out = new StringBuilder();
    for (byte b : value) out.append(String.format("%02x", b & 255));
    return out.toString();
  }
  static byte[] value(long seed, long id) {
    ByteBuffer out = ByteBuffer.allocate(100);
    out.put("PLBENCH1".getBytes(StandardCharsets.US_ASCII)).putLong(seed).putLong(id);
    long word = mix(seed ^ id);
    while (out.hasRemaining()) {
      byte[] next = ByteBuffer.allocate(8).putLong(word).array();
      out.put(next, 0, Math.min(8, out.remaining())); word = mix(word);
    }
    return out.array();
  }
  public static void main(String[] args) throws Exception {
    String bootstrap = args[0], topic = args[1], transaction = topic + "-transaction";
    long seed = 1592590337L;
    Properties cfg = new Properties();
    cfg.put("bootstrap.servers", bootstrap); cfg.put("acks", "all");
    cfg.put("enable.idempotence", "true"); cfg.put("max.in.flight.requests.per.connection", "1");
    cfg.put("transactional.id", transaction); cfg.put("request.timeout.ms", "10000");
    cfg.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    cfg.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    try (BufferedWriter journal = Files.newBufferedWriter(Paths.get(args[2]), StandardOpenOption.CREATE_NEW);
         KafkaProducer<byte[],byte[]> producer = new KafkaProducer<>(cfg)) {
      journal.write("{\"kind\":\"config\",\"schema_version\":1,\"role\":\"producer\",\"topic\":\""+topic+"\",\"acks\":-1,\"idempotent\":true,\"transactional\":true,\"seed\":"+seed+",\"payload_bytes\":100,\"partitions\":1,\"count\":3,\"transactions\":[{\"txn_id\":\""+transaction+"\",\"status\":\"committed\"}]}\n");
      journal.flush(); producer.initTransactions(); producer.beginTransaction();
      byte[] key = "plbench-000000005eed0001-0".getBytes(StandardCharsets.UTF_8);
      for (int id=0; id<3; id++) {
        byte[] value = value(seed,id);
        RecordMetadata md = producer.send(new ProducerRecord<>(topic,0,key,value)).get();
        journal.write("{\"kind\":\"record\",\"id\":\"000000005eed0001:"+id+"\",\"topic\":\""+topic+"\",\"partition\":0,\"offset\":"+md.offset()+",\"key\":\""+hex(key)+"\",\"payload_hash\":\""+hex(MessageDigest.getInstance("SHA-256").digest(value))+"\",\"payload_bytes\":100,\"phase\":\"measure\",\"status\":\"accepted\",\"txn_id\":\""+transaction+"\"}\n");
        journal.flush();
      }
      producer.commitTransaction();
      journal.write("{\"kind\":\"summary\",\"role\":\"producer\",\"completed\":true,\"run_disposition\":\"executed\",\"accepted_total\":3,\"acknowledged_total\":3,\"locally_completed_total\":0,\"warmup_records\":0,\"measured_records\":3,\"produce_errors\":0,\"queue_full_attempts\":0}\n");
      journal.flush(); System.out.println("{\"application_records\":3,\"transaction_committed\":true}");
    }
  }
}
