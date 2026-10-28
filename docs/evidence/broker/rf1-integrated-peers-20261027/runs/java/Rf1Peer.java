import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class Rf1Peer {
  static byte[] bytes(String value) { return value == null ? null : value.getBytes(StandardCharsets.UTF_8); }
  static String repr(byte[] value) { return value == null ? "<null>" : new String(value, StandardCharsets.UTF_8); }
  static String key(int p, int n) { return n == 1 && p == 0 ? null : "key/" + p + "/" + n; }
  static String value(int p, int n) { return n == 2 && p == 1 ? null : "value/" + p + "/" + n + "-é"; }
  static Properties common(String addr) {
    Properties p = new Properties(); p.put("bootstrap.servers",addr); p.put("client.id","rf1-probe");
    p.put("request.timeout.ms","5000"); p.put("default.api.timeout.ms","15000"); return p;
  }
  static void create(String addr,String topic) throws Exception {
    try (Admin admin = Admin.create(common(addr))) {
      admin.createTopics(List.of(new NewTopic(topic,2,(short)1))).all().get(20,TimeUnit.SECONDS);
      System.out.println("CREATE\t"+topic+"\t2\t1");
    }
  }
  static void produce(String addr,String topic,int from,int end) throws Exception {
    Properties p=common(addr); p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
    p.put(ProducerConfig.ACKS_CONFIG,"1");p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"false");
    p.put(ProducerConfig.LINGER_MS_CONFIG,"0");p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"none");
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"10000");p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"10000");
    try (KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(p)) {
      for(int n=from;n<end;n++) for(int partition=0;partition<2;partition++) {
        ProducerRecord<byte[],byte[]> r=new ProducerRecord<>(topic,partition,bytes(key(partition,n)),bytes(value(partition,n)));
        r.headers().add("dup",bytes("first"));r.headers().add("dup",null);r.headers().add("empty",new byte[0]);
        RecordMetadata ack=producer.send(r).get(15,TimeUnit.SECONDS);
        if(ack.partition()!=partition || ack.offset()!=n) throw new AssertionError("ack "+partition+"/"+n+" -> "+ack);
        System.out.println("ACK\t"+partition+"\t"+ack.offset()+"\t"+key(partition,n)+"\t"+value(partition,n));
      }
      producer.flush();
    }
  }
  static void read(String addr,String topic,int count) {
    Properties p=common(addr);p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"100");
    try (KafkaConsumer<byte[],byte[]> c=new KafkaConsumer<>(p)) {
      List<TopicPartition> parts=List.of(new TopicPartition(topic,0),new TopicPartition(topic,1));c.assign(parts);c.seekToBeginning(parts);
      Set<String> observed=new HashSet<>();long limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
      while(observed.size()<count*2 && System.nanoTime()<limit) {
        ConsumerRecords<byte[],byte[]> records=c.poll(Duration.ofMillis(300));
        for(ConsumerRecord<byte[],byte[]> r:records) {
          int part=r.partition();long offset=r.offset();String id=part+":"+offset;
          if(part<0||part>1||offset<0||offset>=count||!observed.add(id))throw new AssertionError("wrong/duplicate "+id);
          if(!Arrays.equals(r.key(),bytes(key(part,(int)offset)))||!Arrays.equals(r.value(),bytes(value(part,(int)offset))))throw new AssertionError("payload "+id);
          List<Header> headers=new ArrayList<>();r.headers().forEach(headers::add);
          if(headers.size()!=3 || !headers.get(0).key().equals("dup") || !Arrays.equals(headers.get(0).value(),bytes("first")) || !headers.get(1).key().equals("dup") || headers.get(1).value()!=null || !headers.get(2).key().equals("empty") || headers.get(2).value()==null || headers.get(2).value().length!=0)throw new AssertionError("headers "+id);
          System.out.println("RECORD\t"+part+"\t"+offset+"\t"+repr(r.key())+"\t"+repr(r.value())+"\theaders=dup:first,dup:null,empty:empty");
        }
      }
      if(observed.size()!=count*2)throw new AssertionError("missing records "+observed);
      for(TopicPartition part:parts)if(c.position(part)!=count)throw new AssertionError("bad position "+part+" "+c.position(part));
      System.out.println("READ_COMPLETE\t"+count*2);
    }
  }
  public static void main(String[] args) throws Exception {
    if(args.length!=3)throw new IllegalArgumentException("seed|recover addr topic");
    if(args[0].equals("seed")){create(args[1],args[2]);produce(args[1],args[2],0,3);read(args[1],args[2],3);}
    else if(args[0].equals("recover")){read(args[1],args[2],3);produce(args[1],args[2],3,4);read(args[1],args[2],4);}
    else throw new IllegalArgumentException(args[0]);
    System.out.println("PHASE_COMPLETE\t"+args[0]);
  }
}
