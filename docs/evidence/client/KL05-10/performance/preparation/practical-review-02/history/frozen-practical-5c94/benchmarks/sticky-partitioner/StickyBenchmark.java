/* Prepared genuine Kafka4.3.1 public producer. No private SDK shadow or RNG override.
 * Run only under a separately granted qualification CPU0,1 or ranking CPU3 lease.
 */
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class StickyBenchmark {
    private static final long MAX_RECORDS = 10_000_000L;
    private static final int WINDOW = 8192;
    private static final byte[] MAGIC = "PLSTK01\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final byte[] VALUE_MAGIC = "PLSTKVAL".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static long mix(long x) {
        x += 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        return x ^ (x >>> 31);
    }
    private static byte[] body(long seed, long id) {
        ByteBuffer value = ByteBuffer.allocate(100);
        value.put(VALUE_MAGIC).putLong(seed).putLong(id);
        long word = mix(seed ^ id);
        while (value.hasRemaining()) {
            byte[] bytes = ByteBuffer.allocate(8).putLong(word).array();
            value.put(bytes, 0, Math.min(value.remaining(), 8));
            word = mix(word);
        }
        return value.array();
    }
    private static byte[] key(long seed, long id, boolean keyed) {
        return keyed ? ByteBuffer.allocate(16).putLong(seed).putLong(id).array() : null;
    }
    private static byte[] hash(MessageDigest sha, byte[] key, byte[] value) {
        sha.reset(); sha.update((byte)(key == null ? 0 : 1));
        if (key != null) sha.update(key);
        return sha.digest(value);
    }
    private static String quote(String value) {
        StringBuilder quoted = new StringBuilder("\"");
        for (int index=0; index<value.length(); index++) {
            char character=value.charAt(index);
            if (character=='"' || character=='\\') quoted.append('\\').append(character);
            else if (character<0x20) {
                String hex=Integer.toHexString(character);
                quoted.append("\\u");
                for (int padding=hex.length(); padding<4; padding++) quoted.append('0');
                quoted.append(hex);
            } else quoted.append(character);
        }
        return quoted.append('"').toString();
    }
    private static void receipt(Path out, String label, String json) throws IOException {
        Files.writeString(out.resolve(label + ".json"), json + "\n", StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }
    private static String metadata(List<PartitionInfo> partitions) {
        List<PartitionInfo> sorted = new ArrayList<>(partitions);
        sorted.sort(Comparator.comparingInt(PartitionInfo::partition));
        StringBuilder result = new StringBuilder("[");
        for (PartitionInfo partition : sorted) {
            if (result.length() != 1) result.append(',');
            result.append("{\"partition\":").append(partition.partition()).append(",\"leader\":")
                  .append(partition.leader() == null ? -1 : partition.leader().id()).append('}');
        }
        return result.append(']').toString();
    }
    private static final class Ack {
        final long id, started, completed; final int phase; final byte[] hash;
        final RecordMetadata metadata; final Exception error;
        Ack(long id, int phase, long started, long completed, byte[] hash, RecordMetadata metadata, Exception error) {
            this.id=id; this.phase=phase; this.started=started; this.completed=completed;
            this.hash=hash; this.metadata=metadata; this.error=error;
        }
    }
    private static final class Phase {
        final String json; final long nextId;
        Phase(String json, long nextId) {this.json=json;this.nextId=nextId;}
    }
    private static Phase phase(KafkaProducer<byte[],byte[]> producer, Path out, DataOutputStream journal,
            String topic, long seed, boolean keyed, long nextId, int phase, long epoch, boolean qualification) throws Exception {
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long cpuBefore=os.getProcessCpuTime();
        if(cpuBefore<0)throw new IOException("process CPU time unavailable");
        long started = System.nanoTime(), duration = (qualification ? 0L : phase == 0 ? 15L : 60L)*1_000_000_000L;
        long minimum = qualification ? phase == 0 ? 8192L : 16384L : phase == 0 ? 10_000L : 1_000_000L, submitted=0, acknowledged=0;
        long maxRecords=qualification ? 24576L : MAX_RECORDS, phaseDeadlineSeconds=qualification ? 90L : 300L;
        int pending=0;
        TreeMap<Integer,Long> partitionCounts = new TreeMap<>();
        ArrayBlockingQueue<Ack> completed = new ArrayBlockingQueue<>(WINDOW);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        while (true) {
            if (System.nanoTime()-started > phaseDeadlineSeconds*1_000_000_000L) throw new IOException("bounded"+phaseDeadlineSeconds+"s phase timeout");
            if (callbackFailure.get()!=null) throw new IOException("callback queue ownership failed", callbackFailure.get());
            boolean stopAdmission = submitted>=minimum && System.nanoTime()-started>=duration;
            if (!stopAdmission && pending<WINDOW) {
                if (nextId>=maxRecords) throw new IOException(maxRecords+" total record cap before duration/minimum; unqualified run");
                final long id=nextId++;
                byte[] key=key(seed,id,keyed), value=body(seed,id), digest=hash(sha,key,value);
                final long sendStarted=System.nanoTime()-epoch;
                ProducerRecord<byte[],byte[]> record = new ProducerRecord<>(topic,key,value);
                if (record.partition()!=null) throw new IOException("explicit partition prohibited");
                producer.send(record,(metadata,error)-> {
                    Ack ack=new Ack(id,phase,sendStarted,System.nanoTime()-epoch,digest,metadata,error);
                    if (!completed.offer(ack)) callbackFailure.compareAndSet(null,new IOException("bounded ack queue overflow"));
                });
                pending++;submitted++;
            }
            Ack ack=completed.poll();
            if (ack==null && pending>0 && (pending==WINDOW || stopAdmission)) ack=completed.poll(1,TimeUnit.SECONDS);
            if (ack!=null) {
                pending--;
                if (ack.error!=null) throw new IOException("public producer terminal failure",ack.error);
                if (ack.metadata==null || ack.metadata.partition()<0 || ack.metadata.offset()<0) throw new IOException("invalid public ack metadata");
                journal.writeLong(ack.id);journal.writeInt(ack.phase);journal.writeInt(ack.metadata.partition());journal.writeLong(ack.metadata.offset());
                journal.writeLong(ack.started);journal.writeLong(ack.completed);journal.write(ack.hash);
                // Shared Rust conservative bound, not Java accumulator/compression history.
                journal.writeInt(keyed ? 141 : 125);
                partitionCounts.merge(ack.metadata.partition(),1L,Long::sum);acknowledged++;
            }
            if (stopAdmission && pending==0) break;
        }
        producer.flush();journal.flush();
        long elapsed=System.nanoTime()-started;
        if (acknowledged!=submitted || acknowledged<minimum || elapsed<duration || elapsed>phaseDeadlineSeconds*1_000_000_000L) throw new IOException("phase barriers failed");
        StringBuilder counts=new StringBuilder("{");
        for (Map.Entry<Integer,Long> entry:partitionCounts.entrySet()) {if(counts.length()!=1)counts.append(',');counts.append(quote(entry.getKey().toString())).append(':').append(entry.getValue());}
        counts.append('}');
        String json="{\"purpose\":"+quote(qualification?"qualification":"ranking")+",\"performance_qualified\":false,\"phase\":"+phase+",\"start_monotonic_ns\":"+(started-epoch)+",\"end_monotonic_ns\":"+(System.nanoTime()-epoch)
            +",\"process_cpu_nanoseconds_delta\":"+(os.getProcessCpuTime()-cpuBefore)+",\"acknowledged\":"+acknowledged+",\"seconds_including_admission_and_drain_flush\":"+(elapsed/1e9)
            +",\"acknowledged_records_per_second\":"+(qualification ? "null" : Double.toString(acknowledged/(elapsed/1e9)))+",\"partition_counts\":"+counts
            +",\"sum_record_size_upper_bound\":"+(acknowledged*(keyed?141L:125L))
            +",\"record_accounting\":\"shared Rust conservative reference only; Java accumulator bytes unobserved\""
            +",\"sticky_61_byte_cohort_overheads\":\"unobserved by public API; not added to this sum\""
            +",\"latency_scope\":\"individual send invocation to callback; closed-loop window, not CO-corrected/open-loop claim\"}";
        receipt(out,"phase-"+phase,json);
        return new Phase(json,nextId);
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=6)throw new IllegalArgumentException("qualify|rank PROFILE BOOTSTRAP TOPIC OUT SEED");
        if(!args[0].equals("qualify")&&!args[0].equals("rank"))throw new IllegalArgumentException("exact purpose required");
        boolean qualification=args[0].equals("qualify");
        String expectedCPU=qualification?"Cpus_allowed_list:\t0-1":"Cpus_allowed_list:\t3";
        if(!Files.readString(Path.of("/proc/self/status")).lines().anyMatch(line->line.equals(expectedCPU)))throw new IllegalArgumentException("producer CPU differs from qualification or ranking lease");
        String profile=args[1],bootstrap=args[2],topic=args[3];Path out=Path.of(args[4]);long seed=Long.parseUnsignedLong(args[5]);
        if(!profile.equals("java-uniform-keyed")&&!profile.equals("java-uniform-null"))throw new IllegalArgumentException("exact Java profile required");
        if(topic.length()>120||!topic.matches("[A-Za-z0-9_-]+"))throw new IllegalArgumentException("bounded fresh topic required");
        boolean keyed=profile.endsWith("-keyed");Files.createDirectory(out);
        Properties properties=new Properties();properties.setProperty("bootstrap.servers",bootstrap);properties.setProperty("client.id","sticky-benchmark");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");properties.setProperty("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("acks","all");properties.setProperty("enable.idempotence","true");properties.setProperty("max.in.flight.requests.per.connection","5");
        properties.setProperty("linger.ms","5");properties.setProperty("batch.size","1048576");properties.setProperty("max.request.size","1048576");properties.setProperty("buffer.memory","33554432");
        properties.setProperty("request.timeout.ms",qualification?"10000":"30000");properties.setProperty("delivery.timeout.ms",qualification?"30000":"120000");properties.setProperty("max.block.ms",qualification?"5000":"60000");
        properties.setProperty("compression.type","none");properties.setProperty("security.protocol","PLAINTEXT");
        properties.setProperty("enable.metrics.push","false");
        properties.setProperty("partitioner.adaptive.partitioning.enable","false");properties.setProperty("partitioner.ignore.keys","false");
        receipt(out,"configuration","{\"purpose\":"+quote(qualification?"qualification":"ranking")+",\"performance_qualified\":false,\"profile\":"+quote(profile)+",\"seed\":"+Long.toUnsignedString(seed)+",\"topic\":"+quote(topic)
            +",\"key_presence\":"+quote(keyed?"non-null":"null")+",\"key_bytes\":"+(keyed?16:0)+",\"value_bytes\":100,\"explicit_partition\":false,\"acks\":-1,\"idempotence\":true,\"max_in_flight\":5,\"linger_ms\":5,\"batch_bytes\":1048576,\"compression\":\"none\",\"enable_metrics_push\":false,\"record_window\":8192,\"max_records\":"+(qualification?24576:10000000)+",\"producer_policy\":\"genuine Java4.3.1 builtin uniform adaptive=false, keyed murmur2\",\"production_rng_seed\":\"not externally configurable; input seed does not seed Java partitioner\",\"request_timeout_ms\":"+(qualification?10000:30000)+",\"delivery_timeout_ms\":"+(qualification?30000:120000)+",\"max_block_ms\":"+(qualification?5000:60000)+",\"cpu_lease_required\":"+quote(qualification?"CPU0,1 future qualification lease":"exclusiveCPU3 future ranking lease")+"}");
        Properties consumerProperties=new Properties();consumerProperties.setProperty("bootstrap.servers",bootstrap);consumerProperties.setProperty("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");consumerProperties.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");consumerProperties.setProperty("enable.auto.commit","false");consumerProperties.setProperty("group.id","sticky-benchmark-baseline");
        KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(properties);
        try(DataOutputStream journal=new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(out.resolve("producer-acks.bin"),StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE),256*1024))) {
            List<PartitionInfo> partitions=producer.partitionsFor(topic);
            if(partitions.size()!=6||partitions.stream().anyMatch(p->p.leader()==null))throw new IOException("six leader-eligible partitions required");
            String before=metadata(partitions);
            try(KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(consumerProperties)) {
                List<TopicPartition> assignment=new ArrayList<>();for(PartitionInfo p:partitions)assignment.add(new TopicPartition(topic,p.partition()));
                consumer.assign(assignment);
                Map<TopicPartition,Long> ends=consumer.endOffsets(assignment,Duration.ofSeconds(30));
                if(ends.size()!=6 || !ends.keySet().containsAll(assignment) || ends.values().stream().anyMatch(x->x!=0L))throw new IOException("fresh empty six-partition topic required");
            }
            receipt(out,"metadata-before","{\"partitions\":"+before+",\"all_end_offsets_zero\":true}");journal.write(MAGIC);
            long epoch=System.nanoTime();Phase warmup=phase(producer,out,journal,topic,seed,keyed,0,0,epoch,qualification);
            Phase measured=phase(producer,out,journal,topic,seed,keyed,warmup.nextId,1,epoch,qualification);
            if(!metadata(producer.partitionsFor(topic)).equals(before))throw new IOException("metadata changed; unqualified history");
            producer.flush();journal.flush();
            receipt(out,"producer-complete","{\"profile\":"+quote(profile)+",\"total_records\":"+measured.nextId+",\"phases\":["+warmup.json+","+measured.json+"],\"independent_delivery_verification_required\":true,\"delivery_qualified\":false}");
        } catch(Exception error) {
            receipt(out,"failure","{\"qualification\":false,\"message\":"+quote(error.toString())+",\"cancelled_sends_may_have_delivered\":true}");
            throw error;
        } finally {
            producer.close(Duration.ofSeconds(qualification?30:120));
        }
    }
}
