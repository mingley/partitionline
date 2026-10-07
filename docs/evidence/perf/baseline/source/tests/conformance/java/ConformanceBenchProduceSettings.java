import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Independent Kafka client readback for the bounded producer-settings cohort. */
public final class ConformanceBenchProduceSettings {
    private static long mix(long value) {
        value += 0x9e3779b97f4a7c15L;
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
    private static byte[] payload(long seed, long id, int length, boolean seeded) {
        byte[] bytes = new byte[length];
        if (!seeded) { Arrays.fill(bytes, (byte) 'x'); return bytes; }
        long state = seed ^ (id * 0x9e3779b97f4a7c15L);
        for (int offset = 0; offset < length; offset += 8) {
            state = mix(state);
            byte[] block = ByteBuffer.allocate(8).putLong(state).array();
            System.arraycopy(block, 0, bytes, offset, Math.min(8, length - offset));
        }
        return bytes;
    }
    public static void main(String[] args) {
        if (args.length != 9) { throw new IllegalArgumentException("bootstrap topic warmup count partitions seed payload key-mode payload-mode"); }
        String bootstrap=args[0], topic=args[1];
        long warmup=Long.parseLong(args[2]), count=Long.parseLong(args[3]);
        int partitions=Integer.parseInt(args[4]), bytes=Integer.parseInt(args[6]);
        long seed=Long.decode(args[5]);
        boolean keys=args[7].equals("id"), seeded=args[8].equals("seeded");
        if (partitions < 1 || count < 1 || warmup < 0 || bytes < 1 || (!keys && !args[7].equals("none"))
                || (!seeded && !args[8].equals("constant-x"))) { throw new IllegalArgumentException("invalid audit profile"); }
        Properties properties=new Properties();
        properties.put("bootstrap.servers",bootstrap);
        properties.put("key.deserializer",ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer",ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit","false");
        properties.put("auto.offset.reset","earliest");
        properties.put("isolation.level","read_uncommitted");
        properties.put("client.id","selected-producer-settings-audit");
        properties.put("default.api.timeout.ms","10000");
        properties.put("max.poll.records","1000");
        long verified=0, warmupVerified=0, measuredVerified=0;
        Map<TopicPartition,Long> positions=new HashMap<>();
        try (KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(properties)) {
            List<TopicPartition> assignment=new ArrayList<>();
            for (int p=0;p<partitions;p++) { assignment.add(new TopicPartition(topic,p)); }
            consumer.assign(assignment);
            var beginning=consumer.beginningOffsets(assignment);
            var end=consumer.endOffsets(assignment);
            for (TopicPartition partition:assignment) {
                long expected=(warmup+partitions-1-partition.partition())/partitions
                    +(count+partitions-1-partition.partition())/partitions;
                if (beginning.get(partition)!=0 || end.get(partition)!=expected) {
                    throw new AssertionError("unexpected partition offset boundary");
                }
                positions.put(partition,0L);
                consumer.seek(partition,0);
            }
            long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();
            while (verified < warmup+count && System.nanoTime()<deadline) {
                for (var record:consumer.poll(Duration.ofMillis(200))) {
                    TopicPartition partition=new TopicPartition(record.topic(),record.partition());
                    long next=positions.get(partition);
                    if (record.offset()!=next) { throw new AssertionError("offset duplicate or gap"); }
                    long warmupPartition=(warmup+partitions-1-record.partition())/partitions;
                    boolean warming=next<warmupPartition;
                    long ordinal=warming ? next : next-warmupPartition;
                    long id=record.partition()+ordinal*partitions;
                    byte[] expectedKey=keys ? ByteBuffer.allocate(16).putLong(id).putLong(mix(seed^id)).array() : null;
                    if (!Arrays.equals(record.key(),expectedKey) || !Arrays.equals(record.value(),payload(seed,id,bytes,seeded))) {
                        throw new AssertionError("record key or payload differs from independent generator");
                    }
                    positions.put(partition,next+1); verified++;
                    if (warming) { warmupVerified++; } else { measuredVerified++; }
                }
            }
            if (warmupVerified!=warmup || measuredVerified!=count) { throw new AssertionError("missing record identities"); }
        }
        System.out.printf("{\"status\":\"pass\",\"verified\":%d,\"warmup\":%d,\"measured\":%d,\"partitions\":%d,\"consumer_closed\":true}%n",
            verified,warmupVerified,measuredVerified,partitions);
    }
}
