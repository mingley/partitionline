import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

/** Public SDK calls against the declared, independent seeded Fetch fixture. */
public final class NullBrokerPeer {
    private static final int COUNT = 512;
    private static long mix(long x) {
        x += 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        return x ^ (x >>> 31);
    }
    private static byte[][] record(long offset) {
        long hash = mix(0x5eed0001L * 0x9e3779b97f4a7c15L + offset);
        byte[] key = ByteBuffer.allocate(16).putInt(0).putLong(offset).putInt((int) hash).array();
        byte[] value = new byte[100];
        ByteBuffer.wrap(value).putLong(offset).putLong(hash);
        for (int at = 16; at < 100; at += 8) {
            hash = mix(hash);
            byte[] word = ByteBuffer.allocate(8).putLong(hash).array();
            System.arraycopy(word, 0, value, at, Math.min(8, 100 - at));
        }
        return new byte[][] {key, value};
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !AppInfoParser.getVersion().equals("4.3.1")) throw new IllegalArgumentException("pinned SDK required");
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", args[0]); p.setProperty("client.id", "nullbroker-qualification");
        p.setProperty("acks", "all"); p.setProperty("enable.idempotence", "false");
        p.setProperty("compression.type", "none"); p.setProperty("linger.ms", "0");
        p.setProperty("request.timeout.ms", "3000"); p.setProperty("delivery.timeout.ms", "4000");
        p.setProperty("max.block.ms", "4000");
        int acknowledged = 0;
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer())) {
            List<Future<RecordMetadata>> deliveries = new ArrayList<>();
            for (int i = 0; i < COUNT; i++) {
                byte[][] r = record(i);
                deliveries.add(producer.send(new ProducerRecord<>("nullbroker-peer", 0, 0L, r[0], r[1])));
            }
            for (int i = 0; i < COUNT; i++) {
                var m = deliveries.get(i).get();
                if (m.partition() != 0 || m.offset() != i) throw new IllegalStateException("delivery offset");
                acknowledged++;
            }
        }
        Properties c = new Properties();
        c.setProperty("bootstrap.servers", args[0]); c.setProperty("client.id", "nullbroker-qualification");
        c.setProperty("enable.auto.commit", "false"); c.setProperty("check.crcs", "true");
        c.setProperty("fetch.min.bytes", "1"); c.setProperty("fetch.max.wait.ms", "10");
        c.setProperty("request.timeout.ms", "3000"); c.setProperty("default.api.timeout.ms", "6000");
        int fetched = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(c, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition tp = new TopicPartition("nullbroker-peer", 0);
            consumer.assign(List.of(tp)); consumer.seek(tp, 0);
            long deadline = System.nanoTime() + Duration.ofSeconds(6).toNanos();
            while (fetched < COUNT && System.nanoTime() < deadline) {
                for (var r : consumer.poll(Duration.ofMillis(100))) {
                    byte[][] expected = record(fetched);
                    if (r.partition() != 0 || r.offset() != fetched || r.timestamp() != 0 ||
                        r.headers().toArray().length != 0 || !Arrays.equals(r.key(), expected[0]) ||
                        !Arrays.equals(r.value(), expected[1])) throw new IllegalStateException("fetch validation " + fetched);
                    fetched++;
                }
            }
        }
        if (fetched != COUNT) throw new IllegalStateException("fetch deadline");
        System.out.printf("{\"peer\":\"apache-java\",\"version\":\"4.3.1\",\"acknowledged\":%d,\"validated_fetch\":%d,\"validation_failures\":0,\"fetch_source\":\"seeded-independent-of-produce\"}%n", acknowledged, fetched);
    }
}
