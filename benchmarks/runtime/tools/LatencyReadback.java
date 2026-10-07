import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Verify the fixed payload and contiguous offsets of the latency driver. */
public final class LatencyReadback {
    public static void main(String[] args) {
        if (args.length != 3) throw new IllegalArgumentException("bootstrap topic expected-count");
        long expected = Long.parseLong(args[2]);
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", args[0]);
        p.setProperty("enable.auto.commit", "false");
        p.setProperty("max.poll.records", "20000");
        p.setProperty("fetch.max.wait.ms", "100");
        p.setProperty("request.timeout.ms", "10000");
        p.setProperty("default.api.timeout.ms", "15000");
        p.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
        p.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
        var partition = new TopicPartition(args[1], 0);
        byte[] value = new byte[100];
        Arrays.fill(value, (byte) 'x');
        long verified = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.assign(List.of(partition));
            if (consumer.endOffsets(List.of(partition)).get(partition) != expected) {
                throw new AssertionError("end offset differs");
            }
            consumer.seek(partition, 0);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (verified < expected) {
                if (System.nanoTime() >= deadline) throw new AssertionError("readback deadline");
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (record.partition() != 0 || record.offset() != verified
                            || record.key() != null || record.headers().toArray().length != 0
                            || !Arrays.equals(record.value(), value)) {
                        throw new AssertionError("offset, key, headers or full payload differs");
                    }
                    verified++;
                    if (verified > expected) throw new AssertionError("unexpected record");
                }
            }
            if (consumer.position(partition) != expected) throw new AssertionError("position differs");
        }
        System.out.printf("{\"status\":\"pass\",\"verified\":%d,\"consumer_closed\":true,\"unique_ids_checked\":false,\"integrity_scope\":\"contiguous partition offsets and complete fixed payload\"}%n", verified);
    }
}
