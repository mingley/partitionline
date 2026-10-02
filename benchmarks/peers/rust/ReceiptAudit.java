// Independent Apache Kafka 3.9.1 consumer: emit actual bytes, never regenerate expectations.
import java.io.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public final class ReceiptAudit {
    static String hex(byte[] data) {
        if (data == null) return "null";
        return "\"" + HexFormat.of().formatHex(data) + "\"";
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("bootstrap topic isolation fences output timeout_ms");
        if (!org.apache.kafka.common.utils.AppInfoParser.getVersion().equals("3.9.1")
                || !org.apache.kafka.common.utils.AppInfoParser.getCommitId().equals("f745dfdcee2b9851"))
            throw new IllegalStateException("independent Kafka client runtime pin mismatch");
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", "independent-rust-peer-audit");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "none");
        props.put("isolation.level", args[2]);
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("default.api.timeout.ms", args[5]);
        // TLS/SASL credentials stay in caller-owned environment, absent from artifacts.
        String security = System.getenv().getOrDefault("SECURITY_PROTOCOL", "PLAINTEXT");
        props.put("security.protocol", security);
        if (!security.equals("PLAINTEXT")) throw new IllegalArgumentException("independent Java audit currently supports PLAINTEXT only");
        List<TopicPartition> partitions = new ArrayList<>();
        Map<TopicPartition, Long> starts = new HashMap<>(), ends = new HashMap<>();
        for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of(args[3]))) {
            String[] fields = line.split("\t");
            if (fields.length != 3) throw new IllegalArgumentException("invalid fence row");
            TopicPartition partition = new TopicPartition(args[1], Integer.parseInt(fields[0]));
            long start = Long.parseLong(fields[1]), end = Long.parseLong(fields[2]);
            if (start < 0 || end < start || ends.put(partition, end) != null) throw new IllegalArgumentException("invalid/duplicate fence");
            starts.put(partition, start); partitions.add(partition);
        }
        if (partitions.isEmpty()) throw new IllegalArgumentException("empty fences");
        long expected = ends.entrySet().stream().mapToLong(e -> e.getValue() - starts.get(e.getKey())).sum();
        long received = 0, deadline = System.nanoTime() + Long.parseLong(args[5]) * 1_000_000L;
        try (KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(props);
             BufferedWriter output = java.nio.file.Files.newBufferedWriter(java.nio.file.Path.of(args[4]), java.nio.file.StandardOpenOption.CREATE_NEW)) {
            consumer.assign(partitions);
            for (TopicPartition partition : partitions) consumer.seek(partition, starts.get(partition));
            while (received < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[],byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    if (record.offset() >= ends.get(partition)) continue;
                    if (record.offset() < starts.get(partition)) throw new IllegalStateException("receipt before fence");
                    output.write("{\"partition\":" + record.partition() + ",\"offset\":" + record.offset()
                        + ",\"key\":" + hex(record.key()) + ",\"value\":" + hex(record.value()) + "}\n");
                    received++;
                }
                for (TopicPartition partition : partitions)
                    if (consumer.position(partition) >= ends.get(partition)) consumer.pause(List.of(partition));
            }
            output.flush();
        }
        if (received != expected) throw new IllegalStateException("receipt count " + received + " != fenced offsets " + expected);
        System.out.println("independent Java receipts=" + received);
    }
}
