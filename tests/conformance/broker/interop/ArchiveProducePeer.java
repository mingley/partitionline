/* Ordinary public Kafka Producer witness for the installed broker archive. */
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class ArchiveProducePeer {
    private static final String TOPIC = "archive-orders";
    private static final int PARTITIONS = 2;
    private static final int RECORDS_PER_PARTITION = 12;
    private static final long TIMESTAMP = 1_700_000_000_000L;

    private ArchiveProducePeer() { }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("loopback port");

        var connection = new Properties();
        connection.setProperty("bootstrap.servers", "127.0.0.1:" + args[0]);
        connection.setProperty("security.protocol", "PLAINTEXT");
        connection.setProperty("client.id", "broker-archive-produce");
        connection.setProperty("request.timeout.ms", "5000");
        connection.setProperty("retries", "0");

        var adminProperties = new Properties();
        adminProperties.putAll(connection);
        adminProperties.setProperty("default.api.timeout.ms", "10000");
        try (var admin = Admin.create(adminProperties)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        }

        var producerProperties = new Properties();
        producerProperties.putAll(connection);
        producerProperties.setProperty("acks", "all");
        producerProperties.setProperty("enable.idempotence", "false");
        producerProperties.setProperty("compression.type", "none");
        producerProperties.setProperty("batch.size", "0");
        producerProperties.setProperty("linger.ms", "0");
        producerProperties.setProperty("max.in.flight.requests.per.connection", "1");
        producerProperties.setProperty("max.block.ms", "5000");
        producerProperties.setProperty("delivery.timeout.ms", "8000");
        producerProperties.setProperty("max.request.size", "65536");
        producerProperties.setProperty("buffer.memory", "1048576");
        try (var producer = new KafkaProducer<byte[], byte[]>(
                producerProperties, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int partition = 0; partition < PARTITIONS; partition++) {
                for (int ordinal = 0; ordinal < RECORDS_PER_PARTITION; ordinal++) {
                    // Match the existing independent CrossReadPeer receipt fixture.
                    String id = TOPIC + ":" + partition + ":" + ordinal;
                    byte[] key = ordinal % 3 == 0 ? null : utf8("key:" + id);
                    byte[] value = ordinal % 4 == 0 ? null
                        : ordinal % 4 == 1 ? new byte[0] : utf8("value:" + id);
                    List<Header> headers = List.of(
                        new RecordHeader("receipt", utf8(id)),
                        new RecordHeader("dup", utf8("a")),
                        new RecordHeader("dup", null));
                    long timestamp = TIMESTAMP + partition * 100L + ordinal;
                    var record = new ProducerRecord<>(TOPIC, partition, timestamp, key, value, headers);
                    var receipt = producer.send(record).get(10, TimeUnit.SECONDS);
                    if (!receipt.topic().equals(TOPIC) || receipt.partition() != partition
                            || !receipt.hasOffset() || receipt.offset() != ordinal) {
                        throw new AssertionError("unexpected Produce receipt for " + id + ": " + receipt);
                    }
                    System.out.printf("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d}%n",
                        receipt.topic(), receipt.partition(), receipt.offset());
                }
            }
        }
    }
}
