import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

/** Finite, pipelined producer plus manual-assignment full-record audit. */
public final class BenchmarkPeer {
    private BenchmarkPeer() { }

    static long number(String name, long fallback, long low, long high) {
        String value = System.getenv(name);
        long result = value == null ? fallback : Long.decode(value);
        if (result < low || result > high) throw new IllegalArgumentException(name + " out of range");
        return result;
    }
    static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
    static long mix(long value) {
        value += 0x9e3779b97f4a7c15L;
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
    static byte[] key(long seed, long id) {
        return ByteBuffer.allocate(16).putLong(id).putLong(mix(seed ^ id)).array();
    }
    static byte[] value(long seed, long id, int length) {
        byte[] result = new byte[length];
        long state = seed ^ (id * 0x9e3779b97f4a7c15L);
        for (int start = 0; start < length; start += 8) {
            state = mix(state);
            for (int i = 0; i < Math.min(8, length - start); i++)
                result[start + i] = (byte)(state >>> (56 - 8 * i));
        }
        return result;
    }
    static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }
    static Map<String, Object> object(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String)pairs[i], pairs[i + 1]);
        return result;
    }
    static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof Map<?, ?> map) {
            List<String> fields = new ArrayList<>();
            map.forEach((key, item) -> fields.add(json(key.toString()) + ":" + json(item)));
            return "{" + String.join(",", fields) + "}";
        }
        if (value instanceof Iterable<?> items) {
            List<String> fields = new ArrayList<>();
            items.forEach(item -> fields.add(json(item)));
            return "[" + String.join(",", fields) + "]";
        }
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        String text = value.toString();
        var result = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\\' || c == '"') result.append('\\').append(c);
            else if (c < 32) result.append(String.format("\\u%04x", (int)c));
            else result.append(c);
        }
        return result.append('"').toString();
    }

    static final class Settings {
        final String bootstrap = env("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
        final String topic = env("KAFKA_TOPIC", "plbench");
        final int count = (int)number("COUNT", 100000, 1, 10000000);
        final int warmup = (int)number("WARMUP", 10000, 0, 1000000);
        final int partitions = (int)number("PARTITIONS", 6, 1, 10000);
        final int bytes = (int)number("PAYLOAD_BYTES", 100, 0, 10000000);
        final int acks = (int)number("ACKS", 1, -1, 1);
        final int inFlight = (int)number("MAX_IN_FLIGHT", 5, 1, 1000000);
        final int queueMessages = (int)number("QUEUE_MESSAGES", 1000000, 1, 1000000);
        final long runMs = number("RUN_TIMEOUT_MS", 120000, 1, 3600000);
        final long consumeMs = number("CONSUME_TIMEOUT_MS", 30000, 1, 3600000);
        final long flushMs = number("FLUSH_TIMEOUT_MS", 35000, 1, 3600000);
        final long latencySamples = number("LATENCY_SAMPLES", 1000000000, 0, 1000000000);
        final long seed;
        final Properties producer = new Properties();
        final Properties consumer = new Properties();
        final Map<String, Object> effective;

        Settings() {
            String seedText = env("RECORD_SEED", "0x5eed0001");
            seed = Long.parseUnsignedLong(seedText.replaceFirst("^0[xX]", ""),
                                         seedText.startsWith("0x") || seedText.startsWith("0X") ? 16 : 10);
            if (!topic.matches("[a-zA-Z0-9._-]{1,249}") || topic.equals(".") || topic.equals(".."))
                throw new IllegalArgumentException("Invalid topic");
            for (String name : List.of("SASL_MECHANISM", "SASL_USERNAME", "SASL_PASSWORD", "TLS_CA_PEM",
                    "TLS_CLIENT_CERT_PEM", "TLS_CLIENT_KEY_PEM", "TLS_SERVER_NAME", "TRANSACTIONAL_ID",
                    "GROUP_ID", "OPEN_LOOP_RATE", "BATCH_RECORDS")) {
                if (!env(name, "").isEmpty()) throw new IllegalArgumentException("Unsupported setting: " + name);
            }
            if (!env("SECURITY_PROTOCOL", "PLAINTEXT").equals("PLAINTEXT")
                    || !env("KEY_MODE", "id").equals("id")
                    || !List.of("seeded", "constant-x").contains(env("PAYLOAD_MODE", "seeded")))
                throw new IllegalArgumentException("Only PLAINTEXT, KEY_MODE=id and seeded|constant-x supported");
            String idemText = env("IDEMPOTENT", "0").toLowerCase(java.util.Locale.ROOT);
            if (!List.of("0", "1", "true", "false").contains(idemText))
                throw new IllegalArgumentException("Invalid IDEMPOTENT");
            boolean idem = idemText.equals("1") || idemText.equals("true");
            if (idem && (acks != -1 || inFlight > 5))
                throw new IllegalArgumentException("Idempotence requires explicit ACKS=-1 and MAX_IN_FLIGHT<=5");
            String codec = env("COMPRESSION", "none");
            if (!List.of("none", "gzip", "snappy", "lz4", "zstd").contains(codec))
                throw new IllegalArgumentException("Invalid compression");
            String isolation = env("ISOLATION", "read_uncommitted");
            if (!List.of("read_uncommitted", "read_committed").contains(isolation))
                throw new IllegalArgumentException("Invalid isolation");
            long linger = number("LINGER_MS", 5, 0, 3600000);
            long delivery = number("DELIVERY_TIMEOUT_MS", 30000, 2, 3600000);
            if (delivery <= linger) throw new IllegalArgumentException("DELIVERY_TIMEOUT_MS must exceed LINGER_MS");
            producer.put("bootstrap.servers", bootstrap);
            producer.put("client.id", "partitionline-java-benchmark");
            producer.put("key.serializer", ByteArraySerializer.class.getName());
            producer.put("value.serializer", ByteArraySerializer.class.getName());
            producer.put("acks", Integer.toString(acks));
            producer.put("enable.idempotence", Boolean.toString(idem));
            producer.put("retries", Integer.toString(Integer.MAX_VALUE));
            producer.put("max.in.flight.requests.per.connection", Integer.toString(inFlight));
            producer.put("linger.ms", Long.toString(linger));
            producer.put("batch.size", Long.toString(number("BATCH_BYTES", 1000000, 1, Integer.MAX_VALUE)));
            producer.put("buffer.memory", Long.toString(number("QUEUE_KBYTES", 32768, 1, 1048576) * 1024));
            producer.put("compression.type", codec);
            if (codec.equals("zstd")) producer.put("compression.zstd.level", "3");
            producer.put("delivery.timeout.ms", Long.toString(delivery));
            producer.put("request.timeout.ms", Long.toString(Math.min(10000, delivery - linger)));
            producer.put("max.block.ms", Long.toString(runMs));
            producer.put("security.protocol", "PLAINTEXT");
            consumer.put("bootstrap.servers", bootstrap);
            consumer.put("client.id", "partitionline-java-audit");
            consumer.put("key.deserializer", ByteArrayDeserializer.class.getName());
            consumer.put("value.deserializer", ByteArrayDeserializer.class.getName());
            consumer.put("enable.auto.commit", "false");
            consumer.put("allow.auto.create.topics", "false");
            consumer.put("auto.offset.reset", "none");
            consumer.put("isolation.level", isolation);
            consumer.put("security.protocol", "PLAINTEXT");
            Map<String, Object> p = selected(new ProducerConfig(producer).values(), List.of(
                "acks", "enable.idempotence", "retries", "max.in.flight.requests.per.connection",
                "linger.ms", "batch.size", "buffer.memory", "compression.type", "compression.zstd.level",
                "delivery.timeout.ms", "request.timeout.ms", "max.block.ms", "security.protocol"));
            Map<String, Object> c = selected(new ConsumerConfig(consumer).values(), List.of(
                "enable.auto.commit", "allow.auto.create.topics", "auto.offset.reset", "isolation.level",
                "fetch.min.bytes", "fetch.max.bytes", "max.partition.fetch.bytes", "max.poll.records",
                "fetch.max.wait.ms", "security.protocol"));
            effective = object("producer", p, "consumer", c, "queue_messages", queueMessages,
                "pipeline", "bounded asynchronous send with completion callbacks",
                "batch_records", "unsupported: Java byte-based batching only",
                "sdk_version", AppInfoParser.getVersion(), "java_version", System.getProperty("java.version"));
            if (!AppInfoParser.getVersion().equals("4.3.1")) throw new IllegalArgumentException("Wrong Apache SDK");
            if (codec.equals("zstd")) effective.put("zstd_backend", object("name", "libzstd",
                "version", "1.5.6", "jni_build", com.github.luben.zstd.util.ZstdVersion.VERSION,
                "version_source", "Pinned JNI jar's bundled libzstd build"));
        }
        static Map<String, Object> selected(Map<String, ?> values, List<String> keys) {
            Map<String, Object> result = new TreeMap<>();
            for (String key : keys) result.put(key, values.get(key));
            return result;
        }
        byte[] payload(long id) {
            if (env("PAYLOAD_MODE", "seeded").equals("seeded")) return value(seed, id, bytes);
            byte[] result = new byte[bytes]; Arrays.fill(result, (byte)'x'); return result;
        }
    }

    static final class Phase {
        final AtomicLong offered = new AtomicLong(), accepted = new AtomicLong(), acknowledged = new AtomicLong();
        final AtomicLong rejected = new AtomicLong(), timedOut = new AtomicLong(), unknown = new AtomicLong();
        final AtomicLong failures = new AtomicLong(), completed = new AtomicLong();
        final List<Map<String, Object>> errors = Collections.synchronizedList(new ArrayList<>());
        volatile double elapsed = 0.000000001;
        void error(String reason) {
            synchronized (errors) {
                if (errors.size() < 16) errors.add(object("code", "JAVA_PEER", "name", reason, "message", reason, "count", 1));
            }
        }
        Map<String, Object> result() {
            long unresolved = Math.max(0, accepted.get() - acknowledged.get() - timedOut.get() - unknown.get());
            return object("offered", offered.get(), "accepted", accepted.get(), "acknowledged", acknowledged.get(),
                "rejected", rejected.get(), "timed_out", timedOut.get(), "unknown", unknown.get() + unresolved,
                "callback_failures", failures.get(), "queue_full_retries", 0, "elapsed_s", elapsed,
                "errors", new ArrayList<>(errors));
        }
    }

    static void produce(Settings s, KafkaProducer<byte[], byte[]> producer, int count, boolean sample,
                        BufferedWriter samples, Phase phase) throws Exception {
        long begin = System.nanoTime();
        long deadline = begin + TimeUnit.MILLISECONDS.toNanos(s.runMs);
        Semaphore permits = new Semaphore(s.queueMessages);
        try {
            for (long id = 0; id < count; id++) {
                long started = System.nanoTime();
                phase.offered.incrementAndGet();
                long remaining = deadline - started;
                if (remaining <= 0 || !permits.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
                    phase.rejected.incrementAndGet(); phase.error("Admission deadline"); break;
                }
                long recordId = id;
                try {
                    var record = new ProducerRecord<>(s.topic, (int)(id % s.partitions), key(s.seed, id), s.payload(id));
                    producer.send(record, (metadata, exception) -> {
                        try {
                            if (exception == null && s.acks != 0) {
                                phase.acknowledged.incrementAndGet();
                                if (sample && recordId < s.latencySamples) synchronized (samples) {
                                    samples.write(recordId + "," + (System.nanoTime() - started) / 1000.0 + "\n");
                                }
                            } else if (exception == null) phase.unknown.incrementAndGet();
                            else {
                                phase.failures.incrementAndGet();
                                if (exception instanceof org.apache.kafka.common.errors.TimeoutException)
                                    phase.timedOut.incrementAndGet();
                                else phase.unknown.incrementAndGet();
                                phase.error(exception.getClass().getSimpleName());
                            }
                        } catch (java.io.IOException failure) { phase.error("Latency artifact write failed"); }
                        finally { phase.completed.incrementAndGet(); permits.release(); }
                    });
                    phase.accepted.incrementAndGet();
                } catch (RuntimeException failure) {
                    permits.release(); phase.rejected.incrementAndGet();
                    phase.error(failure.getClass().getSimpleName()); break;
                }
            }
            long flushDeadline = Math.min(deadline, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(s.flushMs));
            while (phase.completed.get() < phase.accepted.get() && System.nanoTime() < flushDeadline)
                Thread.sleep(1);
            if (phase.completed.get() < phase.accepted.get()) phase.error("Completion deadline");
            if (phase.acknowledged.get() != count && s.acks != 0) phase.error("Not all records acknowledged");
        } finally { phase.elapsed = Math.max(1, System.nanoTime() - begin) / 1e9; }
    }

    static Map<TopicPartition, Long> offsets(Admin admin, List<TopicPartition> partitions, long timeout) throws Exception {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) request.put(partition, OffsetSpec.latest());
        var reply = admin.listOffsets(request, new ListOffsetsOptions().timeoutMs((int)timeout))
            .all().get(timeout, TimeUnit.MILLISECONDS);
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) result.put(partition, reply.get(partition).offset());
        return result;
    }

    static Map<String, Object> audit(Settings s, List<TopicPartition> partitions,
                                   Map<TopicPartition, Long> start, Map<TopicPartition, Long> end) {
        var seen = new BitSet(s.count);
        long duplicates = 0, bad = 0;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(s.consumeMs);
        var consumer = new KafkaConsumer<byte[], byte[]>(s.consumer);
        try {
            consumer.assign(partitions);
            for (TopicPartition partition : partitions) consumer.seek(partition, start.get(partition));
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(Math.min(100,
                    Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())))));
                for (var record : records) {
                    var partition = new TopicPartition(record.topic(), record.partition());
                    if (record.offset() < start.get(partition) || record.offset() >= end.get(partition)) continue;
                    if (record.key() == null || record.key().length != 16 || record.value() == null) { bad++; continue; }
                    long id = ByteBuffer.wrap(record.key()).getLong();
                    if (id < 0 || id >= s.count || id % s.partitions != record.partition()
                            || !Arrays.equals(record.key(), key(s.seed, id))
                            || !Arrays.equals(record.value(), s.payload(id)) || record.headers().iterator().hasNext()) {
                        bad++; continue;
                    }
                    if (seen.get((int)id)) duplicates++;
                    else seen.set((int)id);
                }
                // Check positions too: duplicates later in the timed range cannot be skipped.
                boolean finished = true;
                for (TopicPartition partition : partitions) {
                    if (consumer.position(partition, Duration.ofMillis(Math.max(1,
                            TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())))) < end.get(partition))
                        finished = false;
                }
                if (finished) break;
            }
        } finally { consumer.close(CloseOptions.timeout(Duration.ofSeconds(5))); }
        return object("verified_ids", seen.cardinality(), "duplicate_ids", duplicates, "bad_records", bad);
    }

    static int roundtrip(Settings s, boolean consume) throws Exception {
        var raw = object("version", AppInfoParser.getVersion(), "effective_config", s.effective,
            "cluster_id", "", "broker_nodes", 0, "verification", object("verified_ids", 0, "duplicate_ids", 0, "bad_records", 0),
            "high_watermarks", object("queried", false, "total_offset_delta", 0, "partitions", List.of()),
            "durability", object("verified", false, "replication_factor", 1, "min_insync_replicas", 1));
        var warm = new Phase(); var timed = new Phase();
        int status = 0;
        Properties adminSettings = new Properties();
        adminSettings.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s.bootstrap);
        adminSettings.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        adminSettings.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Long.toString(s.consumeMs));
        Path rawPath = Path.of(env("JAVA_PEER_RAW", "java-peer.raw.json"));
        Path samplePath = Path.of(env("JAVA_PEER_SAMPLES", "java-peer.samples.csv"));
        if (Files.exists(rawPath) || Files.exists(samplePath))
            throw new IllegalArgumentException("Result artifacts already exist");
        try (var samples = Files.newBufferedWriter(samplePath, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
            samples.write("record_id,latency_us\n");
            var admin = Admin.create(adminSettings);
            try {
                raw.put("cluster_id", admin.describeCluster().clusterId().get(s.consumeMs, TimeUnit.MILLISECONDS));
                raw.put("broker_nodes", admin.describeCluster().nodes().get(s.consumeMs, TimeUnit.MILLISECONDS).size());
                var metadata = admin.describeTopics(List.of(s.topic)).allTopicNames().get(s.consumeMs, TimeUnit.MILLISECONDS).get(s.topic);
                if (metadata.partitions().size() != s.partitions) throw new IllegalArgumentException("PARTITIONS differs from broker");
                int replicas = metadata.partitions().getFirst().replicas().size();
                List<TopicPartition> partitions = new ArrayList<>();
                for (var partition : metadata.partitions()) {
                    if (partition.replicas().size() != replicas) throw new IllegalArgumentException("Unequal topic replication factors");
                    partitions.add(new TopicPartition(s.topic, partition.partition()));
                }
                var resource = new ConfigResource(ConfigResource.Type.TOPIC, s.topic);
                Config config = admin.describeConfigs(List.of(resource)).all().get(s.consumeMs, TimeUnit.MILLISECONDS).get(resource);
                int minIsr = Integer.parseInt(config.get("min.insync.replicas").value());
                if (replicas < 1 || minIsr < 1 || minIsr > replicas) throw new IllegalArgumentException("Invalid topic durability");
                raw.put("durability", object("verified", true, "replication_factor", replicas, "min_insync_replicas", minIsr));
                var initial = offsets(admin, partitions, s.consumeMs);
                if (initial.values().stream().anyMatch(offset -> offset != 0)) throw new IllegalArgumentException("A fresh empty isolated topic is required");
                var producer = new KafkaProducer<byte[], byte[]>(s.producer);
                try {
                    produce(s, producer, s.warmup, false, samples, warm);
                    if (!warm.errors.isEmpty()) throw new IllegalStateException("Warmup failed");
                    var start = offsets(admin, partitions, s.consumeMs);
                    if (start.values().stream().mapToLong(Long::longValue).sum() != s.warmup)
                        throw new IllegalStateException("Warmup offsets differ from completions");
                    produce(s, producer, s.count, true, samples, timed);
                    if (!timed.errors.isEmpty()) throw new IllegalStateException("Timed delivery failed");
                    var end = offsets(admin, partitions, s.consumeMs);
                    List<Map<String, Object>> rows = new ArrayList<>(); long total = 0;
                    for (TopicPartition partition : partitions) {
                        long delta = end.get(partition) - start.get(partition); total += delta;
                        rows.add(object("partition", partition.partition(), "start_offset", start.get(partition),
                            "end_offset", end.get(partition), "offset_delta", delta));
                    }
                    raw.put("high_watermarks", object("queried", true, "total_offset_delta", total, "partitions", rows));
                    if (total != timed.acknowledged.get()) throw new IllegalStateException("Offset delta differs from acknowledgments");
                    if (consume) {
                        Map<String, Object> verification = audit(s, partitions, start, end);
                        raw.put("verification", verification);
                        if (((Number)verification.get("verified_ids")).intValue() != s.count
                                || ((Number)verification.get("duplicate_ids")).longValue() != 0
                                || ((Number)verification.get("bad_records")).longValue() != 0)
                            throw new IllegalStateException("Full-record verification failed");
                    }
                } finally { producer.close(Duration.ofSeconds(5)); }
            } catch (Exception failure) {
                status = 1; timed.error(failure.getClass().getSimpleName() + ": " + failure.getMessage());
                System.err.println("Java peer failed: " + failure);
            } finally { admin.close(Duration.ofSeconds(5)); }
        } finally {
            raw.put("warmup", warm.result()); raw.put("timed", timed.result());
            // Host process measurements are supplied by the owning Python parent.
            raw.put("resources", object("user_cpu_seconds", 0, "system_cpu_seconds", 0,
                "peak_rss_bytes", 0, "threads_count", ManagementFactory.getThreadMXBean().getPeakThreadCount()));
            long liveClientThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.isAlive() && thread.getName().startsWith("kafka-")).count();
            raw.put("live_kafka_threads", liveClientThreads);
            raw.put("clients_closed", liveClientThreads == 0);
            if (liveClientThreads != 0) status = 1;
            Files.writeString(rawPath, json(raw) + "\n", StandardOpenOption.CREATE_NEW);
        }
        return status;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Expected emit-config|vectors|produce|roundtrip");
        var settings = new Settings();
        switch (args[0]) {
            case "emit-config" -> System.out.println(json(settings.effective));
            case "vectors" -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (long id : new long[]{0, 1, 255, 65536}) for (int length : new int[]{0, 1, 7, 8, 9, 100})
                    rows.add(object("id", id, "bytes", length, "key", hex(key(settings.seed, id)),
                        "value", hex(value(settings.seed, id, length))));
                System.out.println(json(rows));
            }
            case "produce" -> System.exit(roundtrip(settings, false));
            case "roundtrip" -> System.exit(roundtrip(settings, true));
            default -> throw new IllegalArgumentException("Unsupported command: " + args[0]);
        }
    }
}
