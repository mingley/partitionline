/* Public Apache OAuth client peer; no token, credential, or exception-message output. */
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

public final class OAuthMetadataPeer {
    private static final long OPERATION_MS = 8000;
    private static final Set<String> CONFIG_KEYS = Set.of("release", "bootstrap", "issuer", "token_url",
        "client_id", "client_secret_file", "ca_pem", "topic", "scope", "max_runtime_seconds");
    private static final Set<String> METRICS = Set.of("successful-authentication-total",
        "failed-authentication-total", "connection-creation-total", "connection-close-total",
        "request-total", "response-total");
    private static final AtomicLong CONFIGURED = new AtomicLong();
    private static final AtomicLong TOKEN_CALLBACKS = new AtomicLong();
    private static final AtomicLong TOKEN_SUCCESS = new AtomicLong();
    private static final AtomicLong EXTENSION_CALLBACKS = new AtomicLong();
    private static final AtomicLong CALLBACK_FAILURES = new AtomicLong();
    private static final AtomicLong CLOSED = new AtomicLong();

    /* All token acquisition/validation and SASL callback behavior remains in Apache's handler. */
    public static final class ObservedLoginCallback extends OAuthBearerLoginCallbackHandler {
        @Override
        public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> entries) {
            super.configure(configs, mechanism, entries);
            CONFIGURED.incrementAndGet();
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuthBearerTokenCallback) TOKEN_CALLBACKS.incrementAndGet();
                if (callback instanceof SaslExtensionsCallback) EXTENSION_CALLBACKS.incrementAndGet();
            }
            try {
                super.handle(callbacks);
                for (Callback callback : callbacks) {
                    if (callback instanceof OAuthBearerTokenCallback token && token.token() != null)
                        TOKEN_SUCCESS.incrementAndGet();
                }
            } catch (IOException | UnsupportedCallbackException | RuntimeException error) {
                CALLBACK_FAILURES.incrementAndGet();
                throw error;
            }
        }

        @Override
        public void close() {
            try { super.close(); } finally { CLOSED.incrementAndGet(); }
        }
    }

    private OAuthMetadataPeer() { }

    private static String quoted(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }

    private static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) {
            List<String> fields = new ArrayList<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) fields.add(quoted(entry.getKey().toString()) + ":" + json(entry.getValue()));
            return "{" + String.join(",", fields) + "}";
        }
        if (value instanceof Collection<?> collection) {
            List<String> fields = new ArrayList<>();
            for (Object item : collection) fields.add(json(item));
            return "[" + String.join(",", fields) + "]";
        }
        return quoted(value.toString());
    }

    private static void emit(Map<String, Object> event) {
        event.put("callback", Map.of("configured", CONFIGURED.get(), "token_calls", TOKEN_CALLBACKS.get(),
            "token_success", TOKEN_SUCCESS.get(), "extension_calls", EXTENSION_CALLBACKS.get(),
            "failures", CALLBACK_FAILURES.get(), "closed", CLOSED.get()));
        System.out.println(json(event));
        System.out.flush();
    }

    private static byte[] boundedFile(Path path, long limit, boolean secret) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > limit)
            throw new IllegalArgumentException("Invalid bounded input file");
        if (secret && !Files.getPosixFilePermissions(path).equals(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)))
            throw new IllegalArgumentException("Secret file permission mode must be 0600");
        byte[] data = Files.readAllBytes(path);
        if (data.length > limit) throw new IllegalArgumentException("Input changed above bound");
        return data;
    }

    private static String required(Properties config, String key) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank() || value.length() > 2048) throw new IllegalArgumentException("Missing bounded configuration");
        return value;
    }

    private static String identifier(Properties config, String key) {
        String value = required(config, key);
        if (!value.matches("[A-Za-z0-9._-]{1,96}")) throw new IllegalArgumentException("Invalid synthetic identifier");
        return value;
    }

    private static Path trustStore(Path caPem, Path directory) throws Exception {
        byte[] ca = boundedFile(caPem, 16384, false);
        Collection<? extends Certificate> certificates;
        try (InputStream stream = new java.io.ByteArrayInputStream(ca)) {
            certificates = CertificateFactory.getInstance("X.509").generateCertificates(stream);
        }
        if (certificates.isEmpty() || certificates.size() > 8) throw new IllegalArgumentException("Invalid public CA count");
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        int index = 0;
        for (Certificate certificate : certificates) store.setCertificateEntry("public-ca-" + index++, certificate);
        Path path = Files.createTempFile(directory, "java-public-ca-", ".p12",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        try (OutputStream stream = Files.newOutputStream(path)) { store.store(stream, "public-fixture-ca".toCharArray()); }
        return path;
    }

    private static Map<String, Object> clientConfig(Properties config, Path publicTrustStore) throws IOException {
        String secret = new String(boundedFile(Path.of(required(config, "client_secret_file")), 256, true), StandardCharsets.UTF_8).strip();
        if (!secret.matches("[A-Za-z0-9_-]{16,256}")) throw new IllegalArgumentException("Invalid synthetic secret format");
        String tokenUrl = required(config, "token_url");
        java.net.URI uri = java.net.URI.create(tokenUrl);
        if (!"https".equals(uri.getScheme()) || !"localhost".equals(uri.getHost()) || uri.getUserInfo() != null
            || uri.getFragment() != null || uri.getQuery() != null || uri.getPort() < 1)
            throw new IllegalArgumentException("HTTPS loopback token endpoint required");
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", tokenUrl);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("bootstrap.servers", required(config, "bootstrap"));
        props.put("client.id", "oauth-public-" + identifier(config, "release"));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", ObservedLoginCallback.class);
        props.put("sasl.oauthbearer.jwt.retriever.class", "org.apache.kafka.common.security.oauthbearer.ClientCredentialsJwtRetriever");
        props.put("sasl.oauthbearer.client.credentials.client.id", identifier(config, "client_id"));
        props.put("sasl.oauthbearer.client.credentials.client.secret", secret);
        props.put("sasl.oauthbearer.scope", identifier(config, "scope"));
        props.put("sasl.oauthbearer.token.endpoint.url", tokenUrl);
        props.put("sasl.login.connect.timeout.ms", 2000);
        props.put("sasl.login.read.timeout.ms", 2000);
        props.put("sasl.login.retry.backoff.ms", 100);
        props.put("sasl.login.retry.backoff.max.ms", 1000);
        props.put("sasl.login.refresh.buffer.seconds", (short) 0);
        props.put("sasl.login.refresh.min.period.seconds", (short) 1);
        String jaas = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
            + "ssl.truststore.type=\"PKCS12\" ssl.truststore.location=" + quoted(publicTrustStore.toString())
            + " ssl.truststore.password=\"public-fixture-ca\";";
        props.put("sasl.jaas.config", jaas);
        props.put("ssl.truststore.type", "PKCS12");
        props.put("ssl.truststore.location", publicTrustStore.toString());
        props.put("ssl.truststore.password", "public-fixture-ca");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("request.timeout.ms", (int) OPERATION_MS);
        props.put("default.api.timeout.ms", (int) OPERATION_MS);
        props.put("reconnect.backoff.ms", 100);
        props.put("reconnect.backoff.max.ms", 1000);
        props.put("connections.max.idle.ms", 2000);
        return props;
    }

    private static Map<String, Object> metrics(Map<MetricName, ? extends Metric> all) {
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<MetricName, ? extends Metric> entry : all.entrySet()) {
            String name = entry.getKey().name();
            Object value = entry.getValue().metricValue();
            if (METRICS.contains(name) && value instanceof Number number && Double.isFinite(number.doubleValue()))
                result.put(entry.getKey().group() + ":" + name, number.doubleValue());
        }
        return result;
    }

    private static List<String> errorTypes(Throwable error) {
        List<String> names = new ArrayList<>();
        for (int index = 0; error != null && index < 8; index++, error = error.getCause()) names.add(error.getClass().getName());
        return names;
    }

    private static final class Clients implements AutoCloseable {
        private final Admin admin;
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final Map<String, Object> config;
        private KafkaProducer<byte[], byte[]> producer;

        Clients(Map<String, Object> config) {
            this.config = config;
            admin = Admin.create(config);
            Map<String, Object> props = new LinkedHashMap<>(config);
            props.put("enable.auto.commit", false);
            props.put("allow.auto.create.topics", false);
            props.put("fetch.max.bytes", 4096); props.put("max.partition.fetch.bytes", 4096);
            props.put("max.poll.records", 128);
            try { consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()); }
            catch (RuntimeException error) { admin.close(Duration.ofSeconds(2)); throw error; }
        }

        void write(String phase, String topic, String release) {
            Map<String, Object> event = new LinkedHashMap<>(); event.put("event", "produce"); event.put("phase", phase);
            try {
                if (producer == null) {
                    Map<String, Object> props = new LinkedHashMap<>(config);
                    props.put("client.id", "oauth-public-" + release + "-producer");
                    props.put("enable.idempotence", false); props.put("acks", "all");
                    props.put("compression.type", "none"); props.put("linger.ms", 0); props.put("batch.size", 128);
                    props.put("max.block.ms", OPERATION_MS); props.put("delivery.timeout.ms", (int) OPERATION_MS + 1000);
                    producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
                }
                byte[] key = ("oauth-" + release + "-" + phase).getBytes(StandardCharsets.UTF_8);
                byte[] value = ("public-java-" + release).getBytes(StandardCharsets.UTF_8);
                List<Header> headers = List.of(new RecordHeader("peer", release.getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader("d", null), new RecordHeader("d", new byte[0]));
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, 1700000000123L, key, value, headers);
                RecordMetadata receipt = producer.send(record).get(OPERATION_MS, TimeUnit.MILLISECONDS);
                event.put("passed", true); event.put("topic", receipt.topic()); event.put("partition", receipt.partition());
                event.put("offset", receipt.offset()); event.put("timestamp", receipt.timestamp());
                event.put("key_hex", HexFormat.of().formatHex(key)); event.put("value_hex", HexFormat.of().formatHex(value));
                event.put("public_operation", "Producer.send");
            } catch (Exception error) { event.put("passed", false); event.put("error_types", errorTypes(error)); }
            if (producer != null) event.put("producer_metrics", metrics(producer.metrics()));
            emit(event);
        }

        void read(String phase, String topic) {
            Map<String, Object> event = new LinkedHashMap<>(); event.put("event", "fetch"); event.put("phase", phase);
            try {
                TopicPartition partition = new TopicPartition(topic, 0);
                consumer.assign(List.of(partition));
                long start = consumer.beginningOffsets(List.of(partition), Duration.ofMillis(OPERATION_MS)).get(partition);
                long end = consumer.endOffsets(List.of(partition), Duration.ofMillis(OPERATION_MS)).get(partition);
                if (start < 0 || end < start || end - start > 128) throw new IllegalStateException("Bounded public history required");
                consumer.seek(partition, start); List<Object> history = new ArrayList<>();
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(OPERATION_MS);
                while (consumer.position(partition, Duration.ofMillis(OPERATION_MS)) < end && System.nanoTime() < deadline) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        if (history.size() >= 128 || record.offset() >= end) throw new IllegalStateException("History changed during bounded read");
                        List<Object> headers = new ArrayList<>();
                        for (Header header : record.headers()) {
                            if (headers.size() >= 8 || header.key().length() > 64 || (header.value() != null && header.value().length > 256))
                                throw new IllegalStateException("Bounded public headers required");
                            Map<String, Object> item = new LinkedHashMap<>(); item.put("name", header.key());
                            item.put("value_hex", header.value() == null ? null : HexFormat.of().formatHex(header.value())); headers.add(item);
                        }
                        if ((record.key() != null && record.key().length > 256) || (record.value() != null && record.value().length > 256))
                            throw new IllegalStateException("Bounded public record required");
                        Map<String, Object> row = new LinkedHashMap<>(); row.put("offset", record.offset());
                        row.put("timestamp", record.timestamp()); row.put("timestamp_type", record.timestampType().name());
                        row.put("key_hex", record.key() == null ? null : HexFormat.of().formatHex(record.key()));
                        row.put("value_hex", record.value() == null ? null : HexFormat.of().formatHex(record.value()));
                        row.put("headers", headers); history.add(row);
                    }
                }
                if (consumer.position(partition, Duration.ofMillis(OPERATION_MS)) != end || history.size() != end - start)
                    throw new IllegalStateException("Public ordinary history incomplete");
                event.put("passed", true); event.put("log_start", start); event.put("log_end", end);
                event.put("records", history); event.put("public_operations", List.of("Consumer.assign", "Consumer.seek", "Consumer.poll", "Consumer.beginningOffsets", "Consumer.endOffsets"));
            } catch (Exception error) { event.put("passed", false); event.put("error_types", errorTypes(error)); }
            event.put("consumer_metrics", metrics(consumer.metrics())); emit(event);
        }

        void query(String phase, String topic) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("event", "query"); event.put("phase", phase);
            try {
                List<PartitionInfo> partitions = consumer.partitionsFor(topic, Duration.ofMillis(OPERATION_MS));
                Map<String, List<PartitionInfo>> topics = consumer.listTopics(Duration.ofMillis(OPERATION_MS));
                Set<String> listed = admin.listTopics(new ListTopicsOptions().listInternal(true).timeoutMs((int) OPERATION_MS))
                    .names().get(OPERATION_MS, TimeUnit.MILLISECONDS);
                TopicDescription described = admin.describeTopics(List.of(topic), new DescribeTopicsOptions().timeoutMs((int) OPERATION_MS))
                    .allTopicNames().get(OPERATION_MS, TimeUnit.MILLISECONDS).get(topic);
                if (partitions.size() != 1 || !topics.containsKey(topic) || !listed.contains(topic)
                    || described == null || described.partitions().size() != 1)
                    throw new IllegalStateException("Public metadata result mismatch");
                event.put("passed", true); event.put("topic", topic); event.put("partition_count", partitions.size());
                event.put("consumer_topic_count", topics.size()); event.put("admin_topic_count", listed.size());
                event.put("public_operations", List.of("Consumer.partitionsFor", "Consumer.listTopics", "Admin.listTopics", "Admin.describeTopics"));
            } catch (Exception error) { event.put("passed", false); event.put("error_types", errorTypes(error)); }
            event.put("admin_metrics", metrics(admin.metrics())); event.put("consumer_metrics", metrics(consumer.metrics()));
            emit(event);
        }

        @Override public void close() {
            if (producer != null) producer.close(Duration.ofSeconds(2));
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(2))); admin.close(Duration.ofSeconds(2));
        }
    }

    private static void run(Path configPath) throws Exception {
        Properties config = new Properties();
        byte[] bytes = boundedFile(configPath, 8192, false);
        try (InputStream stream = new java.io.ByteArrayInputStream(bytes)) { config.load(stream); }
        if (!CONFIG_KEYS.containsAll(config.stringPropertyNames())) throw new IllegalArgumentException("Unknown configuration");
        String release = identifier(config, "release");
        if (!Set.of("4.1.2", "4.2.1", "4.3.1").contains(release)) throw new IllegalArgumentException("Unknown pinned SDK");
        String topic = identifier(config, "topic");
        long seconds = Long.parseLong(required(config, "max_runtime_seconds"));
        if (seconds < 1 || seconds > 180) throw new IllegalArgumentException("Runtime bound must be 1..180 seconds");
        Path publicTrustStore = trustStore(Path.of(required(config, "ca_pem")), configPath.toAbsolutePath().getParent());
        Clients clients = null;
        try {
            Map<String, Object> props = clientConfig(config, publicTrustStore);
            clients = new Clients(props);
            emit(new LinkedHashMap<>(Map.of("event", "ready", "release", release, "issuer", required(config, "issuer"),
                "standard_handler", OAuthBearerLoginCallbackHandler.class.getName(), "retriever", "ClientCredentialsJwtRetriever")));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            StringBuilder line = new StringBuilder(); int operations = 0; boolean running = true;
            while (running && System.nanoTime() < deadline) {
                while (System.in.available() > 0) {
                    int ch = System.in.read();
                    if (ch == '\n') {
                        String[] command = line.toString().strip().split(" "); line.setLength(0);
                        if (command.length != 2 || !command[1].matches("[a-z0-9_-]{1,48}") || ++operations > 25)
                            throw new IllegalArgumentException("Invalid bounded operator command");
                        if (command[0].equals("query")) clients.query(command[1], topic);
                        else if (command[0].equals("write")) clients.write(command[1], topic, release);
                        else if (command[0].equals("read")) clients.read(command[1], topic);
                        else if (command[0].equals("recreate")) {
                            clients.close(); clients = null;
                            emit(new LinkedHashMap<>(Map.of("event", "closed", "phase", command[1])));
                            clients = new Clients(props); clients.query(command[1], topic);
                        } else if (command[0].equals("close")) { running = false; break; }
                        else throw new IllegalArgumentException("Unknown bounded operator command");
                    } else if (ch >= 32 && ch <= 126 && line.length() < 96) line.append((char) ch);
                    else throw new IllegalArgumentException("Invalid command bytes");
                }
                Thread.sleep(25);
            }
            emit(new LinkedHashMap<>(Map.of("event", "complete", "operator_commands", operations, "deadline_reached", running)));
        } finally {
            if (clients != null) clients.close();
            Files.deleteIfExists(publicTrustStore);
            emit(new LinkedHashMap<>(Map.of("event", "shutdown", "release", release)));
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length != 1) throw new IllegalArgumentException("One config path required");
            run(Path.of(args[0]));
        } catch (Exception error) {
            emit(new LinkedHashMap<>(Map.of("event", "fatal", "error_types", errorTypes(error))));
            System.exit(1);
        }
    }
}
