import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.utils.Utils;

/** Bounded physical TCP peer using actual Apache message builders and parsers. */
public final class ConformanceUpdateFeaturesPeer {
    private ConformanceUpdateFeaturesPeer() { }
    private static byte[] bytes(Message message, short version) {
        var buffer = MessageUtil.toByteBufferAccessor(message, version).buffer();
        var body = new byte[buffer.remaining()];
        buffer.get(body);
        return body;
    }
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
    private static final AtomicInteger FRAMES = new AtomicInteger();
    private static final AtomicInteger CONNECTIONS = new AtomicInteger();
    private static final ConcurrentHashMap<String, AtomicInteger> UPDATES = new ConcurrentHashMap<>();
    private static final Set<Socket> SOCKETS = ConcurrentHashMap.newKeySet();
    private static final List<String> EVENTS = new ArrayList<>();
    private static synchronized void event(String text) { EVENTS.add(text); }

    private static ApiVersionsResponseData versions(short maximum) {
        var keys = new ApiVersionsResponseData.ApiVersionCollection();
        for (short[] range : new short[][]{{18,0,4},{3,0,13},{19,0,7},{20,0,6},{57,1,maximum}}) {
            keys.add(new ApiVersionsResponseData.ApiVersion().setApiKey(range[0])
                .setMinVersion(range[1]).setMaxVersion(range[2]));
        }
        return new ApiVersionsResponseData().setApiKeys(keys);
    }
    private static MetadataResponseData metadata(int[] ports, int controller) {
        var brokers = new MetadataResponseData.MetadataResponseBrokerCollection();
        for (int node = 0; node < 2; node++) {
            brokers.add(new MetadataResponseData.MetadataResponseBroker().setNodeId(node)
                .setHost("127.0.0.1").setPort(ports[node]));
        }
        return new MetadataResponseData().setBrokers(brokers).setClusterId("update-features-scripted")
            .setControllerId(controller);
    }
    private static void connection(Socket socket, int node, int[] ports, short maximum,
                                   String scenario, Path output) {
        SOCKETS.add(socket);
        try (socket; var input = new DataInputStream(socket.getInputStream());
             var sink = new DataOutputStream(socket.getOutputStream())) {
            socket.setSoTimeout(3000);
            socket.setTcpNoDelay(true);
            while (RUNNING.get()) {
                int length;
                try { length = input.readInt(); } catch (EOFException ended) { break; }
                if (length < 8 || length > 65536) throw new AssertionError("frame budget");
                var frame = input.readNBytes(length);
                if (frame.length != length) throw new AssertionError("truncated frame");
                int ordinal = FRAMES.incrementAndGet();
                if (ordinal > 96) throw new AssertionError("request count budget");
                var buffer = ByteBuffer.wrap(frame);
                var header = RequestHeader.parse(buffer);
                String caller = header.clientId();
                if (!Set.of("update-features-java", "update-features-rust").contains(caller)) {
                    throw new AssertionError("undeclared caller identity");
                }
                var state = UPDATES.computeIfAbsent(caller, unused -> new AtomicInteger());
                short version = header.apiVersion();
                var accessor = new ByteBufferAccessor(buffer);
                Message response;
                Message request;
                if (header.apiKey() == ApiKeys.API_VERSIONS) {
                    request = new ApiVersionsRequestData(accessor, version);
                    response = versions(maximum);
                } else if (header.apiKey() == ApiKeys.METADATA) {
                    request = new MetadataRequestData(accessor, version);
                    response = metadata(ports, scenario.equals("success") || scenario.equals("top-error") ? 0 : Math.min(state.get(), 1));
                } else if (header.apiKey() == ApiKeys.UPDATE_FEATURES) {
                    var update = new UpdateFeaturesRequestData(accessor, version);
                    request = update;
                    if (version != maximum || update.featureUpdates().isEmpty()) throw new AssertionError("unexpected API57 fields");
                    int attempt = state.incrementAndGet();
                    if (attempt > 3) throw new AssertionError("retry count budget");
                    boolean retry = scenario.equals("retry") || scenario.equals("deadline");
                    short code = scenario.equals("top-error") ? Errors.INVALID_REQUEST.code()
                        : retry && attempt == 1 ? Errors.NOT_CONTROLLER.code() : 0;
                    if (retry && node != (attempt == 1 ? 0 : 1)) throw new AssertionError("wrong controller dispatch");
                    if (scenario.equals("deadline")) Thread.sleep(attempt == 1 ? 110 : 200);
                    event("{\"caller\":\"" + caller + "\",\"api\":57,\"version\":" + version
                        + ",\"node\":" + node + ",\"attempt\":" + attempt + ",\"timeout_ms\":" + update.timeoutMs()
                        + ",\"validate_only\":" + update.validateOnly() + ",\"response_code\":" + code + "}");
                    var names = new java.util.TreeSet<String>();
                    for (var feature : update.featureUpdates()) names.add(feature.feature());
                    response = UpdateFeaturesResponse.createWithErrors(new ApiError(Errors.forCode(code)), names, 0).data();
                } else {
                    throw new AssertionError("undeclared API");
                }
                if (buffer.hasRemaining()) throw new AssertionError("trailing request bytes");
                Files.write(output.resolve(ordinal + "-" + caller + "-" + header.apiKey().id + "-v" + version + "-request.bin"), bytes(request, version));
                var body = bytes(response, version);
                Files.write(output.resolve(ordinal + "-" + caller + "-" + header.apiKey().id + "-v" + version + "-response.bin"), body);
                var responseHeader = bytes(new ResponseHeaderData().setCorrelationId(header.correlationId()),
                    header.apiKey().responseHeaderVersion(version));
                sink.writeInt(responseHeader.length + body.length);
                sink.write(responseHeader);
                sink.write(body);
                sink.flush();
            }
        } catch (SocketException | SocketTimeoutException expectedClose) {
            event("{\"socket_closed\":true,\"node\":" + node + "}");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException | AssertionError failed) {
            event("{\"peer_failure\":\"" + failed.getClass().getSimpleName() + "\"}");
            RUNNING.set(false);
        } finally { SOCKETS.remove(socket); }
    }
    private static void server(Path output, short maximum, String scenario) throws Exception {
        Files.createDirectory(output);
        if (maximum != 1 && maximum != 2) throw new AssertionError("version budget");
        if (!Set.of("success", "top-error", "retry", "deadline").contains(scenario)) throw new AssertionError("scenario budget");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        var executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(8));
        var threads = new ArrayList<Thread>();
        try (var first = new ServerSocket(0, 8, java.net.InetAddress.getLoopbackAddress());
             var second = new ServerSocket(0, 8, java.net.InetAddress.getLoopbackAddress())) {
            int[] ports = {first.getLocalPort(), second.getLocalPort()};
            ServerSocket[] listeners = {first, second};
            for (int index = 0; index < 2; index++) {
                final int node = index;
                listeners[index].setSoTimeout(100);
                var thread = new Thread(() -> {
                    while (RUNNING.get() && System.nanoTime() < deadline) {
                        try {
                            Socket accepted = listeners[node].accept();
                            if (CONNECTIONS.incrementAndGet() > 24) { accepted.close(); throw new AssertionError("connection budget"); }
                            executor.execute(() -> connection(accepted, node, ports, maximum, scenario, output));
                        } catch (SocketTimeoutException idle) {
                            continue;
                        } catch (IOException | RuntimeException | AssertionError failure) {
                            RUNNING.set(false);
                            break;
                        }
                    }
                });
                thread.start();
                threads.add(thread);
            }
            Files.writeString(output.resolve("ready.json"), "{\"ports\":[" + ports[0] + "," + ports[1] + "]}\n");
            while (RUNNING.get() && System.nanoTime() < deadline && !Files.exists(output.resolve("stop"))) Thread.sleep(20);
        } finally {
            RUNNING.set(false);
            for (Socket socket : SOCKETS) socket.close();
            executor.shutdownNow();
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) throw new AssertionError("handler join deadline");
            for (Thread thread : threads) { thread.join(1000); if (thread.isAlive()) throw new AssertionError("acceptor join deadline"); }
        }
        Files.writeString(output.resolve("events.json"), "[" + String.join(",", EVENTS) + "]\n");
        if (EVENTS.stream().anyMatch(item -> item.contains("peer_failure"))) throw new AssertionError("peer failure retained");
        Files.writeString(output.resolve("closed.json"), "{\"handlers_joined\":true,\"acceptors_joined\":true,\"sockets_closed\":true}\n");
    }
    private static void client(String bootstrap, String scenario, Path output) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("client.id", "update-features-java");
        properties.put("request.timeout.ms", "250");
        properties.put("default.api.timeout.ms", "2000");
        properties.put("retry.backoff.ms", "10");
        properties.put("retry.backoff.max.ms", "10");
        var admin = Admin.create(properties);
        short code = 0;
        try {
            admin.listTopics().names().get(3, TimeUnit.SECONDS);
            var updates = Map.of("test_feature_1", new FeatureUpdate((short)2, FeatureUpdate.UpgradeType.UPGRADE));
            try {
                admin.updateFeatures(updates, new UpdateFeaturesOptions().timeoutMs(scenario.equals("deadline") ? 250 : 2000)).all().get(3, TimeUnit.SECONDS);
            } catch (ExecutionException failed) { code = Errors.forException(failed.getCause()).code(); }
            short expected = scenario.equals("deadline") ? Errors.REQUEST_TIMED_OUT.code()
                : scenario.equals("top-error") ? Errors.INVALID_REQUEST.code() : 0;
            if (code != expected) throw new AssertionError("unexpected public outcome " + code + " expected " + expected);
            String recovery = "null";
            if (scenario.equals("deadline")) {
                admin.updateFeatures(updates, new UpdateFeaturesOptions().timeoutMs(2000)).all().get(3, TimeUnit.SECONDS);
                recovery = "0";
            }
            Files.writeString(output, "{\"error_code\":" + code + ",\"recovery_code\":" + recovery + "}\n");
        } finally { admin.close(Duration.ofSeconds(1)); }
    }
    private static void values() {
        for (String name : new String[]{"", " ", "\u0000", "\u001f", "\u00a0", "\u2000", "feature"}) {
            System.out.println("{\"name_code\":" + (name.isEmpty() ? -1 : (int)name.charAt(0)) + ",\"blank\":" + Utils.isBlank(name) + "}");
        }
    }
    private static void names(String bootstrap, Path output) throws Exception {
        var properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("client.id", "update-features-java");
        properties.put("request.timeout.ms", "1000");
        properties.put("default.api.timeout.ms", "2000");
        var admin = Admin.create(properties);
        var rows = new ArrayList<String>();
        try {
            admin.listTopics().names().get(3, TimeUnit.SECONDS);
            for (String name : new String[]{"", " ", "\u0000", "\u001f", "\u00a0", "\u2000", "feature"}) {
                boolean rejected = false;
                try {
                    admin.updateFeatures(Map.of(name, new FeatureUpdate((short)1, FeatureUpdate.UpgradeType.UPGRADE)),
                        new UpdateFeaturesOptions().timeoutMs(1000)).all().get(2, TimeUnit.SECONDS);
                } catch (IllegalArgumentException invalid) { rejected = true; }
                rows.add("{\"name_code\":" + (name.isEmpty() ? -1 : (int)name.charAt(0)) + ",\"rejected_locally\":" + rejected + "}");
            }
            Files.writeString(output, "[" + String.join(",", rows) + "]\n");
        } finally { admin.close(Duration.ofSeconds(1)); }
    }
    private static byte[] removeByte(byte[] input, int offset) {
        byte[] output = new byte[input.length - 1];
        System.arraycopy(input, 0, output, 0, offset);
        System.arraycopy(input, offset + 1, output, offset, input.length - offset - 1);
        return output;
    }
    private static void malformed(Path output) throws Exception {
        Files.createDirectory(output);
        int count = 0;
        for (short version = 0; version <= 2; version++) {
            var empty = bytes(new UpdateFeaturesRequestData(), version);
            if (empty[4] != 1) throw new AssertionError("empty array offset");
            empty[4] = 0;
            var features = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
            var feature = new UpdateFeaturesRequestData.FeatureUpdateKey().setFeature("f").setMaxVersionLevel((short)1);
            if (version > 0) feature.setUpgradeType((byte)1);
            features.add(feature);
            var named = bytes(new UpdateFeaturesRequestData().setFeatureUpdates(features), version);
            if (named[5] != 2 || named[6] != 'f') throw new AssertionError("request name offset");
            named[5] = 0;
            named = removeByte(named, 6);
            for (var entry : Map.of("null-array", empty, "null-name", named).entrySet()) {
                boolean rejected = false;
                try { new UpdateFeaturesRequestData(new ByteBufferAccessor(ByteBuffer.wrap(entry.getValue())), version); }
                catch (RuntimeException expected) { rejected = true; }
                if (!rejected) throw new AssertionError("SDK accepted malformed request");
                Files.write(output.resolve("v" + version + "-request-" + entry.getKey() + ".bin"), entry.getValue());
                count++;
            }
            if (version < 2) {
                var response = bytes(new UpdateFeaturesResponseData().setErrorMessage(null), version);
                if (response[6] != 0 || response[7] != 1) throw new AssertionError("response array offset");
                response[7] = 0;
                var results = new UpdateFeaturesResponseData.UpdatableFeatureResultCollection();
                results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult().setFeature("f"));
                var namedResponse = bytes(new UpdateFeaturesResponseData().setErrorMessage(null).setResults(results), version);
                if (namedResponse[8] != 2 || namedResponse[9] != 'f') throw new AssertionError("response name offset");
                namedResponse[8] = 0;
                namedResponse = removeByte(namedResponse, 9);
                for (var entry : Map.of("null-array", response, "null-name", namedResponse).entrySet()) {
                    boolean rejected = false;
                    try { new UpdateFeaturesResponseData(new ByteBufferAccessor(ByteBuffer.wrap(entry.getValue())), version); }
                    catch (RuntimeException expected) { rejected = true; }
                    if (!rejected) throw new AssertionError("SDK accepted malformed response");
                    Files.write(output.resolve("v" + version + "-response-" + entry.getKey() + ".bin"), entry.getValue());
                    count++;
                }
            }
        }
        if (count != 10) throw new AssertionError("malformed cohort incomplete");
        System.out.println("{\"actual_sdk_rejected_malformed_bodies\":" + count + "}");
    }
    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "server" -> server(Path.of(args[1]), Short.parseShort(args[2]), args[3]);
            case "client" -> client(args[1], args[2], Path.of(args[3]));
            case "values" -> values();
            case "client-names" -> names(args[1], Path.of(args[2]));
            case "malformed" -> malformed(Path.of(args[1]));
            default -> throw new AssertionError("undeclared mode");
        }
    }
}
