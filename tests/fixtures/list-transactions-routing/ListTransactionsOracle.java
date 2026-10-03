/* Source-only preparation. Actual JVM outcomes must be captured after approval. */
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.admin.internals.AllBrokersStrategy;
import org.apache.kafka.clients.admin.internals.ListTransactionsHandler;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.utils.LogContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Executed official serializers/handler/futures are independent of Rust.
 * Generate mode is component evidence, not public Admin network evidence.
 * Live mode explicitly invokes genuine Admin.listTransactions on a separately
 * owned peer; the caller must provide its closed source-bound socket history.
 */
public final class ListTransactionsOracle {
    private static final int CORRELATION = 7;
    private static final String CLIENT = "list-transactions-oracle";
    private static final int MAX_VECTOR_BYTES = 128 * 1024;
    private static final int MAX_VECTORS = 64;
    private static int checks;
    private static int vectors;

    private ListTransactionsOracle() { }

    private static void check(boolean condition, String label) {
        checks++;
        if (!condition) throw new AssertionError(label);
    }

    private static byte[] bytes(Message message, short version) {
        // This is the actual MessageUtil API in all three pinned SDK jars.
        ByteBuffer buffer = MessageUtil.toByteBufferAccessor(message, version).buffer();
        check(buffer.remaining() <= MAX_VECTOR_BYTES, "finite vector");
        byte[] value = new byte[buffer.remaining()];
        buffer.get(value);
        return value;
    }

    private static byte[] frame(byte[] header, byte[] body) {
        check(header.length + body.length <= MAX_VECTOR_BYTES, "finite frame");
        ByteBuffer output = ByteBuffer.allocate(4 + header.length + body.length);
        output.putInt(header.length + body.length).put(header).put(body);
        return output.array();
    }

    private static String hex(byte[] value) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] out = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            out[2 * i] = digits[(value[i] & 255) >>> 4];
            out[2 * i + 1] = digits[value[i] & 15];
        }
        return new String(out);
    }

    private static String sha(byte[] value) throws Exception {
        return hex(MessageDigest.getInstance("SHA-256").digest(value));
    }

    private static String quote(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') out.append('\\').append(c);
            else if (c < 32) out.append(String.format("\\u%04x", (int) c));
            else out.append(c);
        }
        return out.append('"').toString();
    }

    private static ListTransactionsResponseData.TransactionState row(String id, long pid, String state) {
        return new ListTransactionsResponseData.TransactionState()
            .setTransactionalId(id).setProducerId(pid).setTransactionState(state);
    }

    private static ListTransactionsResponseData listing(int node, short error) {
        ListTransactionsResponseData response = new ListTransactionsResponseData()
            .setThrottleTimeMs(0).setErrorCode(error).setUnknownStateFilters(List.of());
        if (error == 0) {
            response.setTransactionStates(node == 1
                ? List.of(row("only-one", 1001, "Ongoing"), row("same-id", 41, "PrepareCommit"))
                : List.of(row("only-two", 1002, "CompleteCommit"), row("same-id", 42, "PrepareAbort")));
        }
        return response;
    }

    private static void vector(Path root, StringBuilder manifest, String name, ApiKeys key,
                               short version, Message request, Message response) throws Exception {
        check(++vectors <= MAX_VECTORS, "finite vector count");
        short requestHeaderVersion = key.requestHeaderVersion(version);
        short responseHeaderVersion = key.responseHeaderVersion(version);
        byte[] requestBody = bytes(request, version);
        byte[] responseBody = bytes(response, version);
        byte[] requestFrame = frame(bytes(new RequestHeaderData()
            .setRequestApiKey(key.id).setRequestApiVersion(version)
            .setCorrelationId(CORRELATION).setClientId(CLIENT), requestHeaderVersion), requestBody);
        byte[] responseFrame = frame(bytes(new ResponseHeaderData().setCorrelationId(CORRELATION),
                                          responseHeaderVersion), responseBody);
        Files.write(root.resolve(name + ".request.bin"), requestFrame);
        Files.write(root.resolve(name + ".response.bin"), responseFrame);
        if (vectors > 1) manifest.append(",\n");
        manifest.append("{\"name\":").append(quote(name))
            .append(",\"api_key\":").append(key.id).append(",\"api_version\":").append(version)
            .append(",\"request_header_version\":").append(requestHeaderVersion)
            .append(",\"response_header_version\":").append(responseHeaderVersion)
            .append(",\"request_body_hex\":").append(quote(hex(requestBody)))
            .append(",\"response_body_hex\":").append(quote(hex(responseBody)))
            .append(",\"request_frame_sha256\":").append(quote(sha(requestFrame)))
            .append(",\"response_frame_sha256\":").append(quote(sha(responseFrame)))
            .append(",\"provenance\":\"Official message serializers; response values selected by bounded peer policy\"}");
    }

    private static MetadataResponseData metadata() {
        MetadataResponseData.MetadataResponseBrokerCollection brokers =
            new MetadataResponseData.MetadataResponseBrokerCollection();
        brokers.add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1).setHost("127.0.0.1").setPort(19091).setRack(null));
        brokers.add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(2).setHost("127.0.0.1").setPort(19092).setRack(null));
        return new MetadataResponseData().setBrokers(brokers).setControllerId(1);
    }

    private static AllBrokersStrategy.BrokerKey key(int broker) {
        return new AllBrokersStrategy.BrokerKey(OptionalInt.of(broker));
    }

    private static void components(StringBuilder observations) throws Exception {
        int checksBeforeComponents = checks;
        ListTransactionsHandler handler = new ListTransactionsHandler(new ListTransactionsOptions(), new LogContext());
        check(handler.lookupStrategy() instanceof AllBrokersStrategy, "actual all-brokers lookup");
        AllBrokersStrategy strategy = (AllBrokersStrategy) handler.lookupStrategy();
        AllBrokersStrategy.AllBrokersFuture<Collection<TransactionListing>> future = ListTransactionsHandler.newFuture();
        // Run the actual strategy builder with the initial lookup key, then the
        // actual Metadata response handler. No scripted broker implementation is
        // imported into these component predicates.
        var lookup = strategy.buildRequest(future.lookupKeys()).build((short) 13);
        check(lookup.data().topics() != null && lookup.data().topics().isEmpty(), "no-topic Metadata discovery");
        var mapped = strategy.handleResponse(future.lookupKeys(),
            new org.apache.kafka.common.requests.MetadataResponse(metadata(), (short) 1));
        check(mapped.mappedKeys.size() == 2, "strategy discovers both brokers");
        future.completeLookup(mapped.mappedKeys);
        ListTransactionsResult result = new ListTransactionsResult(future.all());
        var first = handler.handleResponse(new Node(1, "127.0.0.1", 19091), Set.of(key(1)),
                                          new ListTransactionsResponse(listing(1, (short) 0)));
        future.complete(first.completedKeys);
        check(!result.all().isDone(), "complete-only waits for second broker");
        check(result.byBrokerId().get(1, TimeUnit.SECONDS).size() == 2, "origin map available before all complete");
        var loading = handler.handleResponse(new Node(2, "127.0.0.1", 19092), Set.of(key(2)),
                                            new ListTransactionsResponse(listing(2, (short) 14)));
        check(loading.completedKeys.isEmpty() && loading.failedKeys.isEmpty() && loading.unmappedKeys.isEmpty(),
              "load delay retries same mapped broker");
        var second = handler.handleResponse(new Node(2, "127.0.0.1", 19092), Set.of(key(2)),
                                           new ListTransactionsResponse(listing(2, (short) 0)));
        future.complete(second.completedKeys);
        Collection<TransactionListing> union = result.all().get(1, TimeUnit.SECONDS);
        check(union.size() == 4, "all concatenates both listings");
        check(union.stream().filter(r -> r.transactionalId().equals("same-id")).count() == 2,
              "conflicting duplicate IDs remain visible");
        check(result.allByBrokerId().get(1, TimeUnit.SECONDS).keySet().equals(Set.of(1, 2)), "actual allByBrokerId origins");
        for (short code : new short[] {15, 16, 29, 53}) {
            var badFuture = ListTransactionsHandler.newFuture();
            badFuture.completeLookup(Map.of(key(1), 1, key(2), 2));
            var badResult = new ListTransactionsResult(badFuture.all());
            badFuture.complete(first.completedKeys);
            var error = handler.handleResponse(new Node(2, "127.0.0.1", 19092), Set.of(key(2)),
                                              new ListTransactionsResponse(listing(2, code)));
            check(error.failedKeys.size() == 1 && error.completedKeys.isEmpty(), "terminal broker error");
            badFuture.completeExceptionally(error.failedKeys);
            check(badResult.byBrokerId().get(1, TimeUnit.SECONDS).get(1).get(1, TimeUnit.SECONDS).size() == 2,
                  "successful broker survives in byBrokerId");
            for (KafkaFuture<?> value : List.of(badResult.all(), badResult.allByBrokerId())) {
                try { value.get(1, TimeUnit.SECONDS); throw new AssertionError("terminal error became complete success"); }
                catch (ExecutionException expected) { check(expected.getCause() != null, "genuine exceptional future"); }
            }
        }
        observations.append("{\"component_checks\":").append(checks-checksBeforeComponents)
            .append(",\"complete_union_count\":4,\"duplicate_id_count\":2,\"broker_ids\":[1,2],")
            .append("\"scope\":\"Actual AllBrokersStrategy/Handler/Result classes, no public network qualification\"}");
    }

    private static void generate(Path root, String release) throws Exception {
        Files.createDirectories(root);
        StringBuilder manifest = new StringBuilder("{\"schema_version\":1,\"release\":")
            .append(quote(release)).append(",\"framing\":\"four-byte length+header+body\",\"cases\":[\n");
        vector(root, manifest, "metadata-empty-topics-v1", ApiKeys.METADATA, (short) 1,
               new MetadataRequestData().setTopics(List.of()), metadata());
        for (short version : new short[] {0, 1}) {
            ListTransactionsRequestData unfiltered = new ListTransactionsRequestData()
                .setStateFilters(List.of()).setProducerIdFilters(List.of()).setDurationFilter(-1);
            ListTransactionsRequest built = new ListTransactionsRequest.Builder(unfiltered).build(version);
            for (int node : new int[] {1, 2}) {
                ListTransactionsResponseData response = listing(node, (short) 0);
                byte[] encoded = bytes(response, version);
                ByteBuffer consumed = ByteBuffer.wrap(encoded);
                ListTransactionsResponseData parsed = new ListTransactionsResponseData(new ByteBufferAccessor(consumed), version);
                check(consumed.remaining() == 0 && parsed.transactionStates().size() == 2, "actual parser consumes complete body");
                vector(root, manifest, "broker-" + node + "-list-v" + version, ApiKeys.LIST_TRANSACTIONS, version, built.data(), response);
            }
            for (short error : new short[] {14, 15, 16, 29, 53}) {
                vector(root, manifest, "broker-2-error-" + error + "-v" + version,
                       ApiKeys.LIST_TRANSACTIONS, version, built.data(), listing(2, error));
            }
        }
        ListTransactionsRequestData duration = new ListTransactionsRequestData()
            .setStateFilters(List.of("Ongoing")).setProducerIdFilters(List.of(1002L)).setDurationFilter(5000);
        vector(root, manifest, "duration-5000-v1", ApiKeys.LIST_TRANSACTIONS, (short) 1,
               new ListTransactionsRequest.Builder(duration).build((short) 1).data(),
               new ListTransactionsResponseData().setThrottleTimeMs(0).setErrorCode((short)0)
                   .setUnknownStateFilters(List.of()).setTransactionStates(List.of(row("only-two",1002,"Ongoing"))));
        try { new ListTransactionsRequest.Builder(duration).build((short) 0); throw new AssertionError("v0 silently dropped duration"); }
        catch (UnsupportedVersionException expected) { check(true, "v0 duration rejected by actual builder"); }
        StringBuilder observed = new StringBuilder();
        components(observed);
        manifest.append("\n],\"actual_checks\":").append(checks).append(",\"actual_vectors\":").append(vectors)
            .append(",\"component_observations\":").append(observed).append("}\n");
        if (manifest.length() > 512 * 1024) throw new AssertionError("finite manifest");
        Files.writeString(root.resolve("goldens.json"), manifest, StandardCharsets.UTF_8);
        System.out.println("{\"passed\":true,\"checks\":" + checks + ",\"vectors\":" + vectors + "}");
    }

    private static String rows(Collection<TransactionListing> rows) {
        List<TransactionListing> sorted = new ArrayList<>(rows);
        check(sorted.size() <= 16, "finite public outcome");
        sorted.sort(Comparator.comparing(TransactionListing::transactionalId).thenComparingLong(TransactionListing::producerId));
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < sorted.size(); i++) {
            if (i != 0) out.append(',');
            TransactionListing row = sorted.get(i);
            out.append("{\"id\":").append(quote(row.transactionalId())).append(",\"pid\":")
                .append(row.producerId()).append(",\"state\":").append(quote(row.state().toString())).append('}');
        }
        return out.append(']').toString();
    }

    private static void live(String bootstrap, Path root, String release) throws Exception {
        // Genuine public client mode, with bounded optional one-shot settings.
        // The owner supplies a fresh finite socket peer and retains its actual
        // frames/joins. This source alone does not qualify a network execution.
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", bootstrap);
        config.setProperty("client.id", CLIENT + "-" + release);
        config.setProperty("request.timeout.ms", "1000");
        config.setProperty("default.api.timeout.ms", "2000");
        config.setProperty("retry.backoff.ms", "10");
        config.setProperty("retry.backoff.max.ms", "20");
        config.setProperty("reconnect.backoff.ms", "0");
        config.setProperty("reconnect.backoff.max.ms", "0");
        Files.createDirectories(root);
        StringBuilder out = new StringBuilder("{\"schema_version\":1,\"release\":")
            .append(quote(release)).append(",\"genuine_public_Admin_listTransactions\":true,\"by_broker\":{");
        Admin admin = Admin.create(config);
        try {
            ListTransactionsResult result = admin.listTransactions(new ListTransactionsOptions().timeoutMs(2000));
            Map<Integer, KafkaFuture<Collection<TransactionListing>>> byBroker = result.byBrokerId().get(3, TimeUnit.SECONDS);
            check(byBroker.size() == 2 && byBroker.keySet().equals(Set.of(1,2)), "public discovery targets both brokers");
            boolean allSucceeded = true;
            int index = 0;
            for (int id : new int[] {1,2}) {
                if (index++ != 0) out.append(',');
                out.append(quote(Integer.toString(id))).append(':');
                try {
                    Collection<TransactionListing> actual = byBroker.get(id).get(3,TimeUnit.SECONDS);
                    String encoded = rows(actual);
                    Collection<TransactionListing> expected = id==1
                        ? List.of(new TransactionListing("only-one",1001,TransactionState.ONGOING),
                                  new TransactionListing("same-id",41,TransactionState.PREPARE_COMMIT))
                        : List.of(new TransactionListing("only-two",1002,TransactionState.COMPLETE_COMMIT),
                                  new TransactionListing("same-id",42,TransactionState.PREPARE_ABORT));
                    check(encoded.equals(rows(expected)),"exact independently supplied broker listing");
                    out.append("{\"listings\":").append(encoded).append('}');
                }
                catch (ExecutionException e) {
                    allSucceeded = false;
                    out.append("{\"error_class\":").append(quote(e.getCause().getClass().getName())).append('}');
                }
            }
            out.append("},\"all\":");
            if (allSucceeded) {
                Collection<TransactionListing> all = result.all().get(3,TimeUnit.SECONDS);
                check(all.size()==4 && all.stream().filter(r->r.transactionalId().equals("same-id")).count()==2,
                      "public complete union preserves contradictory duplicate ID");
                out.append(rows(all));
                check(result.allByBrokerId().get(3,TimeUnit.SECONDS).size()==2,"public allByBrokerId");
            } else {
                try { result.all().get(3,TimeUnit.SECONDS); throw new AssertionError("public all silently partial"); }
                catch (ExecutionException e) { out.append("{\"error_class\":").append(quote(e.getCause().getClass().getName())).append('}'); }
                try { result.allByBrokerId().get(3,TimeUnit.SECONDS); throw new AssertionError("allByBrokerId silently partial"); }
                catch (ExecutionException expected) { check(true,"public aggregate fails"); }
            }
        } finally { admin.close(Duration.ofSeconds(2)); }
        out.append(",\"actual_checks\":").append(checks).append("}\n");
        Files.writeString(root.resolve("public-outcome.json"),out,StandardCharsets.UTF_8);
        System.out.println("{\"passed\":true,\"checks\":" + checks + "}");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("generate")) generate(Path.of(args[1]), args[2]);
        else if (args.length == 4 && args[0].equals("live")) live(args[1], Path.of(args[2]), args[3]);
        else throw new IllegalArgumentException("generate output release | live bootstrap output release");
    }
}
