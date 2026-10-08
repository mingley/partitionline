package org.apache.kafka.clients.admin;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy;
import org.apache.kafka.clients.admin.internals.ListTransactionsHandler;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.utils.LogContext;

/** Genuine SDK codecs/options/public calls. Responses are selected fixture
 * policy, not a claim that a Kafka broker evaluated these regular expressions. */
public final class ListPatternOracle {
    private ListPatternOracle() { }
    private static String quote(String value) {
        if (value == null) return "null";
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
    private static byte[] bytes(Message message, short version) {
        ByteBuffer input=MessageUtil.toByteBufferAccessor(message,version).buffer();
        byte[] result=new byte[input.remaining()];input.get(result);return result;
    }
    private static byte[] frame(byte[] header,byte[] body) {
        return ByteBuffer.allocate(4+header.length+body.length).putInt(header.length+body.length).put(header).put(body).array();
    }
    private static String hex(String value) {
        return value==null?"-":java.util.HexFormat.of().formatHex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    private static ListTransactionsResponseData.TransactionState row(String id,long pid,String state) {
        return new ListTransactionsResponseData.TransactionState().setTransactionalId(id).setProducerId(pid).setTransactionState(state);
    }
    private static ListTransactionsResponseData policy(String mode,int node) {
        ListTransactionsResponseData response=new ListTransactionsResponseData().setThrottleTimeMs(0)
            .setErrorCode((short)0).setUnknownStateFilters(List.of());
        if (mode.equals("invalid")) return response.setErrorCode(Errors.INVALID_REGULAR_EXPRESSION.code());
        if (mode.equals("no-match") || mode.equals("combined") && node==2) return response;
        List<ListTransactionsResponseData.TransactionState> rows=new ArrayList<>();
        rows.add(node==1?row("only-one",1001,"Ongoing"):row("only-two",1002,"CompleteCommit"));
        if (mode.equals("null") || mode.equals("empty") || mode.equals("v0") || mode.equals("v1")) {
            rows.add(node==1?row("same-id",41,"PrepareCommit"):row("same-id",42,"PrepareAbort"));
        }
        return response.setTransactionStates(rows);
    }
    private static ListTransactionsOptions options(String mode) {
        ListTransactionsOptions result=new ListTransactionsOptions().timeoutMs(2000);
        String pattern=switch(mode) {
            case "empty" -> "";
            case "no-match" -> "missing-.*";
            case "invalid" -> "[";
            case "null", "v0", "v1" -> null;
            default -> "only-.*";
        };
        result.filterOnTransactionalIdPattern(pattern);
        if (mode.equals("combined")) result.filterStates(Set.of(TransactionState.ONGOING))
            .filterProducerIds(Set.of(1001L)).filterOnDuration(0);
        return result;
    }
    private static void writePair(Path root,String id,short version,ListTransactionsRequestData request,ListTransactionsResponseData response) throws Exception {
        byte[] req=bytes(request,version),resp=bytes(response,version);
        ByteBuffer first=ByteBuffer.wrap(req),second=ByteBuffer.wrap(resp);
        ListTransactionsRequestData parsed=new ListTransactionsRequestData(new ByteBufferAccessor(first),version);
        ListTransactionsResponseData decoded=new ListTransactionsResponseData(new ByteBufferAccessor(second),version);
        if (first.hasRemaining() || second.hasRemaining() || !parsed.equals(request) || !decoded.equals(response)) throw new AssertionError("actual full parser mismatch");
        byte[] reqHeader=bytes(new RequestHeaderData().setRequestApiKey((short)66).setRequestApiVersion(version)
            .setCorrelationId(7).setClientId("pattern-oracle"),ApiKeys.LIST_TRANSACTIONS.requestHeaderVersion(version));
        byte[] respHeader=bytes(new ResponseHeaderData().setCorrelationId(7),ApiKeys.LIST_TRANSACTIONS.responseHeaderVersion(version));
        Files.write(root.resolve(id+".request.bin"),frame(reqHeader,req));
        Files.write(root.resolve(id+".response.bin"),frame(respHeader,resp));
    }
    private static void generate(Path root) throws Exception {
        Files.createDirectories(root);StringBuilder index=new StringBuilder();int supported=0,unsupported=0;
        String[] patterns={null,"","only-.*","missing-.*","[","κ-事务.*"};long[] durations={-1,0,5000,Long.MAX_VALUE};
        for (short version=0;version<=2;version++) for (int p=0;p<patterns.length;p++) for (int d=0;d<durations.length;d++) for (int f=0;f<2;f++) {
            String id="raw-v"+version+"-p"+p+"-d"+d+"-f"+f;
            ListTransactionsRequestData data=new ListTransactionsRequestData().setStateFilters(f==0?List.of():List.of("Ongoing","PrepareCommit"))
                .setProducerIdFilters(f==0?List.of():List.of(1001L,Long.MAX_VALUE)).setDurationFilter(durations[d]).setTransactionalIdPattern(patterns[p]);
            boolean ok=true;
            try {new ListTransactionsRequest.Builder(data).build(version);}
            catch (UnsupportedVersionException expected) {ok=false;}
            if (ok) {
                supported++;writePair(root,id,version,data,new ListTransactionsResponseData().setThrottleTimeMs(42)
                    .setErrorCode((short)0).setUnknownStateFilters(List.of("UnknownState"))
                    .setTransactionStates(List.of(row("κ-事务",Long.MAX_VALUE,"Ongoing"))));
            } else unsupported++;
            index.append(id).append('\t').append(version).append('\t').append(hex(patterns[p])).append('\t')
                .append(durations[d]).append('\t').append(f).append('\t').append(ok?"supported":"unsupported").append('\n');
        }
        Files.writeString(root.resolve("raw.tsv"),index);
        String[] modes={"null","empty","match","no-match","invalid","combined","mixed","disconnect","downgrade","v0","v1"};
        for (String mode:modes) {
            ListTransactionsHandler handler=new ListTransactionsHandler(options(mode),new LogContext());
            for (short version=0;version<=2;version++) {
                try {
                    ListTransactionsRequestData data=handler.buildBatchedRequest(1,Set.of(new AllBrokersStrategy.BrokerKey(OptionalInt.of(1)))).build(version).data();
                    writePair(root,"public-"+mode+"-v"+version,version,data,policy(mode,1));
                } catch (UnsupportedVersionException expected) { /* retained by raw.tsv and per-broker public outcomes */ }
            }
            writePair(root,"public-"+mode+"-node-1",(short)2,new ListTransactionsRequestData(),policy(mode,1));
            writePair(root,"public-"+mode+"-node-2",(short)2,new ListTransactionsRequestData(),policy(mode,2));
        }
        Files.writeString(root.resolve("summary.json"),"{\"supported\":"+supported+",\"unsupported\":"+unsupported+",\"raw_cases\":144,\"fixture_policy\":\"Selected SDK-serialized values, not a live regex broker\"}\n");
        System.out.println("supported="+supported+" unsupported="+unsupported);
    }
    private static String rows(Collection<TransactionListing> values) {
        List<TransactionListing> sorted=new ArrayList<>(values);sorted.sort(Comparator.comparing(TransactionListing::transactionalId).thenComparingLong(TransactionListing::producerId));
        StringBuilder out=new StringBuilder("[");
        for (TransactionListing row:sorted) {if(out.length()>1)out.append(',');out.append("{\"id\":").append(quote(row.transactionalId())).append(",\"pid\":").append(row.producerId()).append(",\"state\":").append(quote(row.state().toString())).append('}');}
        return out.append(']').toString();
    }
    private static String failure(Throwable error) {
        return "{\"error_class\":"+quote(error.getClass().getName())+",\"error_code\":"+Errors.forException(error).code()+"}";
    }
    private static void live(String bootstrap,Path root,String mode) throws Exception {
        Properties config=new Properties();config.setProperty("bootstrap.servers",bootstrap);config.setProperty("request.timeout.ms","1000");
        config.setProperty("default.api.timeout.ms","2000");config.setProperty("retry.backoff.ms","10");config.setProperty("retry.backoff.max.ms","20");
        config.setProperty("reconnect.backoff.ms","0");config.setProperty("reconnect.backoff.max.ms","0");
        Admin admin=Admin.create(config);StringBuilder out=new StringBuilder("{\"by_broker\":{");
        try {
            ListTransactionsResult result=admin.listTransactions(options(mode));
            Map<Integer,KafkaFuture<Collection<TransactionListing>>> brokers=result.byBrokerId().get(3,TimeUnit.SECONDS);
            if (!brokers.keySet().equals(Set.of(1,2))) throw new AssertionError("missing broker");
            for (int node=1;node<=2;node++) {
                if(node>1)out.append(',');out.append(quote(Integer.toString(node))).append(':');
                try {Collection<TransactionListing> actual=brokers.get(node).get(3,TimeUnit.SECONDS);out.append("{\"listings\":").append(rows(actual)).append('}');}
                catch(ExecutionException error){out.append(failure(error.getCause()));}
            }
            out.append("},\"all\":");
            try {out.append(rows(result.all().get(3,TimeUnit.SECONDS)));}
            catch(ExecutionException error){out.append(failure(error.getCause()));}
        } finally {admin.close(Duration.ofSeconds(2));}
        Files.writeString(root.resolve("public-outcome.json"),out.append("}\n"));
    }
    private static void parse(Path root) throws Exception {
        int count=0;
        try(var paths=Files.list(root)) {
            for(Path path:paths.filter(p->p.toString().endsWith(".request.bin")).sorted().toList()) {
                ByteBuffer buffer=ByteBuffer.wrap(Files.readAllBytes(path));int size=buffer.getInt();
                if(size!=buffer.remaining())throw new AssertionError("prefix");
                RequestHeaderData header=new RequestHeaderData(new ByteBufferAccessor(buffer),(short)2);
                ListTransactionsRequestData data=new ListTransactionsRequestData(new ByteBufferAccessor(buffer),header.requestApiVersion());
                if(buffer.hasRemaining())throw new AssertionError("request suffix");
                new ListTransactionsRequest.Builder(data).build(header.requestApiVersion());count++;
                ByteBuffer response=ByteBuffer.wrap(Files.readAllBytes(Path.of(path.toString().replace(".request.bin",".response.bin"))));
                if(response.getInt()!=response.remaining())throw new AssertionError("response prefix");
                new ResponseHeaderData(new ByteBufferAccessor(response),(short)1);
                new ListTransactionsResponseData(new ByteBufferAccessor(response),header.requestApiVersion());
                if(response.hasRemaining())throw new AssertionError("response suffix");count++;
            }
        }
        if(count==0)throw new AssertionError("no actual Rust frames");
        System.out.println("actual Rust frame parses="+count);
    }
    public static void main(String[] args) throws Exception {
        switch(args[0]) {
            case "generate" -> generate(Path.of(args[1]));
            case "live" -> live(args[1],Path.of(args[2]),args[3]);
            case "parse" -> parse(Path.of(args[1]));
            default -> throw new IllegalArgumentException("generate/live/parse");
        }
    }
}
