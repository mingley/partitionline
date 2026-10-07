import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.admin.internals.DescribeStreamsGroupsHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
import org.apache.kafka.common.utils.LogContext;

/** Genuine SDK builders, public Admin calls and full typed response comparisons. */
public final class StreamsDescribeOracle {
    private StreamsDescribeOracle() { }
    private static byte[] bytes(Message message) {
        ByteBuffer buffer=MessageUtil.toByteBufferAccessor(message,(short)0).buffer();
        byte[] result=new byte[buffer.remaining()];buffer.get(result);return result;
    }
    private static StreamsGroupDescribeResponseData response(Path fixture,String id,String mode) throws Exception {
        StreamsGroupDescribeResponseData value=new StreamsGroupDescribeResponseData(new ByteBufferAccessor(ByteBuffer.wrap(
            Files.readAllBytes(fixture.resolve("describe-response-full.bin")))),(short)0);
        var group=value.groups().get(0).setGroupId(id).setErrorCode((short)0).setErrorMessage(null)
            .setGroupState("Stable").setGroupEpoch(7).setAssignmentEpoch(8).setAuthorizedOperations((1<<3)|(1<<4));
        group.topology().setEpoch(7);
        if (mode.equals("empty")) { group.setMembers(List.of());group.topology().setSubtopologies(List.of()); }
        if (mode.equals("null-empty")) { group.members().get(0).setInstanceId(null).setRackId("").setUserEndpoint(null); }
        if (mode.equals("error") && id.equals("beta")) { group.setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code()).setErrorMessage("selected denial").setTopology(null).setMembers(List.of()); }
        return value.setGroups(List.of(group));
    }
    private static void generate(Path fixture,Path out) throws Exception {
        Files.createDirectory(out);
        for (String mode:List.of("full","empty","null-empty","error","reroute","disconnect","mixed","downgrade")) {
            for (String id:List.of("alpha","beta")) {
                Files.write(out.resolve(mode+"-"+id+".bin"),bytes(response(fixture,id,mode)));
            }
        }
        var request=new StreamsGroupDescribeRequest.Builder(new StreamsGroupDescribeRequestData()
            .setGroupIds(List.of("alpha","beta")).setIncludeAuthorizedOperations(true)).build((short)0);
        Files.write(out.resolve("request.bin"),bytes(request.data()));
        System.out.println("{\"actual_response_cases\":16}");
    }
    private static void live(String bootstrap,Path directory,String mode,Path fixture) throws Exception {
        Properties properties=new Properties();properties.setProperty("bootstrap.servers",bootstrap);
        properties.setProperty("client.id","streams-describe-java");properties.setProperty("request.timeout.ms","1000");
        properties.setProperty("default.api.timeout.ms","2000");properties.setProperty("retry.backoff.ms","1");
        properties.setProperty("reconnect.backoff.ms","1");properties.setProperty("enable.unstable.api.versions","true");
        List<String> rows=new ArrayList<>();
        try (Admin admin=Admin.create(properties)) {
            var result=admin.describeStreamsGroups(List.of("beta","alpha","beta"),
                new DescribeStreamsGroupsOptions().includeAuthorizedOperations(true).timeoutMs(2000));
            if (result.describedGroups().size()!=2) { throw new AssertionError("SDK Map duplicate policy differs"); }
            for (String id:List.of("alpha","beta")) {
                try {
                    StreamsGroupDescription actual=result.describedGroups().get(id).get(3,TimeUnit.SECONDS);
                    Node coordinator=actual.coordinator();var key=CoordinatorKey.byGroupId(id);
                    var expected=new DescribeStreamsGroupsHandler(true,new LogContext()).handleResponse(coordinator,Set.of(key),
                        new StreamsGroupDescribeResponse(response(fixture,id,mode)));
                    if (!actual.equals(expected.completedKeys.get(key))) { throw new AssertionError("Public SDK projection differs "+id); }
                    rows.add(id+"\t0\t"+actual.groupEpoch()+"\t"+actual.targetAssignmentEpoch()+"\t"+actual.topologyEpoch()+"\t"+actual.members().size()+"\t"+actual.subtopologies().size());
                } catch (ExecutionException error) {
                    short code=Errors.forException(error.getCause()).code();
                    if (!(id.equals("beta") && mode.equals("error") && code==Errors.GROUP_AUTHORIZATION_FAILED.code() || id.equals("beta") && mode.equals("mixed") && code==35 || id.equals("alpha") && mode.equals("downgrade") && code==35)) { throw error; }
                    rows.add(id+"\t"+code+"\t"+error.getCause().getClass().getName());
                }
            }
            admin.close(Duration.ofSeconds(2));
        }
        Files.writeString(directory.resolve("java-outcome.tsv"),String.join("\n",rows)+"\n");
        long live=Thread.getAllStackTraces().keySet().stream().filter(t->t.isAlive() && t.getName().startsWith("kafka-admin-client-thread")).count();
        if(live!=0) { throw new AssertionError("Admin thread remains alive"); }
        System.out.println("{\"actual_public_Admin\":true,\"groups\":2,\"live_admin_threads\":0}");
    }
    private static void parse(Path fixtures,Path proof,String mode) throws Exception {
        int calls=0;int lookups=0;
        for(String row:Files.readAllLines(proof.resolve("frames.tsv"))) {
            String[] cells=row.split("\t",-1);if(cells[0].equals("file"))continue;
            ByteBuffer input=ByteBuffer.wrap(Files.readAllBytes(proof.resolve(cells[0])));
            if(input.getInt()!=input.remaining())throw new AssertionError("Frame length differs");
            RequestHeader header=RequestHeader.parse(input);
            if(header.apiKey().id==89) {
                var req=StreamsGroupDescribeRequest.parse(new ByteBufferAccessor(input),header.apiVersion()).data();
                if(!req.includeAuthorizedOperations() || req.groupIds().isEmpty() || !Set.of("alpha","beta").containsAll(req.groupIds()))throw new AssertionError("Request fields differ");calls++;
            } else if(header.apiKey().id==10) {
                var req=FindCoordinatorRequest.parse(new ByteBufferAccessor(input),header.apiVersion()).data();
                if(req.keyType()!=0)throw new AssertionError("Not GROUP discovery");lookups++;
            } else { throw new AssertionError("Unexpected application API"); }
            if(input.hasRemaining())throw new AssertionError("Trailing request bytes");
        }
        for(String id:List.of("alpha","beta")) {
            Path path=proof.resolve(id+".result.bin");if(!Files.exists(path))continue;
            ByteBuffer input=ByteBuffer.wrap(Files.readAllBytes(path));var actual=StreamsGroupDescribeResponse.parse(new ByteBufferAccessor(input),(short)0);
            var expected=new StreamsGroupDescribeResponseData(new ByteBufferAccessor(ByteBuffer.wrap(Files.readAllBytes(fixtures.resolve(mode+"-"+id+".bin")))),(short)0);
            if(input.hasRemaining() || !actual.data().groups().equals(expected.groups()))throw new AssertionError("Full public Rust result differs");
        }
        System.out.println("{\"actual_describe_frames\":"+calls+",\"actual_GROUP_lookups\":"+lookups+"}");
    }
    public static void main(String[] args)throws Exception {
        switch(args[0]) {
            case "generate" -> generate(Path.of(args[1]),Path.of(args[2]));
            case "live" -> live(args[1],Path.of(args[2]),args[3],Path.of(args[4]));
            case "parse" -> parse(Path.of(args[1]),Path.of(args[2]),args[3]);
            default -> throw new IllegalArgumentException("Unknown mode");
        }
    }
}
