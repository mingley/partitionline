/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Properties;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

/** Actual public options, request builder and generated wire serializers. */
public class ReassignmentOptionsV1 {
    static final Map<String,String> PINS = Map.of(
        "4.1.2", "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
        "4.2.1", "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
        "4.3.1", "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36");
    static String sha(byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }
    static byte[] encode(Message m, short v) {
        var cache = new ObjectSerializationCache();
        var buffer = ByteBuffer.allocate(m.size(cache, v));
        m.write(new ByteBufferAccessor(buffer), cache, v);
        if (buffer.hasRemaining()) throw new AssertionError("size/write mismatch");
        return buffer.array();
    }
    static void file(Path p, byte[] expected, boolean verify) throws Exception {
        if (verify) {
            if (!Arrays.equals(Files.readAllBytes(p), expected)) throw new AssertionError("bytes differ: " + p);
        } else Files.write(p, expected);
    }
    static void publicAdmin(String bootstrap, String profile) throws Exception {
        boolean allow = profile.endsWith("true");
        boolean unsupported = profile.equals("public-v0-false") || profile.equals("public-missing-false");
        var config = new Properties();
        config.setProperty("bootstrap.servers",bootstrap);
        config.setProperty("request.timeout.ms","1000");
        config.setProperty("default.api.timeout.ms","2000");
        config.setProperty("retry.backoff.ms","5");
        config.setProperty("retry.backoff.max.ms","10");
        config.setProperty("reconnect.backoff.ms","0");
        var input = new LinkedHashMap<TopicPartition,Optional<NewPartitionReassignment>>();
        input.put(new TopicPartition("topic",0),Optional.of(new NewPartitionReassignment(List.of(1,2,3))));
        input.put(new TopicPartition("topic",1),Optional.of(new NewPartitionReassignment(List.of(2,3))));
        input.put(new TopicPartition("topic",2),Optional.empty());
        int results = 0;
        try (Admin admin = Admin.create(config)) {
            var outcome = admin.alterPartitionReassignments(input,
                new AlterPartitionReassignmentsOptions().allowReplicationFactorChange(allow).timeoutMs(2000));
            for (var entry : outcome.values().entrySet()) {
                short code = 0;
                try { entry.getValue().get(5,TimeUnit.SECONDS); }
                catch (ExecutionException error) { code = Errors.forException(error.getCause()).code(); }
                int expected = unsupported ? 35 : !allow && entry.getKey().partition()==1 ? 38 : 0;
                if (code != expected) throw new AssertionError("partition "+entry.getKey()+": expected "+expected+" got "+code);
                results++;
            }
            if (results != 3) throw new AssertionError("missing partition results");
            admin.close(Duration.ofSeconds(1));
        }
        System.out.println("{\"status\":\"pass\",\"public_admin\":true,\"profile\":\""+profile+"\",\"partitions\":3,\"admin_closed\":true}");
    }
    static void capture(Path directory) throws Exception {
        int requests = 0, responses = 0;
        try (var paths = Files.list(directory)) {
            for (Path path : paths.filter(p->p.getFileName().toString().endsWith("-request.bin")).sorted().toList()) {
                byte[] frame = Files.readAllBytes(path);
                var bytes = ByteBuffer.wrap(frame);
                if (bytes.getInt() != bytes.remaining()) throw new AssertionError("request frame length");
                var header = RequestHeader.parse(bytes);
                if (header.apiKey().id != 45) continue;
                short version = header.apiVersion();
                int at = bytes.position();
                var request = new AlterPartitionReassignmentsRequestData(new ByteBufferAccessor(bytes),version);
                if (bytes.hasRemaining() || !Arrays.equals(encode(request,version),Arrays.copyOfRange(frame,at,frame.length)))
                    throw new AssertionError("request wire fields: "+path);
                new AlterPartitionReassignmentsRequest.Builder(request).build(version);
                if (request.timeoutMs() > 2000) throw new AssertionError("reset request timeout");
                requests++;
                Path reply = path.resolveSibling(path.getFileName().toString().replace("-request.bin","-response.bin"));
                if (Files.exists(reply)) {
                    byte[] responseFrame = Files.readAllBytes(reply); var input = ByteBuffer.wrap(responseFrame);
                    if (input.getInt() != input.remaining()) throw new AssertionError("response frame length");
                    var responseHeader = ResponseHeader.parse(input,(short)1);
                    if (responseHeader.correlationId()!=header.correlationId()) throw new AssertionError("response correlation");
                    int start=input.position();
                    var response=new AlterPartitionReassignmentsResponseData(new ByteBufferAccessor(input),version);
                    if(input.hasRemaining() || !Arrays.equals(encode(response,version),Arrays.copyOfRange(responseFrame,start,responseFrame.length)))
                        throw new AssertionError("response wire fields: "+reply);
                    responses++;
                }
            }
        }
        System.out.println("{\"status\":\"pass\",\"mode\":\"capture\",\"requests\":"+requests+",\"responses\":"+responses+"}");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("release jar output generate|verify|rust");
        String release = args[0]; Path jar = Path.of(args[1]); Path out = Path.of(args[2]);
        Path loaded = Path.of(AlterPartitionReassignmentsRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(PINS.get(release)))
            throw new AssertionError("unmatched loaded Apache jar");
        if (args[3].startsWith("public-")) { publicAdmin(args[2],args[3]); return; }
        if (args[3].equals("capture")) { capture(out); return; }
        boolean rust = args[3].equals("rust"), verify = !args[3].equals("generate");
        if (!rust && !args[3].equals("verify") && !args[3].equals("generate")) throw new IllegalArgumentException("mode");
        if (!new AlterPartitionReassignmentsOptions().allowReplicationFactorChange())
            throw new AssertionError("default option changed");
        Files.createDirectories(out); int pairs = 0, rejected = 0;
        for (short v = 0; v <= 1; v++) {
            for (String cell : new String[]{"true", "false", "empty", "mixed", "top-error", "tagged"}) {
                boolean allow = !cell.equals("false") && !cell.equals("mixed");
                var option = new AlterPartitionReassignmentsOptions().allowReplicationFactorChange(allow).timeoutMs(2000);
                var request = new AlterPartitionReassignmentsRequestData().setTimeoutMs(option.timeoutMs())
                    .setAllowReplicationFactorChange(option.allowReplicationFactorChange());
                var response = new AlterPartitionReassignmentsResponseData().setThrottleTimeMs(7)
                    .setAllowReplicationFactorChange(allow);
                if (!cell.equals("empty")) {
                    request.setTopics(List.of(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
                        .setName("topic").setPartitions(List.of(
                            new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1, 2, 3)),
                            new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(1).setReplicas(List.of(2, 3)),
                            new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(2).setReplicas(null)))));
                    response.setResponses(List.of(new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
                        .setName("topic").setPartitions(List.of(
                            new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse().setPartitionIndex(0),
                            new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse().setPartitionIndex(1)
                                .setErrorCode((short)(allow ? 0 : 38)).setErrorMessage(allow ? null : "factor change"),
                            new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse().setPartitionIndex(2)))));
                }
                if (cell.equals("top-error")) response.setErrorCode((short)41).setErrorMessage("controller moved");
                if (cell.equals("tagged") && !rust) {
                    request.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1,2,3}));
                    response.unknownTaggedFields().add(new RawTaggedField(7,new byte[]{4,5}));
                }
                if (v == 0 && !allow) {
                    try { new AlterPartitionReassignmentsRequest.Builder(request).build(v);
                        throw new AssertionError("false policy accepted on v0");
                    } catch (UnsupportedVersionException expected) { rejected++; }
                    continue;
                }
                var factory = new AlterPartitionReassignmentsRequest.Builder(request).build(v);
                if (!factory.data().equals(request)) throw new AssertionError("factory fields differ");
                String prefix = "v"+v+"-"+cell;
                file(out.resolve(prefix+"-request.bin"), encode(request,v), verify);
                file(out.resolve(prefix+"-response.bin"), encode(response,v), verify);
                var requestBytes = ByteBuffer.wrap(Files.readAllBytes(out.resolve(prefix+"-request.bin")));
                var responseBytes = ByteBuffer.wrap(Files.readAllBytes(out.resolve(prefix+"-response.bin")));
                var decodedRequest = new AlterPartitionReassignmentsRequestData(new ByteBufferAccessor(requestBytes),v);
                var decodedResponse = new AlterPartitionReassignmentsResponseData(new ByteBufferAccessor(responseBytes),v);
                if (requestBytes.hasRemaining() || responseBytes.hasRemaining() || !request.equals(decodedRequest) || !response.equals(decodedResponse))
                    throw new AssertionError("field or complete-input mismatch: "+prefix);
                pairs++;
            }
        }
        if (pairs != 10 || rejected != 2) throw new AssertionError("missing fixture cells");
        System.out.println("{\"status\":\"pass\",\"release\":\""+release+"\",\"pairs\":"+pairs+",\"v0_false_rejections\":"+rejected+",\"mode\":\""+args[3]+"\",\"jar_sha256\":\""+PINS.get(release)+"\"}");
    }
}
