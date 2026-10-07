import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;

/** Actual Apache builders, error responses and parsing of public Rust socket frames. */
public final class StreamsHeartbeatOracle {
    private StreamsHeartbeatOracle() { }

    private static byte[] bytes(Message message) {
        ByteBuffer buffer = MessageUtil.toByteBufferAccessor(message, (short) 0).buffer();
        byte[] result = new byte[buffer.remaining()];buffer.get(result);return result;
    }

    private static StreamsGroupHeartbeatRequestData request(Path fixtures) throws Exception {
        return new StreamsGroupHeartbeatRequestData(new ByteBufferAccessor(ByteBuffer.wrap(
            Files.readAllBytes(fixtures.resolve("heartbeat-request-full.bin")))), (short) 0);
    }

    private static StreamsGroupHeartbeatRequest build(StreamsGroupHeartbeatRequestData data) throws Exception {
        StreamsGroupHeartbeatRequest.Builder builder;
        try {
            Constructor<StreamsGroupHeartbeatRequest.Builder> constructor =
                StreamsGroupHeartbeatRequest.Builder.class.getConstructor(StreamsGroupHeartbeatRequestData.class, boolean.class);
            builder = constructor.newInstance(data, true);
        } catch (NoSuchMethodException stableOnly) {
            builder = new StreamsGroupHeartbeatRequest.Builder(data);
        }
        return builder.build((short) 0);
    }

    private static void emit(Path out, Path fixtures, String name, int epoch, String changes,
                             Errors error, List<String> index) throws Exception {
        StreamsGroupHeartbeatRequestData data = request(fixtures).setMemberEpoch(epoch);
        if (changes.equals("null")) {
            data.setTopology(null).setInstanceId(null).setRackId(null).setActiveTasks(null)
                .setStandbyTasks(null).setWarmupTasks(null).setProcessId(null).setUserEndpoint(null)
                .setClientTags(null).setTaskOffsets(null).setTaskEndOffsets(null);
        } else if (changes.equals("empty")) {
            data.setInstanceId("").setRackId("").setProcessId("").setActiveTasks(List.of())
                .setStandbyTasks(List.of()).setWarmupTasks(List.of()).setClientTags(List.of())
                .setTaskOffsets(List.of()).setTaskEndOffsets(List.of())
                .setTopology(new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of()));
        }
        StreamsGroupHeartbeatRequest built = build(data);
        StreamsGroupHeartbeatResponseData response;
        if (error == Errors.NONE) {
            response = new StreamsGroupHeartbeatResponseData(new ByteBufferAccessor(ByteBuffer.wrap(
                Files.readAllBytes(fixtures.resolve("heartbeat-response-full.bin")))), (short) 0)
                .setErrorCode((short) 0).setMemberId(data.memberId()).setMemberEpoch(epoch < 0 ? epoch : epoch + 1);
        } else {
            response = ((StreamsGroupHeartbeatResponse) built.getErrorResponse(42, error.exception())).data();
            if (response.errorCode() != error.code()) { throw new IllegalStateException("Error factory mismatch"); }
        }
        Files.write(out.resolve(name + ".request.bin"), bytes(built.data()));
        Files.write(out.resolve(name + ".response.bin"), bytes(response));
        index.add(name + "\t" + error.code() + "\t" + error.name() + "\n");
    }

    private static void generate(Path fixtures, Path out) throws Exception {
        Files.createDirectory(out);List<String> index = new ArrayList<>();
        for (int epoch : new int[] {0, 7, -1, -2}) {
            emit(out, fixtures, "epoch-" + epoch, epoch, "full", Errors.NONE, index);
        }
        emit(out, fixtures, "null-changes", 7, "null", Errors.NONE, index);
        emit(out, fixtures, "empty-changes", 7, "empty", Errors.NONE, index);
        for (short code : new short[] {14, 15, 16, 31, 53, 79}) {
            emit(out, fixtures, "error-" + code, 0, "full", Errors.forCode(code), index);
        }
        for (Errors error : Errors.values()) {
            if (error.name().startsWith("STREAMS_")) {
                emit(out, fixtures, "error-" + error.code(), 0, "full", error, index);
            }
        }
        String defaultOutcome;
        try { new StreamsGroupHeartbeatRequest.Builder(request(fixtures)).build((short) 0);defaultOutcome = "accepted"; }
        catch (RuntimeException unsupported) { defaultOutcome = unsupported.getClass().getName(); }
        Files.writeString(out.resolve("builder-default.txt"), defaultOutcome + "\n");
        Files.writeString(out.resolve("cases.tsv"), "name\tcode\terror_name\n" + String.join("", index));
        System.out.println("{\"actual_builder_cases\":" + index.size() + ",\"broker_execution\":false}");
    }

    private static void parse(Path fixtures, Path proof) throws Exception {
        int calls = 0;int heartbeats = 0;int lookups = 0;
        for (String row : Files.readAllLines(proof.resolve("cases.tsv"))) {
            if (row.startsWith("name\t")) { continue; }
            String[] cells = row.split("\t", -1);String name = cells[0];int count = Integer.parseInt(cells[1]);
            String sourceName = name.substring(0, name.lastIndexOf("-fc"));
            StreamsGroupHeartbeatRequestData expected = new StreamsGroupHeartbeatRequestData(new ByteBufferAccessor(
                ByteBuffer.wrap(Files.readAllBytes(fixtures.resolve(sourceName + ".request.bin")))), (short) 0);
            for (int i = 0; i < count; i++) {
                ByteBuffer input = ByteBuffer.wrap(Files.readAllBytes(proof.resolve(name + "-" + i + ".request.bin")));
                if (input.getInt() != input.remaining()) { throw new IllegalStateException("Frame size mismatch"); }
                RequestHeader header = RequestHeader.parse(input);
                if (header.apiKey().id == 88) {
                    StreamsGroupHeartbeatRequest actual = StreamsGroupHeartbeatRequest.parse(new ByteBufferAccessor(input), header.apiVersion());
                    if (!actual.data().equals(expected)) { throw new IllegalStateException("Heartbeat fields differ"); }
                    heartbeats++;
                } else if (header.apiKey().id == 10) {
                    FindCoordinatorRequest actual = FindCoordinatorRequest.parse(new ByteBufferAccessor(input), header.apiVersion());
                    if (actual.data().keyType() != 0 || (header.apiVersion() >= 4 ?
                        !actual.data().coordinatorKeys().equals(List.of(expected.groupId())) :
                        !actual.data().key().equals(expected.groupId()))) {
                        throw new IllegalStateException("GROUP coordinator request differs");
                    }
                    lookups++;
                } else { throw new IllegalStateException("Unexpected captured application API"); }
                if (input.hasRemaining()) { throw new IllegalStateException("Unconsumed application frame"); }
            }
            String expectedFile = cells[2];
            ByteBuffer expectedInput = ByteBuffer.wrap(Files.readAllBytes(fixtures.resolve(expectedFile)));
            ByteBuffer rustInput = ByteBuffer.wrap(Files.readAllBytes(proof.resolve(name + ".result.bin")));
            StreamsGroupHeartbeatResponse expectedResponse = StreamsGroupHeartbeatResponse.parse(new ByteBufferAccessor(expectedInput), (short) 0);
            StreamsGroupHeartbeatResponse rustResponse = StreamsGroupHeartbeatResponse.parse(new ByteBufferAccessor(rustInput), (short) 0);
            if (expectedInput.hasRemaining() || rustInput.hasRemaining() || !expectedResponse.data().equals(rustResponse.data()) ||
                !Arrays.equals(bytes(expectedResponse.data()), bytes(rustResponse.data())) ||
                !expectedResponse.errorCounts().equals(rustResponse.errorCounts())) {
                throw new IllegalStateException("Public typed response differs from actual SDK");
            }
            calls++;
        }
        System.out.println("{\"public_Rust_calls\":" + calls + ",\"actual_heartbeat_frames\":" + heartbeats +
            ",\"actual_GROUP_discovery_frames\":" + lookups + ",\"broker_execution\":false}");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) { throw new IllegalArgumentException("generate/parse fixture/proof directories required"); }
        switch (args[0]) {
            case "generate" -> generate(Path.of(args[1]), Path.of(args[2]));
            case "parse" -> parse(Path.of(args[1]), Path.of(args[2]));
            default -> throw new IllegalArgumentException("Unknown mode");
        }
    }
}
