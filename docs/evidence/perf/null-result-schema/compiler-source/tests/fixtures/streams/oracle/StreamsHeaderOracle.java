import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;

/** Reverse checks with actual generated Apache headers and Streams body readers. */
public final class StreamsHeaderOracle {
    private StreamsHeaderOracle() { }

    private static Message body(String type, ByteBuffer input) {
        ByteBufferAccessor reader = new ByteBufferAccessor(input);
        return switch (type) {
            case "StreamsGroupHeartbeatRequest" -> new StreamsGroupHeartbeatRequestData(reader, (short) 0);
            case "StreamsGroupHeartbeatResponse" -> new StreamsGroupHeartbeatResponseData(reader, (short) 0);
            case "StreamsGroupDescribeRequest" -> new StreamsGroupDescribeRequestData(reader, (short) 0);
            case "StreamsGroupDescribeResponse" -> new StreamsGroupDescribeResponseData(reader, (short) 0);
            default -> throw new IllegalArgumentException("Unknown Streams body");
        };
    }

    private static byte[] bytes(Message message, short version) {
        ByteBuffer buffer = MessageUtil.toByteBufferAccessor(message, version).buffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) { throw new IllegalArgumentException("Reverse header index required"); }
        Path index = Path.of(args[0]);
        int count = 0;
        for (String row : Files.readAllLines(index)) {
            if (row.startsWith("name\t") || row.isEmpty()) { continue; }
            String[] cells = row.split("\t", -1);
            if (cells.length != 6) { throw new IllegalArgumentException("Invalid header index"); }
            short api = Short.parseShort(cells[1]);
            boolean request = switch (cells[3]) {
                case "true" -> true;
                case "false" -> false;
                default -> throw new IllegalArgumentException("Invalid request flag");
            };
            byte[] original = Files.readAllBytes(index.getParent().resolve(cells[4]));
            byte[] rust = Files.readAllBytes(index.getParent().resolve(cells[5]));
            ByteBuffer expectedInput = ByteBuffer.wrap(original);
            ByteBuffer actualInput = ByteBuffer.wrap(rust);
            short headerVersion = (short) (request ? 2 : 1);
            Message expectedHeader;
            Message actualHeader;
            if (request) {
                RequestHeaderData expected = new RequestHeaderData(new ByteBufferAccessor(expectedInput), headerVersion);
                RequestHeaderData actual = new RequestHeaderData(new ByteBufferAccessor(actualInput), headerVersion);
                if (expected.requestApiKey() != api || expected.requestApiVersion() != 0 ||
                    expected.correlationId() != 0x11223300 + api) {
                    throw new IllegalStateException("Unexpected original request header");
                }
                expectedHeader = expected;
                actualHeader = actual;
            } else {
                ResponseHeaderData expected = new ResponseHeaderData(new ByteBufferAccessor(expectedInput), headerVersion);
                if (expected.correlationId() != 0x11223300 + api) {
                    throw new IllegalStateException("Unexpected original response correlation");
                }
                expectedHeader = expected;
                actualHeader = new ResponseHeaderData(new ByteBufferAccessor(actualInput), headerVersion);
            }
            expectedHeader.unknownTaggedFields().clear();
            int actualHeaderBytes = actualInput.position();
            Message expectedBody = body(cells[2], expectedInput);
            Message actualBody = body(cells[2], actualInput);
            if (expectedInput.hasRemaining() || actualInput.hasRemaining() ||
                !expectedHeader.equals(actualHeader) || !expectedBody.equals(actualBody) ||
                !Arrays.equals(bytes(expectedHeader, headerVersion), Arrays.copyOf(rust, actualHeaderBytes)) ||
                !Arrays.equals(bytes(expectedBody, (short) 0), Arrays.copyOfRange(rust, actualHeaderBytes, rust.length))) {
                throw new IllegalStateException("Apache reverse header/body mismatch: " + cells[0]);
            }
            String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(rust));
            System.out.println(cells[0] + "\t" + rust.length + "\tconsumed\tsemantic-equal\t" + hash);
            count++;
        }
        if (count != 12) { throw new IllegalStateException("Missing reverse headers"); }
        System.out.println("{\"actual_reverse_headers\":" + count + ",\"runtime_broker_claim\":false}");
    }
}
