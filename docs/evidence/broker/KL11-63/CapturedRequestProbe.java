/* Parse captured native requests with the actual official Apache classes. */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import org.apache.kafka.common.requests.*;
public final class CapturedRequestProbe {
    private CapturedRequestProbe() { }
    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
    public static void main(String[] args) throws Exception {
        byte[] input = Files.readAllBytes(Path.of(args[0]));
        ByteBuffer buffer = ByteBuffer.wrap(input);
        RequestHeader header = RequestHeader.parse(buffer);
        int headerLength = buffer.position();
        AbstractRequest request = AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(),
            new org.apache.kafka.common.protocol.ByteBufferAccessor(buffer)).request;
        int consumed = buffer.position();
        byte[] tail = new byte[buffer.remaining()]; buffer.get(tail);
        ByteBuffer canonical = request.serializeWithHeader(header);
        byte[] serialized = new byte[canonical.remaining()]; canonical.get(serialized);
        MetadataRequest metadata = (MetadataRequest) request;
        System.out.println("{\"input_length\":" + input.length + ",\"header_length\":" + headerLength
            + ",\"consumed\":" + consumed + ",\"remaining\":" + tail.length
            + ",\"remaining_hex\":" + quote(HexFormat.of().formatHex(tail))
            + ",\"api_key\":" + header.apiKey().id + ",\"version\":" + header.apiVersion()
            + ",\"is_all_topics\":" + metadata.isAllTopics()
            + ",\"allow_auto_topic_creation\":" + metadata.allowAutoTopicCreation()
            + ",\"include_topic_authorized_operations\":" + metadata.data().includeTopicAuthorizedOperations()
            + ",\"apache_parsed_request\":" + quote(metadata.data().toString())
            + ",\"canonical_hex\":" + quote(HexFormat.of().formatHex(serialized)) + "}");
    }
}
