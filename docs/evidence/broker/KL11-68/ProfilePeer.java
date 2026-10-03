/* Actual API18 versions0–4 on the pinned ordinary Produce five-API router. */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.protocol.ApiKeys;

public final class ProfilePeer {
    private static int checks;
    private ProfilePeer() { }
    private static void check(boolean condition, String label) { checks++; if (!condition) throw new AssertionError(label); }
    private static byte[] encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache(); ByteBuffer buffer = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(buffer), cache, version); check(!buffer.hasRemaining(), "serializer full write"); return buffer.array();
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("release port output-json");
        int port = Integer.parseInt(args[1]); List<String> history = new ArrayList<>(); boolean passed = false;
        Map<Integer, String> expected = Map.of(0, "3:13", 3, "0:13", 18, "0:4", 19, "2:4", 20, "1:6");
        try {
            for (short version = 0; version <= 4; version++) {
                int cid = 200 + version; RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "produce-profile-oracle", cid);
                ApiVersionsRequestData data = new ApiVersionsRequestData();
                if (version >= 3) data.setClientSoftwareName("profile-peer").setClientSoftwareVersion(args[0]);
                byte[] head = encode(header.data(), header.headerVersion()); byte[] body = encode(data, version);
                byte[] frame = Arrays.copyOf(head, head.length + body.length); System.arraycopy(body, 0, frame, head.length, body.length); byte[] response;
                try (Socket socket = new Socket("127.0.0.1", port)) {
                    socket.setSoTimeout(5000); DataOutputStream output = new DataOutputStream(socket.getOutputStream()); output.writeInt(frame.length); output.write(frame); output.flush();
                    DataInputStream input = new DataInputStream(socket.getInputStream()); int length = input.readInt(); check(length >= 4 && length <= 4096, "bounded response length");
                    response = input.readNBytes(length); check(response.length == length, "complete response frame");
                }
                ByteBuffer buffer = ByteBuffer.wrap(response); short hv = ApiKeys.API_VERSIONS.responseHeaderVersion(version);
                check(ResponseHeader.parse(buffer, hv).correlationId() == cid, "exact correlation");
                ApiVersionsResponseData parsed = ApiVersionsResponse.parse(new ByteBufferAccessor(buffer), version).data();
                check(!buffer.hasRemaining(), "full response consumption"); check(parsed.errorCode() == 0 && parsed.throttleTimeMs() == 0, "successful unthrottled response");
                Map<Integer, String> ranges = new HashMap<>();
                for (var entry : parsed.apiKeys()) check(ranges.put((int) entry.apiKey(), entry.minVersion() + ":" + entry.maxVersion()) == null, "unique API keys");
                check(ranges.equals(expected), "exact five-entry ordinary Produce profile, no Fetch/ListOffsets");
                history.add("{\"api_key\":18,\"api_version\":" + version + ",\"request_header_version\":" + header.headerVersion()
                    + ",\"response_header_version\":" + hv + ",\"request_hex\":\"" + HexFormat.of().formatHex(frame)
                    + "\",\"response_hex\":\"" + HexFormat.of().formatHex(response) + "\",\"five_entry_profile_confirmed\":true}");
            }
            passed = true; System.out.println("{\"release\":\"" + args[0] + "\",\"exchanges\":5,\"assertions\":" + checks + ",\"passed\":true}");
        } finally {
            Files.writeString(Path.of(args[2]), "{\"release\":\"" + args[0] + "\",\"passed\":" + passed + ",\"assertions\":" + checks
                + ",\"expected_profile\":\"0:3–13,3:0–13,18:0–4,19:2–4,20:1–6\",\"history\":[" + String.join(",", history) + "]}\n");
        }
    }
}
