/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.InitProducerIdRequest;

/** Independent generated Apache serializers and public request factories. */
public class InitProducerIdV6Fixtures {
    static final Map<String,String> PINS = Map.of(
        "4.1.2", "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
        "4.2.1", "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
        "4.3.1", "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36");
    static String sha(byte[] b) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }
    static byte[] encode(Message m, short v) {
        var cache = new ObjectSerializationCache();
        var b = ByteBuffer.allocate(m.size(cache, v));
        m.write(new ByteBufferAccessor(b), cache, v);
        if (b.hasRemaining()) throw new AssertionError("size/write mismatch");
        return b.array();
    }
    static void file(Path p, byte[] expected, boolean verify) throws Exception {
        if (verify) {
            if (!Arrays.equals(Files.readAllBytes(p), expected)) throw new AssertionError("bytes differ: " + p);
        } else Files.write(p, expected);
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("version jar output generate|verify|rust");
        String release = args[0]; Path jar = Path.of(args[1]); Path out = Path.of(args[2]);
        Path loaded = Path.of(InitProducerIdRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded) || !sha(Files.readAllBytes(jar)).equals(PINS.get(release)))
            throw new AssertionError("unmatched loaded Apache jar");
        boolean rust = args[3].equals("rust"), verify = !args[3].equals("generate");
        if (!rust && !args[3].equals("verify") && !args[3].equals("generate")) throw new IllegalArgumentException("mode");
        Files.createDirectories(out); int pairs = 0;
        for (short v = 0; v <= 6; v++) {
            String[] cells = v < 6 ? new String[]{"ordinary"} :
                new String[]{"ordinary", "flags00", "flags01", "flags10", "flags11", "error", "tagged"};
            for (String cell : cells) {
                var req = new InitProducerIdRequestData().setTransactionalId(null).setTransactionTimeoutMs(45000);
                var resp = new InitProducerIdResponseData().setThrottleTimeMs(42).setProducerId(1234).setProducerEpoch((short)7);
                if (!cell.equals("ordinary")) {
                    req.setTransactionalId("tid").setProducerId(1234).setProducerEpoch((short)7);
                    resp.setOngoingTxnProducerId(9999).setOngoingTxnProducerEpoch((short)11);
                }
                if (cell.startsWith("flags")) {
                    req.setEnable2Pc(cell.charAt(5)=='1').setKeepPreparedTxn(cell.charAt(6)=='1');
                }
                if (cell.equals("error")) resp.setErrorCode((short)90);
                if (cell.equals("tagged")) {
                    req.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1,2,3}));
                    resp.unknownTaggedFields().add(new RawTaggedField(7,new byte[]{4,5}));
                }
                var factory = new InitProducerIdRequest.Builder(req).build(v);
                if (!factory.data().equals(req)) throw new AssertionError("factory fields differ");
                byte[] request = encode(req,v), response = encode(resp,v);
                String prefix = "v"+v+"-"+cell;
                if (rust && cell.equals("tagged")) {
                    // The codec skips unknown tags. Rust emits the same known fields.
                    req.unknownTaggedFields().clear();resp.unknownTaggedFields().clear();
                    request=encode(req,v);response=encode(resp,v);
                }
                file(out.resolve(prefix+"-request.bin"), request, verify);
                file(out.resolve(prefix+"-response.bin"), response, verify);
                var requestBytes = ByteBuffer.wrap(Files.readAllBytes(out.resolve(prefix+"-request.bin")));
                var responseBytes = ByteBuffer.wrap(Files.readAllBytes(out.resolve(prefix+"-response.bin")));
                var decodedReq = new InitProducerIdRequestData(new ByteBufferAccessor(requestBytes),v);
                var decodedResp = new InitProducerIdResponseData(new ByteBufferAccessor(responseBytes),v);
                if (requestBytes.hasRemaining() || responseBytes.hasRemaining() || !req.equals(decodedReq) || !resp.equals(decodedResp))
                    throw new AssertionError("fields or complete input differ: "+prefix);
                pairs++;
            }
        }
        System.out.println("{\"status\":\"pass\",\"release\":\""+release+"\",\"pairs\":"+pairs+",\"mode\":\""+args[3]+"\",\"jar_sha256\":\""+PINS.get(release)+"\"}");
    }
}
