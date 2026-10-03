/* Actual Apache canonical serializer/crypto, matched to the numeric digest of
 * the public synthetic native fixture. This does not log live SASL tokens. */
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

public final class NativeAlterFixture {
    private NativeAlterFixture() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new AssertionError("exact output directory");
        Path output = Path.of(args[0]);
        Files.createDirectories(output);
        HexFormat hex = HexFormat.of();
        byte[] salt = hex.parseHex("000102030405060708090a0b0c0d0e0f");
        byte[] salted = new ScramFormatter(ScramMechanism.SCRAM_SHA_256)
                .saltedPassword("pencil", salt, 4096);
        AlterUserScramCredentialsRequestData data = new AlterUserScramCredentialsRequestData()
                .setUpsertions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                        .setName("native-created").setMechanism((byte) 1).setIterations(4096)
                        .setSalt(salt).setSaltedPassword(salted)));
        byte[] header = SaslWireOracle.serialize(new RequestHeader(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                (short) 0, "sasl-native-peer", 7).data(), (short) 2);
        byte[] payload = SaslWireOracle.concat(header, SaslWireOracle.serialize(data, (short) 0));
        byte[] canonical = SaslWireOracle.frame(payload);
        byte[] nativeFrame = SaslWireOracle.frame(SaslWireOracle.concat(payload, new byte[] {0}));
        if (canonical.length != 105 || nativeFrame.length != 106)
            throw new AssertionError("expected exact public frame sizes");
        Files.writeString(output.resolve("native-alter-canonical.frame.hex"), hex.formatHex(canonical) + "\n");
        Files.writeString(output.resolve("native-alter-redundant-empty-tag.frame.hex"), hex.formatHex(nativeFrame) + "\n");
        System.out.println("{\"canonical_bytes\":105,\"native_bytes\":106,\"canonical_sha256\":\""
                + SaslWireOracle.hash(canonical) + "\",\"native_sha256\":\"" + SaslWireOracle.hash(nativeFrame) + "\"}");
    }
}
