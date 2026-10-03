/* Apache generated serializers/parsers and public KL11-66 crypto vectors.
 * This fixture oracle is not a server or a live listener interoperability claim.
 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

public final class SaslWireOracle {
    private static final HexFormat HEX = HexFormat.of();
    private static final byte[] SALT = HEX.parseHex("000102030405060708090a0b0c0d0e0f");
    private static final List<String> ROWS = new ArrayList<>();
    private static final List<String> PARSER = new ArrayList<>(List.of(
            "kind\tname\tapi\tversion\ttruncated_rejected\ttrailing_bytes_remaining"));
    private static int correlation = 1000;
    private SaslWireOracle() { }

    private static void require(boolean ok, String context) {
        if (!ok) throw new AssertionError(context);
    }
    static byte[] serialize(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer bytes = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(bytes), cache, version);
        require(!bytes.hasRemaining(), "serializer size mismatch");
        return bytes.array();
    }
    static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }
    static byte[] frame(byte[] payload) {
        return ByteBuffer.allocate(payload.length + 4).putInt(payload.length).put(payload).array();
    }
    static String hash(byte[] bytes) throws Exception {
        return HEX.formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static Message parse(Message message, byte[] body, short version) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(body);
        Message parsed = (Message) message.getClass().getConstructor(
                org.apache.kafka.common.protocol.Readable.class, short.class)
                .newInstance(new ByteBufferAccessor(bytes), version);
        require(!bytes.hasRemaining(), "parser did not consume fixture");
        require(message.equals(parsed), "parsed object differs");
        require(Arrays.equals(body, serialize(parsed, version)), "roundtrip differs");
        ByteBuffer trailing = ByteBuffer.wrap(concat(body, new byte[] {42}));
        message.getClass().getConstructor(org.apache.kafka.common.protocol.Readable.class, short.class)
                .newInstance(new ByteBufferAccessor(trailing), version);
        require(trailing.remaining() == 1, "unexpected Apache trailing-byte behavior");
        return parsed;
    }
    private static void emit(String kind, String name, ApiKeys api, short version,
                             short error, Message message) throws Exception {
        byte[] body = serialize(message, version);
        parse(message, body, version);
        boolean truncatedRejected = false;
        try {
            message.getClass().getConstructor(org.apache.kafka.common.protocol.Readable.class, short.class)
                    .newInstance(new ByteBufferAccessor(ByteBuffer.wrap(Arrays.copyOf(body, body.length - 1))), version);
        } catch (java.lang.reflect.InvocationTargetException expected) {
            require(expected.getCause() instanceof RuntimeException, "unexpected parser failure type");
            truncatedRejected = true;
        }
        require(truncatedRejected, "truncated fixture accepted");
        PARSER.add(String.join("\t", kind, name, Short.toString(api.id), Short.toString(version), "true", "1"));
        int id = ++correlation;
        short headerVersion;
        byte[] header;
        if (kind.equals("request")) {
            RequestHeader request = new RequestHeader(api, version, "public-wire-oracle", id);
            headerVersion = request.headerVersion();
            header = serialize(request.data(), headerVersion);
            ByteBuffer check = ByteBuffer.wrap(concat(header, body));
            RequestHeader parsed = RequestHeader.parse(check);
            require(parsed.correlationId() == id && parsed.apiKey() == api
                    && parsed.apiVersion() == version && check.remaining() == body.length,
                    "request header parse mismatch");
        } else {
            headerVersion = api.responseHeaderVersion(version);
            ResponseHeader response = new ResponseHeader(id, headerVersion);
            header = serialize(response.data(), headerVersion);
            ByteBuffer check = ByteBuffer.wrap(concat(header, body));
            require(ResponseHeader.parse(check, headerVersion).correlationId() == id
                    && check.remaining() == body.length, "response header parse mismatch");
        }
        byte[] payload = concat(header, body);
        ROWS.add(String.join("\t", kind, name, Short.toString(api.id), Short.toString(version),
                Short.toString(headerVersion), Integer.toString(id), Short.toString(error),
                HEX.formatHex(body), HEX.formatHex(payload), HEX.formatHex(frame(payload))));
    }
    private static byte[][] transcript(Path path, String mechanism) throws Exception {
        for (String row : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String[] cells = row.split("\t", -1);
            if (!cells[0].equals("canonical-basic") || !cells[1].equals(mechanism)) continue;
            byte[][] values = new byte[4][];
            for (int index = 0; index < 4; index++) values[index] = HEX.parseHex(cells[index + 5]);
            ScramFormatter formatter = new ScramFormatter(mechanism.equals("SCRAM-SHA-256")
                    ? ScramMechanism.SCRAM_SHA_256 : ScramMechanism.SCRAM_SHA_512);
            byte[] salted = formatter.saltedPassword("pencil", SALT, 4096);
            require(Arrays.equals(formatter.storedKey(formatter.clientKey(salted)),
                    HEX.parseHex(cells[9])), "reference StoredKey differs");
            require(Arrays.equals(formatter.serverKey(salted), HEX.parseHex(cells[10])),
                    "reference ServerKey differs");
            return values;
        }
        throw new AssertionError("canonical mechanism vector missing");
    }
    private static AlterUserScramCredentialsRequestData.ScramCredentialUpsertion upsert(
            String username, byte mechanism, String password) throws Exception {
        ScramFormatter formatter = new ScramFormatter(mechanism == 1
                ? ScramMechanism.SCRAM_SHA_256 : ScramMechanism.SCRAM_SHA_512);
        return new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                .setName(username).setMechanism(mechanism).setIterations(4096).setSalt(SALT.clone())
                .setSaltedPassword(formatter.saltedPassword(password, SALT, 4096));
    }
    private static void handshake() throws Exception {
        for (short version = 0; version <= 1; version++) {
            for (String mechanism : List.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER")) {
                emit("request", "handshake-" + mechanism.toLowerCase(), ApiKeys.SASL_HANDSHAKE,
                        version, (short) -1, new SaslHandshakeRequestData().setMechanism(mechanism));
            }
            emit("response", "handshake-tls-enabled", ApiKeys.SASL_HANDSHAKE, version, (short) 0,
                    new SaslHandshakeResponseData().setMechanisms(List.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")));
            emit("response", "handshake-plaintext-scram", ApiKeys.SASL_HANDSHAKE, version, (short) 0,
                    new SaslHandshakeResponseData().setMechanisms(List.of("SCRAM-SHA-256", "SCRAM-SHA-512")));
            for (Errors error : List.of(Errors.UNSUPPORTED_SASL_MECHANISM, Errors.ILLEGAL_SASL_STATE)) {
                SaslHandshakeRequest request = new SaslHandshakeRequest(
                        new SaslHandshakeRequestData().setMechanism("PLAIN"), version);
                emit("response", "handshake-library-error-" + error.code(), ApiKeys.SASL_HANDSHAKE,
                        version, error.code(), request.getErrorResponse(0,
                                error.exception("public fixture error")).data());
            }
        }
    }
    private static void authenticate(byte[][] sha256, byte[][] sha512) throws Exception {
        for (short version = 0; version <= 2; version++) {
            List<String> names = List.of("plain", "empty", "sha256-first", "sha256-final", "sha512-first", "sha512-final");
            byte[][] tokens = {new byte[] {0, 'u', 's', 'e', 'r', 0, 'p', 'e', 'n', 'c', 'i', 'l'},
                    new byte[0], sha256[0], sha256[2], sha512[0], sha512[2]};
            for (int index = 0; index < names.size(); index++) {
                emit("request", "authenticate-" + names.get(index), ApiKeys.SASL_AUTHENTICATE,
                        version, (short) -1, new SaslAuthenticateRequestData().setAuthBytes(tokens[index]));
            }
            if (version == 2) {
                SaslAuthenticateRequestData tagged = new SaslAuthenticateRequestData().setAuthBytes(sha256[0]);
                tagged.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {1, 2, 3}));
                emit("request", "authenticate-unknown-tag", ApiKeys.SASL_AUTHENTICATE,
                        version, (short) -1, tagged);
            }
            List<String> replies = List.of("plain", "sha256-challenge", "sha256-final", "sha512-challenge", "sha512-final");
            byte[][] responseTokens = {new byte[0], sha256[1], sha256[3], sha512[1], sha512[3]};
            for (int index = 0; index < replies.size(); index++) {
                emit("response", "authenticate-" + replies.get(index), ApiKeys.SASL_AUTHENTICATE,
                        version, (short) 0, new SaslAuthenticateResponseData()
                                .setAuthBytes(responseTokens[index]).setSessionLifetimeMs(0));
            }
            for (Errors error : List.of(Errors.SASL_AUTHENTICATION_FAILED, Errors.ILLEGAL_SASL_STATE)) {
                SaslAuthenticateRequest request = new SaslAuthenticateRequest(new SaslAuthenticateRequestData(), version);
                emit("response", "authenticate-library-error-" + error.code(), ApiKeys.SASL_AUTHENTICATE,
                        version, error.code(), request.getErrorResponse(0,
                                error.exception("public fixture error")).data());
            }
        }
    }
    private static void describe() throws Exception {
        DescribeUserScramCredentialsRequestData named = new DescribeUserScramCredentialsRequestData().setUsers(List.of(
                new DescribeUserScramCredentialsRequestData.UserName().setName("user"),
                new DescribeUserScramCredentialsRequestData.UserName().setName("missing")));
        for (String variant : List.of("null", "empty", "named", "duplicate", "unknown-tag")) {
            DescribeUserScramCredentialsRequestData request = named.duplicate();
            if (variant.equals("null")) request.setUsers(null);
            if (variant.equals("empty")) request.setUsers(List.of());
            if (variant.equals("duplicate")) request.setUsers(List.of(named.users().get(0), named.users().get(0).duplicate()));
            if (variant.equals("unknown-tag")) request.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {42}));
            emit("request", "describe-" + variant, ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
                    (short) 0, (short) -1, request);
        }
        DescribeUserScramCredentialsResponseData metadata = new DescribeUserScramCredentialsResponseData().setResults(List.of(
                new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser("user")
                        .setCredentialInfos(List.of(
                                new DescribeUserScramCredentialsResponseData.CredentialInfo().setMechanism((byte) 1).setIterations(4096),
                                new DescribeUserScramCredentialsResponseData.CredentialInfo().setMechanism((byte) 2).setIterations(8192)))));
        emit("response", "describe-metadata-only", ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, (short) 0, (short) 0, metadata);
        for (Errors error : List.of(Errors.RESOURCE_NOT_FOUND, Errors.DUPLICATE_RESOURCE)) {
            emit("response", "describe-user-error-" + error.code(), ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
                    (short) 0, error.code(), new DescribeUserScramCredentialsResponseData().setResults(List.of(
                            new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                                    .setUser("user").setErrorCode(error.code()).setErrorMessage("public fixture error"))));
        }
        DescribeUserScramCredentialsRequest request = DescribeUserScramCredentialsRequest.parse(
                new ByteBufferAccessor(ByteBuffer.wrap(serialize(named, (short) 0))), (short) 0);
        emit("response", "describe-library-error-31", ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS,
                (short) 0, Errors.CLUSTER_AUTHORIZATION_FAILED.code(), request.getErrorResponse(0,
                        Errors.CLUSTER_AUTHORIZATION_FAILED.exception("public fixture error")).data());
    }
    private static void alter() throws Exception {
        for (String variant : List.of("upsert256", "upsert512", "both", "delete", "duplicate", "invalid-iterations", "empty-salt", "short-salted", "unknown-tag")) {
            AlterUserScramCredentialsRequestData request = new AlterUserScramCredentialsRequestData();
            var first = upsert("user", (byte) 1, "pencil");
            if (variant.equals("upsert512")) first = upsert("user", (byte) 2, "pencil");
            if (variant.equals("invalid-iterations")) first.setIterations(1);
            if (variant.equals("empty-salt")) first.setSalt(new byte[0]);
            if (variant.equals("short-salted")) first.setSaltedPassword(new byte[] {42});
            request.setUpsertions(List.of(first));
            if (variant.equals("both")) request.setUpsertions(List.of(first, upsert("user", (byte) 2, "pencil")));
            if (variant.equals("duplicate")) request.setUpsertions(List.of(first, first.duplicate()));
            if (variant.equals("delete")) request.setUpsertions(List.of()).setDeletions(List.of(
                    new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName("user").setMechanism((byte) 1)));
            if (variant.equals("unknown-tag")) request.unknownTaggedFields().add(new RawTaggedField(13, new byte[] {4, 5}));
            emit("request", "alter-" + variant, ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, (short) 0, (short) -1, request);
        }
        emit("response", "alter-success", ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, (short) 0, (short) 0,
                new AlterUserScramCredentialsResponseData().setResults(List.of(
                        new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult().setUser("user"))));
        AlterUserScramCredentialsRequest request = AlterUserScramCredentialsRequest.parse(new ByteBufferAccessor(ByteBuffer.wrap(
                serialize(new AlterUserScramCredentialsRequestData().setUpsertions(List.of(upsert("user", (byte) 1, "pencil"))),
                        (short) 0))), (short) 0);
        for (Errors error : List.of(Errors.CLUSTER_AUTHORIZATION_FAILED, Errors.RESOURCE_NOT_FOUND,
                Errors.DUPLICATE_RESOURCE, Errors.UNACCEPTABLE_CREDENTIAL)) {
            emit("response", "alter-library-error-" + error.code(), ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                    (short) 0, error.code(), request.getErrorResponse(0, error.exception("public fixture error")).data());
        }
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 2, "usage: outputDirectory cryptoFixture");
        Path output = Path.of(args[0]);
        Files.createDirectories(output);
        byte[][] sha256 = transcript(Path.of(args[1]), "SCRAM-SHA-256");
        byte[][] sha512 = transcript(Path.of(args[1]), "SCRAM-SHA-512");
        ROWS.add("kind\tname\tapi\tversion\theader_version\tcorrelation\terror_code\tbody_hex\tpayload_hex\tframe_hex");
        handshake(); authenticate(sha256, sha512); describe(); alter();
        Files.write(output.resolve("apache-wire.tsv"), ROWS, StandardCharsets.UTF_8);
        List<String> errors = new ArrayList<>(List.of("name\tcode"));
        for (Errors error : List.of(Errors.UNSUPPORTED_SASL_MECHANISM, Errors.ILLEGAL_SASL_STATE,
                Errors.SASL_AUTHENTICATION_FAILED, Errors.CLUSTER_AUTHORIZATION_FAILED,
                Errors.RESOURCE_NOT_FOUND, Errors.DUPLICATE_RESOURCE, Errors.UNACCEPTABLE_CREDENTIAL)) {
            errors.add(error.name() + "\t" + error.code());
        }
        Files.write(output.resolve("apache-errors.tsv"), errors, StandardCharsets.UTF_8);
        Files.write(output.resolve("apache-parser-outcomes.tsv"), PARSER, StandardCharsets.UTF_8);
        List<String> bootstrap = new ArrayList<>(List.of(
                "user\tmechanism\titerations\tsalt_hex\tsalted_password_hex"));
        for (String user : List.of("user", "admin", "unicode")) {
            for (ScramMechanism mechanism : List.of(ScramMechanism.SCRAM_SHA_256, ScramMechanism.SCRAM_SHA_512)) {
                ScramFormatter formatter = new ScramFormatter(mechanism);
                String password = user.equals("unicode") ? "péncil-🔑" : "pencil";
                bootstrap.add(String.join("\t", user, mechanism.mechanismName(), "4096", HEX.formatHex(SALT),
                        HEX.formatHex(formatter.saltedPassword(password, SALT, 4096))));
            }
        }
        Files.write(output.resolve("apache-bootstrap.tsv"), bootstrap, StandardCharsets.UTF_8);
        System.out.println("PASS wire_cases=" + (ROWS.size() - 1) + " crypto_forms=2 correlation_headers=verified lifetime_ms=0");
    }
}
