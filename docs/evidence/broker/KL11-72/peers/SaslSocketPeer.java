/* Independent bounded socket peer: Apache generated wire classes and actual
 * ScramSaslClient/JDK PLAIN, against the real Rust listener. Public test users
 * and passwords are synthetic fixtures. No auth token/proof/password logging.
 */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramMessages;
import org.apache.kafka.common.security.scram.internals.ScramSaslClient;

public final class SaslSocketPeer {
    private static final int MAX_FRAME = 1024 * 1024;
    private static final byte[] SALT = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f");
    private static final String PASSWORD = "pencil";
    private static final String ROTATED = "rotated-public-fixture";
    private static int assertions;
    private static int correlation = 1000;
    private static final HashSet<String> SERVER_NONCES = new HashSet<>();
    private SaslSocketPeer() { }
    private static void require(boolean value, String label) {
        assertions++;
        if (!value) throw new AssertionError(label);
    }
    private static void receipt(String operation, String variant, int error) {
        System.out.printf("{\"operation\":\"%s\",\"variant\":\"%s\",\"error\":%d}%n", operation, variant, error);
    }
    private static void closedReceipt(String operation, String variant) {
        System.out.printf("{\"operation\":\"%s\",\"variant\":\"%s\",\"socket_outcome\":\"terminal\",\"protocol_error\":null}%n", operation, variant);
    }
    private static CallbackHandler callback(String username, String password) {
        return callbacks -> {
            for (Callback item : callbacks) {
                if (item instanceof NameCallback name) name.setName(username);
                else if (item instanceof PasswordCallback secret) secret.setPassword(password.toCharArray());
                else throw new UnsupportedCallbackException(item);
            }
        };
    }
    private static SaslClient client(String mechanism, String user, String password) throws Exception {
        if (!mechanism.equals("PLAIN")) return new ScramSaslClient(
                mechanism.equals("SCRAM-SHA-256") ? ScramMechanism.SCRAM_SHA_256 : ScramMechanism.SCRAM_SHA_512,
                callback(user, password));
        SaslClient result = Sasl.createSaslClient(new String[] {"PLAIN"}, null, "kafka", "localhost",
                Map.of(Sasl.POLICY_NOPLAINTEXT, "false"), callback(user, password));
        require(result != null, "JDK PLAIN client available");
        return result;
    }
    private static SSLContext context(Path ca) throws Exception {
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        try (var input = Files.newInputStream(ca)) {
            trust.setCertificateEntry("public-test-ca", CertificateFactory.getInstance("X.509").generateCertificate(input));
        }
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(trust);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(null, factory.getTrustManagers(), null);
        return tls;
    }
    private static final class Connection implements AutoCloseable {
        final Socket socket;
        final DataInputStream input;
        final DataOutputStream output;
        Connection(int port, SSLContext tls) throws Exception {
            socket = tls == null ? new Socket() : tls.getSocketFactory().createSocket();
            socket.connect(new InetSocketAddress("localhost", port), 5000);
            socket.setSoTimeout(6000);
            if (socket instanceof SSLSocket secure) {
                SSLParameters parameters = secure.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                secure.setSSLParameters(parameters);
                secure.startHandshake();
            }
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
        }
        void send(byte[] body) throws Exception {
            require(body.length <= MAX_FRAME, "outgoing bounded frame");
            output.writeInt(body.length); output.write(body); output.flush();
        }
        byte[] read() throws Exception {
            int length = input.readInt();
            require(length >= 0 && length <= MAX_FRAME, "incoming bounded length");
            byte[] body = input.readNBytes(length);
            require(body.length == length, "whole response frame");
            return body;
        }
        byte[] request(ApiKeys api, short version, Message message, int id) throws Exception {
            RequestHeader header = new RequestHeader(api, version, "sasl-java-peer", id);
            send(SaslWireOracle.concat(SaslWireOracle.serialize(header.data(), header.headerVersion()),
                    SaslWireOracle.serialize(message, version)));
            ByteBuffer bytes = ByteBuffer.wrap(read());
            require(ResponseHeader.parse(bytes, api.responseHeaderVersion(version)).correlationId() == id,
                    "exact response correlation including signed IDs");
            byte[] result = new byte[bytes.remaining()]; bytes.get(result);
            return result;
        }
        byte[] request(ApiKeys api, short version, Message message) throws Exception {
            return request(api, version, message, ++correlation);
        }
        SaslHandshakeResponseData handshake(String mechanism, short version, int expected) throws Exception {
            ByteBuffer bytes = ByteBuffer.wrap(request(ApiKeys.SASL_HANDSHAKE, version,
                    new SaslHandshakeRequestData().setMechanism(mechanism)));
            var result = new SaslHandshakeResponseData(new ByteBufferAccessor(bytes), version);
            require(!bytes.hasRemaining() && result.errorCode() == expected, "handshake expected error");
            return result;
        }
        SaslAuthenticateResponseData authenticate(byte[] token, short version, int expected) throws Exception {
            ByteBuffer bytes = ByteBuffer.wrap(request(ApiKeys.SASL_AUTHENTICATE, version,
                    new SaslAuthenticateRequestData().setAuthBytes(token)));
            var result = new SaslAuthenticateResponseData(new ByteBufferAccessor(bytes), version);
            require(!bytes.hasRemaining() && result.errorCode() == expected, "authentication expected error");
            if (version >= 1) require(result.sessionLifetimeMs() == 0, "no KIP368 lifetime advertisement");
            return result;
        }
        void closed() throws Exception {
            try {
                require(input.read() == -1, "terminal state drops socket before read deadline");
            } catch (EOFException | java.net.SocketException | javax.net.ssl.SSLException terminal) {
                // A close with unread bytes can reset TCP or omit TLS close_notify.
                // Read timeouts are not accepted as terminal connection proof.
                closedReceipt("terminal-socket", terminal.getClass().getSimpleName());
            }
        }
        @Override public void close() throws java.io.IOException { socket.close(); }
    }
    private static void login(Connection connection, String mechanism, String user, String password,
                              short handshake, short authenticate, int expected) throws Exception {
        connection.handshake(mechanism, handshake, 0);
        SaslClient client = client(mechanism, user, password);
        try {
            byte[] token = client.evaluateChallenge(new byte[0]);
            String clientNonce = mechanism.equals("PLAIN") ? null : new ScramMessages.ClientFirstMessage(token).nonce();
            for (int round = 0; round < 3; round++) {
                byte[] reply;
                if (handshake == 0) {
                    connection.send(token);
                    if (expected != 0) {
                        connection.closed();
                        receipt("login", mechanism + "-legacy-failure", expected);
                        return;
                    }
                    reply = connection.read();
                } else {
                    int error = client.isComplete() || round > 0 ? expected : 0;
                    var response = connection.authenticate(token, authenticate, error);
                    if (response.errorCode() != 0) {
                        connection.closed();
                        receipt("login", mechanism + "-framed-failure", expected);
                        return;
                    }
                    reply = response.authBytes();
                }
                if (client.isComplete()) {
                    require(expected == 0 && reply.length == 0, "PLAIN final empty token");
                    receipt("login", mechanism + (handshake == 0 ? "-legacy" : "-framed-v" + authenticate), 0);
                    return;
                }
                if (round == 0) {
                    String combined = new ScramMessages.ServerFirstMessage(reply).nonce();
                    require(combined.startsWith(clientNonce) && combined.length() > clientNonce.length(), "real server nonce extends client nonce");
                    String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                            .digest(combined.substring(clientNonce.length()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                    require(SERVER_NONCES.add(digest), "distinct actual server nonce suffix across live sessions");
                }
                token = client.evaluateChallenge(reply);
                if (client.isComplete()) {
                    require(expected == 0 && token == null, "actual Apache verified server signature");
                    receipt("login", mechanism + (handshake == 0 ? "-legacy" : "-framed-v" + authenticate), 0);
                    return;
                }
            }
            throw new AssertionError("bounded SASL round limit");
        } finally { client.dispose(); }
    }
    private static void versions(Connection c, boolean authenticated) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(c.request(ApiKeys.API_VERSIONS, (short) 3,
                new ApiVersionsRequestData().setClientSoftwareName("sasl-java-peer").setClientSoftwareVersion("public-test"), -42));
        var result = new ApiVersionsResponseData(new ByteBufferAccessor(bytes), (short) 3);
        require(!bytes.hasRemaining() && result.errorCode() == 0, "ApiVersions pre/post-auth response");
        var handshake = result.apiKeys().find((short) 17);
        var auth = result.apiKeys().find((short) 36);
        require(handshake != null && handshake.minVersion() == 0 && handshake.maxVersion() == 1,
                "actual listener handshake versions");
        require(auth != null && auth.minVersion() == 0 && auth.maxVersion() == 2,
                "actual listener authenticate versions");
        receipt("api-versions", authenticated ? "authenticated" : "preauth", 0);
    }
    private static void metadata(Connection c, int id, String variant) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(c.request(ApiKeys.METADATA, (short) 1,
                new MetadataRequestData().setTopics(List.of()), id));
        var result = new MetadataResponseData(new ByteBufferAccessor(bytes), (short) 1);
        require(!bytes.hasRemaining() && result.brokers().size() == 1 && result.brokers().find(0) != null,
                "actual authenticated MetadataHandler node0 response");
        receipt("metadata", variant, 0);
    }
    private static void sessions(int tlsPort, int plainPort, SSLContext tls) throws Exception {
        for (String mechanism : List.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")) {
            for (short version = 0; version <= 2; version++) {
                try (Connection c = new Connection(tlsPort, tls)) {
                    versions(c, false); login(c, mechanism, "user", PASSWORD, (short) 1, version, 0); versions(c, true);
                    metadata(c, ++correlation, mechanism);
                }
            }
            try (Connection c = new Connection(tlsPort, tls)) { login(c, mechanism, "user", PASSWORD, (short) 0, (short) 0, 0); metadata(c, ++correlation, mechanism + "-legacy"); }
            try (Connection c = new Connection(tlsPort, tls)) { login(c, mechanism, "user", "wrong-public-fixture", (short) 1, (short) 2, 58); }
        }
        for (String mechanism : List.of("SCRAM-SHA-256", "SCRAM-SHA-512")) {
            try (Connection c = new Connection(plainPort, null)) { login(c, mechanism, "user", PASSWORD, (short) 1, (short) 2, 0); }
            try (Connection c = new Connection(plainPort, null)) { login(c, mechanism, "user", PASSWORD, (short) 0, (short) 0, 0); }
        }
        for (String mechanism : List.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")) {
            try (Connection c = new Connection(tlsPort, tls)) {
                login(c, mechanism, "unicode", "péncil-🔑", (short) 1, (short) 2, 0);
                metadata(c, ++correlation, "unicode-" + mechanism);
            }
        }
        try (Connection c = new Connection(plainPort, null)) {
            var r = c.handshake("PLAIN", (short) 1, 33);
            require(!r.mechanisms().contains("PLAIN"), "default plaintext PLAIN forbidden");
            c.closed(); receipt("handshake", "plaintext-plain-policy", 33);
        }
        try (Connection c = new Connection(tlsPort, tls)) { c.handshake("OAUTHBEARER", (short) 1, 33); c.closed(); receipt("handshake", "unsupported", 33); }
        try (Connection c = new Connection(tlsPort, tls)) { c.authenticate(new byte[0], (short) 2, 34); c.closed(); receipt("state", "auth-before-handshake", 34); }
        try (Connection c = new Connection(tlsPort, tls)) {
            c.handshake("SCRAM-SHA-256", (short) 1, 0); c.handshake("SCRAM-SHA-256", (short) 1, 34); c.closed();
            receipt("state", "duplicate-handshake", 34);
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 1, "sasl-java-peer", 727200);
            c.send(SaslWireOracle.concat(SaslWireOracle.serialize(header.data(), header.headerVersion()),
                    SaslWireOracle.serialize(new MetadataRequestData().setTopics(List.of()), (short) 1)));
            c.closed(); closedReceipt("state", "application-before-authentication");
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            c.handshake("PLAIN", (short) 1, 0);
            RequestHeader header = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, (short) 2, "sasl-java-peer", ++correlation);
            byte[] body = SaslWireOracle.serialize(new SaslAuthenticateRequestData().setAuthBytes(new byte[0]), (short) 2);
            c.send(SaslWireOracle.concat(SaslWireOracle.serialize(header.data(), header.headerVersion()), Arrays.copyOf(body, body.length - 1)));
            c.closed(); closedReceipt("framing", "truncated-flex-authentication");
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            login(c, "PLAIN", "user", PASSWORD, (short) 1, (short) 2, 0);
            c.authenticate(new byte[0], (short) 2, 34); c.closed(); receipt("state", "reauthentication-not-implemented", 34);
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            c.handshake("PLAIN", (short) 1, 0);
            SaslClient actual = client("PLAIN", "user", PASSWORD);
            try {
                byte[] body = SaslWireOracle.serialize(new SaslAuthenticateRequestData()
                        .setAuthBytes(actual.evaluateChallenge(new byte[0])), (short) 2);
                RequestHeader header = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, (short) 2, "sasl-java-peer", ++correlation);
                c.send(SaslWireOracle.concat(SaslWireOracle.serialize(header.data(), header.headerVersion()),
                        SaslWireOracle.concat(body, new byte[] {42})));
                c.closed(); closedReceipt("framing", "whole-input-trailing-rejection");
            } finally { actual.dispose(); }
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            c.output.writeByte(0); c.output.flush(); c.closed(); closedReceipt("deadline", "partial-prefix");
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            c.handshake("SCRAM-SHA-256", (short) 1, 0);
            SaslClient actual = client("SCRAM-SHA-256", "user", PASSWORD);
            try { c.authenticate(actual.evaluateChallenge(new byte[0]), (short) 2, 0); c.closed(); }
            finally { actual.dispose(); }
            closedReceipt("deadline", "challenge-no-proof");
        }
        for (int length : new int[] {-1, 16385, Integer.MAX_VALUE}) {
            try (Connection c = new Connection(tlsPort, tls)) {
                c.output.writeInt(length); c.output.flush(); c.closed(); closedReceipt("framing", "hostile-length-" + length);
            }
        }
        try (Connection c = new Connection(tlsPort, tls)) {
            for (int round = 0; round < 8; round++) versions(c, false);
            RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "sasl-java-peer", ++correlation);
            c.send(SaslWireOracle.concat(SaslWireOracle.serialize(header.data(), header.headerVersion()),
                    SaslWireOracle.serialize(new ApiVersionsRequestData()
                            .setClientSoftwareName("sasl-java-peer").setClientSoftwareVersion("public-test"), (short) 3)));
            c.closed(); closedReceipt("budget", "ninth-preauth-control-round");
        }
    }
    private static int describe(Connection c, String user, int expected, int algorithms) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(c.request(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, (short) 0,
                new DescribeUserScramCredentialsRequestData().setUsers(List.of(
                        new DescribeUserScramCredentialsRequestData.UserName().setName(user)))));
        var response = new DescribeUserScramCredentialsResponseData(new ByteBufferAccessor(bytes), (short) 0);
        require(!bytes.hasRemaining(), "describe whole response");
        int error = response.errorCode();
        if (error == 0) {
            require(response.results().size() == 1 && response.results().get(0).user().equals(user), "describe exact user");
            error = response.results().get(0).errorCode();
            require(response.results().get(0).credentialInfos().size() == algorithms, "describe algorithm count");
        } else {
            require(response.results().isEmpty(), "describe top-level error has no user results");
            if (error == 31) require(response.errorMessage() == null, "describe denied nullable error message");
            receipt("describe-schema", "whole-generated-response-empty-results", error);
        }
        require(error == expected, "describe expected status");
        receipt("describe", user, error);
        return error;
    }
    private static void alter(Connection c, String user, String password, boolean delete, int expected) throws Exception {
        var request = new AlterUserScramCredentialsRequestData();
        if (delete) request.setDeletions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialDeletion()
                .setName(user).setMechanism((byte) 1)));
        else {
            ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
            request.setUpsertions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                    .setName(user).setMechanism((byte) 1).setIterations(4096).setSalt(SALT.clone())
                    .setSaltedPassword(formatter.saltedPassword(password, SALT, 4096))));
        }
        alterRequest(c, request, user, expected);
        receipt(delete ? "delete" : "upsert", user, expected);
    }
    private static void alterRequest(Connection c, AlterUserScramCredentialsRequestData request,
                                     String user, int expected) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(c.request(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, (short) 0, request));
        var result = new AlterUserScramCredentialsResponseData(new ByteBufferAccessor(bytes), (short) 0);
        require(!bytes.hasRemaining() && result.results().size() == 1
                && result.results().get(0).user().equals(user) && result.results().get(0).errorCode() == expected,
                "alter exact status");
    }
    private static void adminEdges(Connection c) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(c.request(ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, (short) 0,
                new DescribeUserScramCredentialsRequestData().setUsers(List.of(
                        new DescribeUserScramCredentialsRequestData.UserName().setName("user"),
                        new DescribeUserScramCredentialsRequestData.UserName().setName("user")))));
        var result = new DescribeUserScramCredentialsResponseData(new ByteBufferAccessor(bytes), (short) 0);
        require(!bytes.hasRemaining() && result.errorCode() == 0 && result.results().size() == 1
                && result.results().get(0).user().equals("user") && result.results().get(0).errorCode() == 92
                && result.results().get(0).credentialInfos().isEmpty(), "pinned unique-user duplicate describe92");
        receipt("admin-policy", "duplicate-describe-one-result", 92);
        for (String variant : List.of("unknown-algorithm", "invalid-iterations", "empty-salt", "short-salted", "cross-algorithm-duplicate")) {
            ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
            var first = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                    .setName("edge-user").setMechanism((byte) 1).setIterations(4096).setSalt(SALT.clone())
                    .setSaltedPassword(formatter.saltedPassword(PASSWORD, SALT, 4096));
            int expected = 93;
            if (variant.equals("unknown-algorithm")) { first.setMechanism((byte) 0); expected = 33; }
            if (variant.equals("invalid-iterations")) first.setIterations(1);
            if (variant.equals("empty-salt")) first.setSalt(new byte[0]);
            if (variant.equals("short-salted")) first.setSaltedPassword(new byte[] {42});
            var request = new AlterUserScramCredentialsRequestData().setUpsertions(List.of(first));
            if (variant.equals("cross-algorithm-duplicate")) {
                formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_512);
                request.setUpsertions(List.of(first, first.duplicate().setMechanism((byte) 2)
                        .setSaltedPassword(formatter.saltedPassword(PASSWORD, SALT, 4096))));
                expected = 92;
            }
            alterRequest(c, request, "edge-user", expected);
            receipt("admin-policy", variant, expected);
        }
        describe(c, "edge-user", 91, 0);
    }
    private static Connection admin(int port, SSLContext tls) throws Exception {
        Connection c = new Connection(port, tls);
        try { login(c, "SCRAM-SHA-256", "admin", PASSWORD, (short) 1, (short) 2, 0); return c; }
        catch (Exception | AssertionError error) { c.close(); throw error; }
    }
    private static void administration(int port, SSLContext tls, boolean restarted) throws Exception {
        if (!restarted) {
            try (Connection c = new Connection(port, tls)) {
                login(c, "SCRAM-SHA-256", "user", PASSWORD, (short) 1, (short) 2, 0);
                describe(c, "user", 31, 0); alter(c, "forbidden-user", PASSWORD, false, 31);
            }
            try (Connection c = admin(port, tls)) {
                adminEdges(c);
                describe(c, "created-user", 91, 0); alter(c, "created-user", PASSWORD, false, 0); describe(c, "created-user", 0, 1);
            }
            try (Connection old = new Connection(port, tls); Connection c = admin(port, tls)) {
                old.handshake("SCRAM-SHA-256", (short) 1, 0);
                SaslClient actual = client("SCRAM-SHA-256", "created-user", PASSWORD);
                try {
                    byte[] challenge = old.authenticate(actual.evaluateChallenge(new byte[0]), (short) 2, 0).authBytes();
                    alter(c, "created-user", ROTATED, false, 0);
                    byte[] reply = old.authenticate(actual.evaluateChallenge(challenge), (short) 2, 0).authBytes();
                    require(actual.evaluateChallenge(reply) == null && actual.isComplete(), "captured old generation finishes across rotation");
                    versions(old, true); metadata(old, 727201, "old-captured-generation"); receipt("rotation", "captured-session-continuity", 0);
                } finally { actual.dispose(); }
            }
            try (Connection c = new Connection(port, tls)) { login(c, "SCRAM-SHA-256", "created-user", PASSWORD, (short) 1, (short) 2, 58); }
            try (Connection c = new Connection(port, tls)) { login(c, "SCRAM-SHA-256", "created-user", ROTATED, (short) 1, (short) 2, 0); metadata(c, 727202, "new-rotation-generation"); }
        } else {
            try (Connection c = new Connection(port, tls)) { login(c, "SCRAM-SHA-256", "created-user", ROTATED, (short) 1, (short) 2, 0); metadata(c, 727203, "replayed-credential"); }
            try (Connection c = admin(port, tls)) { describe(c, "created-user", 0, 1); alter(c, "created-user", null, true, 0); describe(c, "created-user", 91, 0); alter(c, "created-user", null, true, 91); }
            try (Connection c = new Connection(port, tls)) {
                c.handshake("SCRAM-SHA-256", (short) 1, 0);
                SaslClient actual = client("SCRAM-SHA-256", "created-user", ROTATED);
                try { c.authenticate(actual.evaluateChallenge(new byte[0]), (short) 2, 58); c.closed(); }
                finally { actual.dispose(); }
                receipt("login", "deleted-credential", 58);
            }
        }
    }
    private static void run(String[] args) throws Exception {
        require(args.length == 4, "usage tlsPort plainPort caPem sessions|admin|restart");
        int tlsPort = Integer.parseInt(args[0]), plainPort = Integer.parseInt(args[1]);
        SSLContext tls = context(Path.of(args[2]));
        switch (args[3]) {
            case "sessions" -> sessions(tlsPort, plainPort, tls);
            case "admin" -> administration(tlsPort, tls, false);
            case "restart" -> administration(tlsPort, tls, true);
            case "admin-denied" -> {
                try (Connection c = admin(tlsPort, tls)) {
                    describe(c, "user", 31, 0); alter(c, "default-forbidden", PASSWORD, false, 31);
                    metadata(c, 727204, "authenticated-admin-empty-allowlist");
                }
                for (String user : List.of("created-user", "native-created")) {
                    try (Connection c = new Connection(tlsPort, tls)) {
                        c.handshake("SCRAM-SHA-256", (short) 1, 0);
                        SaslClient actual = client("SCRAM-SHA-256", user, ROTATED);
                        try { c.authenticate(actual.evaluateChallenge(new byte[0]), (short) 2, 58); c.closed(); }
                        finally { actual.dispose(); }
                        receipt("restart", "persisted-deletion-" + user, 58);
                    }
                }
            }
            default -> throw new IllegalArgumentException("unknown bounded phase");
        }
        System.out.printf("{\"status\":\"pass\",\"phase\":\"%s\",\"assertions\":%d}%n", args[3], assertions);
    }
    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception | AssertionError failure) {
            // SASL exception messages can embed auth tokens; retain class and
            // locations only. Our fixed assertion labels contain no secrets.
            System.err.println("peer failure class=" + failure.getClass().getName());
            if (failure instanceof AssertionError) System.err.println("assertion=" + failure.getMessage());
            for (StackTraceElement location : failure.getStackTrace()) System.err.println("at " + location);
            System.exit(1);
        }
    }
}
