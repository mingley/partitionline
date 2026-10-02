/* Authentic Apache mechanism oracle. Every credential/nonce is public fixture data. */
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramCredentialCallback;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramSaslClient;
import org.apache.kafka.common.security.scram.internals.ScramSaslServer;

public final class SaslMechanismOracle {
    private static final byte[] SALT = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f");
    private static final String NONCE = "publicClientNonce0123456789";
    private static final List<String> OUTCOMES = new ArrayList<>();
    private static boolean mutateRfc;
    private SaslMechanismOracle() { }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String text(byte[] value) { return new String(value, StandardCharsets.UTF_8); }
    private static String hex(byte[] value) { return HexFormat.of().formatHex(value); }
    private static String hex(String value) { return hex(bytes(value)); }
    private static String b64(byte[] value) { return Base64.getEncoder().encodeToString(value); }
    private static void write(Path path, List<String> rows) throws Exception {
        Files.write(path, rows, StandardCharsets.UTF_8);
    }
    private static void assertTrue(boolean condition, String description) {
        if (!condition) throw new AssertionError(description);
    }

    /* This hook controls test entropy only; it is never production RNG evidence. */
    private static final class PublicFixtureRandom extends SecureRandom {
        private static final long serialVersionUID = 1L;
        private final int value;
        PublicFixtureRandom(int value) { this.value = value; }
        @Override public void nextBytes(byte[] data) {
            for (int i = 0; i < data.length; i++) data[i] = (byte) (value + i);
        }
    }
    private static void fixtureRandom(Object mechanism, int value) throws Exception {
        Field formatterField = mechanism.getClass().getDeclaredField("formatter");
        formatterField.setAccessible(true);
        ScramFormatter formatter = (ScramFormatter) formatterField.get(mechanism);
        Field randomField = ScramFormatter.class.getDeclaredField("random");
        randomField.setAccessible(true);
        randomField.set(formatter, new PublicFixtureRandom(value));
    }
    private static CallbackHandler serverCallback(String username, ScramCredential credential) {
        return callbacks -> {
            String requested = null;
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback name) requested = name.getDefaultName();
            }
            for (Callback callback : callbacks) {
                if (callback instanceof ScramCredentialCallback target) {
                    if (username.equals(requested)) target.scramCredential(credential);
                } else if (!(callback instanceof NameCallback)) {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };
    }
    private static CallbackHandler clientCallback(String username, String password) {
        return callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback name) name.setName(username);
                else if (callback instanceof PasswordCallback secret) secret.setPassword(password.toCharArray());
                else throw new UnsupportedCallbackException(callback);
            }
        };
    }
    private static void outcome(String kind, String name, boolean accepted, boolean complete,
                                String exception, String extra) {
        OUTCOMES.add(String.join("\t", kind, name, Boolean.toString(accepted), Boolean.toString(complete), exception, extra));
    }
    private static void scramRow(List<String> rows, ScramMechanism mechanism, String variant) throws Exception {
        String username = variant.equals("escaped") ? "escape,user=ok"
            : variant.equals("utf8-username") ? "用户" : "user";
        String password = variant.startsWith("utf8-") ? "päss💫" : "pencil";
        int iterations = variant.equals("iterations-max") ? 16384
            : variant.equals("iterations-above-max") ? 16385 : variant.equals("iterations-low") ? 4095 : 4096;
        String prefix = variant.startsWith("hardening-") || variant.equals("iterations-above-max")
            ? "hardening-" : "canonical-";
        String name = variant.startsWith("hardening-") ? variant : prefix + variant;
        ScramFormatter formatter = new ScramFormatter(mechanism);
        byte[] salted = formatter.saltedPassword(password, SALT, iterations);
        ScramCredential credential = formatter.generateCredential(SALT, salted, iterations);
        ScramSaslServer server = new ScramSaslServer(mechanism, Map.of(), serverCallback(username, credential));
        fixtureRandom(server, 33);
        String first = "n,,n=" + ScramFormatter.saslName(username) + ",r=" + NONCE;
        switch (variant) {
            case "self-authzid": first = "n,a=user,n=user,r=" + NONCE; break;
            case "other-authzid": first = "n,a=other,n=user,r=" + NONCE; break;
            case "unknown-user": first = "n,,n=missing,r=" + NONCE; break;
            case "empty-user": first = "n,,n=,r=" + NONCE; break;
            case "invalid-escape": first = "n,,n=bad=2Xname,r=" + NONCE; break;
            case "unsupported-binding": first = "p=tls-server-end-point,,n=user,r=" + NONCE; break;
            case "hardening-m": first = "n,,m=required,n=user,r=" + NONCE; break;
            case "hardening-first-duplicate": first += ",n=other"; break;
            default: break;
        }
        byte[] serverFirst = new byte[0];
        byte[] clientFinal = new byte[0];
        byte[] serverFinal = new byte[0];
        byte[] validFinal = new byte[0];
        boolean accepted = false;
        boolean retryAccepted = false;
        String failure = "none";
        try {
            serverFirst = server.evaluateResponse(bytes(first));
            ClientFirstMessage parsedFirst = new ClientFirstMessage(bytes(first));
            ServerFirstMessage parsedServer = new ServerFirstMessage(serverFirst);
            int secondComma = first.indexOf(',', first.indexOf(',') + 1);
            byte[] binding = bytes(first.substring(0, secondComma + 1));
            ClientFinalMessage finalMessage = new ClientFinalMessage(binding, parsedServer.nonce());
            byte[] clientSalted = variant.equals("wrong-password")
                ? formatter.saltedPassword("wrong-password", SALT, iterations) : salted;
            finalMessage.proof(formatter.clientProof(clientSalted, parsedFirst, parsedServer, finalMessage));
            validFinal = finalMessage.toBytes();
            clientFinal = validFinal;
            switch (variant) {
                case "proof-bitflip": {
                    byte[] proof = finalMessage.proof().clone(); proof[0] ^= 1;
                    finalMessage.proof(proof); clientFinal = finalMessage.toBytes(); break;
                }
                case "proof-short": finalMessage.proof(new byte[]{0}); clientFinal = finalMessage.toBytes(); break;
                case "wrong-final-nonce": clientFinal = bytes(text(clientFinal).replace(parsedServer.nonce(), parsedServer.nonce() + "wrong")); break;
                case "proof-invalid-base64": clientFinal = bytes(text(clientFinal).replace(",p=", ",p=%%%")); break;
                case "binding-unresigned": clientFinal = bytes(text(clientFinal).replace("c=" + b64(binding), "c=eSws")); break;
                case "hardening-binding-resigned": {
                    finalMessage = new ClientFinalMessage(bytes("y,,"), parsedServer.nonce());
                    finalMessage.proof(formatter.clientProof(salted, parsedFirst, parsedServer, finalMessage));
                    clientFinal = finalMessage.toBytes(); break;
                }
                case "hardening-final-duplicate": clientFinal = bytes(text(clientFinal).replace(",p=", ",r=duplicate,p=")); break;
                case "hardening-final-extension": clientFinal = bytes(text(clientFinal).replace(",p=", ",x=added,p=")); break;
                default: break;
            }
            serverFinal = server.evaluateResponse(clientFinal);
            accepted = server.isComplete();
            assertTrue(accepted && server.getAuthorizationID().equals(username), "wrong authenticated identity");
            ServerFinalMessage finalResult = new ServerFinalMessage(serverFinal);
            ClientFinalMessage actualFinal = new ClientFinalMessage(clientFinal);
            assertTrue(Arrays.equals(finalResult.serverSignature(),
                formatter.serverSignature(credential.serverKey(), parsedFirst, parsedServer, actualFinal)), "bad authentic server signature");
        } catch (Exception error) {
            failure = error.getClass().getSimpleName();
            assertTrue(!server.isComplete(), "failed initial exchange falsely complete");
            if (validFinal.length > 0) {
                try {
                    server.evaluateResponse(validFinal);
                    retryAccepted = server.isComplete();
                } catch (Exception ignored) { retryAccepted = false; }
            }
        }
        boolean expected = switch (variant) {
            case "wrong-password", "proof-bitflip", "proof-short", "wrong-final-nonce", "proof-invalid-base64",
                "unknown-user", "empty-user", "invalid-escape", "unsupported-binding", "other-authzid",
                "binding-unresigned", "iterations-low", "utf8-username" -> false;
            default -> true;
        };
        assertTrue(accepted == expected, mechanism + " unexpected server decision for " + variant + " " + failure);
        rows.add(String.join("\t", name, mechanism.mechanismName(), hex(password), hex(SALT), Integer.toString(iterations),
            hex(first), hex(serverFirst), hex(clientFinal), hex(serverFinal), hex(credential.storedKey()),
            hex(credential.serverKey()), Boolean.toString(accepted)));
        outcome(mechanism.mechanismName(), name, accepted, server.isComplete(), failure,
            "valid_retry_after_failure=" + retryAccepted);
        server.dispose();
    }
    private static List<String> scram() throws Exception {
        List<String> rows = new ArrayList<>();
        rows.add("name\tmechanism\tpassword_hex\tsalt_hex\titerations\tclient_first_hex\tserver_first_hex\tclient_final_hex\tserver_final_hex\tstored_key_hex\tserver_key_hex\tserver_accepted");
        List<String> variants = List.of("basic", "escaped", "self-authzid", "iterations-max", "wrong-password", "proof-bitflip",
            "proof-short", "wrong-final-nonce", "proof-invalid-base64", "unknown-user", "empty-user", "invalid-escape",
            "unsupported-binding", "other-authzid", "binding-unresigned", "iterations-low", "hardening-m",
            "hardening-first-duplicate", "hardening-final-duplicate", "hardening-binding-resigned", "hardening-final-extension", "iterations-above-max",
            "utf8-password", "utf8-username");
        for (ScramMechanism mechanism : ScramMechanism.values()) for (String variant : variants) scramRow(rows, mechanism, variant);
        return rows;
    }
    private static void message(List<String> rows, String name, String kind, byte[] data) {
        boolean accepted = true;
        String failure = "none";
        try {
            switch (kind) {
                case "client-first": new ClientFirstMessage(data); break;
                case "server-first": new ServerFirstMessage(data); break;
                case "client-final": new ClientFinalMessage(data); break;
                case "server-final": new ServerFinalMessage(data); break;
                default: throw new AssertionError("unknown parser");
            }
        } catch (Exception error) { accepted = false; failure = error.getClass().getSimpleName(); }
        rows.add(String.join("\t", name, kind, hex(data), Boolean.toString(accepted)));
        outcome("parser", name, accepted, false, failure, kind);
    }
    private static List<String> messages() {
        List<String> rows = new ArrayList<>();
        rows.add("name\tkind\tmessage_hex\taccepted");
        String[][] cases = {
            {"canonical-first", "client-first", "n,,n=user,r=nonce"},
            {"canonical-first-escaped", "client-first", "n,,n=escape=2Cuser=3Dok,r=nonce"},
            {"canonical-self-authzid", "client-first", "n,a=user,n=user,r=nonce"},
            {"canonical-first-invalid-escape", "client-first", "n,,n=bad=2X,r=nonce"},
            {"canonical-first-empty-nonce", "client-first", "n,,n=user,r="},
            {"canonical-first-comma-nonce", "client-first", "n,,n=user,r=bad,nonce"},
            {"canonical-first-binding-y", "client-first", "y,,n=user,r=nonce"},
            {"canonical-first-binding-p", "client-first", "p=tls-server-end-point,,n=user,r=nonce"},
            {"hardening-first-m", "client-first", "n,,m=required,n=user,r=nonce"},
            {"hardening-first-duplicate-n", "client-first", "n,,n=user,r=nonce,n=other"},
            {"hardening-first-duplicate-r", "client-first", "n,,n=user,r=nonce,r=other"},
            {"canonical-server-first", "server-first", "r=nonceserver,s=AAECAwQFBgcICQoLDA0ODw==,i=4096"},
            {"canonical-server-first-zero", "server-first", "r=nonceserver,s=AA==,i=0"},
            {"canonical-server-first-negative", "server-first", "r=nonceserver,s=AA==,i=-1"},
            {"canonical-server-first-overflow", "server-first", "r=nonceserver,s=AA==,i=2147483648"},
            {"canonical-server-first-bad-base64", "server-first", "r=nonceserver,s=%%,i=4096"},
            {"hardening-server-first-one", "server-first", "r=nonceserver,s=AA==,i=1"},
            {"hardening-server-first-large", "server-first", "r=nonceserver,s=AA==,i=16385"},
            {"hardening-server-first-empty-salt", "server-first", "r=nonceserver,s=,i=4096"},
            {"hardening-server-first-m", "server-first", "m=required,r=nonceserver,s=AA==,i=4096"},
            {"hardening-server-first-duplicate-i", "server-first", "r=nonceserver,s=AA==,i=4096,i=1"},
            {"canonical-client-final", "client-final", "c=biws,r=nonceserver,p=AA=="},
            {"canonical-final-bad-base64", "client-final", "c=biws,r=nonceserver,p=%%%"},
            {"canonical-final-empty-nonce", "client-final", "c=biws,r=,p=AA=="},
            {"hardening-final-empty-proof", "client-final", "c=biws,r=nonceserver,p="},
            {"hardening-final-empty-binding", "client-final", "c=,r=nonceserver,p=AA=="},
            {"hardening-final-y-binding", "client-final", "c=eSws,r=nonceserver,p=AA=="},
            {"hardening-final-duplicate-r", "client-final", "c=biws,r=nonceserver,r=other,p=AA=="},
            {"hardening-final-duplicate-p", "client-final", "c=biws,r=nonceserver,p=AQ==,p=AA=="},
            {"canonical-server-final", "server-final", "v=AA=="},
            {"canonical-server-error", "server-final", "e=invalid-proof"},
            {"canonical-server-both", "server-final", "e=invalid-proof,v=AA=="},
            {"canonical-server-bad-base64", "server-final", "v=%%%"},
            {"hardening-server-empty-signature", "server-final", "v="},
            {"hardening-server-duplicate-v", "server-final", "v=AA==,v=AQ=="},
        };
        for (String[] row : cases) message(rows, row[0], row[1], bytes(row[2]));
        message(rows, "canonical-first-invalid-utf8", "client-first", new byte[]{'n', ',', ',', 'n', '=', (byte) 0xff, ',', 'r', '=', 'a'});
        return rows;
    }
    private static List<String> plain() {
        List<String> rows = new ArrayList<>();
        rows.add("name\tmessage_hex\tserver_accepted\tauthenticated_identity_hex");
        String[][] cases = {
            {"canonical-basic", "\0user\0pencil"},
            {"canonical-self-authzid", "user\0user\0pencil"},
            {"canonical-escaped-identity", "\0escape,user=ok\0pencil"},
            {"canonical-utf8", "\0用户\0päss💫"},
            {"canonical-wrong-password", "\0user\0wrong"},
            {"canonical-unknown-user", "\0missing\0pencil"},
            {"canonical-other-authzid", "other\0user\0pencil"},
            {"canonical-empty-user", "\0\0pencil"},
            {"canonical-empty-password", "\0user\0"},
            {"canonical-no-separator", "user"},
            {"canonical-one-separator", "user\0pencil"},
            {"canonical-extra-separator", "\0user\0pencil\0extra"},
        };
        CallbackHandler handler = callbacks -> {
            String username = null;
            for (Callback callback : callbacks) if (callback instanceof NameCallback name) username = name.getDefaultName();
            for (Callback callback : callbacks) {
                if (callback instanceof PlainAuthenticateCallback target) target.authenticated(
                    (("user".equals(username) || "escape,user=ok".equals(username)) && Arrays.equals(target.password(), "pencil".toCharArray()))
                    || ("用户".equals(username) && Arrays.equals(target.password(), "päss💫".toCharArray())));
                else if (!(callback instanceof NameCallback)) throw new UnsupportedCallbackException(callback);
            }
        };
        for (String[] row : cases) {
            PlainSaslServer server = new PlainSaslServer(handler);
            boolean accepted = true; String failure = "none"; String identity = "";
            try { server.evaluateResponse(bytes(row[1])); identity = server.getAuthorizationID(); }
            catch (Exception error) { accepted = false; failure = error.getClass().getSimpleName(); }
            assertTrue(accepted == (row[0].equals("canonical-basic") || row[0].equals("canonical-self-authzid")
                || row[0].equals("canonical-escaped-identity") || row[0].equals("canonical-utf8")), "PLAIN unexpected decision");
            rows.add(String.join("\t", row[0], hex(row[1]), Boolean.toString(accepted), hex(identity)));
            outcome("PLAIN", row[0], accepted, server.isComplete(), failure, "none");
        }
        return rows;
    }
    private static List<String> rfc() throws Exception {
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
        String first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        String server = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        String client = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
        if (mutateRfc) client = client.replace("p=dHzb", "p=eHzb");
        String last = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
        ClientFirstMessage cf = new ClientFirstMessage(bytes(first));
        ServerFirstMessage sf = new ServerFirstMessage(bytes(server));
        ClientFinalMessage fin = new ClientFinalMessage(bytes(client));
        byte[] salted = formatter.saltedPassword("pencil", sf.salt(), sf.iterations());
        assertTrue(Arrays.equals(fin.proof(), formatter.clientProof(salted, cf, sf, fin)), "RFC 7677 literal client proof differs");
        ScramCredential credential = formatter.generateCredential(sf.salt(), salted, sf.iterations());
        assertTrue(Arrays.equals(new ServerFinalMessage(bytes(last)).serverSignature(),
            formatter.serverSignature(credential.serverKey(), cf, sf, fin)), "RFC 7677 literal server signature differs");
        return List.of("name\tmechanism\tpassword_hex\tsalt_hex\titerations\tclient_first_hex\tserver_first_hex\tclient_final_hex\tserver_final_hex\tstored_key_hex\tserver_key_hex\tserver_accepted",
            String.join("\t", "rfc7677-section3", "SCRAM-SHA-256", hex("pencil"), hex(sf.salt()), "4096", hex(first), hex(server), hex(client), hex(last), hex(credential.storedKey()), hex(credential.serverKey()), "true"));
    }
    private static void clientProofs() throws Exception {
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            for (String variant : List.of("valid", "bad-signature", "server-error", "empty-signature", "bad-nonce", "low-iterations")) {
                ScramFormatter formatter = new ScramFormatter(mechanism);
                byte[] salted = formatter.saltedPassword("pencil", SALT, 4096);
                ScramCredential credential = formatter.generateCredential(SALT, salted, 4096);
                ScramSaslServer server = new ScramSaslServer(mechanism, Map.of(), serverCallback("user", credential));
                ScramSaslClient client = new ScramSaslClient(mechanism, clientCallback("user", "pencil"));
                fixtureRandom(server, 33); fixtureRandom(client, 65);
                boolean accepted = false; String failure = "none";
                byte[] first = client.evaluateChallenge(new byte[0]);
                byte[] challenge = server.evaluateResponse(first);
                if (variant.equals("bad-nonce")) challenge = bytes(text(challenge).replace("r=", "r=wrong"));
                if (variant.equals("low-iterations")) challenge = bytes(text(challenge).replace("i=4096", "i=4095"));
                byte[] finalResponse = new byte[0]; byte[] last = new byte[0];
                try {
                    finalResponse = client.evaluateChallenge(challenge);
                    last = server.evaluateResponse(finalResponse);
                    if (variant.equals("server-error")) last = bytes("e=invalid-proof");
                    if (variant.equals("empty-signature")) last = bytes("v=");
                    if (variant.equals("bad-signature")) {
                        byte[] signature = new ServerFinalMessage(last).serverSignature(); signature[0] ^= 1;
                        last = bytes("v=" + b64(signature));
                    }
                    client.evaluateChallenge(last); accepted = client.isComplete();
                } catch (Exception error) { failure = error.getClass().getSimpleName(); }
                assertTrue(accepted == variant.equals("valid"), "Apache client proof decision mismatch");
                outcome("client-" + mechanism.mechanismName(), variant, accepted, client.isComplete(), failure,
                    String.join(":", hex(first), hex(challenge), hex(finalResponse), hex(last)));
                client.dispose(); server.dispose();
            }
        }
    }
    private static List<String> rfcExtensions() throws Exception {
        List<String> rows = new ArrayList<>();
        rows.add("name\tmechanism\tpassword_hex\tsalt_hex\titerations\tclient_first_hex\tserver_first_hex\tclient_final_hex\tserver_final_hex\tstored_key_hex\tserver_key_hex\tserver_accepted");
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            ScramFormatter formatter = new ScramFormatter(mechanism);
            byte[] salted = formatter.saltedPassword("pencil", SALT, 4096);
            ScramCredential credential = formatter.generateCredential(SALT, salted, 4096);
            ScramSaslServer server = new ScramSaslServer(mechanism, Map.of(), serverCallback("user", credential));
            fixtureRandom(server, 33);
            String first = "n,,n=user,r=" + NONCE + ",x=first";
            String challenge = text(server.evaluateResponse(bytes(first)));
            String finalWithoutProof = "c=biws,r=" + new ServerFirstMessage(bytes(challenge)).nonce() + ",x=bound";
            /* RFC 5802 AuthMessage preserves both raw extension-bearing inputs.
             * Use actual Apache crypto functions, not its lossy parsed renderer. */
            byte[] auth = bytes(ScramFormatter.authMessage(first.substring(3), challenge, finalWithoutProof));
            byte[] clientSignature = formatter.hmac(credential.storedKey(), auth);
            byte[] proof = ScramFormatter.xor(formatter.clientKey(salted), clientSignature);
            assertTrue(Arrays.equals(formatter.storedKey(clientSignature, proof), credential.storedKey()), "raw RFC proof equation fails");
            String finalMessage = finalWithoutProof + ",p=" + b64(proof);
            String last = "v=" + b64(formatter.hmac(credential.serverKey(), auth));
            boolean apacheAccepted = false; String failure = "none";
            try { server.evaluateResponse(bytes(finalMessage)); apacheAccepted = server.isComplete(); }
            catch (Exception error) { failure = error.getClass().getSimpleName(); }
            assertTrue(!apacheAccepted && !server.isComplete(), "Apache unexpectedly binds raw final extension");
            rows.add(String.join("\t", "rfc-bound-optional-extensions", mechanism.mechanismName(), hex("pencil"),
                hex(SALT), "4096", hex(first), hex(challenge), hex(finalMessage), hex(last),
                hex(credential.storedKey()), hex(credential.serverKey()), "true"));
            outcome("rfc-raw-" + mechanism.mechanismName(), "rfc-bound-optional-extensions", apacheAccepted,
                server.isComplete(), failure, String.join(":", hex(first), hex(challenge), hex(finalMessage), hex(last)));
            server.dispose();
        }
        return rows;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1 && !(args.length == 2 && args[1].equals("mutate-rfc")))
            throw new IllegalArgumentException("output directory and optional mutate-rfc required");
        mutateRfc = args.length == 2;
        Path directory = Path.of(args[0]); Files.createDirectory(directory);
        OUTCOMES.add("kind\tname\taccepted\tcomplete_after_optional_retry\texception_class\textra");
        write(directory.resolve("apache-scram.tsv"), scram());
        write(directory.resolve("apache-messages.tsv"), messages());
        write(directory.resolve("apache-plain.tsv"), plain());
        write(directory.resolve("rfc-scram-sha256.tsv"), rfc());
        write(directory.resolve("rfc-scram-extensions.tsv"), rfcExtensions());
        clientProofs();
        write(directory.resolve("outcomes.tsv"), OUTCOMES);
        System.out.println("PASS server_scram=48 parser=36 plain=12 rfc=1 rfc_extensions=2 client_proof=12");
    }
}
