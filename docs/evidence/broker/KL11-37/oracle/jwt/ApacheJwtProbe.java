/* Component probe of unmodified official Apache SDK validators. Synthetic test tokens only. */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator;
import org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

public final class ApacheJwtProbe {
    private ApacheJwtProbe() { }
    private static final Set<String> MUST_ACCEPT = Set.of("valid-rsa2048", "valid-rsa4096", "valid-p256",
        "valid-audience-array", "valid-unicode-subject", "valid-no-jti", "valid-no-scope", "valid-lifetime-boundary");
    private static final Set<String> MUST_REJECT = Set.of("wrong-issuer", "wrong-audience", "subject-empty",
        "subject-numeric", "missing-iss", "missing-aud", "missing-sub", "missing-iat", "missing-exp",
        "unknown-key", "algorithm-key-confusion", "unsupported-algorithm", "none-algorithm",
        "malformed-header", "malformed-payload", "array-payload", "tampered-signature-rsa",
        "tampered-signature-ec", "ec-der-signature", "padded-header", "trailing-segment");

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) throw new IllegalArgumentException("release input output [wrong-valid]");
        String release = args[0];
        Path input = Path.of(args[1]).toAbsolutePath();
        Path output = Path.of(args[2]);
        boolean wrong = args.length == 4;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode fixture = mapper.readTree(input.resolve("fixtures.json").toFile());
        String url = input.resolve("jwks.json").toUri().toString();
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", url);
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.files", input.resolve("jwks.json").toString());
        Map<String, Object> config = new HashMap<>();
        config.put("sasl.oauthbearer.jwks.endpoint.url", url);
        config.put("sasl.oauthbearer.expected.issuer", fixture.get("issuer").asText());
        config.put("sasl.oauthbearer.expected.audience", List.of("partitionline"));
        config.put("sasl.oauthbearer.clock.skew.seconds", 5);
        config.put("sasl.oauthbearer.scope.claim.name", "scope");
        config.put("sasl.oauthbearer.sub.claim.name", "sub");
        List<AppConfigurationEntry> jaas = List.of(new AppConfigurationEntry(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Map.of()));
        ObjectNode report = mapper.createObjectNode();
        report.put("schema_version", 1);
        report.put("release", release);
        report.put("scope", "Actual configured BrokerJwtValidator and DefaultJwtValidator with VerificationKeyResolverFactory file authority; real JVM clock, not network OIDC or SASL socket interoperability.");
        report.put("epoch_anchor", fixture.get("epoch_anchor").asLong());
        report.put("started_epoch", Instant.now().getEpochSecond());
        report.put("file_authority_explicitly_allowlisted", true);
        ArrayNode outcomes = report.putArray("cases");
        int assertions = 0;
        for (String kind : List.of("BrokerJwtValidator", "DefaultJwtValidator")) {
            try (JwtValidator validator = kind.equals("BrokerJwtValidator") ? new BrokerJwtValidator() : new DefaultJwtValidator()) {
                validator.configure(config, "OAUTHBEARER", jaas);
                for (JsonNode row : fixture.get("cases")) {
                    String id = row.get("id").asText();
                    String token = Files.readString(input.resolve(row.get("compact_jws_file").asText()), StandardCharsets.UTF_8);
                    long epoch = Instant.now().getEpochSecond();
                    boolean accepted = false;
                    String failure = null;
                    String subject = null;
                    long expiry = -1;
                    try {
                        OAuthBearerToken result = validator.validate(token);
                        accepted = true;
                        subject = result.principalName();
                        expiry = result.lifetimeMs();
                    } catch (RuntimeException e) {
                        failure = e.getClass().getName();
                    }
                    ObjectNode observation = outcomes.addObject();
                    observation.put("id", id);
                    observation.put("validator", kind);
                    observation.put("observed_epoch", epoch);
                    observation.put("accepted", accepted);
                    if (failure == null) observation.putNull("failure_class"); else observation.put("failure_class", failure);
                    if (subject == null) observation.putNull("subject"); else observation.put("subject", subject);
                    observation.put("expiry_ms", expiry);
                    observation.put("strict_local_expected", row.get("expected_policy_decision").asText());
                    observation.put("local_validation_epoch", row.get("validation_epoch").asLong());
                    observation.put("same_local_validation_epoch", epoch == row.get("validation_epoch").asLong());
                    if (wrong && id.equals("valid-rsa2048") && accepted)
                        throw new AssertionError("controlled_wrong_valid_token_verdict");
                    if (MUST_ACCEPT.contains(id)) {
                        if (!accepted) throw new AssertionError("required_component_accept:" + kind + ":" + id);
                        assertions++;
                    }
                    if (MUST_REJECT.contains(id)) {
                        if (accepted) throw new AssertionError("required_component_reject:" + kind + ":" + id);
                        assertions++;
                    }
                }
            }
        }
        report.put("finished_epoch", Instant.now().getEpochSecond());
        report.put("actual_validator_executions", outcomes.size());
        report.put("required_component_assertions", assertions);
        report.put("passed", true);
        mapper.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        System.out.println("passed release=" + release + " executions=" + outcomes.size() + " assertions=" + assertions);
    }
}
