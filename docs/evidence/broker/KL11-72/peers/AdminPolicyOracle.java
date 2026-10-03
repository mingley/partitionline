/* Invoke authentic Apache metadata image/controller methods from pinned jars.
 * Package access is required by the actual ControllerResult/Builder API.
 * This is object-level policy execution, not an Apache broker socket run.
 */
package org.apache.kafka.controller;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.server.common.MetadataVersion;

public final class AdminPolicyOracle {
    private static final HexFormat HEX = HexFormat.of();
    private static final byte[] SALT = HEX.parseHex("000102030405060708090a0b0c0d0e0f");
    private AdminPolicyOracle() { }
    private static void require(boolean value, String label) {
        if (!value) throw new AssertionError(label);
    }
    private static ScramCredentialData credential(boolean sha512) throws Exception {
        var mechanism = sha512
                ? org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_512
                : org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_256;
        ScramFormatter formatter = new ScramFormatter(mechanism);
        byte[] salted = formatter.saltedPassword("pencil", SALT, 4096);
        return new ScramCredentialData(SALT, formatter.storedKey(formatter.clientKey(salted)),
                formatter.serverKey(salted), 4096);
    }
    private static ScramControlManager manager() throws Exception {
        var manager = new ScramControlManager.Builder().build();
        var credential = credential(false);
        manager.replay(new UserScramCredentialRecord().setName("user").setMechanism((byte) 1)
                .setSalt(credential.salt()).setIterations(credential.iterations())
                .setStoredKey(credential.storedKey()).setServerKey(credential.serverKey()));
        return manager;
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 2, "wireFixture outputTsv");
        byte[] source = Files.readAllBytes(Path.of(args[0]));
        require(source.length <= 256 * 1024, "bounded public fixture table");
        ScramImage image = new ScramImage(Map.of(
                ScramMechanism.SCRAM_SHA_256, Map.of("user", credential(false)),
                ScramMechanism.SCRAM_SHA_512, Map.of("user", credential(true))));
        List<String> outcomes = new ArrayList<>(List.of(
                "case\tapi\tresult_count\tuser\terror_code\tcredential_infos\tcontroller_records"));
        int cases = 0;
        for (String line : new String(source, java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
            String[] row = line.split("\t", -1);
            if (!row[0].equals("request") || (!row[2].equals("50") && !row[2].equals("51"))) continue;
            ByteBuffer body = ByteBuffer.wrap(HEX.parseHex(row[7]));
            if (row[2].equals("50")) {
                var request = new DescribeUserScramCredentialsRequestData(new ByteBufferAccessor(body), (short) 0);
                var response = image.describe(request);
                require(!body.hasRemaining() && response.errorCode() == 0, "actual image whole input/no global error");
                var results = new ArrayList<>(response.results());
                results.sort(Comparator.comparing(result -> result.user()));
                for (var result : results) {
                    int expected = row[1].equals("describe-duplicate") ? 92 : result.user().equals("missing") ? 91 : 0;
                    require(result.errorCode() == expected, "actual duplicate/missing describe policy");
                    if (row[1].equals("describe-duplicate")) require(results.size() == 1, "duplicate one unique-user result");
                    outcomes.add(String.join("\t", row[1], "50", Integer.toString(results.size()), result.user(),
                            Short.toString(result.errorCode()), Integer.toString(result.credentialInfos().size()), "0"));
                }
            } else {
                var request = new AlterUserScramCredentialsRequestData(new ByteBufferAccessor(body), (short) 0);
                var result = manager().alterCredentials(request, MetadataVersion.latestProduction());
                require(!body.hasRemaining() && result.response().results().size() == 1, "actual controller one result/user");
                var response = result.response().results().get(0);
                int expected = switch (row[1]) {
                    case "alter-both", "alter-duplicate" -> 92;
                    case "alter-invalid-iterations" -> 93;
                    default -> 0;
                };
                require(response.errorCode() == expected, "actual controller mutation status");
                require(result.records().size() == (expected == 0 ? 1 : 0), "actual controller mutation record count");
                outcomes.add(String.join("\t", row[1], "51", "1", response.user(), Short.toString(response.errorCode()),
                        "0", Integer.toString(result.records().size())));
            }
            cases++;
        }
        require(cases == 14, "exact authenticated admin fixture request count");
        Files.write(Path.of(args[1]), outcomes);
        System.out.println("PASS actual_admin_cases=14 duplicate_describe_unique=1 duplicate_cross_algorithm=92 empty_salt=accepted short_salted=accepted");
    }
}
