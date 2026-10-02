/* Independent compiled Apache API assertions; no broker or network is used. */
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.errors.UnsupportedVersionException;

public final class MatrixRuntimeOracle {
    private MatrixRuntimeOracle() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("expected inventory TSV path");
        List<String> expected = Files.readAllLines(Path.of(args[0]), StandardCharsets.UTF_8);
        List<String> actual = new ArrayList<>();
        int headers = 0;
        int removed = 0;
        for (ApiKeys api : ApiKeys.values()) {
            String listeners = api.messageType.listeners().stream()
                .map(value -> value.name().toLowerCase(Locale.ROOT)).sorted()
                .collect(Collectors.joining(","));
            List<String> mappings = new ArrayList<>();
            for (short version = api.oldestVersion(); version <= api.latestVersion(); version++) {
                mappings.add(version + ":" + api.requestHeaderVersion(version) + ":" + api.responseHeaderVersion(version));
                headers++;
            }
            if (!api.hasValidVersion()) {
                removed++;
                boolean requestRejected = false;
                boolean responseRejected = false;
                try { api.requestHeaderVersion((short) 0); }
                catch (UnsupportedVersionException error) { requestRejected = true; }
                try { api.responseHeaderVersion((short) 0); }
                catch (UnsupportedVersionException error) { responseRejected = true; }
                if (!requestRejected || !responseRejected)
                    throw new AssertionError("removed key accepted a header: " + api.id);
            }
            actual.add(api.id + "\t" + api.name() + "\t" + api.oldestVersion() + "\t" + api.latestVersion()
                + "\t" + api.latestVersion(false) + "\t" + api.clusterAction + "\t" + api.forwardable
                + "\t" + api.messageType.latestVersionUnstable() + "\t" + listeners + "\t"
                + String.join(",", mappings));
        }
        if (actual.size() != 93 || expected.size() != 93) throw new AssertionError("expected all 93 API keys");
        for (int index = 0; index < actual.size(); index++) {
            if (!actual.get(index).equals(expected.get(index)))
                throw new AssertionError("API key " + index + " mismatch\nexpected=" + expected.get(index)
                    + "\nactual=" + actual.get(index));
            System.out.println(actual.get(index));
        }
        if (ApiKeys.PRODUCE.oldestVersion() != 3 ||
            ApiKeys.PRODUCE.toApiVersionForApiResponse(false, ApiMessageType.ListenerType.BROKER).orElseThrow().minVersion() != 0)
            throw new AssertionError("Produce schema/advertisement exception mismatch");
        if (!ApiKeys.API_VERSIONS.isVersionEnabled(Short.MAX_VALUE, false))
            throw new AssertionError("ApiVersions negotiation exception mismatch");
        System.out.println("PASS keys=" + actual.size() + " headers=" + headers + " removed=" + removed
            + " produce_schema_min=3 produce_upstream_advertised_min=0");
    }
}
