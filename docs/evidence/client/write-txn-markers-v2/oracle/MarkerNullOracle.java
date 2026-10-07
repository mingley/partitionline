/* Mutate authentic SDK frames at pinned nonnullable compact field boundaries. */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

public final class MarkerNullOracle {
    private MarkerNullOracle() { }
    public static void main(String[] args) throws Exception {
        Path fixtures = Path.of(args[0]);
        List<String> rows = new ArrayList<>();
        String release = org.apache.kafka.common.utils.AppInfoParser.getVersion();
        for (short version : release.equals("4.1.2") ? new short[]{1} : new short[]{1, 2}) {
            for (String side : new String[]{"request", "response"}) {
                byte[] original = Files.readAllBytes(fixtures.resolve("api-27-v" + version + "-tv-0." + side + ".bin"));
                ByteBuffer cursor = ByteBuffer.wrap(original);
                if (side.equals("request")) RequestHeader.parse(cursor);
                else ResponseHeader.parse(cursor, ApiKeys.WRITE_TXN_MARKERS.responseHeaderVersion(version));
                int start = cursor.position();
                int[] offsets = side.equals("request") ? new int[]{0,12,13,15} : new int[]{0,9,10,12};
                String[] fields = {"markers", "topics", "name", "partitions"};
                for (int index = 0; index < offsets.length; index++) {
                    byte[] changed = original.clone();
                    if (changed[start + offsets[index]] != 2) throw new AssertionError("Pinned schema boundary");
                    changed[start + offsets[index]] = 0;
                    cursor = ByteBuffer.wrap(changed);
                    boolean rejected = false;
                    String failure = "";
                    try {
                        if (side.equals("request")) {
                            RequestHeader.parse(cursor);
                            AbstractRequest.parseRequest(ApiKeys.WRITE_TXN_MARKERS, version, new ByteBufferAccessor(cursor));
                        } else {
                            ResponseHeader.parse(cursor, ApiKeys.WRITE_TXN_MARKERS.responseHeaderVersion(version));
                            AbstractResponse.parseResponse(ApiKeys.WRITE_TXN_MARKERS, new ByteBufferAccessor(cursor), version);
                        }
                    } catch (RuntimeException expected) {
                        if (expected instanceof org.apache.kafka.common.errors.UnsupportedVersionException) throw expected;
                        rejected = true;
                        failure = expected.getClass().getName();
                    }
                    if (!rejected) throw new AssertionError("SDK accepted null " + side + ":" + fields[index]);
                    String name = "null-v" + version + "-" + side + "-" + fields[index];
                    Files.write(fixtures.resolve(name + ".bin"), changed, StandardOpenOption.CREATE_NEW);
                    rows.add(name + "\t" + side + "\t" + version + "\t" + fields[index] + "\t" + failure);
                }
            }
        }
        Files.writeString(fixtures.resolve("null-controls.tsv"), String.join("\n", rows) + "\n", StandardOpenOption.CREATE_NEW);
        System.out.println("Actual pinned SDK rejected " + rows.size() + " null-field frames");
    }
}
