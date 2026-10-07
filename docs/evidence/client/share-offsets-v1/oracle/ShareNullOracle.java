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

public final class ShareNullOracle {
    private ShareNullOracle() { }
    public static void main(String[] args) throws Exception {
        Path fixtures = Path.of(args[0]);
        List<String> rows = new ArrayList<>();
        String release = org.apache.kafka.common.utils.AppInfoParser.getVersion();
        for (short version : release.equals("4.1.2") ? new short[]{0} : new short[]{0, 1}) {
            for (String side : new String[]{"request", "response"}) {
                byte[] original = Files.readAllBytes(fixtures.resolve("api-90-v" + version + "-topics-named." + side + ".bin"));
                ByteBuffer cursor = ByteBuffer.wrap(original);
                if (side.equals("request")) RequestHeader.parse(cursor);
                else ResponseHeader.parse(cursor, ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS.responseHeaderVersion(version));
                int start = cursor.position();
                int[] offsets = side.equals("request") ? new int[]{0,1,4,6} : new int[]{4,5,7,8,26};
                String[] fields = side.equals("request") ? new String[]{"groups", "group-name", "topic-name", "partitions"} : new String[]{"groups", "group-name", "topics", "topic-name", "partitions"};
                for (int index = 0; index < offsets.length; index++) {
                    byte[] changed = original.clone();
                    if (changed[start + offsets[index]] != 2 && changed[start + offsets[index]] != 3) throw new AssertionError("Pinned schema boundary");
                    changed[start + offsets[index]] = 0;
                    cursor = ByteBuffer.wrap(changed);
                    boolean rejected = false;
                    String failure = "";
                    try {
                        if (side.equals("request")) {
                            RequestHeader.parse(cursor);
                            AbstractRequest.parseRequest(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, version, new ByteBufferAccessor(cursor));
                        } else {
                            ResponseHeader.parse(cursor, ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS.responseHeaderVersion(version));
                            AbstractResponse.parseResponse(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, new ByteBufferAccessor(cursor), version);
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
