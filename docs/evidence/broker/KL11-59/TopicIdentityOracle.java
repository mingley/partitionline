/* Runs real Topic/Uuid semantics from independently pinned Apache jars. */
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;

public final class TopicIdentityOracle {
    private TopicIdentityOracle() { }
    public static void main(String[] args) throws Exception {
        boolean mutate = args.length == 2 && args[1].equals("mutate");
        int count = 0;
        for (String line : Files.readAllLines(Path.of(args[0]), StandardCharsets.UTF_8)) {
            String name = new String(HexFormat.of().parseHex(line), StandardCharsets.UTF_8);
            boolean valid = true;
            try { Topic.validate(name); }
            catch (InvalidTopicException error) { valid = false; }
            if (valid != Topic.isValid(name)) throw new AssertionError("isValid/validate disagree");
            if (mutate && name.equals("..") && !valid)
                throw new AssertionError("deliberate variant incorrectly expects '..' valid");
            System.out.println(line + "\t" + valid);
            count++;
        }
        if (count != 29) throw new AssertionError("incomplete name vectors");
        if (!Topic.hasCollision("a.b", "a_b") || Topic.hasCollision("Ab", "ab"))
            throw new AssertionError("collision semantics mismatch");
        if (!Uuid.RESERVED.contains(new Uuid(0, 0)) || !Uuid.RESERVED.contains(new Uuid(0, 1)) ||
            Uuid.RESERVED.contains(new Uuid(0, 2)) || !Uuid.METADATA_TOPIC_ID.equals(Uuid.ONE_UUID))
            throw new AssertionError("reserved identity semantics mismatch");
        System.err.println("PASS names=29 collision_pairs=2 reserved_identity_assertions=4");
    }
}
