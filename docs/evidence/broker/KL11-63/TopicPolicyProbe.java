/* Execute upstream topic-name checks separately from local-policy goldens. */
import java.util.Arrays;
import org.apache.kafka.common.internals.Topic;
public final class TopicPolicyProbe {
    private TopicPolicyProbe() { }
    private static String quote(String value) {
        if (value == null) return "null";
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
    public static void main(String[] args) {
        for (String name : Arrays.asList("alpha", "__consumer_offsets", "", ".", "..", "bad/name", "a".repeat(249), "a".repeat(250), "hyphen-name", "dot.name", "under_name", null)) {
            try {
                Topic.validate(name);
                System.out.println("{\"name\":" + quote(name) + ",\"outcome\":\"accepted\",\"is_internal\":" + Topic.isInternal(name) + "}");
            } catch (RuntimeException error) {
                System.out.println("{\"name\":" + quote(name) + ",\"outcome\":" + quote(error.getClass().getSimpleName()) + ",\"message\":" + quote(error.getMessage()) + "}");
            }
        }
        System.out.println("{\"collision_pair\":[\"dot.name\",\"dot_name\"],\"collides\":" + Topic.hasCollision("dot.name", "dot_name") + "}");
    }
}
