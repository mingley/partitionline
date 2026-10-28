import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

/** Independent Kafka Java Admin probe for the metadata-only profile. */
public final class QuorumPeer {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("bootstrap action topic");
        Properties config = new Properties();
        config.put("bootstrap.servers", args[0]);
        config.put("request.timeout.ms", "5000");
        config.put("default.api.timeout.ms", "15000");
        config.put("retries", "3");
        try (Admin admin = Admin.create(config)) {
            switch (args[1]) {
                case "create" -> {
                    admin.createTopics(List.of(new NewTopic(args[2], 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
                    System.out.println("created=" + args[2]);
                }
                case "list" -> {
                    Collection<String> names = admin.listTopics().names().get(15, TimeUnit.SECONDS);
                    if (!names.contains(args[2])) throw new AssertionError("topic absent: " + args[2] + " names=" + names);
                    System.out.println("present=" + args[2]);
                }
                case "absent" -> {
                    Collection<String> names = admin.listTopics().names().get(15, TimeUnit.SECONDS);
                    if (names.contains(args[2])) throw new AssertionError("topic still present: " + args[2]);
                    System.out.println("absent=" + args[2]);
                }
                case "delete" -> {
                    admin.deleteTopics(List.of(args[2])).all().get(15, TimeUnit.SECONDS);
                    System.out.println("deleted=" + args[2]);
                }
                default -> throw new IllegalArgumentException("unknown action");
            }
        }
    }
}
