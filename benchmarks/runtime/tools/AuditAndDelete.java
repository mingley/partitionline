import java.util.Arrays;

/** Delete an owned topic only after its independent readback completes. */
public final class AuditAndDelete {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) throw new IllegalArgumentException("bulk|latency bootstrap topic readback-args");
        String[] readback = Arrays.copyOfRange(args, 1, args.length);
        if (args[0].equals("bulk")) {
            ConformanceBenchProduceSettings.main(readback);
        } else if (args[0].equals("latency")) {
            LatencyReadback.main(readback);
        } else {
            throw new IllegalArgumentException("unknown readback mode");
        }
        RuntimeTopic.delete(args[1], args[2]);
    }
}
