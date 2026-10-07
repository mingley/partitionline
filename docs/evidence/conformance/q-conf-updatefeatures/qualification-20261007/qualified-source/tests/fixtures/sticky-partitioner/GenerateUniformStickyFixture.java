// WORK preparation only. Calls the genuine Kafka 4.3.1 BuiltInPartitioner.
// No copy of the partition algorithm, accumulator, compression, or RNG is used.
package org.apache.kafka.clients.producer.internals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.LogContext;

public final class GenerateUniformStickyFixture {
    private static final String TOPIC = "t";
    private static final Node NODE = new Node(0, "localhost", 9092);
    private static final int MAX_INPUT_BYTES = 32768;
    private static final int MAX_CASES = 32;
    private static final int MAX_EVENTS = 256;
    private static final int MAX_DRAWS = 128;

    private static final class Controlled extends BuiltInPartitioner {
        private final int[] draws;
        private int cursor;
        Controlled(int batchBytes, int[] draws) {
            super(new LogContext(), TOPIC, batchBytes);
            this.draws = draws;
            updatePartitionLoadStats(null, null, 0); // adaptive=false
        }
        @Override int randomPartition() {
            if (cursor == draws.length)
                throw new IllegalStateException("fixed accepted draw trace exhausted");
            return draws[cursor++];
        }
    }

    private static int bounded(String text, int low, int high) {
        int value = Integer.parseInt(text);
        if (value < low || value > high) throw new IllegalArgumentException("bounded input");
        return value;
    }

    private static boolean full(String text) {
        return bounded(text, 0, 1) == 1;
    }

    private static Cluster cluster(int partitions, int mask) {
        List<PartitionInfo> info = new ArrayList<>();
        for (int p = 0; p < partitions; ++p) {
            Node leader = (mask & (1 << p)) != 0 ? NODE : null;
            info.add(new PartitionInfo(TOPIC, p, leader, new Node[]{NODE}, new Node[]{NODE}));
        }
        return new Cluster("oracle", Collections.singletonList(NODE), info,
                Collections.emptySet(), Collections.emptySet());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("one bounded trace input path required");
        Path input = Path.of(args[0]);
        if (Files.size(input) > MAX_INPUT_BYTES) throw new IllegalArgumentException("oversized trace");
        List<String> lines = Files.readAllLines(input, StandardCharsets.UTF_8);
        if (lines.size() > MAX_CASES + MAX_EVENTS + 8) throw new IllegalArgumentException("too many lines");
        System.out.println("# official-kafka-4.3.1-uniform-rust-packed-events-v1");
        Controlled policy = null;
        Cluster cluster = null;
        String name = null;
        int partitions = 0;
        int cases = 0;
        int totalEvents = 0;
        int event = 0;
        int generation = 0;
        Set<String> ids = new HashSet<>();
        for (String line : lines) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] fields = line.split("\t", -1);
            if (fields[0].equals("CASE")) {
                if (fields.length != 8 || ++cases > MAX_CASES)
                    throw new IllegalArgumentException("case layout");
                if (!ids.isEmpty()) throw new IllegalArgumentException("unreleased membership");
                name = fields[1];
                if (!name.matches("[a-zA-Z0-9_]{1,64}")) throw new IllegalArgumentException("case name");
                int batchBytes = bounded(fields[2], 1, 65536);
                bounded(fields[3], 1, 256); // Rust cohort record limit, explicit in shared input
                partitions = bounded(fields[4], 1, 8);
                int mask = bounded(fields[5], 0, (1 << partitions) - 1);
                Long.parseUnsignedLong(fields[6], 16); // Rust seed; Java does not imitate its RNG
                String[] rawDraws = fields[7].split(",", -1);
                if (rawDraws.length < 1 || rawDraws.length > MAX_DRAWS)
                    throw new IllegalArgumentException("draw count");
                int[] draws = new int[rawDraws.length];
                for (int i = 0; i < draws.length; ++i)
                    draws[i] = bounded(rawDraws[i], 0, Integer.MAX_VALUE);
                policy = new Controlled(batchBytes, draws);
                cluster = cluster(partitions, mask);
                event = 0;
                generation = 0;
                continue;
            }
            if (policy == null || ++totalEvents > MAX_EVENTS)
                throw new IllegalArgumentException("event without case or event bound");
            BuiltInPartitioner.StickyPartitionInfo before = policy.peekCurrentPartitionInfo(cluster);
            int priorGeneration = generation;
            switch (fields[0]) {
                case "U":
                case "K":
                case "E": {
                    if (fields.length != 7) throw new IllegalArgumentException("append layout");
                    if (!fields[1].matches("[a-zA-Z0-9_]{1,32}") || !ids.add(fields[1]))
                        throw new IllegalArgumentException("record alias");
                    bounded(fields[2], -1, partitions - 1); // Rust explicit/keyed route
                    bounded(fields[3], -1, 65536); // value size; -1 is null
                    bounded(fields[4], 1, 65536); // independently prescribed record bound
                    int delta = bounded(fields[5], 1, 65597);
                    policy.updatePartitionInfo(fields[0].equals("U") ? before : null,
                            delta, cluster, full(fields[6]));
                    break;
                }
                case "D":
                    if (fields.length != 3) throw new IllegalArgumentException("drain layout");
                    if (!ids.contains(fields[1])) throw new IllegalArgumentException("unknown drain alias");
                    policy.updatePartitionInfo(before, 0, cluster, full(fields[2]));
                    break;
                case "R":
                    if (fields.length != 2) throw new IllegalArgumentException("release layout");
                    if (!ids.remove(fields[1])) throw new IllegalArgumentException("unknown release alias");
                    break; // Terminal membership removal has no Java accounting call.
                case "P":
                case "F":
                    if (fields.length != 1) throw new IllegalArgumentException("peek layout");
                    break; // Pure/failed admission has no Java accounting call.
                case "M":
                    if (fields.length != 2) throw new IllegalArgumentException("metadata layout");
                    cluster = cluster(partitions, bounded(fields[1], 0, (1 << partitions) - 1));
                    break;
                default:
                    throw new IllegalArgumentException("unknown event");
            }
            BuiltInPartitioner.StickyPartitionInfo after = policy.peekCurrentPartitionInfo(cluster);
            if (before != after) ++generation;
            System.out.println(name + "\t" + event++ + "\t" + before.partition() + "\t"
                    + after.partition() + "\t" + priorGeneration + "\t" + generation);
        }
        if (cases == 0 || totalEvents == 0 || !ids.isEmpty())
            throw new IllegalArgumentException("empty reference corpus or unreleased membership");
    }
}
