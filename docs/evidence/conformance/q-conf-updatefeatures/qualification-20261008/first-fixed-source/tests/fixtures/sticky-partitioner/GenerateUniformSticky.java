// Prepared official SDK oracle caller. NOT compiled or executed in source preparation.
// Compile in the package below against the verified official Kafka 4.3.1 JAR.
// The state machine is in that JAR; this class does not reproduce it.
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.LogContext;

public final class GenerateUniformSticky {
    private static final String TOPIC = "uniform_oracle";
    private static final Node NODE = new Node(0, "localhost", 9092);

    private static final class Controlled extends BuiltInPartitioner {
        private final int[] draws;
        private int cursor;

        Controlled(int batchBytes, int[] draws) {
            super(new LogContext(), TOPIC, batchBytes);
            this.draws = draws;
            updatePartitionLoadStats(null, null, 0); // adaptive disabled
        }

        @Override
        int randomPartition() {
            if (cursor == draws.length)
                throw new IllegalStateException("controlled draw trace exhausted");
            return draws[cursor++] & 0x7fffffff;
        }
    }

    private static Cluster cluster(int partitions, int availableMask) {
        List<PartitionInfo> info = new ArrayList<>();
        for (int p = 0; p < partitions; ++p) {
            Node leader = (availableMask & (1 << p)) != 0 ? NODE : null;
            info.add(new PartitionInfo(TOPIC, p, leader, new Node[]{NODE}, new Node[]{NODE}));
        }
        return new Cluster("oracle", Collections.singletonList(NODE), info,
                Collections.emptySet(), Collections.emptySet());
    }

    // Each update is [appended Rust packed bytes, enableSwitch, keyed/explicit].
    private static void emit(String name, int batchBytes, int partitions, int availableMask,
                             int[] draws, int[][] updates) {
        Controlled policy = new Controlled(batchBytes, draws);
        Cluster cluster = cluster(partitions, availableMask);
        int generation = 0;
        System.out.print("{\"case\":\"" + name + "\",\"batch_bytes\":" + batchBytes
                + ",\"partitions\":" + partitions + ",\"available_mask\":" + availableMask
                + ",\"updates\":[");
        for (int i = 0; i < updates.length; ++i) {
            BuiltInPartitioner.StickyPartitionInfo before = policy.peekCurrentPartitionInfo(cluster);
            int[] update = updates[i];
            BuiltInPartitioner.StickyPartitionInfo input = update[2] == 0 ? before : null;
            policy.updatePartitionInfo(input, update[0], cluster, update[1] != 0);
            BuiltInPartitioner.StickyPartitionInfo after = policy.peekCurrentPartitionInfo(cluster);
            boolean changed = before != after;
            int oldGeneration = generation;
            if (changed) ++generation;
            if (i != 0) System.out.print(",");
            System.out.print("{\"delta\":" + update[0] + ",\"enable_switch\":" + (update[1] != 0)
                    + ",\"null_info\":" + (input == null)
                    + ",\"before_partition\":" + before.partition()
                    + ",\"after_partition\":" + after.partition()
                    + ",\"before_generation\":" + oldGeneration
                    + ",\"after_generation\":" + generation + "}");
        }
        System.out.println("]}");
    }

    public static void main(String[] args) {
        System.out.println("{\"scope\":\"official BuiltInPartitioner with explicit Rust admitted-byte events; no Java accumulator parity\"}");
        emit("B_boundary", 300, 3, 7, new int[]{0, 1}, new int[][]{{100,1,0},{100,1,0},{100,1,0}});
        emit("B_deferred_then_zero_completion", 300, 3, 7, new int[]{0,1}, new int[][]{{300,0,0},{299,0,0},{0,1,0}});
        emit("two_B_force", 300, 3, 7, new int[]{0,1}, new int[][]{{600,0,0}});
        emit("same_partition_redraw", 100, 3, 7, new int[]{2,2}, new int[][]{{100,1,0}});
        emit("one_partition", 100, 1, 1, new int[]{9,17}, new int[][]{{100,1,0}});
        emit("available_leaders_only", 100, 3, 5, new int[]{0,1,2,3}, new int[][]{{100,1,0},{100,1,0},{100,1,0}});
        emit("all_unavailable_fallback", 100, 3, 0, new int[]{2,1}, new int[][]{{100,1,0}});
        emit("partial_below_B", 300, 3, 7, new int[]{0}, new int[][]{{161,0,0},{0,1,0}});
        emit("mixed_keyed_null_info", 300, 3, 7, new int[]{0,1}, new int[][]{{161,0,0},{100,1,1},{161,0,0},{0,1,0}});
    }
}
