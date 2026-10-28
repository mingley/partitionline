import org.apache.kafka.common.record.internal.DefaultRecordBatch;

public class SequenceOracle3 {
    public static void main(String[] args) {
        int[][] cases = {{0, 1}, {1, 1}, {0, 2}, {1, 3}, {Integer.MAX_VALUE, Integer.MAX_VALUE}};
        for (int[] test : cases) {
            int actual = DefaultRecordBatch.decrementSequence(test[0], test[1]);
            long modulus = (long)Integer.MAX_VALUE + 1;
            int expected = (int)(((long)test[0] - test[1] + modulus) % modulus);
            if (actual != expected) throw new AssertionError(actual + " != " + expected);
            System.out.printf("base=%d decrement=%d sequence=%d%n", test[0], test[1], actual);
        }
    }
}
