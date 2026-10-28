import org.apache.kafka.common.record.DefaultRecordBatch;

public class SequenceOracle {
    public static void main(String[] args) {
        int[][] cases = {{Integer.MAX_VALUE, 1}, {Integer.MAX_VALUE - 1, 1},
            {Integer.MAX_VALUE - 1, 2}, {Integer.MAX_VALUE - 1, 3}, {0, Integer.MAX_VALUE}};
        for (int[] test : cases) {
            int actual = DefaultRecordBatch.incrementSequence(test[0], test[1]);
            int expected = (int)(((long)test[0] + test[1]) % ((long)Integer.MAX_VALUE + 1));
            if (actual != expected) throw new AssertionError(actual + " != " + expected);
            System.out.printf("base=%d increment=%d sequence=%d%n", test[0], test[1], actual);
        }
    }
}
