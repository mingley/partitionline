import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.internals.IdentitySerde;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.snapshot.FileRawSnapshotReader;
import org.apache.kafka.snapshot.FileRawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.Snapshots;

/** Scratch component preparation; no partitionline snapshot implementation claim. */
public final class ApacheSnapshotProbe {
    private static int checks;
    private ApacheSnapshotProbe() { }
    private static final class FixedTime implements Time {
        public long milliseconds() { return 1000; }
        public long nanoseconds() { return 1000000000; }
        public void sleep(long milliseconds) { throw new UnsupportedOperationException(); }
        public void waitObject(Object object, Supplier<Boolean> condition, long deadline) {
            throw new UnsupportedOperationException();
        }
    }
    private static void eq(String label, long actual, long expected) {
        boolean pass = actual == expected;
        System.out.println("{\"case\":\"" + label + "\",\"actual\":" + actual
            + ",\"expected\":" + expected + ",\"passed\":" + pass + "}");
        checks++;
        if (!pass) throw new AssertionError(label);
    }
    private static String read(Path parent, OffsetAndEpoch id) {
        try (FileRawSnapshotReader raw = FileRawSnapshotReader.open(parent, id);
             RecordsSnapshotReader<ByteBuffer> reader = RecordsSnapshotReader.of(raw,
                 IdentitySerde.INSTANCE, BufferSupplier.NO_CACHING, 4096, true, new LogContext())) {
            eq("reader.last-contained-offset", reader.lastContainedLogOffset(), 2);
            eq("reader.last-contained-epoch", reader.lastContainedLogEpoch(), 5);
            eq("reader.last-contained-timestamp", reader.lastContainedLogTimestamp(), 1000);
            StringBuilder text = new StringBuilder();
            while (reader.hasNext()) {
                for (ByteBuffer record : reader.next().records()) {
                    byte[] bytes = new byte[record.remaining()]; record.get(bytes);
                    text.append(new String(bytes, StandardCharsets.UTF_8)).append(';');
                }
            }
            return text.toString();
        }
    }
    public static void main(String[] args) throws Exception {
        Path parent = Path.of(args[0]); Files.createDirectories(parent);
        OffsetAndEpoch id = new OffsetAndEpoch(3, 5);
        Path published = Snapshots.snapshotPath(parent, id);
        try (FileRawSnapshotWriter raw = FileRawSnapshotWriter.create(parent, id)) {
            raw.append(MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(1000L, "unfinished".getBytes(StandardCharsets.UTF_8))));
            eq("incomplete.not-published", Files.exists(published) ? 1 : 0, 0);
            try (var paths = Files.list(parent)) { eq("incomplete.partial-present", paths.count(), 1); }
        }
        try (var paths = Files.list(parent)) { eq("incomplete.close-removes-partial", paths.count(), 0); }
        try (FileRawSnapshotWriter raw = FileRawSnapshotWriter.create(parent, id);
             RecordsSnapshotWriter<ByteBuffer> writer = new RecordsSnapshotWriter.Builder()
                 .setRawSnapshotWriter(raw).setTime(new FixedTime())
                 .setMaxBatchSizeBytes(4096).setLastContainedLogTimestamp(1000)
                 .build(IdentitySerde.INSTANCE)) {
            eq("writer.exclusive-end-maps-inclusive-offset", writer.lastContainedLogOffset(), 2);
            writer.append(List.of(ByteBuffer.wrap("alpha=a".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("beta=b".getBytes(StandardCharsets.UTF_8))));
            eq("writer.not-published-before-freeze", Files.exists(published) ? 1 : 0, 0);
            long bytes = writer.freeze();
            eq("writer.published-after-freeze", Files.exists(published) ? 1 : 0, 1);
            eq("writer.exact-published-size", Files.size(published), bytes);
            eq("writer.frozen", writer.isFrozen() ? 1 : 0, 1);
            boolean rejected = false;
            try { writer.append(List.of(ByteBuffer.wrap(new byte[] {1}))); }
            catch (IllegalStateException expected) { rejected = true; }
            eq("writer.append-after-freeze-rejected", rejected ? 1 : 0, 1);
        }
        String records = read(parent, id);
        eq("reader.exact-record-order", records.equals("alpha=a;beta=b;") ? 1 : 0, 1);
        byte[] original = Files.readAllBytes(published);
        Path corruption = parent.resolve("corrupt"); Files.createDirectories(corruption);
        byte[] changed = original.clone(); changed[21] ^= 1;
        Files.write(Snapshots.snapshotPath(corruption, id), changed);
        String exception = "none";
        try { read(corruption, id); }
        catch (RuntimeException error) { exception = error.getClass().getSimpleName(); }
        System.out.println("{\"case\":\"reader.corrupt-crc\",\"actual_exception\":\"" + exception + "\"}");
        eq("reader.corrupt-crc-rejected", exception.equals("CorruptRecordException") ? 1 : 0, 1);
        int footerBytes = 0;
        for (var batch : MemoryRecords.readableRecords(ByteBuffer.wrap(original)).batches()) {
            footerBytes = batch.sizeInBytes();
        }
        Path footerless = parent.resolve("footerless"); Files.createDirectories(footerless);
        Files.write(Snapshots.snapshotPath(footerless, id),
            java.util.Arrays.copyOf(original, original.length - footerBytes));
        // A published byte stream lacking its footer violates the full snapshot
        // transfer contract. This bare reader still accepts its complete prefix;
        // the partitionline installer must validate an explicit completion seal.
        eq("component-boundary.footerless-prefix-accepted",
            read(footerless, id).equals("alpha=a;beta=b;") ? 1 : 0, 1);
        System.out.println("{\"summary\":true,\"checks\":" + checks + ",\"passed\":true}");
    }
}
