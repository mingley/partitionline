import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

// Kafka's Java gzip codec is java.util.zip GZIP{Input,Output}Stream (KIP-390 adds a level).
public class Interop {
    static class LevelGzip extends GZIPOutputStream {
        LevelGzip(OutputStream out, int level) throws IOException { super(out, 8 * 1024); def.setLevel(level); }
    }
    public static void main(String[] a) throws Exception {
        Path dir = Paths.get(a[0]);
        int fails = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "rust-*.gz")) {
            List<Path> ps = new ArrayList<>(); ds.forEach(ps::add); Collections.sort(ps);
            for (Path p : ps) {
                String name = p.getFileName().toString().replace(".gz", "");
                String n = name.substring(name.lastIndexOf('-') + 1);
                byte[] want = Files.readAllBytes(dir.resolve("section-" + n + ".bin"));
                byte[] got = new GZIPInputStream(new ByteArrayInputStream(Files.readAllBytes(p))).readAllBytes();
                boolean ok = Arrays.equals(want, got);
                if (!ok) fails++;
                System.out.println("java decodes " + name + ": " + (ok ? "ok" : "MISMATCH"));
            }
        }
        for (int n : new int[] {1, 16, 256}) {
            byte[] section = Files.readAllBytes(dir.resolve("section-" + n + ".bin"));
            for (int level : new int[] {Deflater.DEFAULT_COMPRESSION, 1, 9}) {
                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                try (OutputStream o = new BufferedOutputStream(new LevelGzip(bo, level), 16 * 1024)) { o.write(section); }
                String lv = level < 0 ? "default" : Integer.toString(level);
                Files.write(dir.resolve("java-l" + lv + "-" + n + ".gz"), bo.toByteArray());
            }
        }
        System.out.println("java wrote sections (zlib " + Runtime.version() + ")");
        System.exit(fails == 0 ? 0 : 1);
    }
}
