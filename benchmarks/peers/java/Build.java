import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.ToolProvider;

/** Deterministic class archive built with the pinned JDK's compiler. */
class Build {
    public static void main(String[] args) throws Exception {
        Path classes = Path.of(args[2]);
        Files.createDirectory(classes);
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IllegalStateException("A full JDK is required");
        int result = compiler.run(null, System.out, System.err,
            "-source", "21", "-target", "21", "-Xlint:all", "-Werror", "-classpath", args[1],
            "-d", classes.toString(), args[0]);
        if (result != 0) System.exit(result);
        try (var output = new JarOutputStream(Files.newOutputStream(Path.of(args[3])));
             var paths = Files.walk(classes)) {
            List<Path> files = paths.filter(Files::isRegularFile).sorted().toList();
            for (Path file : files) {
                var entry = new JarEntry(classes.relativize(file).toString().replace('\\', '/'));
                entry.setTime(315532800000L);
                output.putNextEntry(entry);
                Files.copy(file, output);
                output.closeEntry();
            }
        }
    }
}
