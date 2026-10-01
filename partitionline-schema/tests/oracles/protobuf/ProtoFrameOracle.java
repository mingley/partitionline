/* SPDX-License-Identifier: Apache-2.0 */
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import org.apache.kafka.common.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/** Independent Confluent indexes plus caller-provided protoc payload. */
public final class ProtoFrameOracle {
    static final String JAR_SHA = "180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb";
    public static void main(String[] args) throws Exception {
        if (args.length != 6 || !List.of("encode", "decode").contains(args[0]))
            throw new IllegalArgumentException("encode|decode <jar> <schema-id> <indexes-csv> <payload> <frame>");
        Path jar = Path.of(args[1]);
        Path loaded = Path.of(ByteUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar)));
        if (!Files.isSameFile(jar, loaded) || !digest.equals(JAR_SHA))
            throw new AssertionError("wrong loaded Apache 4.1.0 distribution jar");
        long schemaId = Long.parseUnsignedLong(args[2]);
        if (schemaId > 0xffff_ffffL) throw new IllegalArgumentException("schema ID exceeds u32");
        List<Integer> path = Arrays.stream(args[3].split(",")).map(Integer::valueOf).toList();
        if (path.isEmpty() || path.stream().anyMatch(i -> i < 0)) throw new IllegalArgumentException("invalid indexes");
        byte[] payload = Files.readAllBytes(Path.of(args[4]));
        Path frame = Path.of(args[5]);
        if (args[0].equals("encode")) {
            byte[] indexes = new MessageIndexes(path).toByteArray();
            ByteBuffer bytes = ByteBuffer.allocate(5 + indexes.length + payload.length);
            bytes.put((byte) 0).putInt((int) schemaId).put(indexes).put(payload);
            Files.write(frame, bytes.array());
        }
        ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(frame));
        if (bytes.get() != 0 || Integer.toUnsignedLong(bytes.getInt()) != schemaId)
            throw new AssertionError("schema header mismatch");
        if (!MessageIndexes.readFrom(bytes).indexes().equals(path)) throw new AssertionError("path mismatch");
        byte[] body = new byte[bytes.remaining()]; bytes.get(body);
        if (!Arrays.equals(body, payload)) throw new AssertionError("payload mismatch");
        System.out.println("verified schema=" + schemaId + " indexes=" + path + " payload=" + payload.length);
    }
}
