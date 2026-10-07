import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Independent Apache Avro Java 1.12.1 peer for the selected Rust codec. */
public final class ConformanceAvroSelectedCodec {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String[] EVENTS = {"null", "present", "negative", "intmax"};

    private static Schema eventSchema(Path fixtures, String root, String metadata) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        parser.parse(Files.readString(fixtures.resolve(metadata)));
        return parser.parse(Files.readString(fixtures.resolve(root)));
    }

    private static Object event(JsonNode input, Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", input.get("id").intValue());
        GenericData.Record metadata = new GenericData.Record(schema.getField("metadata").schema());
        metadata.put("source", input.get("metadata").get("source").textValue());
        record.put("metadata", metadata);
        record.put("note", input.get("note").isNull() ? null : input.get("note").textValue());
        return record;
    }

    private static byte[] encode(Schema writer, Object value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().directBinaryEncoder(output, null);
        new GenericDatumWriter<Object>(writer).write(value, encoder);
        encoder.flush();
        byte[] payload = output.toByteArray();
        ByteBuffer frame = ByteBuffer.allocate(5 + payload.length);
        frame.put((byte) 0).putInt(42).put(payload);
        return frame.array();
    }

    private static Object decode(Schema writer, Schema reader, byte[] frame) throws IOException {
        if (frame.length < 5 || frame[0] != 0 || ByteBuffer.wrap(frame, 1, 4).getInt() != 42) {
            throw new IOException("invalid selected header");
        }
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(frame, 5, frame.length - 5, null);
        Object value = new GenericDatumReader<Object>(writer, reader).read(null, decoder);
        // Complete consumption is our frame contract, not Avro's raw reader contract.
        if (!decoder.isEnd()) { throw new IOException("trailing datum bytes"); }
        return value;
    }

    private static Object normalized(Object value) {
        if (value instanceof GenericData.Record record) {
            Map<String, Object> fields = new LinkedHashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                fields.put(field.name(), normalized(record.get(field.name())));
            }
            return fields;
        }
        if (value instanceof CharSequence text) { return text.toString(); }
        return value;
    }

    private static void equalJson(Object expected, Object actual) throws IOException {
        JsonNode left = JSON.readTree(JSON.writeValueAsBytes(normalized(expected)));
        JsonNode right = JSON.readTree(JSON.writeValueAsBytes(normalized(actual)));
        if (!left.equals(right)) { throw new AssertionError("decoded value mismatch"); }
    }

    private static void checkCase(String name, Schema writer, Schema reader, Object input,
            Path fixtures, Path output, boolean verify) throws IOException {
        byte[] frame = encode(writer, input);
        Object evolved = decode(writer, reader, frame);
        if (!verify) {
            Files.write(fixtures.resolve(name + ".frame.bin"), frame);
            JSON.writerWithDefaultPrettyPrinter().writeValue(fixtures.resolve(name + ".writer.json").toFile(), normalized(input));
            JSON.writerWithDefaultPrettyPrinter().writeValue(fixtures.resolve(name + ".reader.json").toFile(), normalized(evolved));
        } else {
            if (!Arrays.equals(frame, Files.readAllBytes(fixtures.resolve(name + ".frame.bin")))) {
                throw new AssertionError("peer fixture regeneration differs: " + name);
            }
            byte[] rust = Files.readAllBytes(output.resolve(name + ".frame.bin"));
            if (!Arrays.equals(frame, rust)) { throw new AssertionError("Rust encoded bytes differ: " + name); }
            equalJson(input, decode(writer, writer, rust));
            equalJson(evolved, decode(writer, reader, rust));
            equalJson(evolved, JSON.readTree(Files.readString(output.resolve(name + ".reader.json"))));
        }
    }

    @FunctionalInterface private interface Operation { void run() throws Exception; }
    private static void rejects(Map<String, String> outcomes, String name, Operation operation) throws Exception {
        try { operation.run(); }
        catch (IOException | org.apache.avro.AvroRuntimeException | IndexOutOfBoundsException error) {
            outcomes.put(name, error.getClass().getName());
            return;
        }
        throw new AssertionError("unexpected success: " + name);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !(args[0].equals("generate") || args[0].equals("verify"))) {
            throw new IllegalArgumentException("generate|verify <schema inputs> <fixtures> <rust output>");
        }
        boolean verify = args[0].equals("verify");
        Path inputs = Path.of(args[1]), fixtures = Path.of(args[2]), output = Path.of(args[3]);
        Files.createDirectories(fixtures);
        Schema writer = eventSchema(inputs, "writer.avsc", "metadata-v1.avsc");
        Schema reader = eventSchema(inputs, "reader.avsc", "metadata-v2.avsc");
        for (String name : EVENTS) {
            JsonNode input = JSON.readTree(Files.readString(inputs.resolve(name + ".writer.json")));
            checkCase(name, writer, reader, event(input, writer), fixtures, output, verify);
        }
        String[][] primitives = {
            {"root-null", "null", "null"}, {"boolean", "boolean", "boolean"},
            {"int-long", "int", "long"}, {"long-double", "long", "double"},
            {"float-double", "float", "double"}
        };
        Object[] values = {null, true, Integer.MIN_VALUE, 9007199254740993L, 1.5f};
        for (int i = 0; i < primitives.length; i++) {
            String[] cell = primitives[i];
            Schema w = new Schema.Parser().parse("\"" + cell[1] + "\"");
            Schema r = new Schema.Parser().parse("\"" + cell[2] + "\"");
            checkCase(cell[0], w, r, values[i], fixtures, output, verify);
        }
        Map<String, String> outcomes = new LinkedHashMap<>();
        byte[] valid = Files.readAllBytes(fixtures.resolve("present.frame.bin"));
        rejects(outcomes, "incompatible", () -> decode(writer, eventSchema(inputs, "incompatible.avsc", "metadata-v2.avsc"), valid));
        rejects(outcomes, "required_without_default", () -> decode(writer, eventSchema(inputs, "required.avsc", "metadata-v2.avsc"), valid));
        rejects(outcomes, "missing_reference", () -> new Schema.Parser().parse(Files.readString(inputs.resolve("writer.avsc"))));
        rejects(outcomes, "truncated", () -> decode(writer, reader, Arrays.copyOf(valid, valid.length - 1)));
        byte[] union = Files.readAllBytes(fixtures.resolve("null.frame.bin"));
        union[union.length - 1] = 4;
        rejects(outcomes, "invalid_union", () -> decode(writer, reader, union));
        rejects(outcomes, "trailing", () -> decode(writer, reader, Arrays.copyOf(valid, valid.length + 1)));
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("peer", "Apache Avro Java 1.12.1"); report.put("mode", args[0]);
        report.put("cases", 9); report.put("negative_cases", outcomes); report.put("status", "pass");
        System.out.println(JSON.writeValueAsString(report));
    }
}
