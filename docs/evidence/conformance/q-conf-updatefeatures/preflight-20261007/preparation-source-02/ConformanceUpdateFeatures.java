import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;

/** Finite API57 generated bodies and independently parsed Rust bodies. */
public final class ConformanceUpdateFeatures {
    private ConformanceUpdateFeatures() { }

    private static byte[] bytes(Message value, short version) {
        ByteBuffer buffer = MessageUtil.toByteBufferAccessor(value, version).buffer();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static short version(String name) {
        return Short.parseShort(name.substring(1, name.indexOf('-')));
    }

    private static Message parse(String name, byte[] body) {
        ByteBuffer buffer = ByteBuffer.wrap(body);
        var input = new ByteBufferAccessor(buffer);
        Message value = name.endsWith("request.bin")
            ? new UpdateFeaturesRequestData(input, version(name))
            : new UpdateFeaturesResponseData(input, version(name));
        if (buffer.hasRemaining()) throw new AssertionError("trailing body " + name);
        return value;
    }

    private static void strip(Message value) {
        value.unknownTaggedFields().clear();
        if (value instanceof UpdateFeaturesRequestData request) {
            for (var update : request.featureUpdates()) update.unknownTaggedFields().clear();
        } else if (value instanceof UpdateFeaturesResponseData response) {
            for (var result : response.results()) result.unknownTaggedFields().clear();
        }
    }

    private static void write(Path out, short version, int index, String kind, Message value) throws Exception {
        String name = "v" + version + "-case-" + index + "-" + kind + ".bin";
        byte[] body = bytes(value, version);
        if (body.length > 65536) throw new AssertionError("declared fixture body limit");
        Message normalized = parse(name, body);
        if (!Arrays.equals(bytes(normalized, version), body)) throw new AssertionError("SDK canonical bytes differ");
        for (int size = 0; size < body.length; size++) {
            boolean rejected = false;
            try {
                parse(name, Arrays.copyOf(body, size));
            } catch (RuntimeException | AssertionError expected) {
                rejected = true;
            }
            if (!rejected) throw new AssertionError("accepted truncated SDK body " + name + " prefix " + size);
        }
        Files.write(out.resolve(name), body);
    }

    private static UpdateFeaturesRequestData request(short version, String name, short level, byte type) {
        var updates = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
        var update = new UpdateFeaturesRequestData.FeatureUpdateKey().setFeature(name).setMaxVersionLevel(level);
        if (version == 0) update.setAllowDowngrade(type != 1);
        else update.setUpgradeType(type);
        updates.add(update);
        return new UpdateFeaturesRequestData().setTimeoutMs(10000).setFeatureUpdates(updates);
    }

    private static void publicValues() {
        var options = new UpdateFeaturesOptions();
        if (options.validateOnly()) throw new AssertionError("validate-only default");
        options.validateOnly(true).timeoutMs(1234);
        if (!options.validateOnly() || options.timeoutMs() != 1234) throw new AssertionError("option accessors");
        for (int code : new int[]{Byte.MIN_VALUE, 0, 1, 2, 3, Byte.MAX_VALUE}) {
            var type = FeatureUpdate.UpgradeType.fromCode(code);
            int expected = code >= 1 && code <= 3 ? code : 0;
            if (type.code() != expected) throw new AssertionError("unknown upgrade code normalization");
            for (short level : new short[]{Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE}) {
                boolean invalid = level < 0 || level == 0 && type == FeatureUpdate.UpgradeType.UPGRADE;
                try {
                    var update = new FeatureUpdate(level, type);
                    if (invalid || update.maxVersionLevel() != level || update.upgradeType() != type
                            || !update.toString().equals("FeatureUpdate{maxVersionLevel:" + level + ", upgradeType:" + type + "}")) {
                        throw new AssertionError("feature value contract");
                    }
                } catch (IllegalArgumentException error) {
                    if (!invalid) throw error;
                }
            }
        }
        for (short version = 1; version <= 2; version++) {
            boolean rejected = false;
            try {
                bytes(new UpdateFeaturesRequestData().setFeatureUpdates(
                    request((short)0, "f", (short)1, (byte)2).featureUpdates()), version);
            } catch (UnsupportedVersionException expected) {
                rejected = true;
            }
            if (!rejected) throw new AssertionError("obsolete allow-downgrade accepted on v" + version);
        }
        boolean rejected = false;
        try {
            bytes(new UpdateFeaturesRequestData().setValidateOnly(true), (short)0);
        } catch (UnsupportedVersionException expected) {
            rejected = true;
        }
        if (!rejected) throw new AssertionError("validate-only silently discarded on v0");
    }

    private static void generate(Path out) throws Exception {
        Files.createDirectory(out);
        publicValues();
        int total = 0;
        for (short version = 0; version <= 2; version++) {
            int count = 0;
            write(out, version, count++, "request", new UpdateFeaturesRequestData());
            for (int timeout : new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}) {
                write(out, version, count++, "request", request(version, "f", (short)1, (byte)1).setTimeoutMs(timeout));
            }
            byte[] types = version == 0 ? new byte[]{1, 2} : new byte[]{Byte.MIN_VALUE, 0, 1, 2, 3, Byte.MAX_VALUE};
            for (byte type : types) {
                for (short level : new short[]{0, 1, Short.MAX_VALUE}) {
                    if (type == 1 && level == 0) continue;
                    write(out, version, count++, "request", request(version, "f", level, type));
                }
            }
            var duplicate = request(version, "duplicate", (short)1, (byte)1);
            duplicate.featureUpdates().add(request(version, "duplicate", (short)2, (byte)2).featureUpdates().iterator().next());
            var wrapper = new UpdateFeaturesRequest.Builder(duplicate).build(version);
            if (wrapper.featureUpdates().size() != 2 || wrapper.featureUpdates().stream().anyMatch(item -> item.versionLevel() != 1)) {
                throw new AssertionError("duplicate feature first-match contract");
            }
            write(out, version, count++, "request", duplicate);
            var tagged = request(version, "\u00fc-" + (char)0, (short)1, (byte)1);
            tagged.unknownTaggedFields().add(new RawTaggedField(9, new byte[]{1, 2}));
            tagged.featureUpdates().iterator().next().unknownTaggedFields().add(new RawTaggedField(9, new byte[]{3}));
            write(out, version, count++, "request", tagged);
            if (version >= 1) write(out, version, count++, "request", request(version, "f", (short)1, (byte)1).setValidateOnly(true));

            var success = UpdateFeaturesResponse.createWithErrors(ApiError.NONE, new TreeSet<>(List.of("a", "b")), 17);
            write(out, version, count++, "response", success.data());
            for (short error : new short[]{-1, 31, 41, 42, 96}) {
                var response = wrapper.getErrorResponse(17, Errors.forCode(error).exception());
                if (response.data().errorCode() != error || !response.errorCounts().equals(Map.of(Errors.forCode(error), 1))
                        || response.throttleTimeMs() != 17 || !response.data().results().isEmpty()) {
                    throw new AssertionError("error response factory");
                }
                write(out, version, count++, "response", response.data());
            }
            for (short top : new short[]{0, 41, Short.MIN_VALUE, Short.MAX_VALUE}) {
                var results = new UpdateFeaturesResponseData.UpdatableFeatureResultCollection();
                results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult().setFeature("a").setErrorCode((short)-1));
                results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult().setFeature("a").setErrorCode((short)96).setErrorMessage("failure"));
                var response = new UpdateFeaturesResponseData().setThrottleTimeMs(Integer.MAX_VALUE).setErrorCode(top).setErrorMessage("message").setResults(results);
                write(out, version, count++, "response", response);
                response.unknownTaggedFields().add(new RawTaggedField(9, new byte[]{1}));
                for (var result : response.results()) result.unknownTaggedFields().add(new RawTaggedField(9, new byte[]{2}));
                write(out, version, count++, "response", response);
            }
            write(out, version, count++, "response", new UpdateFeaturesResponseData());
            total += count;
        }
        System.out.println("{\"actual_sdk\":true,\"generated_bodies\":" + total + ",\"versions\":[0,1,2],\"feature_value_invocations\":30}");
    }

    private static void verify(Path candidate, Path golden) throws Exception {
        int count = 0;
        try (var files = Files.list(golden)) {
            for (Path reference : files.sorted().toList()) {
                String name = reference.getFileName().toString();
                if (!name.endsWith(".bin")) continue;
                Message expected = parse(name, Files.readAllBytes(reference));
                strip(expected);
                if (!parse(name, Files.readAllBytes(candidate.resolve(name))).equals(expected)) {
                    throw new AssertionError("Rust known fields differ " + name);
                }
                count++;
            }
        }
        if (count != 101) throw new AssertionError("incomplete reverse corpus: " + count);
        System.out.println("{\"independently_parsed_Rust_bodies\":" + count + "}");
    }

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "generate" -> generate(Path.of(args[1]));
            case "verify" -> verify(Path.of(args[1]), Path.of(args[2]));
            default -> throw new IllegalArgumentException("mode");
        }
    }
}
