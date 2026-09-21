package org.apache.kafka.conformance;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * KL01-03: Pinned Apache Kafka wire-protocol fixture generator.
 *
 * Generates reference wire fixtures directly using Apache Kafka's official
 * message serialization implementations (Message.write). Generation is
 * completely independent of partitionline encoders.
 */
public class FixtureGenerator {

    public static final String UPSTREAM_SHA = "ce24d9b6aedca74f53c26f1ad2b2cc8ad950a03c";
    public static final String ARTIFACT_VERSION = "org.apache.kafka:kafka-clients:3.9.1";
    public static final String PIN_VERSION = "3.9.1";

    public static void main(String[] args) throws Exception {
        Path outDir = Path.of("tests/fixtures/protocol_oracles");
        boolean verify = false;

        for (int i = 0; i < args.length; i++) {
            if ("--out-dir".equals(args[i]) && i + 1 < args.length) {
                outDir = Path.of(args[++i]);
            } else if ("--verify".equals(args[i])) {
                verify = true;
            } else if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                System.out.println("Usage: java FixtureGenerator [--out-dir <path>] [--verify]");
                System.exit(0);
            }
        }

        Files.createDirectories(outDir);
        System.out.println("FixtureGenerator: generating smoke fixture using " + ARTIFACT_VERSION);
        System.out.println("Upstream SHA: " + UPSTREAM_SHA);
        System.out.println("Target directory: " + outDir.toAbsolutePath());

        SmokePair pair = generateProduceV9Smoke();

        // Self-validation in Java: ensure reference Apache classes can decode what they encoded.
        verifySelfRoundtrip(pair);

        String reqHash = sha256Hex(pair.requestBytes);
        String respHash = sha256Hex(pair.responseBytes);
        String reqHex = HexFormat.of().formatHex(pair.requestBytes);
        String respHex = HexFormat.of().formatHex(pair.responseBytes);

        Path reqPath = outDir.resolve("smoke_produce_v9_request.bin");
        Path respPath = outDir.resolve("smoke_produce_v9_response.bin");
        Path jsonPath = outDir.resolve("smoke_produce_v9.json");

        String jsonContent = buildJsonMetadata(pair, reqHash, respHash, reqHex, respHex);

        if (verify) {
            System.out.println("FixtureGenerator: verifying against existing committed files...");
            if (!Files.exists(reqPath) || !Files.exists(respPath) || !Files.exists(jsonPath)) {
                System.err.println("FAIL: One or more fixture files missing in " + outDir);
                System.exit(1);
            }

            byte[] existingReq = Files.readAllBytes(reqPath);
            byte[] existingResp = Files.readAllBytes(respPath);
            String existingJson = Files.readString(jsonPath);

            if (!Arrays.equals(pair.requestBytes, existingReq)) {
                System.err.println("FAIL: Request bytes mismatch!");
                System.err.println("  Expected SHA256: " + reqHash);
                System.err.println("  Found SHA256:    " + sha256Hex(existingReq));
                System.exit(1);
            }

            if (!Arrays.equals(pair.responseBytes, existingResp)) {
                System.err.println("FAIL: Response bytes mismatch!");
                System.err.println("  Expected SHA256: " + respHash);
                System.err.println("  Found SHA256:    " + sha256Hex(existingResp));
                System.exit(1);
            }

            if (!normalizeJson(jsonContent).equals(normalizeJson(existingJson))) {
                System.err.println("FAIL: Metadata JSON mismatch!");
                System.exit(1);
            }

            System.out.println("FixtureGenerator: VERIFICATION PASSED (all fixtures match byte-for-byte)");
        } else {
            Files.write(reqPath, pair.requestBytes);
            Files.write(respPath, pair.responseBytes);
            Files.writeString(jsonPath, jsonContent);

            System.out.println("Wrote " + reqPath + " (" + pair.requestBytes.length + " bytes, sha256: " + reqHash + ")");
            System.out.println("Wrote " + respPath + " (" + pair.responseBytes.length + " bytes, sha256: " + respHash + ")");
            System.out.println("Wrote " + jsonPath + " (" + jsonContent.length() + " chars)");
            System.out.println("FixtureGenerator: SUCCESS");
        }
    }

    public static class SmokePair {
        public final short version = 9;
        public final ProduceRequestData requestData;
        public final ProduceResponseData responseData;
        public final byte[] requestBytes;
        public final byte[] responseBytes;

        public SmokePair(ProduceRequestData requestData, ProduceResponseData responseData,
                         byte[] requestBytes, byte[] responseBytes) {
            this.requestData = requestData;
            this.responseData = responseData;
            this.requestBytes = requestBytes;
            this.responseBytes = responseBytes;
        }
    }

    public static SmokePair generateProduceV9Smoke() {
        short version = 9;

        // Construct reference ProduceRequestData
        ProduceRequestData req = new ProduceRequestData()
            .setAcks((short) 1)
            .setTimeoutMs(5000);

        ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData()
            .setName("smoke-topic");
        topicData.partitionData().add(new ProduceRequestData.PartitionProduceData()
            .setIndex(0)
            .setRecords(MemoryRecords.EMPTY));
        req.topicData().add(topicData);

        ObjectSerializationCache reqCache = new ObjectSerializationCache();
        int reqSize = req.size(reqCache, version);
        ByteBuffer reqBuf = ByteBuffer.allocate(reqSize);
        req.write(new ByteBufferAccessor(reqBuf), reqCache, version);
        byte[] reqBytes = reqBuf.array();

        // Construct reference ProduceResponseData
        ProduceResponseData resp = new ProduceResponseData()
            .setThrottleTimeMs(42);

        ProduceResponseData.TopicProduceResponse topicResp = new ProduceResponseData.TopicProduceResponse()
            .setName("smoke-topic");
        topicResp.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
            .setIndex(0)
            .setErrorCode((short) 0)
            .setBaseOffset(100L)
            .setLogAppendTimeMs(1700000000000L)
            .setLogStartOffset(0L));
        resp.responses().add(topicResp);

        ObjectSerializationCache respCache = new ObjectSerializationCache();
        int respSize = resp.size(respCache, version);
        ByteBuffer respBuf = ByteBuffer.allocate(respSize);
        resp.write(new ByteBufferAccessor(respBuf), respCache, version);
        byte[] respBytes = respBuf.array();

        return new SmokePair(req, resp, reqBytes, respBytes);
    }

    private static void verifySelfRoundtrip(SmokePair pair) {
        // Roundtrip request
        ProduceRequestData decodedReq = new ProduceRequestData();
        decodedReq.read(new ByteBufferAccessor(ByteBuffer.wrap(pair.requestBytes)), pair.version);
        if (decodedReq.acks() != 1 || decodedReq.timeoutMs() != 5000) {
            throw new AssertionError("Self-roundtrip failed on ProduceRequest acks/timeout");
        }
        if (decodedReq.topicData().size() != 1 || !"smoke-topic".equals(decodedReq.topicData().iterator().next().name())) {
            throw new AssertionError("Self-roundtrip failed on ProduceRequest topicData");
        }

        // Roundtrip response
        ProduceResponseData decodedResp = new ProduceResponseData();
        decodedResp.read(new ByteBufferAccessor(ByteBuffer.wrap(pair.responseBytes)), pair.version);
        if (decodedResp.throttleTimeMs() != 42) {
            throw new AssertionError("Self-roundtrip failed on ProduceResponse throttleTimeMs");
        }
        if (decodedResp.responses().size() != 1) {
            throw new AssertionError("Self-roundtrip failed on ProduceResponse responses count");
        }
        ProduceResponseData.TopicProduceResponse top = decodedResp.responses().iterator().next();
        if (!"smoke-topic".equals(top.name()) || top.partitionResponses().size() != 1) {
            throw new AssertionError("Self-roundtrip failed on ProduceResponse topic name/parts");
        }
        ProduceResponseData.PartitionProduceResponse part = top.partitionResponses().iterator().next();
        if (part.index() != 0 || part.errorCode() != 0 || part.baseOffset() != 100L ||
            part.logAppendTimeMs() != 1700000000000L || part.logStartOffset() != 0L) {
            throw new AssertionError("Self-roundtrip failed on ProduceResponse partition fields");
        }
    }

    private static String buildJsonMetadata(SmokePair pair, String reqHash, String respHash,
                                           String reqHex, String respHex) {
        return "{\n" +
            "  \"schema_version\": 1,\n" +
            "  \"fixture_id\": \"smoke-produce-v9\",\n" +
            "  \"api\": \"Produce\",\n" +
            "  \"api_key\": 0,\n" +
            "  \"api_version\": 9,\n" +
            "  \"pin\": \"" + PIN_VERSION + "\",\n" +
            "  \"upstream_repo\": \"https://github.com/apache/kafka.git\",\n" +
            "  \"upstream_sha\": \"" + UPSTREAM_SHA + "\",\n" +
            "  \"artifact\": \"" + ARTIFACT_VERSION + "\",\n" +
            "  \"generator\": \"tests/conformance/java/src/main/java/org/apache/kafka/conformance/FixtureGenerator.java\",\n" +
            "  \"request\": {\n" +
            "    \"file\": \"smoke_produce_v9_request.bin\",\n" +
            "    \"size_bytes\": " + pair.requestBytes.length + ",\n" +
            "    \"sha256\": \"" + reqHash + "\",\n" +
            "    \"hex\": \"" + reqHex + "\"\n" +
            "  },\n" +
            "  \"response\": {\n" +
            "    \"file\": \"smoke_produce_v9_response.bin\",\n" +
            "    \"size_bytes\": " + pair.responseBytes.length + ",\n" +
            "    \"sha256\": \"" + respHash + "\",\n" +
            "    \"hex\": \"" + respHex + "\"\n" +
            "  }\n" +
            "}\n";
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(bytes);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String normalizeJson(String s) {
        return s.replaceAll("\\r\\n", "\n").trim();
    }
}
