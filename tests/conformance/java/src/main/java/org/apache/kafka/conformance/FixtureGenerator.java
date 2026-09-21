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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/**
 * KL01-03 & KL01-04: Pinned Apache Kafka wire-protocol fixture generator.
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
        if (args.length >= 4 && "--decode-rust".equals(args[0])) {
            decodeRust(args[1], Short.parseShort(args[2]), args[3]);
            return;
        }

        Path outDir = Path.of("tests/fixtures/protocol_oracles");
        boolean verify = false;

        for (int i = 0; i < args.length; i++) {
            if ("--out-dir".equals(args[i]) && i + 1 < args.length) {
                outDir = Path.of(args[++i]);
            } else if ("--verify".equals(args[i])) {
                verify = true;
            } else if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                System.out.println("Usage: java FixtureGenerator [--out-dir <path>] [--verify] | [--decode-rust <req|resp> <version> <file_or_hex>]");
                System.exit(0);
            }
        }

        Files.createDirectories(outDir);
        System.out.println("FixtureGenerator: generating Produce fixtures using " + ARTIFACT_VERSION);
        System.out.println("Upstream SHA: " + UPSTREAM_SHA);
        System.out.println("Target directory: " + outDir.toAbsolutePath());

        List<ProduceFixture> fixtures = createAllProduceFixtures();

        for (ProduceFixture f : fixtures) {
            // Self-validation in Java: ensure reference Apache classes can decode what they encoded.
            verifySelfRoundtrip(f);

            String reqHash = sha256Hex(f.requestBytes);
            String respHash = sha256Hex(f.responseBytes);
            String reqHex = HexFormat.of().formatHex(f.requestBytes);
            String respHex = HexFormat.of().formatHex(f.responseBytes);

            Path reqPath = outDir.resolve(f.filePrefix + "_request.bin");
            Path respPath = outDir.resolve(f.filePrefix + "_response.bin");
            Path jsonPath = outDir.resolve(f.filePrefix + ".json");

            String jsonContent = buildJsonMetadata(f, reqHash, respHash, reqHex, respHex);

            if (verify) {
                System.out.println("FixtureGenerator: verifying " + f.id + " against existing committed files...");
                if (!Files.exists(reqPath) || !Files.exists(respPath) || !Files.exists(jsonPath)) {
                    System.err.println("FAIL: One or more fixture files missing for " + f.id + " in " + outDir);
                    System.exit(1);
                }

                byte[] existingReq = Files.readAllBytes(reqPath);
                byte[] existingResp = Files.readAllBytes(respPath);
                String existingJson = Files.readString(jsonPath);

                if (!Arrays.equals(f.requestBytes, existingReq)) {
                    System.err.println("FAIL: Request bytes mismatch for " + f.id + "!");
                    System.err.println("  Expected SHA256: " + reqHash);
                    System.err.println("  Found SHA256:    " + sha256Hex(existingReq));
                    System.exit(1);
                }

                if (!Arrays.equals(f.responseBytes, existingResp)) {
                    System.err.println("FAIL: Response bytes mismatch for " + f.id + "!");
                    System.err.println("  Expected SHA256: " + respHash);
                    System.err.println("  Found SHA256:    " + sha256Hex(existingResp));
                    System.exit(1);
                }

                if (!normalizeJson(jsonContent).equals(normalizeJson(existingJson))) {
                    System.err.println("FAIL: Metadata JSON mismatch for " + f.id + "!");
                    System.exit(1);
                }
            } else {
                Files.write(reqPath, f.requestBytes);
                Files.write(respPath, f.responseBytes);
                Files.writeString(jsonPath, jsonContent);

                System.out.println("Wrote " + reqPath + " (" + f.requestBytes.length + " bytes, sha256: " + reqHash + ")");
                System.out.println("Wrote " + respPath + " (" + f.responseBytes.length + " bytes, sha256: " + respHash + ")");
                System.out.println("Wrote " + jsonPath + " (" + jsonContent.length() + " chars)");
            }
        }

        if (verify) {
            System.out.println("FixtureGenerator: VERIFICATION PASSED (all fixtures match byte-for-byte)");
        } else {
            System.out.println("FixtureGenerator: SUCCESS (" + fixtures.size() + " fixtures generated)");
        }
    }

    private static void decodeRust(String kind, short version, String input) throws Exception {
        byte[] bytes;
        Path p = Path.of(input);
        if (Files.exists(p)) {
            bytes = Files.readAllBytes(p);
        } else {
            bytes = HexFormat.of().parseHex(input);
        }

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        if ("req".equals(kind)) {
            ProduceRequestData req = new ProduceRequestData();
            req.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in request: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: req v" + version + " acks=" + req.acks() + " timeout=" + req.timeoutMs()
                + " txn=" + req.transactionalId() + " topics=" + req.topicData().size());
        } else if ("resp".equals(kind)) {
            ProduceResponseData resp = new ProduceResponseData();
            resp.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in response: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: resp v" + version + " throttle=" + resp.throttleTimeMs()
                + " topics=" + resp.responses().size());
        } else {
            System.err.println("FAIL: Unknown kind: " + kind);
            System.exit(1);
        }
    }

    public static class ProduceFixture {
        public final String id;
        public final String filePrefix;
        public final short version;
        public final String description;
        public final ProduceRequestData requestData;
        public final ProduceResponseData responseData;
        public final byte[] requestBytes;
        public final byte[] responseBytes;

        public ProduceFixture(String id, String filePrefix, short version, String description,
                              ProduceRequestData requestData, ProduceResponseData responseData,
                              byte[] requestBytes, byte[] responseBytes) {
            this.id = id;
            this.filePrefix = filePrefix;
            this.version = version;
            this.description = description;
            this.requestData = requestData;
            this.responseData = responseData;
            this.requestBytes = requestBytes;
            this.responseBytes = responseBytes;
        }
    }

    private static ProduceFixture createFixture(String id, String filePrefix, short version, String description,
                                                ProduceRequestData req, ProduceResponseData resp) {
        ObjectSerializationCache reqCache = new ObjectSerializationCache();
        int reqSize = req.size(reqCache, version);
        ByteBuffer reqBuf = ByteBuffer.allocate(reqSize);
        req.write(new ByteBufferAccessor(reqBuf), reqCache, version);
        byte[] reqBytes = reqBuf.array();

        ObjectSerializationCache respCache = new ObjectSerializationCache();
        int respSize = resp.size(respCache, version);
        ByteBuffer respBuf = ByteBuffer.allocate(respSize);
        resp.write(new ByteBufferAccessor(respBuf), respCache, version);
        byte[] respBytes = respBuf.array();

        return new ProduceFixture(id, filePrefix, version, description, req, resp, reqBytes, respBytes);
    }

    public static List<ProduceFixture> createAllProduceFixtures() {
        List<ProduceFixture> list = new ArrayList<>();

        // 1. Existing smoke Produce v9
        list.add(generateProduceV9Smoke());

        // 2. KL01-04 Case: v3 classic (oldest spoken, null transactional_id, null records, partition errors, throttle placement)
        {
            short v = 3;
            ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) -1)
                .setTimeoutMs(5000)
                .setTransactionalId(null);

            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName("produce-v3-classic");
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(null));
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(null));
            req.topicData().add(topic);

            ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(25);
            ProduceResponseData.TopicProduceResponse respTopic = new ProduceResponseData.TopicProduceResponse().setName("produce-v3-classic");
            respTopic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode((short) 0).setBaseOffset(100L).setLogAppendTimeMs(1710000000000L));
            respTopic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(1).setErrorCode((short) 3).setBaseOffset(-1L).setLogAppendTimeMs(-1L));
            resp.responses().add(respTopic);

            list.add(createFixture(
                "produce-v3-classic",
                "produce_v3_classic",
                v,
                "Produce v3 classic wire format with null transactional_id, null records, partition error and throttle placement",
                req, resp
            ));
        }

        // 3. KL01-04 Case: v5 classic log_start_offset gate (non-null transactional_id, empty records, log_start_offset present)
        {
            short v = 5;
            ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(3000)
                .setTransactionalId("txn-produce-v5");

            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName("produce-v5-start-offset");
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.EMPTY));
            req.topicData().add(topic);

            ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(40);
            ProduceResponseData.TopicProduceResponse respTopic = new ProduceResponseData.TopicProduceResponse().setName("produce-v5-start-offset");
            respTopic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode((short) 0).setBaseOffset(200L).setLogAppendTimeMs(1710000001000L).setLogStartOffset(50L));
            resp.responses().add(respTopic);

            list.add(createFixture(
                "produce-v5-start-offset",
                "produce_v5_start_offset",
                v,
                "Produce v5 classic wire format with log_start_offset gate, non-null transactional_id, and empty records",
                req, resp
            ));
        }

        // 4. KL01-04 Case: v8 classic errors boundary (last classic version before flexible, record_errors, error_message)
        {
            short v = 8;
            ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) -1)
                .setTimeoutMs(7500)
                .setTransactionalId("txn-produce-v8");

            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName("produce-v8-classic-errors");
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.EMPTY));
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(null));
            req.topicData().add(topic);

            ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(60);
            ProduceResponseData.TopicProduceResponse respTopic = new ProduceResponseData.TopicProduceResponse().setName("produce-v8-classic-errors");
            respTopic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode((short) 0).setBaseOffset(300L).setLogAppendTimeMs(1710000002000L).setLogStartOffset(120L));

            ProduceResponseData.PartitionProduceResponse part1 = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(1).setErrorCode((short) 6).setBaseOffset(-1L).setLogAppendTimeMs(-1L).setLogStartOffset(-1L)
                .setErrorMessage("Not leader for partition");
            part1.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage().setBatchIndex(0).setBatchIndexErrorMessage("Record invalid"));
            part1.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage().setBatchIndex(1).setBatchIndexErrorMessage("Offset out of range"));
            respTopic.partitionResponses().add(part1);
            resp.responses().add(respTopic);

            list.add(createFixture(
                "produce-v8-classic-errors",
                "produce_v8_classic_errors",
                v,
                "Produce v8 classic wire format boundary before flexible transition, with record_errors and error_message",
                req, resp
            ));
        }

        // 5. KL01-04 Case: v9 flexible nulls (first flexible version, null transactional_id, null records, partition errors)
        {
            short v = 9;
            ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(4000)
                .setTransactionalId(null);

            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName("produce-v9-flexible-nulls");
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(null));
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(null));
            req.topicData().add(topic);

            ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(99);
            ProduceResponseData.TopicProduceResponse respTopic = new ProduceResponseData.TopicProduceResponse().setName("produce-v9-flexible-nulls");
            respTopic.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode((short) 0).setBaseOffset(400L).setLogAppendTimeMs(1710000003000L).setLogStartOffset(150L));

            ProduceResponseData.PartitionProduceResponse part1 = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(1).setErrorCode((short) 3).setBaseOffset(-1L).setLogAppendTimeMs(-1L).setLogStartOffset(-1L)
                .setErrorMessage("Unknown topic");
            part1.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage().setBatchIndex(0).setBatchIndexErrorMessage("Invalid batch"));
            respTopic.partitionResponses().add(part1);
            resp.responses().add(respTopic);

            list.add(createFixture(
                "produce-v9-flexible-nulls",
                "produce_v9_flexible_nulls",
                v,
                "Produce v9 flexible wire format boundary with null transactional_id, null records, and partition errors",
                req, resp
            ));
        }

        // 6. KL01-04 Case: v10 current leader (flexible, current_leader tag present vs omitted/default)
        {
            short v = 10;
            ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) -1)
                .setTimeoutMs(6000)
                .setTransactionalId("txn-produce-v10");

            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName("produce-v10-current-leader");
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.EMPTY));
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(MemoryRecords.EMPTY));
            req.topicData().add(topic);

            ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(150);
            ProduceResponseData.TopicProduceResponse respTopic = new ProduceResponseData.TopicProduceResponse().setName("produce-v10-current-leader");

            // Partition 0: present current-leader tag (leaderId: 3, leaderEpoch: 12)
            ProduceResponseData.PartitionProduceResponse part0 = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode((short) 0).setBaseOffset(500L).setLogAppendTimeMs(1710000004000L).setLogStartOffset(200L)
                .setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderId(3).setLeaderEpoch(12));
            respTopic.partitionResponses().add(part0);

            // Partition 1: omitted / default current-leader tag (-1, -1)
            ProduceResponseData.PartitionProduceResponse part1 = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(1).setErrorCode((short) 6).setBaseOffset(-1L).setLogAppendTimeMs(-1L).setLogStartOffset(-1L)
                .setErrorMessage("Broker not leader")
                .setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderId(-1).setLeaderEpoch(-1));
            respTopic.partitionResponses().add(part1);
            resp.responses().add(respTopic);

            list.add(createFixture(
                "produce-v10-current-leader",
                "produce_v10_current_leader",
                v,
                "Produce v10 flexible wire format with current-leader tagged field present on partition 0 and omitted on partition 1",
                req, resp
            ));
        }

        // 7. KL01-04 Case: v11 max 3.9.1 valid version boundary
        {
            short v = 11;
            ProduceRequestData req = new ProduceRequestData()
                .setAcks((short) -1)
                .setTimeoutMs(10000)
                .setTransactionalId("txn-produce-v11");

            ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData().setName("produce-v11-max");
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.EMPTY));
            req.topicData().add(topic);

            ProduceResponseData resp = new ProduceResponseData().setThrottleTimeMs(200);
            ProduceResponseData.TopicProduceResponse respTopic = new ProduceResponseData.TopicProduceResponse().setName("produce-v11-max");

            ProduceResponseData.PartitionProduceResponse part0 = new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0).setErrorCode((short) 0).setBaseOffset(600L).setLogAppendTimeMs(1710000005000L).setLogStartOffset(250L)
                .setErrorMessage("transaction abortable error")
                .setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderId(5).setLeaderEpoch(20));
            part0.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage().setBatchIndex(0).setBatchIndexErrorMessage("aborted txn"));
            respTopic.partitionResponses().add(part0);
            resp.responses().add(respTopic);

            list.add(createFixture(
                "produce-v11-max-3-9-1",
                "produce_v11_max_3_9_1",
                v,
                "Produce v11 flexible wire format at highest Apache Kafka 3.9.1 valid version boundary",
                req, resp
            ));
        }

        return list;
    }

    public static ProduceFixture generateProduceV9Smoke() {
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

        return createFixture("smoke-produce-v9", "smoke_produce_v9", version, "Smoke produce v9 fixture", req, resp);
    }

    private static void verifySelfRoundtrip(ProduceFixture f) {
        // Roundtrip request
        ProduceRequestData decodedReq = new ProduceRequestData();
        decodedReq.read(new ByteBufferAccessor(ByteBuffer.wrap(f.requestBytes)), f.version);
        if (!decodedReq.equals(f.requestData)) {
            throw new AssertionError("Self-roundtrip failed on ProduceRequest " + f.id);
        }

        // Roundtrip response
        ProduceResponseData decodedResp = new ProduceResponseData();
        decodedResp.read(new ByteBufferAccessor(ByteBuffer.wrap(f.responseBytes)), f.version);
        if (!decodedResp.equals(f.responseData)) {
            throw new AssertionError("Self-roundtrip failed on ProduceResponse " + f.id);
        }
    }

    private static String buildJsonMetadata(ProduceFixture f, String reqHash, String respHash,
                                           String reqHex, String respHex) {
        if ("smoke-produce-v9".equals(f.id)) {
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
                "    \"size_bytes\": " + f.requestBytes.length + ",\n" +
                "    \"sha256\": \"" + reqHash + "\",\n" +
                "    \"hex\": \"" + reqHex + "\"\n" +
                "  },\n" +
                "  \"response\": {\n" +
                "    \"file\": \"smoke_produce_v9_response.bin\",\n" +
                "    \"size_bytes\": " + f.responseBytes.length + ",\n" +
                "    \"sha256\": \"" + respHash + "\",\n" +
                "    \"hex\": \"" + respHex + "\"\n" +
                "  }\n" +
                "}\n";
        }

        return "{\n" +
            "  \"schema_version\": 1,\n" +
            "  \"fixture_id\": \"" + f.id + "\",\n" +
            "  \"api\": \"Produce\",\n" +
            "  \"api_key\": 0,\n" +
            "  \"api_version\": " + f.version + ",\n" +
            "  \"pin\": \"" + PIN_VERSION + "\",\n" +
            "  \"upstream_repo\": \"https://github.com/apache/kafka.git\",\n" +
            "  \"upstream_sha\": \"" + UPSTREAM_SHA + "\",\n" +
            "  \"artifact\": \"" + ARTIFACT_VERSION + "\",\n" +
            "  \"generator\": \"tests/conformance/java/src/main/java/org/apache/kafka/conformance/FixtureGenerator.java\",\n" +
            "  \"description\": \"" + f.description + "\",\n" +
            "  \"request\": {\n" +
            "    \"file\": \"" + f.filePrefix + "_request.bin\",\n" +
            "    \"size_bytes\": " + f.requestBytes.length + ",\n" +
            "    \"sha256\": \"" + reqHash + "\",\n" +
            "    \"hex\": \"" + reqHex + "\"\n" +
            "  },\n" +
            "  \"response\": {\n" +
            "    \"file\": \"" + f.filePrefix + "_response.bin\",\n" +
            "    \"size_bytes\": " + f.responseBytes.length + ",\n" +
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
