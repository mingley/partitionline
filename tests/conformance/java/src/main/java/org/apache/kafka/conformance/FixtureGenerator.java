package org.apache.kafka.conformance;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.Uuid;

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
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * KL01-03, KL01-04, KL01-05 & KL01-06: Pinned Apache Kafka wire-protocol fixture generator.
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
                System.out.println("Usage: java FixtureGenerator [--out-dir <path>] [--verify] | [--decode-rust <req|resp|fetch-req|fetch-resp|metadata-req|metadata-resp> <version> <file_or_hex>]");
                System.exit(0);
            }
        }

        Files.createDirectories(outDir);
        System.out.println("FixtureGenerator: generating Produce, Fetch, and Metadata fixtures using " + ARTIFACT_VERSION);
        System.out.println("Upstream SHA: " + UPSTREAM_SHA);
        System.out.println("Target directory: " + outDir.toAbsolutePath());

        List<ProduceFixture> produceFixtures = createAllProduceFixtures();
        List<FetchFixture> fetchFixtures = createAllFetchFixtures();
        List<MetadataFixture> metadataFixtures = createAllMetadataFixtures();

        // 1. Produce fixtures
        for (ProduceFixture f : produceFixtures) {
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
                verifyCommitted(f.id, reqPath, respPath, jsonPath, f.requestBytes, f.responseBytes, jsonContent, reqHash, respHash);
            } else {
                writeFixtureFiles(reqPath, respPath, jsonPath, f.requestBytes, f.responseBytes, jsonContent, reqHash, respHash);
            }
        }

        // 2. Fetch fixtures
        for (FetchFixture f : fetchFixtures) {
            verifySelfRoundtrip(f);

            String reqHash = sha256Hex(f.requestBytes);
            String respHash = sha256Hex(f.responseBytes);
            String reqHex = HexFormat.of().formatHex(f.requestBytes);
            String respHex = HexFormat.of().formatHex(f.responseBytes);

            Path reqPath = outDir.resolve(f.filePrefix + "_request.bin");
            Path respPath = outDir.resolve(f.filePrefix + "_response.bin");
            Path jsonPath = outDir.resolve(f.filePrefix + ".json");

            String jsonContent = buildFetchJsonMetadata(f, reqHash, respHash, reqHex, respHex);

            if (verify) {
                System.out.println("FixtureGenerator: verifying " + f.id + " against existing committed files...");
                verifyCommitted(f.id, reqPath, respPath, jsonPath, f.requestBytes, f.responseBytes, jsonContent, reqHash, respHash);
            } else {
                writeFixtureFiles(reqPath, respPath, jsonPath, f.requestBytes, f.responseBytes, jsonContent, reqHash, respHash);
            }
        }

        // 3. Metadata fixtures
        for (MetadataFixture f : metadataFixtures) {
            verifySelfRoundtrip(f);

            String reqHash = sha256Hex(f.requestBytes);
            String respHash = sha256Hex(f.responseBytes);
            String reqHex = HexFormat.of().formatHex(f.requestBytes);
            String respHex = HexFormat.of().formatHex(f.responseBytes);

            Path reqPath = outDir.resolve(f.filePrefix + "_request.bin");
            Path respPath = outDir.resolve(f.filePrefix + "_response.bin");
            Path jsonPath = outDir.resolve(f.filePrefix + ".json");

            String jsonContent = buildMetadataJsonMetadata(f, reqHash, respHash, reqHex, respHex);

            if (verify) {
                System.out.println("FixtureGenerator: verifying " + f.id + " against existing committed files...");
                verifyCommitted(f.id, reqPath, respPath, jsonPath, f.requestBytes, f.responseBytes, jsonContent, reqHash, respHash);
            } else {
                writeFixtureFiles(reqPath, respPath, jsonPath, f.requestBytes, f.responseBytes, jsonContent, reqHash, respHash);
            }
        }

        int total = produceFixtures.size() + fetchFixtures.size() + metadataFixtures.size();
        if (verify) {
            System.out.println("FixtureGenerator: VERIFICATION PASSED (all " + total + " fixtures match byte-for-byte)");
        } else {
            System.out.println("FixtureGenerator: SUCCESS (" + total + " fixtures generated: "
                + produceFixtures.size() + " Produce, " + fetchFixtures.size() + " Fetch, " + metadataFixtures.size() + " Metadata)");
        }
    }

    private static void verifyCommitted(String id, Path reqPath, Path respPath, Path jsonPath,
                                       byte[] requestBytes, byte[] responseBytes, String jsonContent,
                                       String reqHash, String respHash) throws IOException {
        if (!Files.exists(reqPath) || !Files.exists(respPath) || !Files.exists(jsonPath)) {
            System.err.println("FAIL: One or more fixture files missing for " + id);
            System.exit(1);
        }

        byte[] existingReq = Files.readAllBytes(reqPath);
        byte[] existingResp = Files.readAllBytes(respPath);
        String existingJson = Files.readString(jsonPath);

        if (!Arrays.equals(requestBytes, existingReq)) {
            System.err.println("FAIL: Request bytes mismatch for " + id + "!");
            System.err.println("  Expected SHA256: " + reqHash);
            System.err.println("  Found SHA256:    " + sha256Hex(existingReq));
            System.exit(1);
        }

        if (!Arrays.equals(responseBytes, existingResp)) {
            System.err.println("FAIL: Response bytes mismatch for " + id + "!");
            System.err.println("  Expected SHA256: " + respHash);
            System.err.println("  Found SHA256:    " + sha256Hex(existingResp));
            System.exit(1);
        }

        if (!normalizeJson(jsonContent).equals(normalizeJson(existingJson))) {
            System.err.println("FAIL: Metadata JSON mismatch for " + id + "!");
            System.exit(1);
        }
    }

    private static void writeFixtureFiles(Path reqPath, Path respPath, Path jsonPath,
                                         byte[] requestBytes, byte[] responseBytes, String jsonContent,
                                         String reqHash, String respHash) throws IOException {
        Files.write(reqPath, requestBytes);
        Files.write(respPath, responseBytes);
        Files.writeString(jsonPath, jsonContent);

        System.out.println("Wrote " + reqPath + " (" + requestBytes.length + " bytes, sha256: " + reqHash + ")");
        System.out.println("Wrote " + respPath + " (" + responseBytes.length + " bytes, sha256: " + respHash + ")");
        System.out.println("Wrote " + jsonPath + " (" + jsonContent.length() + " chars)");
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
        if ("req".equals(kind) || "produce-req".equals(kind) || "produce_req".equals(kind)) {
            ProduceRequestData req = new ProduceRequestData();
            req.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in request: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: req v" + version + " acks=" + req.acks() + " timeout=" + req.timeoutMs()
                + " txn=" + req.transactionalId() + " topics=" + req.topicData().size());
        } else if ("resp".equals(kind) || "produce-resp".equals(kind) || "produce_resp".equals(kind)) {
            ProduceResponseData resp = new ProduceResponseData();
            resp.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in response: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: resp v" + version + " throttle=" + resp.throttleTimeMs()
                + " topics=" + resp.responses().size());
        } else if ("fetch-req".equals(kind) || "fetch_req".equals(kind)) {
            FetchRequestData req = new FetchRequestData();
            req.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in FetchRequest: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: req v" + version + " replicaId=" + req.replicaId() + " maxWait=" + req.maxWaitMs()
                + " topics=" + req.topics().size() + " session=" + req.sessionId());
        } else if ("fetch-resp".equals(kind) || "fetch_resp".equals(kind)) {
            FetchResponseData resp = new FetchResponseData();
            resp.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in FetchResponse: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: resp v" + version + " throttle=" + resp.throttleTimeMs()
                + " topics=" + resp.responses().size() + " session=" + resp.sessionId());
        } else if ("metadata-req".equals(kind) || "metadata_req".equals(kind)) {
            MetadataRequestData req = new MetadataRequestData();
            req.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in MetadataRequest: " + buf.remaining());
                System.exit(1);
            }
            int topicCount = req.topics() == null ? -1 : req.topics().size();
            System.out.println("OK: req v" + version + " topics=" + topicCount
                + " allowAuto=" + req.allowAutoTopicCreation());
        } else if ("metadata-resp".equals(kind) || "metadata_resp".equals(kind)) {
            MetadataResponseData resp = new MetadataResponseData();
            resp.read(new ByteBufferAccessor(buf), version);
            if (buf.hasRemaining()) {
                System.err.println("FAIL: Leftover bytes in MetadataResponse: " + buf.remaining());
                System.exit(1);
            }
            System.out.println("OK: resp v" + version + " throttle=" + resp.throttleTimeMs()
                + " brokers=" + resp.brokers().size() + " topics=" + resp.topics().size());
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

    public static class FetchFixture {
        public final String id;
        public final String filePrefix;
        public final short version;
        public final String description;
        public final FetchRequestData requestData;
        public final FetchResponseData responseData;
        public final byte[] requestBytes;
        public final byte[] responseBytes;

        public FetchFixture(String id, String filePrefix, short version, String description,
                            FetchRequestData requestData, FetchResponseData responseData,
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

    private static FetchFixture createFetchFixture(String id, String filePrefix, short version, String description,
                                                   FetchRequestData req, FetchResponseData resp) {
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

        return new FetchFixture(id, filePrefix, version, description, req, resp, reqBytes, respBytes);
    }

    private static void verifySelfRoundtrip(FetchFixture f) {
        FetchRequestData decodedReq = new FetchRequestData();
        decodedReq.read(new ByteBufferAccessor(ByteBuffer.wrap(f.requestBytes)), f.version);
        if (!decodedReq.equals(f.requestData)) {
            throw new AssertionError("Self-roundtrip failed on FetchRequest " + f.id);
        }

        FetchResponseData decodedResp = new FetchResponseData();
        decodedResp.read(new ByteBufferAccessor(ByteBuffer.wrap(f.responseBytes)), f.version);
        if (!decodedResp.equals(f.responseData)) {
            throw new AssertionError("Self-roundtrip failed on FetchResponse " + f.id);
        }
    }

    public static List<FetchFixture> createAllFetchFixtures() {
        List<FetchFixture> list = new ArrayList<>();

        // 1. Fetch v4: classic wire format (oldest spoken, topic name, untagged replicaId = -1, omitted logStartOffset)
        {
            short v = 4;
            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(500)
                .setMinBytes(1)
                .setMaxBytes(10485760)
                .setIsolationLevel((byte) 0);
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic("fetch-v4-classic");
            topic.partitions().add(new FetchRequestData.FetchPartition().setPartition(0).setFetchOffset(0L).setPartitionMaxBytes(1048576));
            topic.partitions().add(new FetchRequestData.FetchPartition().setPartition(1).setFetchOffset(100L).setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData().setThrottleTimeMs(25);
            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse().setTopic("fetch-v4-classic");
            respTopic.partitions().add(new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(50L)
                .setLastStableOffset(45L)
                .setRecords(MemoryRecords.EMPTY));
            respTopic.partitions().add(new FetchResponseData.PartitionData()
                .setPartitionIndex(1)
                .setErrorCode((short) 3)
                .setHighWatermark(-1L)
                .setLastStableOffset(-1L)
                .setRecords(null));
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v4-classic",
                "fetch_v4_classic",
                v,
                "Fetch v4 classic wire format with topic names, untagged consumer replicaId, partition errors, and omitted logStartOffset",
                req, resp
            ));
        }

        // 2. Fetch v5: classic wire format, follower replica fetch, logStartOffset gate
        {
            short v = 5;
            FetchRequestData req = new FetchRequestData()
                .setReplicaId(2)
                .setMaxWaitMs(1000)
                .setMinBytes(1)
                .setMaxBytes(10485760)
                .setIsolationLevel((byte) 0);
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic("fetch-v5-start-offset");
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setFetchOffset(150L)
                .setLogStartOffset(100L)
                .setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData().setThrottleTimeMs(35);
            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse().setTopic("fetch-v5-start-offset");
            respTopic.partitions().add(new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(250L)
                .setLastStableOffset(240L)
                .setLogStartOffset(100L)
                .setRecords(MemoryRecords.EMPTY));
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v5-start-offset",
                "fetch_v5_start_offset",
                v,
                "Fetch v5 classic wire format with follower replica fetch and logStartOffset gate present on wire",
                req, resp
            ));
        }

        // 3. Fetch v7: classic wire format, session metadata gate (sessionId, sessionEpoch, forgottenTopicsData), abortedTransactions
        {
            short v = 7;
            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(5000)
                .setMinBytes(1)
                .setMaxBytes(10485760)
                .setIsolationLevel((byte) 1)
                .setSessionId(42)
                .setSessionEpoch(3);
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic("fetch-v7-session");
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setFetchOffset(50L)
                .setLogStartOffset(0L)
                .setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchRequestData.ForgottenTopic forgotten = new FetchRequestData.ForgottenTopic()
                .setTopic("forgotten-v7");
            forgotten.partitions().add(0);
            forgotten.partitions().add(1);
            req.forgottenTopicsData().add(forgotten);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(45)
                .setErrorCode((short) 0)
                .setSessionId(42);
            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse().setTopic("fetch-v7-session");
            FetchResponseData.PartitionData part0 = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(100L)
                .setLastStableOffset(90L)
                .setLogStartOffset(0L)
                .setRecords(MemoryRecords.EMPTY);
            part0.abortedTransactions().add(new FetchResponseData.AbortedTransaction().setProducerId(12345L).setFirstOffset(10L));
            part0.abortedTransactions().add(new FetchResponseData.AbortedTransaction().setProducerId(67890L).setFirstOffset(40L));
            respTopic.partitions().add(part0);
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v7-session",
                "fetch_v7_session",
                v,
                "Fetch v7 classic wire format with session metadata, forgotten topics, and aborted transactions list",
                req, resp
            ));
        }

        // 4. Fetch v11: classic format boundary before flexible, rackId, preferredReadReplica, realistic record batches
        {
            short v = 11;
            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(2500)
                .setMinBytes(1)
                .setMaxBytes(10485760)
                .setIsolationLevel((byte) 1)
                .setSessionId(100)
                .setSessionEpoch(1)
                .setRackId("rack-east-az1");
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic("fetch-v11-batches");
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(5)
                .setFetchOffset(102L)
                .setLogStartOffset(100L)
                .setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(55)
                .setErrorCode((short) 0)
                .setSessionId(100);
            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse().setTopic("fetch-v11-batches");
            FetchResponseData.PartitionData part0 = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(120L)
                .setLastStableOffset(115L)
                .setLogStartOffset(100L)
                .setPreferredReadReplica(4)
                .setRecords(createV11BatchesRecords());
            part0.abortedTransactions().add(new FetchResponseData.AbortedTransaction().setProducerId(9001L).setFirstOffset(104L));
            respTopic.partitions().add(part0);
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v11-batches-records",
                "fetch_v11_batches_records",
                v,
                "Fetch v11 classic wire format with rackId, preferredReadReplica, aborted transactions, and realistic record batches including records before requested offset and ABORT/COMMIT marker sequences",
                req, resp
            ));
        }

        // 5. Fetch v12: flexible wire format boundary, compact strings/arrays/bytes, clusterId, partition tags (DivergingEpoch, CurrentLeader, SnapshotId), unknown tags
        {
            short v = 12;
            FetchRequestData req = new FetchRequestData()
                .setClusterId("cluster-v12")
                .setReplicaId(-1)
                .setMaxWaitMs(1500)
                .setMinBytes(1)
                .setMaxBytes(5242880)
                .setIsolationLevel((byte) 0)
                .setSessionId(200)
                .setSessionEpoch(2)
                .setRackId("rack-west-1");
            req.unknownTaggedFields().add(new RawTaggedField(1000, new byte[]{(byte) 0x55, (byte) 0x66}));

            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic().setTopic("fetch-v12-flexible");
            FetchRequestData.FetchPartition reqPart0 = new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(8)
                .setFetchOffset(200L)
                .setLastFetchedEpoch(5)
                .setLogStartOffset(180L)
                .setPartitionMaxBytes(1048576);
            reqPart0.unknownTaggedFields().add(new RawTaggedField(999, new byte[]{(byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04}));
            topic.partitions().add(reqPart0);
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(70)
                .setErrorCode((short) 0)
                .setSessionId(200);
            resp.unknownTaggedFields().add(new RawTaggedField(1000, new byte[]{(byte) 0x77, (byte) 0x88}));

            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse().setTopic("fetch-v12-flexible");
            FetchResponseData.PartitionData respPart0 = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(250L)
                .setLastStableOffset(240L)
                .setLogStartOffset(180L)
                .setPreferredReadReplica(3)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(2).setLeaderEpoch(10))
                .setDivergingEpoch(new FetchResponseData.EpochEndOffset().setEpoch(4).setEndOffset(195L))
                .setSnapshotId(new FetchResponseData.SnapshotId().setEndOffset(190L).setEpoch(4))
                .setRecords(createV12Records());
            respPart0.unknownTaggedFields().add(new RawTaggedField(999, new byte[]{(byte) 0x0a, (byte) 0x0b, (byte) 0x0c}));
            respTopic.partitions().add(respPart0);
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v12-flexible-tags",
                "fetch_v12_flexible_tags",
                v,
                "Fetch v12 flexible wire format with clusterId, compact encoding, partition tagged fields (DivergingEpoch, CurrentLeader, SnapshotId), and unknown tags",
                req, resp
            ));
        }

        // 6. Fetch v13: topic IDs wire transition (topicId replaces topic name on wire)
        {
            short v = 13;
            Uuid topicId = new Uuid(0x1111222233334444L, 0x5555666677778888L);
            Uuid forgottenId = new Uuid(0xaaaabbbbccccddddL, 0x1111222233334444L);

            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(2000)
                .setMinBytes(1)
                .setMaxBytes(5242880)
                .setIsolationLevel((byte) 1)
                .setSessionId(300)
                .setSessionEpoch(1)
                .setRackId("rack-1");
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic()
                .setTopic("")
                .setTopicId(topicId);
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(12)
                .setFetchOffset(300L)
                .setLastFetchedEpoch(10)
                .setLogStartOffset(250L)
                .setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchRequestData.ForgottenTopic forgotten = new FetchRequestData.ForgottenTopic()
                .setTopic("")
                .setTopicId(forgottenId);
            forgotten.partitions().add(0);
            req.forgottenTopicsData().add(forgotten);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(80)
                .setErrorCode((short) 0)
                .setSessionId(300);
            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse()
                .setTopic("")
                .setTopicId(topicId);
            FetchResponseData.PartitionData respPart0 = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(350L)
                .setLastStableOffset(340L)
                .setLogStartOffset(250L)
                .setPreferredReadReplica(-1)
                .setRecords(createV13Records());
            respPart0.abortedTransactions().add(new FetchResponseData.AbortedTransaction().setProducerId(55555L).setFirstOffset(290L));
            respTopic.partitions().add(respPart0);
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v13-topic-ids",
                "fetch_v13_topic_ids",
                v,
                "Fetch v13 flexible wire format with topic IDs transition replacing topic names for request, forgotten topics, and response",
                req, resp
            ));
        }

        // 7. Fetch v15: replica-field transition (untagged replicaId omitted, replicaState tagged field 1 added)
        {
            short v = 15;
            Uuid topicId = new Uuid(0x9999888877776666L, 0x5555444433332222L);

            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(1000)
                .setMinBytes(1)
                .setMaxBytes(10485760)
                .setIsolationLevel((byte) 0)
                .setSessionId(400)
                .setSessionEpoch(4)
                .setRackId("rack-replica");
            req.replicaState().setReplicaId(5).setReplicaEpoch(12345L);

            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic()
                .setTopic("")
                .setTopicId(topicId);
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(15)
                .setFetchOffset(500L)
                .setLastFetchedEpoch(14)
                .setLogStartOffset(450L)
                .setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(90)
                .setErrorCode((short) 0)
                .setSessionId(400);
            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse()
                .setTopic("")
                .setTopicId(topicId);
            respTopic.partitions().add(new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(550L)
                .setLastStableOffset(540L)
                .setLogStartOffset(450L)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(15))
                .setRecords(MemoryRecords.EMPTY));
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v15-replica-state",
                "fetch_v15_replica_state",
                v,
                "Fetch v15 flexible wire format with replica-field transition omitting untagged replicaId and using replicaState tagged field 1",
                req, resp
            ));
        }

        // 8. Fetch v16: top-level nodeEndpoints tagged field 0 in response
        {
            short v = 16;
            Uuid topicId = new Uuid(0x1234123412341234L, 0x5678567856785678L);

            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(3000)
                .setMinBytes(1)
                .setMaxBytes(5242880)
                .setIsolationLevel((byte) 0)
                .setSessionId(500)
                .setSessionEpoch(1)
                .setRackId("rack-consumer");
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic()
                .setTopic("")
                .setTopicId(topicId);
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(2)
                .setLastFetchedEpoch(1)
                .setLogStartOffset(0L)
                .setPartitionMaxBytes(1048576));
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(110)
                .setErrorCode((short) 0)
                .setSessionId(500);

            FetchResponseData.NodeEndpoint ep1 = new FetchResponseData.NodeEndpoint();
            ep1.setNodeId(1);
            ep1.setHost("broker1.kafka.local");
            ep1.setPort(9092);
            ep1.setRack("rack-a");
            resp.nodeEndpoints().add(ep1);

            FetchResponseData.NodeEndpoint ep2 = new FetchResponseData.NodeEndpoint();
            ep2.setNodeId(2);
            ep2.setHost("broker2.kafka.local");
            ep2.setPort(9092);
            ep2.setRack("rack-b");
            resp.nodeEndpoints().add(ep2);

            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse()
                .setTopic("")
                .setTopicId(topicId);
            respTopic.partitions().add(new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(20L)
                .setLastStableOffset(18L)
                .setLogStartOffset(0L)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(2))
                .setRecords(MemoryRecords.EMPTY));
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v16-endpoints",
                "fetch_v16_endpoints",
                v,
                "Fetch v16 flexible wire format with top-level nodeEndpoints tagged field 0 in response",
                req, resp
            ));
        }

        // 9. Fetch v17: highest valid version in Apache Kafka 3.9.1, partition replicaDirectoryId tagged field 0
        {
            short v = 17;
            Uuid topicId = new Uuid(0xfacefeedcafebeefL, 0x0102030405060708L);
            Uuid replicaDirId = new Uuid(0xaaaabbbb00001111L, 0x2222333344445555L);

            FetchRequestData req = new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(1200)
                .setMinBytes(1)
                .setMaxBytes(10485760)
                .setIsolationLevel((byte) 0)
                .setSessionId(600)
                .setSessionEpoch(5)
                .setRackId("rack-v17");
            FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic()
                .setTopic("")
                .setTopicId(topicId);
            topic.partitions().add(new FetchRequestData.FetchPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(20)
                .setLastFetchedEpoch(19)
                .setFetchOffset(1000L)
                .setLogStartOffset(900L)
                .setPartitionMaxBytes(1048576)
                .setReplicaDirectoryId(replicaDirId));
            req.topics().add(topic);

            FetchResponseData resp = new FetchResponseData()
                .setThrottleTimeMs(120)
                .setErrorCode((short) 0)
                .setSessionId(600);

            FetchResponseData.NodeEndpoint ep = new FetchResponseData.NodeEndpoint();
            ep.setNodeId(3);
            ep.setHost("broker3.kafka.local");
            ep.setPort(9093);
            ep.setRack("rack-c");
            resp.nodeEndpoints().add(ep);

            FetchResponseData.FetchableTopicResponse respTopic = new FetchResponseData.FetchableTopicResponse()
                .setTopic("")
                .setTopicId(topicId);
            respTopic.partitions().add(new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setErrorCode((short) 0)
                .setHighWatermark(1100L)
                .setLastStableOffset(1090L)
                .setLogStartOffset(900L)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(3).setLeaderEpoch(20))
                .setDivergingEpoch(new FetchResponseData.EpochEndOffset().setEpoch(19).setEndOffset(995L))
                .setRecords(MemoryRecords.EMPTY));
            resp.responses().add(respTopic);

            list.add(createFetchFixture(
                "fetch-v17-max-3-9-1",
                "fetch_v17_max_3_9_1",
                v,
                "Fetch v17 flexible wire format at highest Apache Kafka 3.9.1 valid version boundary with partition replicaDirectoryId tagged field 0",
                req, resp
            ));
        }

        return list;
    }

    private static MemoryRecords createV11BatchesRecords() {
        ByteBuffer buf = ByteBuffer.allocate(8192);

        // Batch 1: regular batch at baseOffset 100, 4 records (100, 101, 102, 103)
        // Request asks for offset 102, so offsets 100 and 101 are before the requested offset.
        MemoryRecordsBuilder b1 = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            100L, 1710000000000L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE, false, false, 5
        );
        b1.append(1710000000000L, "k100".getBytes(), "v100".getBytes());
        b1.append(1710000000001L, "k101".getBytes(), "v101".getBytes());
        b1.append(1710000000002L, "k102".getBytes(), "v102".getBytes());
        b1.append(1710000000003L, "k103".getBytes(), "v103".getBytes());
        b1.close();

        // Batch 2: transactional batch at baseOffset 104, producerId 9001, epoch 1, records 104, 105
        long pid = 9001L;
        short epoch = 1;
        MemoryRecordsBuilder b2 = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            104L, 1710000000010L, pid, epoch, 0, true, false, 5
        );
        b2.append(1710000000010L, "tx-k1".getBytes(), "tx-v1".getBytes());
        b2.append(1710000000011L, "tx-k2".getBytes(), "tx-v2".getBytes());
        b2.close();

        // Batch 3: control batch with ABORT marker at offset 106
        MemoryRecordsBuilder b3 = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            106L, 1710000000020L, pid, epoch, RecordBatch.NO_SEQUENCE, true, true, 5
        );
        b3.appendEndTxnMarker(1710000000020L, new EndTransactionMarker(ControlRecordType.ABORT, 1));
        b3.close();

        // Batch 4: transactional batch at baseOffset 107, producerId 9001, epoch 1, record 107
        MemoryRecordsBuilder b4 = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            107L, 1710000000030L, pid, epoch, 2, true, false, 5
        );
        b4.append(1710000000030L, "tx-k3".getBytes(), "tx-v3".getBytes());
        b4.close();

        // Batch 5: control batch with COMMIT marker at offset 108
        MemoryRecordsBuilder b5 = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            108L, 1710000000040L, pid, epoch, RecordBatch.NO_SEQUENCE, true, true, 5
        );
        b5.appendEndTxnMarker(1710000000040L, new EndTransactionMarker(ControlRecordType.COMMIT, 1));
        b5.close();

        buf.flip();
        return MemoryRecords.readableRecords(buf);
    }

    private static MemoryRecords createV12Records() {
        ByteBuffer buf = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder b = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            200L, 1710000000100L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE, false, false, 8
        );
        b.append(1710000000100L, "k200".getBytes(), "v200".getBytes());
        b.append(1710000000101L, "k201".getBytes(), "v201".getBytes());
        b.close();
        buf.flip();
        return MemoryRecords.readableRecords(buf);
    }

    private static MemoryRecords createV13Records() {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder b = MemoryRecords.builder(
            buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
            300L, 1710000000200L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE, false, false, 12
        );
        b.append(1710000000200L, "k300".getBytes(), "v300".getBytes());
        b.close();
        buf.flip();
        return MemoryRecords.readableRecords(buf);
    }

    private static String buildFetchJsonMetadata(FetchFixture f, String reqHash, String respHash,
                                                String reqHex, String respHex) {
        return "{\n" +
            "  \"schema_version\": 1,\n" +
            "  \"fixture_id\": \"" + f.id + "\",\n" +
            "  \"api\": \"Fetch\",\n" +
            "  \"api_key\": 1,\n" +
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

    public static class MetadataFixture {
        public final String id;
        public final String filePrefix;
        public final short version;
        public final String description;
        public final MetadataRequestData requestData;
        public final MetadataResponseData responseData;
        public final byte[] requestBytes;
        public final byte[] responseBytes;

        public MetadataFixture(String id, String filePrefix, short version, String description,
                               MetadataRequestData requestData, MetadataResponseData responseData,
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

    private static MetadataFixture createMetadataFixture(String id, String filePrefix, short version, String description,
                                                         MetadataRequestData req, MetadataResponseData resp) {
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

        return new MetadataFixture(id, filePrefix, version, description, req, resp, reqBytes, respBytes);
    }

    private static void verifySelfRoundtrip(MetadataFixture f) {
        MetadataRequestData decodedReq = new MetadataRequestData();
        decodedReq.read(new ByteBufferAccessor(ByteBuffer.wrap(f.requestBytes)), f.version);
        if (!decodedReq.equals(f.requestData)) {
            throw new AssertionError("Self-roundtrip failed on MetadataRequest " + f.id);
        }

        MetadataResponseData decodedResp = new MetadataResponseData();
        decodedResp.read(new ByteBufferAccessor(ByteBuffer.wrap(f.responseBytes)), f.version);
        if (!decodedResp.equals(f.responseData)) {
            throw new AssertionError("Self-roundtrip failed on MetadataResponse " + f.id);
        }
    }

    private static String buildMetadataJsonMetadata(MetadataFixture f, String reqHash, String respHash,
                                                    String reqHex, String respHex) {
        return "{\n" +
            "  \"schema_version\": 1,\n" +
            "  \"fixture_id\": \"" + f.id + "\",\n" +
            "  \"api\": \"Metadata\",\n" +
            "  \"api_key\": 3,\n" +
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

    public static List<MetadataFixture> createAllMetadataFixtures() {
        List<MetadataFixture> list = new ArrayList<>();

        // 1. Metadata v1: classic wire format (oldest spoken), null topics (all topics), multiple brokers, controllerId, isInternal, unknown leader
        {
            short v = 1;
            MetadataRequestData req = new MetadataRequestData();
            req.setTopics(null);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setControllerId(1);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack(null));

            MetadataResponseTopic t1 = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v1-topic").setIsInternal(false);
            t1.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(1).setReplicaNodes(Arrays.asList(1, 2)).setIsrNodes(Arrays.asList(1, 2)));
            t1.partitions().add(new MetadataResponsePartition().setErrorCode((short) 5).setPartitionIndex(1).setLeaderId(-1).setReplicaNodes(Arrays.asList(2, 3)).setIsrNodes(Arrays.asList(2)));

            MetadataResponseTopic t2 = new MetadataResponseTopic().setErrorCode((short) 3).setName("meta-v1-missing").setIsInternal(false);
            resp.topics().add(t1);
            resp.topics().add(t2);

            list.add(createMetadataFixture(
                "metadata-v1-classic",
                "metadata_v1_classic",
                v,
                "Metadata v1 classic wire format with null topics (all topics), multiple brokers across racks, controllerId, isInternal, and partition with unknown leader",
                req, resp
            ));
        }

        // 2. Metadata v4: classic wire format, empty topics array, allowAutoTopicCreation gate (false), clusterId, throttleTimeMs
        {
            short v = 4;
            MetadataRequestData req = new MetadataRequestData();
            req.setTopics(new ArrayList<>());
            req.setAllowAutoTopicCreation(false);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(30);
            resp.setClusterId("cluster-meta-v4");
            resp.setControllerId(2);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack("rack-c"));

            list.add(createMetadataFixture(
                "metadata-v4-empty-topics",
                "metadata_v4_empty_topics",
                v,
                "Metadata v4 classic wire format with empty topics array, allowAutoTopicCreation gate (false), clusterId, and throttleTimeMs",
                req, resp
            ));
        }

        // 3. Metadata v5: classic wire format, offlineReplicas gate present, multiple brokers, unknown leader
        {
            short v = 5;
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName("meta-v5-offline"));
            req.setAllowAutoTopicCreation(true);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(45);
            resp.setClusterId("cluster-meta-v5");
            resp.setControllerId(1);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack(null));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v5-offline").setIsInternal(false);
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(2).setReplicaNodes(Arrays.asList(1, 2, 3)).setIsrNodes(Arrays.asList(1, 2)).setOfflineReplicas(Arrays.asList(3)));
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 6).setPartitionIndex(1).setLeaderId(-1).setReplicaNodes(Arrays.asList(1, 3)).setIsrNodes(Arrays.asList(1)).setOfflineReplicas(Arrays.asList(3)));
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v5-offline-replicas",
                "metadata_v5_offline_replicas",
                v,
                "Metadata v5 classic wire format with offlineReplicas gate present, multiple brokers, and unknown leader on partitioned topic",
                req, resp
            ));
        }

        // 4. Metadata v7: classic wire format, leaderEpoch gate present across multiple partitions, active and stale leader epochs
        {
            short v = 7;
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName("meta-v7-epoch"));
            req.setAllowAutoTopicCreation(true);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(50);
            resp.setClusterId("cluster-meta-v7");
            resp.setControllerId(1);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack(null));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v7-epoch").setIsInternal(false);
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(1).setLeaderEpoch(8).setReplicaNodes(Arrays.asList(1, 2)).setIsrNodes(Arrays.asList(1, 2)).setOfflineReplicas(Collections.emptyList()));
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(1).setLeaderId(2).setLeaderEpoch(5).setReplicaNodes(Arrays.asList(2, 3)).setIsrNodes(Arrays.asList(2)).setOfflineReplicas(Collections.emptyList()));
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v7-leader-epoch",
                "metadata_v7_leader_epoch",
                v,
                "Metadata v7 classic wire format with leaderEpoch gate present across multiple partitions, active and stale leader epochs",
                req, resp
            ));
        }

        // 5. Metadata v8: classic format boundary before flexible, includeClusterAuthorizedOperations, includeTopicAuthorizedOperations, clusterAuthorizedOperations, and topicAuthorizedOperations
        {
            short v = 8;
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName("meta-v8-ops"));
            req.setAllowAutoTopicCreation(true);
            req.setIncludeClusterAuthorizedOperations(true);
            req.setIncludeTopicAuthorizedOperations(true);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(60);
            resp.setClusterId("cluster-meta-v8");
            resp.setControllerId(2);
            resp.setClusterAuthorizedOperations(0x000000df);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack("rack-c"));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v8-ops").setIsInternal(false).setTopicAuthorizedOperations(0x0000001f);
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(1).setLeaderEpoch(12).setReplicaNodes(Arrays.asList(1, 2)).setIsrNodes(Arrays.asList(1)).setOfflineReplicas(Arrays.asList(2)));
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v8-authorized-ops",
                "metadata_v8_authorized_ops",
                v,
                "Metadata v8 classic format boundary before flexible with includeClusterAuthorizedOperations, includeTopicAuthorizedOperations, clusterAuthorizedOperations, and topicAuthorizedOperations",
                req, resp
            ));
        }

        // 6. Metadata v9: flexible wire format boundary, compact encoding, clusterId, multiple brokers across racks, and unknown tagged fields
        {
            short v = 9;
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName("meta-v9-flex"));
            req.setAllowAutoTopicCreation(true);
            req.setIncludeClusterAuthorizedOperations(false);
            req.setIncludeTopicAuthorizedOperations(true);
            req.unknownTaggedFields().add(new RawTaggedField(100, new byte[]{(byte) 0x01, (byte) 0x02}));

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(75);
            resp.setClusterId("cluster-meta-v9");
            resp.setControllerId(3);

            MetadataResponseBroker b1 = new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a");
            b1.unknownTaggedFields().add(new RawTaggedField(200, new byte[]{(byte) 0x03, (byte) 0x04}));
            resp.brokers().add(b1);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack("rack-c"));
            resp.unknownTaggedFields().add(new RawTaggedField(100, new byte[]{(byte) 0xaa, (byte) 0xbb, (byte) 0xcc}));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v9-flex").setIsInternal(false).setTopicAuthorizedOperations(0x000000df);
            t.unknownTaggedFields().add(new RawTaggedField(300, new byte[]{(byte) 0x10, (byte) 0x20}));

            MetadataResponsePartition p0 = new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(3).setLeaderEpoch(15).setReplicaNodes(Arrays.asList(1, 2, 3)).setIsrNodes(Arrays.asList(2, 3)).setOfflineReplicas(Arrays.asList(1));
            p0.unknownTaggedFields().add(new RawTaggedField(400, new byte[]{(byte) 0x99}));

            MetadataResponsePartition p1 = new MetadataResponsePartition().setErrorCode((short) 5).setPartitionIndex(1).setLeaderId(-1).setLeaderEpoch(-1).setReplicaNodes(Arrays.asList(1, 2)).setIsrNodes(Collections.emptyList()).setOfflineReplicas(Arrays.asList(1, 2));

            t.partitions().add(p0);
            t.partitions().add(p1);
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v9-flexible",
                "metadata_v9_flexible",
                v,
                "Metadata v9 flexible wire format boundary with compact encoding, clusterId, multiple brokers across racks, and unknown tagged fields on request, response, brokers, topics, and partitions",
                req, resp
            ));
        }

        // 7. Metadata v10: flexible wire format, topicId wire transition on request and response topics, clusterAuthorizedOperations, and unknown leader
        {
            short v = 10;
            Uuid topicId10 = new Uuid(0x1111222233334444L, 0x5555666677778888L);
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName("meta-v10-ids").setTopicId(topicId10));
            req.setAllowAutoTopicCreation(false);
            req.setIncludeClusterAuthorizedOperations(true);
            req.setIncludeTopicAuthorizedOperations(true);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(80);
            resp.setClusterId("cluster-meta-v10");
            resp.setControllerId(1);
            resp.setClusterAuthorizedOperations(0x000000df);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack(null));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v10-ids").setTopicId(topicId10).setIsInternal(false).setTopicAuthorizedOperations(0x0000001f);
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(1).setLeaderEpoch(20).setReplicaNodes(Arrays.asList(1, 2)).setIsrNodes(Arrays.asList(1, 2)).setOfflineReplicas(Collections.emptyList()));
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 6).setPartitionIndex(1).setLeaderId(-1).setLeaderEpoch(18).setReplicaNodes(Arrays.asList(2, 3)).setIsrNodes(Arrays.asList(2)).setOfflineReplicas(Arrays.asList(3)));
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v10-topic-ids",
                "metadata_v10_topic_ids",
                v,
                "Metadata v10 flexible wire format with topicId wire transition on request and response topics, clusterAuthorizedOperations, and unknown leader",
                req, resp
            ));
        }

        // 8. Metadata v11: flexible wire format boundary where clusterAuthorizedOperations is dropped from wire while topicAuthorizedOperations is retained
        {
            short v = 11;
            Uuid topicId11 = new Uuid(0x2222333344445555L, 0x6666777788889999L);
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName("meta-v11-auth").setTopicId(topicId11));
            req.setAllowAutoTopicCreation(true);
            req.setIncludeTopicAuthorizedOperations(true);

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(90);
            resp.setClusterId("cluster-meta-v11");
            resp.setControllerId(2);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack("rack-c"));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v11-auth").setTopicId(topicId11).setIsInternal(false).setTopicAuthorizedOperations(0x000000df);
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(2).setLeaderEpoch(25).setReplicaNodes(Arrays.asList(1, 2)).setIsrNodes(Arrays.asList(1, 2)).setOfflineReplicas(Collections.emptyList()));
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v11-no-cluster-auth",
                "metadata_v11_no_cluster_auth",
                v,
                "Metadata v11 flexible wire format boundary where clusterAuthorizedOperations is dropped from wire while topicAuthorizedOperations is retained",
                req, resp
            ));
        }

        // 9. Metadata v12: flexible wire format at highest Apache Kafka 3.9.1 valid version boundary with ID-only describe (null name), unknown tagged fields, and multiple brokers
        {
            short v = 12;
            Uuid topicId12 = new Uuid(0x3333444455556666L, 0x777788889999aaaaL);
            MetadataRequestData req = new MetadataRequestData();
            req.topics().add(new MetadataRequestTopic().setName(null).setTopicId(topicId12));
            req.setAllowAutoTopicCreation(false);
            req.setIncludeTopicAuthorizedOperations(true);
            req.unknownTaggedFields().add(new RawTaggedField(1200, new byte[]{(byte) 0xde, (byte) 0xad}));

            MetadataResponseData resp = new MetadataResponseData();
            resp.setThrottleTimeMs(100);
            resp.setClusterId("cluster-meta-v12");
            resp.setControllerId(3);
            resp.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("broker1.kafka.local").setPort(9092).setRack("rack-a"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(2).setHost("broker2.kafka.local").setPort(9092).setRack("rack-b"));
            resp.brokers().add(new MetadataResponseBroker().setNodeId(3).setHost("broker3.kafka.local").setPort(9093).setRack("rack-c"));
            resp.unknownTaggedFields().add(new RawTaggedField(1200, new byte[]{(byte) 0xbe, (byte) 0xef}));

            MetadataResponseTopic t = new MetadataResponseTopic().setErrorCode((short) 0).setName("meta-v12-resolved").setTopicId(topicId12).setIsInternal(false).setTopicAuthorizedOperations(0x0000001f);
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 0).setPartitionIndex(0).setLeaderId(3).setLeaderEpoch(30).setReplicaNodes(Arrays.asList(2, 3)).setIsrNodes(Arrays.asList(2, 3)).setOfflineReplicas(Collections.emptyList()));
            t.partitions().add(new MetadataResponsePartition().setErrorCode((short) 5).setPartitionIndex(1).setLeaderId(-1).setLeaderEpoch(28).setReplicaNodes(Arrays.asList(1, 3)).setIsrNodes(Arrays.asList(1)).setOfflineReplicas(Arrays.asList(3)));
            resp.topics().add(t);

            list.add(createMetadataFixture(
                "metadata-v12-id-only",
                "metadata_v12_id_only",
                v,
                "Metadata v12 flexible wire format at highest Apache Kafka 3.9.1 valid version boundary with ID-only describe (null name), unknown tagged fields, and multiple brokers",
                req, resp
            ));
        }

        return list;
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
