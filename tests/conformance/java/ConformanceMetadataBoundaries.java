/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

/** Finite Metadata boundary body comparisons with a supplied Apache SDK. */
public final class ConformanceMetadataBoundaries {
    private static final short[] VERSIONS = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    private static final Uuid ID = new Uuid(1L, 17L);

    private static List<String> cases(short version) {
        var cells = new ArrayList<>(List.of("empty", "name"));
        if (version >= 1) cells.add("all");
        if (version >= 9) cells.add("tagged");
        if (version >= 12) cells.add("id");
        return cells;
    }

    private static MetadataRequestData request(short version, String cell, boolean tagged) {
        var req = new MetadataRequestData();
        if (version >= 4) req.setAllowAutoTopicCreation(cell.equals("all"));
        if (version >= 8) req.setIncludeTopicAuthorizedOperations(!cell.equals("all"));
        if (version >= 8 && version <= 10) req.setIncludeClusterAuthorizedOperations(cell.equals("name"));
        if (cell.equals("all")) {
            req.setTopics(null);
        } else {
            req.setTopics(new ArrayList<>());
            if (cell.equals("name") || cell.equals("tagged")) {
                req.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("café"));
            } else if (cell.equals("id")) {
                req.topics().add(new MetadataRequestData.MetadataRequestTopic().setTopicId(ID).setName(null));
            } else if (!cell.equals("empty")) {
                throw new IllegalArgumentException("unknown cell");
            }
        }
        if (tagged) {
            req.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {1, 2}));
            req.topics().get(0).unknownTaggedFields().add(new RawTaggedField(8, new byte[] {3}));
        }
        return req;
    }

    private static MetadataResponseData response(short version, boolean tagged) {
        var resp = new MetadataResponseData();
        if (version >= 3) resp.setThrottleTimeMs(37);
        if (version >= 2) resp.setClusterId("cluster-α");
        if (version >= 1) resp.setControllerId(3);
        if (version >= 8 && version <= 10) resp.setClusterAuthorizedOperations(13);
        var broker = new MetadataResponseData.MetadataResponseBroker().setNodeId(3)
            .setHost("broker.example").setPort(9092);
        if (version >= 1) broker.setRack("rack-a");
        var topic = new MetadataResponseData.MetadataResponseTopic().setName("café").setErrorCode((short) 0);
        if (version >= 1) topic.setIsInternal(true);
        if (version >= 8) topic.setTopicAuthorizedOperations(5);
        if (version >= 10) topic.setTopicId(ID);
        var part = new MetadataResponseData.MetadataResponsePartition().setErrorCode((short) 0)
            .setPartitionIndex(0).setLeaderId(3).setReplicaNodes(Arrays.asList(3, 4)).setIsrNodes(List.of(3));
        if (version >= 5) part.setOfflineReplicas(List.of(4));
        if (version >= 7) part.setLeaderEpoch(7);
        if (tagged) {
            resp.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {4}));
            broker.unknownTaggedFields().add(new RawTaggedField(10, new byte[] {5}));
            topic.unknownTaggedFields().add(new RawTaggedField(11, new byte[] {6}));
            part.unknownTaggedFields().add(new RawTaggedField(12, new byte[] {7}));
        }
        resp.brokers().add(broker);
        topic.partitions().add(part);
        resp.topics().add(topic);
        return resp;
    }

    private static byte[] encode(Message message, short version) {
        var cache = new ObjectSerializationCache();
        var bytes = ByteBuffer.allocate(message.size(cache, version));
        message.write(new ByteBufferAccessor(bytes), cache, version);
        if (bytes.hasRemaining()) throw new AssertionError("size/write mismatch");
        return bytes.array();
    }

    private static void generate(Path path) throws Exception {
        var out = new ArrayList<String>();
        for (short version : VERSIONS) {
            for (var cell : cases(version)) {
                boolean tagged = cell.equals("tagged");
                out.add(version + ":" + cell + " "
                    + HexFormat.of().formatHex(encode(request(version, cell, tagged), version)) + " "
                    + HexFormat.of().formatHex(encode(response(version, tagged), version)));
            }
        }
        Files.write(path, out);
        System.out.println("generated_pairs=" + out.size());
    }

    private static void verify(Path path) throws Exception {
        if (Files.size(path) > 32768) throw new AssertionError("reverse fixture size");
        var lines = Files.readAllLines(path);
        int i = 0;
        for (short version : VERSIONS) {
            for (var cell : cases(version)) {
                if (i >= lines.size()) throw new AssertionError("missing reverse case");
                var fields = lines.get(i++).split(" ");
                if (fields.length != 3 || !fields[0].equals(version + ":" + cell)) {
                    throw new AssertionError("reverse case order");
                }
                var reqBuffer = ByteBuffer.wrap(HexFormat.of().parseHex(fields[1]));
                var respBuffer = ByteBuffer.wrap(HexFormat.of().parseHex(fields[2]));
                var req = new MetadataRequestData(new ByteBufferAccessor(reqBuffer), version);
                var resp = new MetadataResponseData(new ByteBufferAccessor(respBuffer), version);
                if (reqBuffer.hasRemaining() || respBuffer.hasRemaining()) {
                    throw new AssertionError("unconsumed body");
                }
                if (!req.equals(request(version, cell, false))) {
                    throw new AssertionError("Rust request fields differ " + version + ":" + cell);
                }
                if (!resp.equals(response(version, false))) {
                    throw new AssertionError("Rust response fields differ " + version + ":" + cell);
                }
            }
        }
        if (i != lines.size()) throw new AssertionError("extra reverse case");
        System.out.println("independently_parsed_pairs=" + i);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("<jar> <generate|verify|unsupported13> <path>");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(MetadataRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded)) throw new AssertionError("wrong Kafka SDK loaded");
        switch (args[1]) {
            case "generate" -> generate(Path.of(args[2]));
            case "verify" -> verify(Path.of(args[2]));
            case "unsupported13" -> {
                var request = new MetadataRequestData();
                if (request.highestSupportedVersion() != 12) {
                    throw new AssertionError("unexpected 3.9.1 Metadata maximum");
                }
                // Generated message writes can accept versions beyond their declared range.
                var raw = encode(request, (short) 13);
                var response = new MetadataResponseData();
                if (response.highestSupportedVersion() != 12) {
                    throw new AssertionError("unexpected 3.9.1 Metadata response maximum");
                }
                if (!Arrays.equals(encode(response, (short) 12), encode(response, (short) 13))) {
                    throw new AssertionError("3.9.1 raw response differs between v12 and v13");
                }
                System.out.println("metadata_v13_declared_max=12_raw_write_bytes=" + raw.length
                    + "_response_same_as_v12=true");
            }
            default -> throw new IllegalArgumentException("unknown mode");
        }
    }
}
