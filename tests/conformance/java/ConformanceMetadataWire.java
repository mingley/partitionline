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

/** A finite Metadata v13 body comparison with the loaded Apache SDK. */
public final class ConformanceMetadataWire {
    private static final short VERSION = 13;
    private static final List<String> CASES = List.of("all", "empty", "name", "id", "tagged");
    private static final Uuid ID = new Uuid(1L, 17L);

    private static MetadataRequestData request(String cell, boolean tagged) {
        var req = new MetadataRequestData().setAllowAutoTopicCreation(cell.equals("all"))
            .setIncludeTopicAuthorizedOperations(!cell.equals("all"));
        if (cell.equals("all")) {
            req.setTopics(null);
        } else {
            req.setTopics(new ArrayList<>());
            if (cell.equals("name")) {
                req.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("café"));
            } else if (cell.equals("id") || cell.equals("tagged")) {
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

    private static MetadataResponseData response(boolean tagged) {
        var resp = new MetadataResponseData().setThrottleTimeMs(37).setClusterId("cluster-α")
            .setControllerId(3).setErrorCode((short) 31);
        var broker = new MetadataResponseData.MetadataResponseBroker().setNodeId(3)
            .setHost("broker.example").setPort(9092).setRack("rack-a");
        var topic = new MetadataResponseData.MetadataResponseTopic().setName("café")
            .setTopicId(ID).setTopicAuthorizedOperations(5).setIsInternal(false).setErrorCode((short) 0);
        var part = new MetadataResponseData.MetadataResponsePartition().setErrorCode((short) 0)
            .setPartitionIndex(0).setLeaderId(3).setLeaderEpoch(7).setReplicaNodes(Arrays.asList(3, 4))
            .setIsrNodes(List.of(3)).setOfflineReplicas(List.of(4));
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

    private static byte[] encode(Message message) {
        var cache = new ObjectSerializationCache();
        var bytes = ByteBuffer.allocate(message.size(cache, VERSION));
        message.write(new ByteBufferAccessor(bytes), cache, VERSION);
        if (bytes.hasRemaining()) throw new AssertionError("size/write mismatch");
        return bytes.array();
    }

    private static void generate(Path path) throws Exception {
        var out = new ArrayList<String>();
        for (var cell : CASES) {
            boolean tagged = cell.equals("tagged");
            out.add(cell + " " + HexFormat.of().formatHex(encode(request(cell, tagged))) + " "
                + HexFormat.of().formatHex(encode(response(tagged))));
        }
        Files.write(path, out);
        System.out.println("generated_pairs=" + out.size());
    }

    private static void verify(Path path) throws Exception {
        if (Files.size(path) > 32768) throw new AssertionError("reverse fixture size");
        var lines = Files.readAllLines(path);
        if (lines.size() != CASES.size()) throw new AssertionError("reverse case count");
        for (int i = 0; i < lines.size(); i++) {
            var fields = lines.get(i).split(" ");
            if (fields.length != 3 || !fields[0].equals(CASES.get(i))) throw new AssertionError("reverse case order");
            var reqBuffer = ByteBuffer.wrap(HexFormat.of().parseHex(fields[1]));
            var respBuffer = ByteBuffer.wrap(HexFormat.of().parseHex(fields[2]));
            var req = new MetadataRequestData(new ByteBufferAccessor(reqBuffer), VERSION);
            var resp = new MetadataResponseData(new ByteBufferAccessor(respBuffer), VERSION);
            if (reqBuffer.hasRemaining() || respBuffer.hasRemaining()) throw new AssertionError("unconsumed body");
            if (!req.equals(request(fields[0], false))) throw new AssertionError("Rust request fields differ");
            if (!resp.equals(response(false))) throw new AssertionError("Rust response fields differ");
        }
        System.out.println("independently_parsed_pairs=" + lines.size());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("<jar> <generate|verify> <path>");
        Path jar = Path.of(args[0]);
        Path loaded = Path.of(MetadataRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isSameFile(jar, loaded)) throw new AssertionError("wrong Kafka SDK loaded");
        switch (args[1]) {
            case "generate" -> generate(Path.of(args[2]));
            case "verify" -> verify(Path.of(args[2]));
            default -> throw new IllegalArgumentException("unknown mode");
        }
    }
}
