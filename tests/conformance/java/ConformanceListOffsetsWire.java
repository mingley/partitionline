/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;

/** Selected ListOffsets bodies from pinned Apache SDKs. */
public final class ConformanceListOffsetsWire {
    private static final String RECENT = "orders.recent";
    private static final String ARCHIVE = "orders.archive";
    private static final long TIME = 1_710_000_000_000L;
    private static final long OFFSET = 4_000_000_000L;
    private static final HexFormat HEX = HexFormat.of();

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static byte[] encode(Message body, short version) {
        var cache = new ObjectSerializationCache();
        var bytes = ByteBuffer.allocate(body.size(cache, version));
        body.write(new ByteBufferAccessor(bytes), cache, version);
        check(!bytes.hasRemaining(), "Apache body size did not match bytes written");
        return bytes.array();
    }

    private static List<String> cases(short version) {
        var result = new ArrayList<>(List.of("empty", "queries"));
        if (version >= 6) {
            result.add("tagged");
        }
        if (version == 5 || version == 6 || version == 11) {
            result.add("error");
        }
        return result;
    }

    private static ListOffsetsRequestData.ListOffsetsPartition query(
            short version, int partition, long timestamp) {
        return new ListOffsetsRequestData.ListOffsetsPartition()
            .setPartitionIndex(partition)
            .setCurrentLeaderEpoch(version >= 4 ? 17 : -1)
            .setTimestamp(timestamp);
    }

    private static List<ListOffsetsRequestData.ListOffsetsTopic> topics(short version, String cell) {
        if (cell.equals("empty")) {
            return List.of();
        }
        var recent = new ListOffsetsRequestData.ListOffsetsTopic().setName(RECENT);
        var archive = new ListOffsetsRequestData.ListOffsetsTopic().setName(ARCHIVE);
        recent.partitions().add(query(version, 3, -2));
        archive.partitions().add(query(version, 7, -1));
        if (!cell.equals("error")) {
            recent.partitions().add(query(version, 1, TIME));
            archive.partitions().add(query(version, 2, TIME + 10));
            if (version >= 7) archive.partitions().add(query(version, 9, -3));
            if (version >= 8) archive.partitions().add(query(version, 11, -4));
            if (version >= 9) archive.partitions().add(query(version, 15, -5));
            if (version >= 11) archive.partitions().add(query(version, 17, -6));
        }
        return List.of(recent, archive);
    }

    private static boolean tagged(String cell, boolean includeTags) {
        return includeTags && cell.equals("tagged");
    }

    private static ListOffsetsRequestData request(short version, String cell, boolean includeTags)
            throws Exception {
        var topics = topics(version, cell);
        var request = cell.equals("error")
            ? ListOffsetsRequest.Builder.forReplica(version, -2).setTargetTimes(topics).build(version).data()
            : new ListOffsetsRequestData().setReplicaId(-1).setTopics(topics);
        if (version >= 10 && !cell.equals("empty")) {
            // The 3.9.1 SDK predates the setter and only runs versions 1–9.
            request.getClass().getMethod("setTimeoutMs", int.class).invoke(request, 1250 + version);
        }
        if (tagged(cell, includeTags)) {
            request.unknownTaggedFields().add(new RawTaggedField(9, new byte[] {1, 2}));
            request.topics().getFirst().unknownTaggedFields().add(new RawTaggedField(11, new byte[] {3}));
            request.topics().getFirst().partitions().getFirst().unknownTaggedFields()
                .add(new RawTaggedField(12, new byte[] {4}));
        }
        return request;
    }

    private static int throttle(short version, String cell) {
        if (version < 2 || cell.equals("empty")) {
            return 0;
        }
        return cell.equals("error") ? 43 : 30 + version;
    }

    private static ListOffsetsResponseData response(short version, String cell, boolean includeTags) {
        var topics = topics(version, cell);
        if (cell.equals("error")) {
            var request = ListOffsetsRequest.Builder.forReplica(version, -2)
                .setTargetTimes(topics).build(version);
            return ((ListOffsetsResponse) request.getErrorResponse(throttle(version, cell),
                new NotLeaderOrFollowerException("fixture"))).data();
        }
        var response = new ListOffsetsResponseData().setThrottleTimeMs(throttle(version, cell));
        for (var topic : topics) {
            var results = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(topic.name());
            for (var partition : topic.partitions()) {
                int index = partition.partitionIndex();
                boolean failed = index == 2;
                long timestamp = partition.timestamp();
                var result = new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                    .setPartitionIndex(index)
                    .setErrorCode((short) (failed ? 3 : 0))
                    .setTimestamp(failed ? -1 : timestamp >= 0 ? timestamp + 1 : -1)
                    .setOffset(failed ? -1 : OFFSET + index)
                    .setLeaderEpoch(version >= 4 && !failed ? 18 : -1);
                results.partitions().add(result);
            }
            response.topics().add(results);
        }
        if (tagged(cell, includeTags)) {
            response.unknownTaggedFields().add(new RawTaggedField(7, new byte[] {5}));
            response.topics().getFirst().unknownTaggedFields().add(new RawTaggedField(8, new byte[] {6}));
            response.topics().getFirst().partitions().getFirst().unknownTaggedFields()
                .add(new RawTaggedField(13, new byte[] {7}));
        }
        return response;
    }

    private static void verify(short version, String cell, String row, boolean includeTags)
            throws Exception {
        var fields = row.split(" ");
        String identity = version + ":" + cell;
        check(fields.length == 3 && fields[0].equals(identity), "wrong ListOffsets row " + identity);
        var requestBytes = ByteBuffer.wrap(HEX.parseHex(fields[1]));
        var responseBytes = ByteBuffer.wrap(HEX.parseHex(fields[2]));
        var actualRequest = new ListOffsetsRequestData(new ByteBufferAccessor(requestBytes), version);
        var actualResponse = new ListOffsetsResponseData(new ByteBufferAccessor(responseBytes), version);
        check(!requestBytes.hasRemaining() && !responseBytes.hasRemaining(),
            "unconsumed ListOffsets bytes " + identity);
        check(actualRequest.equals(request(version, cell, includeTags)),
            "ListOffsets request fields differ " + identity);
        check(actualResponse.equals(response(version, cell, includeTags)),
            "ListOffsets response fields differ " + identity);
    }

    public static void main(String[] args) throws Exception {
        check(args.length == 5, "<sdk-jar> <declared-min> <declared-max> <generate|verify> <file>");
        var jar = Path.of(args[0]);
        var loaded = Path.of(ListOffsetsRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        check(Files.isSameFile(jar, loaded), "wrong Apache SDK loaded");
        short min = Short.parseShort(args[1]);
        short max = Short.parseShort(args[2]);
        var req = new ListOffsetsRequestData();
        var resp = new ListOffsetsResponseData();
        check(req.lowestSupportedVersion() == min && resp.lowestSupportedVersion() == min
            && req.highestSupportedVersion() == max && resp.highestSupportedVersion() == max,
            "Apache SDK ListOffsets version range changed");
        var path = Path.of(args[4]);
        if (args[3].equals("generate")) {
            var rows = new ArrayList<String>();
            for (short version = 1; version <= max; version++) {
                for (String cell : cases(version)) {
                    var row = version + ":" + cell + " "
                        + HEX.formatHex(encode(request(version, cell, true), version))
                        + " " + HEX.formatHex(encode(response(version, cell, true), version));
                    verify(version, cell, row, true);
                    rows.add(row);
                }
            }
            Files.writeString(path, String.join("\n", rows) + "\n");
            System.out.println("generated_pairs=" + rows.size());
        } else if (args[3].equals("verify")) {
            check(Files.size(path) <= 32_768, "ListOffsets corpus exceeds fixture budget");
            var rows = Files.readAllLines(path);
            int index = 0;
            for (short version = 1; version <= max; version++) {
                for (String cell : cases(version)) {
                    check(index < rows.size(), "missing ListOffsets row");
                    verify(version, cell, rows.get(index++), false);
                }
            }
            check(index == rows.size(), "extra ListOffsets row");
            System.out.println("independently_parsed_pairs=" + index);
        } else {
            throw new IllegalArgumentException("expected generate or verify");
        }
    }
}
