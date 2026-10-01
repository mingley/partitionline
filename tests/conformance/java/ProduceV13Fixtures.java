/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

/** Apache 4.1.0 body serializer for KIP-516 Produce topic IDs. */
public final class ProduceV13Fixtures {
    static final String PIN="180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb";
    static String sha(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }
    static byte[] encode(Message data) {
        var cache=new ObjectSerializationCache();
        var buffer=ByteBuffer.allocate(data.size(cache,(short)13));
        data.write(new ByteBufferAccessor(buffer),cache,(short)13);
        if(buffer.hasRemaining()) throw new AssertionError("size/write mismatch");
        return buffer.array();
    }
    static void output(Path path,byte[] data,boolean verify) throws Exception {
        if(verify) { if(!Arrays.equals(Files.readAllBytes(path),data)) throw new AssertionError("fixture differs: "+path); }
        else Files.write(path,data);
    }
    public static void main(String[] args) throws Exception {
        if(args.length<2||args.length>3) throw new IllegalArgumentException("<pinned-jar> <output> [--verify]");
        Path jar=Path.of(args[0]);
        Path loaded=Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if(!Files.isSameFile(jar,loaded)||!sha(Files.readAllBytes(jar)).equals(PIN)) throw new AssertionError("wrong Apache 4.1.0 jar");
        boolean verify=args.length==3&&args[2].equals("--verify");
        Path dir=Path.of(args[1]);Files.createDirectories(dir);
        for(String cell:new String[]{"empty","paired","reversed","errors","tagged","zero-id"}) {
            var request=new ProduceRequestData().setAcks((short)-1).setTimeoutMs(1234);
            var response=new ProduceResponseData().setThrottleTimeMs(37);
            if(!cell.equals("empty")) for(int i=0;i<2;i++) {
                var id=cell.equals("zero-id")&&i==0?Uuid.ZERO_UUID:new Uuid(i+1,i+17);
                var topic=new ProduceRequestData.TopicProduceData().setTopicId(id);
                var part=new ProduceRequestData.PartitionProduceData().setIndex(i*3).setRecords(
                    MemoryRecords.withRecords(Compression.NONE,new SimpleRecord(42L+i,
                        ("key-"+i).getBytes(StandardCharsets.UTF_8),("value-"+i).getBytes(StandardCharsets.UTF_8))));
                topic.partitionData().add(part);request.topicData().add(topic);
                var result=new ProduceResponseData.TopicProduceResponse().setTopicId(id);
                var resultPart=new ProduceResponseData.PartitionProduceResponse().setIndex(i*3)
                    .setErrorCode((short)(cell.equals("errors")&&i==0?100:0))
                    .setBaseOffset(cell.equals("errors")&&i==0?-1:100+i*3)
                    .setLogAppendTimeMs(-1).setLogStartOffset(0);
                if(cell.equals("errors")&&i==0) resultPart.setErrorMessage("stale topic ID");
                result.partitionResponses().add(resultPart);
                response.responses().add(result);
                if(cell.equals("tagged")) {
                    part.unknownTaggedFields().add(new RawTaggedField(127,new byte[]{1,2}));
                    topic.unknownTaggedFields().add(new RawTaggedField(126,new byte[]{3}));
                    resultPart.setCurrentLeader(new ProduceResponseData.LeaderIdAndEpoch().setLeaderId(7).setLeaderEpoch(9));
                }
            }
            if(cell.equals("reversed")) {
                var reversed=new java.util.ArrayList<ProduceResponseData.TopicProduceResponse>();
                for(var topic:response.responses()) reversed.add(new ProduceResponseData.TopicProduceResponse()
                    .setTopicId(topic.topicId()).setPartitionResponses(topic.partitionResponses()));
                java.util.Collections.reverse(reversed);
                response.responses().clear();response.responses().addAll(reversed);
            }
            if(cell.equals("tagged")) {
                request.unknownTaggedFields().add(new RawTaggedField(125,new byte[]{4}));
                response.nodeEndpoints().add(new ProduceResponseData.NodeEndpoint().setNodeId(7)
                    .setHost("broker").setPort(9092).setRack(null));
            }
            int expected=cell.equals("empty")?0:2;
            if(request.topicData().size()!=expected||response.responses().size()!=expected)
                throw new AssertionError("fixture lost a topic row: "+cell);
            byte[] req=encode(request),res=encode(response);
            var decodedRequest=new ProduceRequestData(new ByteBufferAccessor(ByteBuffer.wrap(req)),(short)13);
            var decodedResponse=new ProduceResponseData(new ByteBufferAccessor(ByteBuffer.wrap(res)),(short)13);
            if(!request.equals(decodedRequest)||!response.equals(decodedResponse)) throw new AssertionError("Apache roundtrip mismatch");
            String stem="produce_v13_"+cell.replace('-','_');
            output(dir.resolve(stem+"_request.bin"),req,verify);output(dir.resolve(stem+"_response.bin"),res,verify);
            System.out.println("{\"cell\":\""+cell+"\",\"version\":13,\"request_sha256\":\""+sha(req)
                +"\",\"response_sha256\":\""+sha(res)+"\",\"request_bytes\":"+req.length+",\"response_bytes\":"+res.length+"}");
        }
    }
}
