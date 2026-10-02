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
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

/** Independently pinned Apache body serializer for KIP-516 Produce topic IDs. */
public final class ProduceV13Fixtures {
    static final java.util.Map<String,String> PINS=java.util.Map.of(
        "180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb","4.1.0",
        "33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed","4.1.2",
        "9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159","4.2.1",
        "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e","4.3.1");
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
        if(args.length<2||args.length>4) throw new IllegalArgumentException("<pinned-jar> <output> [--verify]");
        Path jar=Path.of(args[0]);
        Path loaded=Path.of(ProduceRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if(!Files.isSameFile(jar,loaded)||!PINS.containsKey(sha(Files.readAllBytes(jar)))) throw new AssertionError("wrong pinned Apache jar");
        if(args.length==4 && args[2].equals("--decode-rust")) {
            byte[] requestBytes=HexFormat.of().parseHex(args[1]);
            byte[] responseBytes=HexFormat.of().parseHex(args[3]);
            var requestBuffer=ByteBuffer.wrap(requestBytes);
            var responseBuffer=ByteBuffer.wrap(responseBytes);
            var request=new ProduceRequestData(new ByteBufferAccessor(requestBuffer),(short)13);
            var response=new ProduceResponseData(new ByteBufferAccessor(responseBuffer),(short)13);
            if(requestBuffer.hasRemaining()||responseBuffer.hasRemaining()) throw new AssertionError("unconsumed Rust body bytes");
            if(request.acks()!=-1||request.timeoutMs()!=1234||request.topicData().size()!=2||response.responses().size()!=2||response.throttleTimeMs()!=37)
                throw new AssertionError("Rust body top-level fields differ");
            for(int i=0;i<2;i++) {
                var id=new Uuid(i+1,i+17);
                var topic=request.topicData().find("",id);
                if(topic==null||topic.partitionData().size()!=1||topic.partitionData().get(0).index()!=i*3)
                    throw new AssertionError("Rust request identity/partition mismatch");
                var records=(MemoryRecords)topic.partitionData().get(0).records();
                int recordCount=0;
                for(var batch:records.batches()) {
                    batch.ensureValid();
                    for(var record:batch) {
                        byte[] key=new byte[record.key().remaining()];record.key().duplicate().get(key);
                        byte[] value=new byte[record.value().remaining()];record.value().duplicate().get(value);
                        if(record.timestamp()!=42L+i||!Arrays.equals(key,("key-"+i).getBytes(StandardCharsets.UTF_8))||!Arrays.equals(value,("value-"+i).getBytes(StandardCharsets.UTF_8)))
                            throw new AssertionError("Rust record content mismatch");
                        recordCount++;
                    }
                }
                if(recordCount!=1)throw new AssertionError("Rust record count mismatch");
                var result=response.responses().find("",id);
                if(result==null||result.partitionResponses().size()!=1)throw new AssertionError("Rust response identity mismatch");
                var part=result.partitionResponses().get(0);
                if(part.index()!=i*3||part.errorCode()!=0||part.baseOffset()!=100+i*3||part.logAppendTimeMs()!=-1||part.logStartOffset()!=0)
                    throw new AssertionError("Rust response outcome mismatch");
            }
            System.out.println("OK: Produce13 Rust request/response two topic IDs, records/CRC, offsets and throttle");
            return;
        }
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
