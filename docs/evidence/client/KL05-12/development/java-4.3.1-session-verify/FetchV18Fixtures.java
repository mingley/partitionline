/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.internal.MemoryRecords;

/** Independent, pinned Apache KIP-1166 body serializer and Rust-body parser. */
public final class FetchV18Fixtures {
    static final java.util.Map<String,String> PINS=java.util.Map.of(
        "180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb","4.1.0",
        "33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed","4.1.2",
        "9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159","4.2.1",
        "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e","4.3.1");
    static final String[] CELLS={"empty","consumer","default","unknown","zero","known","both_tags","signed_min","unknown_tags","session"};
    static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    static byte[] encode(Message data,short version) {
        var cache=new ObjectSerializationCache();
        var buffer=ByteBuffer.allocate(data.size(cache,version));
        data.write(new ByteBufferAccessor(buffer),cache,version);
        if(buffer.hasRemaining())throw new AssertionError("size/write mismatch");
        return buffer.array();
    }
    static long watermark(String cell,int i) {
        return switch(cell) {
            case "unknown" -> -1L;
            case "zero" -> 0L;
            case "known","both_tags","unknown_tags" -> 41L+i;
            case "signed_min" -> Long.MIN_VALUE;
            default -> Long.MAX_VALUE;
        };
    }
    static boolean consumer(String cell) { return cell.equals("consumer")||cell.equals("empty")||cell.equals("session"); }
    static FetchRequestData request(String cell) {
        var data=new FetchRequestData().setMaxWaitMs(1234).setMinBytes(2).setMaxBytes(8192)
            .setIsolationLevel((byte)1).setRackId("rack-v18").setSessionId(0).setSessionEpoch(-1);
        if(!consumer(cell))data.setClusterId("cluster-v18").setReplicaState(
            new FetchRequestData.ReplicaState().setReplicaId(7).setReplicaEpoch(9));
        if(!cell.equals("empty"))for(int i=0;i<2;i++) {
            var part=new FetchRequestData.FetchPartition().setPartition(i*3).setCurrentLeaderEpoch(11+i)
                .setFetchOffset(50+i).setLastFetchedEpoch(10+i).setLogStartOffset(3+i)
                .setPartitionMaxBytes(4096).setHighWatermark(watermark(cell,i));
            if(cell.equals("both_tags")||cell.equals("unknown_tags"))part.setReplicaDirectoryId(new Uuid(i+33,i+49));
            if(cell.equals("unknown_tags"))part.unknownTaggedFields().add(new RawTaggedField(127,new byte[]{1,2,3}));
            var topic=new FetchRequestData.FetchTopic().setTopicId(new Uuid(i+1,i+17));
            topic.partitions().add(part);data.topics().add(topic);
            if(cell.equals("unknown_tags"))topic.unknownTaggedFields().add(new RawTaggedField(126,new byte[]{4}));
        }
        if(cell.equals("unknown_tags"))data.unknownTaggedFields().add(new RawTaggedField(125,new byte[]{5}));
        if(cell.equals("session")) {
            data.setSessionId(91).setSessionEpoch(4);
            data.forgottenTopicsData().add(new FetchRequestData.ForgottenTopic().setTopicId(new Uuid(3,19))
                .setPartitions(java.util.List.of(2)));
        }
        return data;
    }
    static FetchResponseData response(String cell) {
        var data=new FetchResponseData().setThrottleTimeMs(cell.equals("session")?0:37).setSessionId(cell.equals("session")?91:0);
        if(!cell.equals("empty"))for(int i=0;i<2;i++) {
            var part=new FetchResponseData.PartitionData().setPartitionIndex(i*3).setHighWatermark(100+i)
                .setLastStableOffset(90+i).setLogStartOffset(3+i).setAbortedTransactions(java.util.List.of())
                .setRecords(MemoryRecords.EMPTY);
            var topic=new FetchResponseData.FetchableTopicResponse().setTopicId(new Uuid(i+1,i+17));
            topic.partitions().add(part);data.responses().add(topic);
            if(cell.equals("unknown_tags"))part.unknownTaggedFields().add(new RawTaggedField(127,new byte[]{6}));
        }
        return data;
    }
    static void checkBodies(byte[] requestBytes,byte[] responseBytes,String cell,short version,boolean exact) {
        var reqBuffer=ByteBuffer.wrap(requestBytes);var resBuffer=ByteBuffer.wrap(responseBytes);
        var req=new FetchRequestData(new ByteBufferAccessor(reqBuffer),version);
        var res=new FetchResponseData(new ByteBufferAccessor(resBuffer),version);
        if(reqBuffer.hasRemaining()||resBuffer.hasRemaining())throw new AssertionError("unconsumed body bytes");
        var expectedReq=request(cell);var expectedRes=response(cell);
        if(version<18)for(var t:expectedReq.topics())for(var p:t.partitions())p.setHighWatermark(Long.MAX_VALUE);
        if(!req.equals(expectedReq)||!res.equals(expectedRes))throw new AssertionError("Apache required fields differ: "+cell+" v"+version);
        if(exact&&(!Arrays.equals(encode(req,version),requestBytes)||!Arrays.equals(encode(res,version),responseBytes)))
            throw new AssertionError("Apache byte roundtrip differs");
    }
    static void output(Path path,byte[] bytes,boolean verify) throws Exception {
        if(verify){if(!Arrays.equals(Files.readAllBytes(path),bytes))throw new AssertionError("fixture differs: "+path);}
        else Files.write(path,bytes);
    }
    public static void main(String[] args) throws Exception {
        if(args.length<2||args.length>3)throw new IllegalArgumentException("<pinned-jar> <output> [--verify|--decode-rust]");
        Path jar=Path.of(args[0]);Path loaded=Path.of(FetchRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if(!Files.isSameFile(jar,loaded)||!PINS.containsKey(sha(Files.readAllBytes(jar))))throw new AssertionError("wrong pinned Apache jar");
        Path dir=Path.of(args[1]);boolean decode=args.length==3&&args[2].equals("--decode-rust");
        if(decode) {
            for(String cell:new String[]{"consumer","known","both_tags","session"}) {
                String stem="fetch_v18_"+cell;
                checkBodies(Files.readAllBytes(dir.resolve(stem+"_request.bin")),Files.readAllBytes(dir.resolve(stem+"_response.bin")),cell,(short)18,true);
            }
            System.out.println("OK: Apache parsed four Rust Fetch18 request/response pairs, UUIDs, sessions, replica state, directory and watermarks");
            return;
        }
        boolean verify=args.length==3&&args[2].equals("--verify");Files.createDirectories(dir);
        for(String cell:CELLS) {
            byte[] req=encode(request(cell),(short)18),res=encode(response(cell),(short)18);
            checkBodies(req,res,cell,(short)18,true);
            String stem="fetch_v18_"+cell;
            output(dir.resolve(stem+"_request.bin"),req,verify);output(dir.resolve(stem+"_response.bin"),res,verify);
            System.out.println("{\"cell\":\""+cell+"\",\"version\":18,\"request_sha256\":\""+sha(req)
                +"\",\"response_sha256\":\""+sha(res)+"\",\"request_bytes\":"+req.length+",\"response_bytes\":"+res.length+"}");
        }
        for(String cell:new String[]{"consumer","default","known","both_tags","session"}) {
            byte[] req=encode(request(cell),(short)17),res=encode(response(cell),(short)17);
            checkBodies(req,res,cell,(short)17,true);
            output(dir.resolve("fetch_v17_delta_"+cell+"_request.bin"),req,verify);
            output(dir.resolve("fetch_v17_delta_"+cell+"_response.bin"),res,verify);
        }
    }
}
