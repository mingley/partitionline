import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.errors.UnsupportedVersionException;

/** Selected actual Apache legacy messages and request-builder policies. */
public final class LegacyDiscoveryOracle {
    private LegacyDiscoveryOracle() { }
    private static byte[] bytes(Message message,short version) {
        ByteBuffer buffer=MessageUtil.toByteBufferAccessor(message,version).buffer();
        byte[] data=new byte[buffer.remaining()];buffer.get(data);return data;
    }
    private static MetadataRequestData metadataRequest(String mode,short version) {
        var data=new MetadataRequestData();
        if(mode.equals("all") && version>=1)return data.setTopics(null);
        return data.setTopics(!mode.equals("named")?List.of():List.of(new MetadataRequestData.MetadataRequestTopic().setName("topic-κ")));
    }
    private static MetadataResponseData metadataResponse(String mode,short version) {
        var partition=new MetadataResponseData.MetadataResponsePartition().setPartitionIndex(0).setLeaderId(7)
            .setReplicaNodes(List.of(7,9)).setIsrNodes(List.of(7));
        var topic=new MetadataResponseData.MetadataResponseTopic().setName("topic-κ").setErrorCode((short)(mode.equals("error")?3:0))
            .setPartitions(mode.equals("error")?List.of():List.of(partition));
        var broker=new MetadataResponseData.MetadataResponseBroker().setNodeId(7).setHost("::1").setPort(9092);
        var data=new MetadataResponseData().setBrokers(new MetadataResponseData.MetadataResponseBrokerCollection(List.of(broker).iterator()))
            .setTopics(new MetadataResponseData.MetadataResponseTopicCollection((mode.equals("empty")?List.<MetadataResponseData.MetadataResponseTopic>of():List.of(topic)).iterator()));
        if(version>=1) {data.setControllerId(7);broker.setRack("");topic.setIsInternal(true);}
        if(version>=2)data.setClusterId("cluster");
        if(version>=7)partition.setLeaderEpoch(4);
        if(version>=10)topic.setTopicId(new Uuid(1,17));
        return data;
    }
    private static void reject(Runnable action,String label) {
        try { action.run();throw new AssertionError("missing refusal: "+label); }
        catch(UnsupportedVersionException expected) { }
    }
    private static Message parse(String name,byte[] body) {
        short version=Short.parseShort(name.split("-")[1].split("\\.")[0].substring(1));
        ByteBuffer buffer=ByteBuffer.wrap(body);var input=new ByteBufferAccessor(buffer);Message parsed;
        if(name.startsWith("metadata-"))parsed=name.endsWith("request.bin")?new MetadataRequestData(input,version):new MetadataResponseData(input,version);
        else parsed=name.endsWith("request.bin")?new FindCoordinatorRequestData(input,version):new FindCoordinatorResponseData(input,version);
        if(buffer.hasRemaining())throw new AssertionError("trailing body: "+name);return parsed;
    }
    public static void main(String[] args)throws Exception {
        if(args.length==2 && args[0].equals("parse-runtime")) {
            Path directory=Path.of(args[1]);int frames=0,metadata=0,find=0,offset=0;
            for(String line:Files.readAllLines(directory.resolve("frames.tsv")).subList(1,Files.readAllLines(directory.resolve("frames.tsv")).size())) {
                String[] row=line.split("\t");boolean request=row[0].equals("request");short api=Short.parseShort(row[3]),version=Short.parseShort(row[4]);
                ByteBuffer buffer=ByteBuffer.wrap(Files.readAllBytes(directory.resolve(row[0]+"-"+api+"-"+version+"-"+row[1]+".frame")));var input=new ByteBufferAccessor(buffer);var key=ApiKeys.forId(api);
                if(request)new RequestHeaderData(input,key.requestHeaderVersion(version));else new ResponseHeaderData(input,key.responseHeaderVersion(version));
                byte[] body=new byte[buffer.remaining()];buffer.duplicate().get(body);Message parsed;
                switch(api) {
                    case 18 -> parsed=request?new ApiVersionsRequestData(input,version):new ApiVersionsResponseData(input,ByteBuffer.wrap(body).getShort()==35?0:version);
                    case 3 -> {parsed=request?new MetadataRequestData(input,version):new MetadataResponseData(input,version);if(request)metadata++;
                        if(version==0 && !request) {var data=(MetadataResponseData)parsed;if(data.controllerId()!=-1 || data.clusterId()!=null || data.brokers().iterator().next().rack()!=null || !data.topics().iterator().next().topicId().equals(Uuid.ZERO_UUID) || data.topics().iterator().next().isInternal() || data.topics().iterator().next().partitions().get(0).leaderEpoch()!=-1)throw new AssertionError("invented v0 fields");}}
                    case 10 -> {parsed=request?new FindCoordinatorRequestData(input,version):new FindCoordinatorResponseData(input,version);if(request){find++;if(version==0 && ((FindCoordinatorRequestData)parsed).keyType()!=0)throw new AssertionError("non-GROUP0");}}
                    case 9 -> {parsed=request?new OffsetFetchRequestData(input,version):new OffsetFetchResponseData(input,version);if(request)offset++;}
                    case 1 -> parsed=request?new FetchRequestData(input,version):new FetchResponseData(input,version);
                    default -> throw new AssertionError("unrelated API"+api);
                }
                if(buffer.hasRemaining())throw new AssertionError("trailing runtime message"+line);
                // ApiVersions error responses use v0 even when the request was v3.
                if(!(api==18 && !request) && !java.util.Arrays.equals(bytes(parsed,version),body))throw new AssertionError("runtime canonical fields differ"+line);
                frames++;
            }
            System.out.println("{\"frames\":"+frames+",\"metadata_requests\":"+metadata+",\"coordinator_requests\":"+find+",\"offset_requests\":"+offset+"}");return;
        }
        if(args.length==3 && args[0].equals("verify")) {
            int bodies=0;
            try(var files=Files.list(Path.of(args[2]))) {
                for(Path golden:files.sorted().toList()) {
                    String name=golden.getFileName().toString();if(!name.endsWith(".bin"))continue;
                    if(!parse(name,Files.readAllBytes(golden)).equals(parse(name,Files.readAllBytes(Path.of(args[1]).resolve(name)))))throw new AssertionError("Rust fields differ: "+name);bodies++;
                }
            }
            if(bodies!=42)throw new AssertionError("missing reverse bodies");
            System.out.println("{\"actual_sdk\":true,\"independently_parsed_rust_bodies\":"+bodies+"}");return;
        }
        if(args.length!=1)throw new IllegalArgumentException("fixture output");
        Path out=Path.of(args[0]);Files.createDirectory(out);int files=0;
        for(short version:new short[]{0,1,13}) {
            for(String mode:List.of("all","empty","named")) {
                var data=metadataRequest(mode,version);var raw=bytes(data,version);
                var buffer=ByteBuffer.wrap(raw);var decoded=new MetadataRequestData(new ByteBufferAccessor(buffer),version);
                if(buffer.hasRemaining() || !decoded.equals(data))throw new AssertionError("Metadata request differs");
                if(new MetadataRequest(decoded,version).isAllTopics()!=(mode.equals("all") || version==0 && mode.equals("empty")))throw new AssertionError("Metadata all-topic meaning differs");
                Files.write(out.resolve("metadata-v"+version+"-"+mode+".request.bin"),raw);files++;
            }
            for(String mode:List.of("full","empty","error")) {
                var data=metadataResponse(mode,version);var raw=bytes(data,version);var buffer=ByteBuffer.wrap(raw);
                var decoded=new MetadataResponseData(new ByteBufferAccessor(buffer),version);
                if(buffer.hasRemaining() || !decoded.equals(data))throw new AssertionError("Metadata response differs: "+decoded+" expected "+data);
                Files.write(out.resolve("metadata-v"+version+"-"+mode+".response.bin"),raw);files++;
            }
        }
        reject(()->new MetadataRequest.Builder(metadataRequest("named",(short)0)).build((short)0),"Metadata builder0");
        reject(()->new MetadataRequest.Builder(metadataRequest("named",(short)0).setAllowAutoTopicCreation(false)).build((short)1),"auto-create policy1");
        for(short version:new short[]{0,1,3,6}) {
            var data=new FindCoordinatorRequestData();
            if(version>=4)data.setCoordinatorKeys(List.of("group-κ"));else data.setKey("group-κ");
            var request=new FindCoordinatorRequest.Builder(data).build(version);var raw=bytes(request.data(),version);
            var buffer=ByteBuffer.wrap(raw);var decoded=new FindCoordinatorRequestData(new ByteBufferAccessor(buffer),version);
            if(buffer.hasRemaining() || !decoded.equals(data))throw new AssertionError("FindCoordinator request differs");
            Files.write(out.resolve("find-v"+version+".request.bin"),raw);files++;
            for(short code:new short[]{0,14,15,16,30}) {
                var response=new FindCoordinatorResponseData();
                if(version>=4)response.setCoordinators(List.of(new FindCoordinatorResponseData.Coordinator().setKey("group-κ").setNodeId(7).setHost("::1").setPort(9092).setErrorCode(code)));
                else response.setNodeId(7).setHost("::1").setPort(9092).setErrorCode(code);
                raw=bytes(response,version);buffer=ByteBuffer.wrap(raw);
                var parsed=new FindCoordinatorResponseData(new ByteBufferAccessor(buffer),version);
                if(buffer.hasRemaining() || !parsed.equals(response))throw new AssertionError("FindCoordinator response differs");
                Files.write(out.resolve("find-v"+version+"-"+code+".response.bin"),raw);files++;
            }
        }
        reject(()->new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey("other").setKeyType((byte)1)).build((short)0),"TRANSACTION builder0");
        var share0=new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKey("other").setKeyType((byte)2)).build((short)0);
        reject(()->bytes(share0.data(),(short)0),"SHARE serializer0");
        System.out.println("{\"actual_sdk\":true,\"bodies\":"+files+",\"metadata_builder0\":\"unsupported\",\"non_group_find0\":\"unsupported\"}");
    }
}
