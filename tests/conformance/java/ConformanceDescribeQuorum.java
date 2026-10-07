import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;

/** Genuine SDK serializers, parsers, raw native calls and public Admin results. */
public final class ConformanceDescribeQuorum {
    private ConformanceDescribeQuorum() { }
    private static byte[] bytes(Message value,short version) {
        ByteBuffer buffer=MessageUtil.toByteBufferAccessor(value,version).buffer();byte[] result=new byte[buffer.remaining()];buffer.get(result);return result;
    }
    private static Message parse(String name,byte[] data,short version) {
        ByteBuffer buffer=ByteBuffer.wrap(data);var input=new ByteBufferAccessor(buffer);
        Message value=name.endsWith("request.bin")?new DescribeQuorumRequestData(input,version):new DescribeQuorumResponseData(input,version);
        if(buffer.hasRemaining())throw new AssertionError("trailing SDK body "+name);return value;
    }
    private static DescribeQuorumRequestData singleton() {return DescribeQuorumRequest.singletonRequest(new TopicPartition("__cluster_metadata",0));}
    private static DescribeQuorumRequestData multi() {
        return new DescribeQuorumRequestData().setTopics(List.of(
            new DescribeQuorumRequestData.TopicData().setTopicName("alpha").setPartitions(List.of(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(0),new DescribeQuorumRequestData.PartitionData().setPartitionIndex(-1),new DescribeQuorumRequestData.PartitionData().setPartitionIndex(Integer.MAX_VALUE))),
            new DescribeQuorumRequestData.TopicData().setTopicName("alpha").setPartitions(List.of()),
            new DescribeQuorumRequestData.TopicData().setTopicName("snowman-\u2603").setPartitions(List.of(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(Integer.MIN_VALUE)))));
    }
    private static DescribeQuorumResponseData.ReplicaState replica(short version,int id,long offset) {
        var value=new DescribeQuorumResponseData.ReplicaState().setReplicaId(id).setLogEndOffset(offset);
        if(version>=1)value.setLastFetchTimestamp(123).setLastCaughtUpTimestamp(Long.MAX_VALUE);
        if(version>=2)value.setReplicaDirectoryId(new Uuid(id+1,7));return value;
    }
    private static DescribeQuorumResponseData success(short version,boolean emptyOptional) {
        var voter=replica(version,1,456);var observer=replica(version,2,Long.MIN_VALUE);
        if(emptyOptional){voter.setLastFetchTimestamp(-1).setLastCaughtUpTimestamp(-1).setReplicaDirectoryId(Uuid.ZERO_UUID);observer.setLastFetchTimestamp(-1).setLastCaughtUpTimestamp(-1).setReplicaDirectoryId(Uuid.ZERO_UUID);}
        var partition=new DescribeQuorumResponseData.PartitionData().setPartitionIndex(0).setLeaderId(1).setLeaderEpoch(7).setHighWatermark(123).setCurrentVoters(List.of(voter)).setObservers(List.of(observer));
        var value=new DescribeQuorumResponseData().setTopics(List.of(new DescribeQuorumResponseData.TopicData().setTopicName("__cluster_metadata").setPartitions(List.of(partition))));
        if(version>=2 && !emptyOptional) {
            var listeners=new DescribeQuorumResponseData.ListenerCollection();listeners.add(new DescribeQuorumResponseData.Listener().setName("CONTROLLER").setHost("::1").setPort(65535));
            var nodes=new DescribeQuorumResponseData.NodeCollection();nodes.add(new DescribeQuorumResponseData.Node().setNodeId(1).setListeners(listeners));value.setNodes(nodes);
        }
        return value;
    }
    private static void stripUnknown(Message value) {
        value.unknownTaggedFields().clear();
        if(value instanceof DescribeQuorumRequestData request)for(var topic:request.topics()){topic.unknownTaggedFields().clear();for(var partition:topic.partitions())partition.unknownTaggedFields().clear();}
        if(value instanceof DescribeQuorumResponseData response) {
            for(var topic:response.topics()){topic.unknownTaggedFields().clear();for(var partition:topic.partitions()){partition.unknownTaggedFields().clear();for(var replica:partition.currentVoters())replica.unknownTaggedFields().clear();for(var replica:partition.observers())replica.unknownTaggedFields().clear();}}
            for(var node:response.nodes()){node.unknownTaggedFields().clear();for(var listener:node.listeners())listener.unknownTaggedFields().clear();}
        }
    }
    private static int fixture(Path out,short version,int index,Message value,boolean request)throws Exception {
        String name="case-"+version+"-"+index+(request?"-request.bin":"-response.bin");byte[] data=bytes(value,version);
        if(!java.util.Arrays.equals(bytes(parse(name,data,version),version),data))throw new AssertionError("SDK wire roundtrip "+name);
        for(int length=0;length<data.length;length++){boolean rejected=false;try{parse(name,java.util.Arrays.copyOf(data,length),version);}catch(RuntimeException expected){rejected=true;}if(!rejected)throw new AssertionError("SDK accepted prefix "+name+" length"+length);}
        Files.write(out.resolve(name),data);return index+1;
    }
    private static void generate(Path out)throws Exception {
        Files.createDirectory(out);int total=0;
        for(short version=0;version<=2;version++) {
            int n=0;
            for(var request:List.of(new DescribeQuorumRequestData(),singleton(),multi()))n=fixture(out,version,n,new DescribeQuorumRequest.Builder(request).build(version).data(),true);
            for(short error:new short[]{0,31,42,6,41,-1}) {
                DescribeQuorumResponse response=error==0?new DescribeQuorumResponse(new DescribeQuorumResponseData()):(DescribeQuorumResponse)new DescribeQuorumRequest.Builder(singleton()).build(version).getErrorResponse(123,Errors.forCode(error).exception());
                if(response.data().errorCode()!=error || !response.errorCounts().equals(Map.of(Errors.forCode(error),1)))throw new AssertionError("SDK error factory/counts");
                if(response.throttleTimeMs()!=0 || response.shouldClientThrottle(version))throw new AssertionError("unexpected throttle");response.maybeSetThrottleTimeMs(17);if(response.throttleTimeMs()!=0)throw new AssertionError("throttle mutation");
                n=fixture(out,version,n,response.data(),false);
            }
            for(short error:new short[]{31,6}) {
                var data=DescribeQuorumRequest.getPartitionLevelErrorResponse(multi(),Errors.forCode(error));
                if(!new DescribeQuorumResponse(data).errorCounts().equals(Map.of(Errors.NONE,1,Errors.forCode(error),4)))throw new AssertionError("nested error counts");
                n=fixture(out,version,n,data,false);
            }
            n=fixture(out,version,n,success(version,false),false);
            n=fixture(out,version,n,new DescribeQuorumResponseData().setErrorMessage(null),false);
            var taggedRequest=singleton();taggedRequest.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1,2,3}));n=fixture(out,version,n,taggedRequest,true);
            var taggedResponse=success(version,true);taggedResponse.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1,2,3}));n=fixture(out,version,n,taggedResponse,false);
            if(n!=15)throw new AssertionError("missing declared SDK bodies");total+=n;
        }
        var bigTopics=new ArrayList<DescribeQuorumRequestData.TopicData>();for(int i=0;i<8193;i++)bigTopics.add(new DescribeQuorumRequestData.TopicData().setTopicName(""));
        byte[] large=bytes(new DescribeQuorumRequestData().setTopics(bigTopics),(short)2);
        if(((DescribeQuorumRequestData)parse("request.bin",large,(short)2)).topics().size()!=8193)throw new AssertionError("reference array policy");Files.write(out.resolve("policy-array-8193.request"),large);
        for(int length:new int[]{32767,32768,65536,65537}) {
            boolean supported=false;String failure="";try {var request=DescribeQuorumRequest.singletonRequest(new TopicPartition("a".repeat(length),0));byte[] data=bytes(request,(short)2);parse("request.bin",data,(short)2);supported=true;Files.write(out.resolve("policy-string-"+length+".request"),data);}catch(RuntimeException error){failure=error.getClass().getName();}
            Files.writeString(out.resolve("policy-string-"+length+".json"),"{\"length\":"+length+",\"SDK_supported\":"+supported+",\"failure\":\""+failure+"\"}\n");
        }
        int refusals=0;
        for(short version=0;version<2;version++)for(String field:List.of("directory","nodes")) {
            var response=success((short)2,false);
            if(field.equals("directory"))response.setNodes(new DescribeQuorumResponseData.NodeCollection());
            else for(var topic:response.topics())for(var partition:topic.partitions()){for(var replica:partition.currentVoters())replica.setReplicaDirectoryId(Uuid.ZERO_UUID);for(var replica:partition.observers())replica.setReplicaDirectoryId(Uuid.ZERO_UUID);}
            boolean refused=false;try{bytes(response,version);}catch(org.apache.kafka.common.errors.UnsupportedVersionException expected){refused=true;}
            if(!refused)throw new AssertionError("nonignorable future field was silently dropped "+field);refusals++;
        }
        Files.writeString(out.resolve("nonignorable-refusals.json"),"{\"actual_SDK_refusals\":"+refusals+",\"versions\":[0,1],\"fields\":[\"directory\",\"nodes\"]}\n");
        System.out.println("{\"actual_sdk\":true,\"generated_bodies\":"+total+"}");
    }
    private static void generatePublic(Path out)throws Exception {
        Files.createDirectory(out);
        for(short version=0;version<=2;version++)for(String mode:List.of("full","empty-optionals","retry-partition6","retry-controller41","top42","partition42","top31","partition31","bad-topic-count","bad-topic-name","bad-partition-count","bad-partition-index","all-malformed","all-malformed-errors")) {
            var value=success(version,mode.equals("empty-optionals"));var topic=value.topics().get(0);var partition=topic.partitions().get(0);
            if(mode.equals("retry-controller41"))value.setErrorCode((short)41);
            if(mode.equals("retry-partition6"))partition.setErrorCode((short)6);
            if(mode.equals("top42")||mode.equals("all-malformed-errors"))value.setErrorCode((short)42);
            if(mode.equals("partition42")||mode.equals("all-malformed-errors"))partition.setErrorCode((short)42);
            if(mode.equals("top31"))value.setErrorCode((short)31);if(mode.equals("partition31"))partition.setErrorCode((short)31);
            if(mode.equals("bad-topic-name")||mode.startsWith("all-malformed"))topic.setTopicName("wrong");
            if(mode.equals("bad-partition-index")||mode.startsWith("all-malformed"))partition.setPartitionIndex(1);
            if(mode.equals("bad-partition-count")||mode.startsWith("all-malformed"))topic.setPartitions(List.of(partition,partition.duplicate()));
            if(mode.equals("bad-topic-count")||mode.startsWith("all-malformed"))value.setTopics(List.of(topic,topic.duplicate()));
            byte[] body=bytes(value,version);parse("response.bin",body,version);Files.write(out.resolve("public-"+version+"-"+mode+".response"),body);
        }
        System.out.println("{\"generated_public_responses\":42}");
    }
    private static String dataInfo(DescribeQuorumResponseData value) {
        var partition=value.topics().get(0).partitions().get(0);
        java.util.function.Function<List<DescribeQuorumResponseData.ReplicaState>,String> replicas=states->states.stream().map(state->"{\"id\":"+state.replicaId()+",\"directory\":"+quote(directory(state.replicaDirectoryId()))+",\"end\":"+state.logEndOffset()+",\"fetch\":"+state.lastFetchTimestamp()+",\"caught\":"+state.lastCaughtUpTimestamp()+"}").collect(java.util.stream.Collectors.joining(",","[","]"));
        String nodes=value.nodes().stream().sorted(Comparator.comparingInt(DescribeQuorumResponseData.Node::nodeId)).map(node->"{\"id\":"+node.nodeId()+",\"listeners\":"+node.listeners().stream().map(endpoint->"{\"name\":"+quote(endpoint.name())+",\"host\":"+quote(endpoint.host())+",\"port\":"+endpoint.port()+"}").collect(java.util.stream.Collectors.joining(",","[","]"))+"}").collect(java.util.stream.Collectors.joining(",","[","]"));
        return "{\"leader\":"+partition.leaderId()+",\"epoch\":"+partition.leaderEpoch()+",\"watermark\":"+partition.highWatermark()+",\"voters\":"+replicas.apply(partition.currentVoters())+",\"observers\":"+replicas.apply(partition.observers())+",\"nodes\":"+nodes+"}";
    }
    private static void parseFrames(Path directory)throws Exception {
        int requests=0,responses=0;
        try(var files=Files.list(directory)){for(Path file:files.sorted().toList()) {
            String name=file.getFileName().toString();if(!name.endsWith(".frame"))continue;
            String[] fields=name.split("-");short api=Short.parseShort(fields[2]);short version=Short.parseShort(fields[3]);if(api!=55)continue;
            ByteBuffer buffer=ByteBuffer.wrap(Files.readAllBytes(file));var input=new ByteBufferAccessor(buffer);
            if(name.endsWith("-request.frame")) {
                var header=new RequestHeaderData(input,ApiKeys.DESCRIBE_QUORUM.requestHeaderVersion(version));
                if(header.requestApiKey()!=55||header.requestApiVersion()!=version)throw new AssertionError("request header differs");
                var request=new DescribeQuorumRequestData(input,version);if(!request.equals(singleton()))throw new AssertionError("public request selection differs");requests++;
            }else {
                var header=new ResponseHeaderData(input,ApiKeys.DESCRIBE_QUORUM.responseHeaderVersion(version));int expected=ByteBuffer.wrap(Files.readAllBytes(directory.resolve(name.replace("-response.frame","-request.frame")))).getInt(4);if(header.correlationId()!=expected)throw new AssertionError("response correlation differs");var response=new DescribeQuorumResponseData(input,version);responses++;
                if(response.errorCode()==0 && response.topics().size()==1 && response.topics().get(0).topicName().equals("__cluster_metadata") && response.topics().get(0).partitions().size()==1 && response.topics().get(0).partitions().get(0).partitionIndex()==0 && response.topics().get(0).partitions().get(0).errorCode()==0)Files.writeString(directory.resolve(name+".info.json"),dataInfo(response)+"\n");
            }
            if(buffer.hasRemaining())throw new AssertionError("trailing captured frame");
        }}
        System.out.println("{\"parsed_public_requests\":"+requests+",\"parsed_public_responses\":"+responses+"}");
    }
    private static void verify(Path candidate,Path golden)throws Exception {
        int count=0;try(var files=Files.list(golden)){for(Path reference:files.sorted().toList()){String name=reference.getFileName().toString();if(!name.endsWith(".bin"))continue;short version=Short.parseShort(name.split("-")[1]);Message expected=parse(name,Files.readAllBytes(reference),version);stripUnknown(expected);Message actual=parse(name,Files.readAllBytes(candidate.resolve(name)),version);if(!actual.equals(expected))throw new AssertionError("Rust fields differ "+name);count++;}}
        if(count!=45)throw new AssertionError("incomplete reverse corpus");System.out.println("{\"independently_parsed_Rust_bodies\":"+count+"}");
    }
    private static String quote(String value) {return "\""+value.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")+"\"";}
    private static String directory(Uuid value) {return java.util.HexFormat.of().formatHex(ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()).array());}
    private static String replicas(List<QuorumInfo.ReplicaState> values) {return values.stream().map(value->"{\"id\":"+value.replicaId()+",\"directory\":"+quote(directory(value.replicaDirectoryId()))+",\"end\":"+value.logEndOffset()+",\"fetch\":"+value.lastFetchTimestamp().orElse(-1)+",\"caught\":"+value.lastCaughtUpTimestamp().orElse(-1)+"}").collect(java.util.stream.Collectors.joining(",","[","]"));}
    private static String info(QuorumInfo value) {
        String nodes=value.nodes().values().stream().sorted(Comparator.comparingInt(QuorumInfo.Node::nodeId)).map(node->"{\"id\":"+node.nodeId()+",\"listeners\":"+node.endpoints().stream().map(endpoint->"{\"name\":"+quote(endpoint.listener())+",\"host\":"+quote(endpoint.host())+",\"port\":"+endpoint.port()+"}").collect(java.util.stream.Collectors.joining(",","[","]"))+"}").collect(java.util.stream.Collectors.joining(",","[","]"));
        return "{\"leader\":"+value.leaderId()+",\"epoch\":"+value.leaderEpoch()+",\"watermark\":"+value.highWatermark()+",\"voters\":"+replicas(value.voters())+",\"observers\":"+replicas(value.observers())+",\"nodes\":"+nodes+"}";
    }
    private static void publicCall(String address,String expected,Path out)throws Exception {
        Properties properties=new Properties();properties.put("bootstrap.servers",address);properties.put("client.id","describe-quorum-java");properties.put("request.timeout.ms","2500");properties.put("default.api.timeout.ms","5000");properties.put("retry.backoff.ms","1");properties.put("retry.backoff.max.ms","1");properties.put("reconnect.backoff.ms","1");properties.put("reconnect.backoff.max.ms","1");
        String result=null;Throwable failure=null;long started=System.nanoTime();int timeout=expected.equals("deadline")||expected.equals("retry-exhaustion")?300:1500;
        Admin admin=Admin.create(properties);
        try {if(expected.equals("deadline")||expected.equals("retry-exhaustion")){admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions().timeoutMs(1500)).quorumInfo().get(3,TimeUnit.SECONDS);started=System.nanoTime();}result=info(admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions().timeoutMs(timeout)).quorumInfo().get(3,TimeUnit.SECONDS));}
        catch(Exception error){error.printStackTrace(System.err);failure=error;while((failure instanceof java.util.concurrent.ExecutionException || failure instanceof java.util.concurrent.CompletionException) && failure.getCause()!=null)failure=failure.getCause();}
        finally {admin.close(Duration.ofSeconds(1));}
        long elapsed=System.nanoTime()-started;
        long threads=Thread.getAllStackTraces().keySet().stream().filter(thread->thread.isAlive() && thread.getName().startsWith("kafka-admin-client-thread")).count();if(threads!=0)throw new AssertionError("unjoined Admin thread");
        if(expected.equals("success") && failure!=null)throw new AssertionError("public Admin failed",failure);
        if(!expected.equals("success") && failure==null)throw new AssertionError("missing public failure");
        String receipt="{\"result\":"+(result==null?"null":result)+",\"failure\":"+(failure==null?"null":quote(failure.getClass().getSimpleName()))+",\"elapsed_ns\":"+elapsed+",\"live_admin_threads\":0}";Files.writeString(out,receipt+"\n");System.out.println(receipt);
    }
    private static void nativeRaw(String address,Path out)throws Exception {
        Files.createDirectory(out);String[] hostPort=address.split(":");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        try(Socket socket=new Socket()){socket.connect(new InetSocketAddress(hostPort[0],Integer.parseInt(hostPort[1])),2000);socket.setTcpNoDelay(true);var input=new DataInputStream(socket.getInputStream());var output=new DataOutputStream(socket.getOutputStream());
            for(short version=0;version<=2;version++) {
                var request=new DescribeQuorumRequest.Builder(singleton()).build(version);var header=new RequestHeaderData().setRequestApiKey((short)55).setRequestApiVersion(version).setCorrelationId(version+1).setClientId("quorum-java-raw");byte[] head=bytes(header,ApiKeys.DESCRIBE_QUORUM.requestHeaderVersion(version));byte[] body=bytes(request.data(),version);
                output.writeInt(head.length+body.length);output.write(head);output.write(body);output.flush();Files.write(out.resolve("native-"+version+"-request.bin"),body);
                long remaining=deadline-System.nanoTime();if(remaining<=0)throw new AssertionError("original raw deadline");socket.setSoTimeout((int)Math.max(1,TimeUnit.NANOSECONDS.toMillis(remaining)));int length=input.readInt();if(length<5 || length>1048576)throw new AssertionError("native response envelope");byte[] frame=new byte[length];input.readFully(frame);ByteBuffer buffer=ByteBuffer.wrap(frame);var accessor=new ByteBufferAccessor(buffer);var responseHeader=new ResponseHeaderData(accessor,ApiKeys.DESCRIBE_QUORUM.responseHeaderVersion(version));if(responseHeader.correlationId()!=version+1)throw new AssertionError("native correlation");byte[] responseBody=new byte[buffer.remaining()];buffer.duplicate().get(responseBody);var response=new DescribeQuorumResponseData(accessor,version);if(buffer.hasRemaining() || response.errorCode()!=0 || response.topics().size()!=1 || !response.topics().get(0).topicName().equals("__cluster_metadata") || response.topics().get(0).partitions().size()!=1)throw new AssertionError("native quorum fields");var partition=response.topics().get(0).partitions().get(0);if(partition.errorCode()!=0 || partition.partitionIndex()!=0 || partition.leaderId()!=1 || partition.highWatermark()<0 || partition.currentVoters().size()!=1 || partition.currentVoters().get(0).replicaId()!=1)throw new AssertionError("native quorum state");Files.write(out.resolve("native-"+version+"-response.bin"),responseBody);
            }
        }
        Files.writeString(out.resolve("receipt.json"),"{\"actual_native_raw_requests\":3,\"versions\":[0,1,2],\"socket_closed\":true}\n");
    }
    private static void parseNative(Path directory)throws Exception {int count=0;try(var files=Files.list(directory)){for(Path file:files.sorted().toList()){String name=file.getFileName().toString();if(!name.endsWith(".bin"))continue;short version=Short.parseShort(name.split("-")[1]);parse(name,Files.readAllBytes(file),version);count++;}}if(count!=6)throw new AssertionError("missing native body");System.out.println("{\"independently_parsed_native_bodies\":"+count+"}");}
    public static void main(String[] args)throws Exception {
        switch(args[0]) {
            case "generate" -> generate(Path.of(args[1]));
            case "generate-public" -> generatePublic(Path.of(args[1]));
            case "parse-frames" -> parseFrames(Path.of(args[1]));
            case "verify" -> verify(Path.of(args[1]),Path.of(args[2]));
            case "public" -> publicCall(args[1],args[2],Path.of(args[3]));
            case "native-raw" -> nativeRaw(args[1],Path.of(args[2]));
            case "parse-native" -> parseNative(Path.of(args[1]));
            default -> throw new IllegalArgumentException("unknown mode");
        }
    }
}
