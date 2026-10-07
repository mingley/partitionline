import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsResponse;

/** Apache-generated bodies and finite raw calls to an owned controller. */
public final class ConformanceAssignReplicasToDirsRaw {
    private ConformanceAssignReplicasToDirsRaw() { }
    private static byte[] bytes(Message value, short version) {
        ByteBuffer buffer=MessageUtil.toByteBufferAccessor(value,version).buffer();
        byte[] result=new byte[buffer.remaining()];buffer.get(result);return result;
    }
    private static Message parse(String name, byte[] body) {
        ByteBuffer buffer=ByteBuffer.wrap(body);var input=new ByteBufferAccessor(buffer);
        Message value=name.endsWith("request.bin")?new AssignReplicasToDirsRequestData(input,(short)0):new AssignReplicasToDirsResponseData(input,(short)0);
        if(buffer.hasRemaining())throw new AssertionError("trailing body "+name);return value;
    }
    private static AssignReplicasToDirsRequestData nested(int id,long epoch,Uuid directory,Uuid topic,int... partitions) {
        var parts=new ArrayList<AssignReplicasToDirsRequestData.PartitionData>();
        for(int p:partitions)parts.add(new AssignReplicasToDirsRequestData.PartitionData().setPartitionIndex(p));
        return new AssignReplicasToDirsRequestData().setBrokerId(id).setBrokerEpoch(epoch).setDirectories(List.of(
            new AssignReplicasToDirsRequestData.DirectoryData().setId(directory).setTopics(List.of(
                new AssignReplicasToDirsRequestData.TopicData().setTopicId(topic).setPartitions(parts)))));
    }
    private static void strip(Message value) {
        value.unknownTaggedFields().clear();
        if(value instanceof AssignReplicasToDirsRequestData request) {
            for(var d:request.directories()){d.unknownTaggedFields().clear();for(var t:d.topics()){t.unknownTaggedFields().clear();for(var p:t.partitions())p.unknownTaggedFields().clear();}}
        } else if(value instanceof AssignReplicasToDirsResponseData response) {
            for(var d:response.directories()){d.unknownTaggedFields().clear();for(var t:d.topics()){t.unknownTaggedFields().clear();for(var p:t.partitions())p.unknownTaggedFields().clear();}}
        }
    }
    private static void write(Path out,int index,String kind,Message value)throws Exception {
        String name="case-"+index+"-"+kind+".bin";byte[] body=bytes(value,(short)0);
        if(!parse(name,body).equals(value))throw new AssertionError("generated fields differ");
        for(int size=0;size<body.length;size++) {
            boolean rejected=false;try{parse(name,java.util.Arrays.copyOf(body,size));}catch(RuntimeException expected){rejected=true;}
            if(!rejected)throw new AssertionError("accepted truncated SDK body "+name+" prefix "+size);
        }
        Files.write(out.resolve(name),body);
    }
    private static void generate(Path out)throws Exception {
        Files.createDirectory(out);int count=0;
        if(AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST!=2250)throw new AssertionError("reference assignment hint changed");
        write(out,count++,"request",new AssignReplicasToDirsRequestData());
        for(int id:new int[]{Integer.MIN_VALUE,Integer.MAX_VALUE})for(long epoch:new long[]{Long.MIN_VALUE,Long.MAX_VALUE})
            write(out,count++,"request",new AssignReplicasToDirsRequestData().setBrokerId(id).setBrokerEpoch(epoch));
        var request=nested(1,123,new Uuid(1,2),new Uuid(3,4),Integer.MIN_VALUE,0,Integer.MAX_VALUE);
        request.setDirectories(List.of(request.directories().get(0),request.directories().get(0).duplicate()));
        write(out,count++,"request",request);
        var tagged=request.duplicate();tagged.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1,2,3}));
        for(var d:tagged.directories()){d.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{4}));for(var t:d.topics()){t.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{5}));for(var p:t.partitions())p.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{6}));}}
        write(out,count++,"request",tagged);
        for(int length:new int[]{2250,2251}) {
            int[] parts=new int[length];for(int i=0;i<length;i++)parts[i]=i;
            write(out,count++,"request",nested(1,123,Uuid.ZERO_UUID,Uuid.ONE_UUID,parts));
        }
        var original=new AssignReplicasToDirsRequest.Builder(request).build((short)0);
        for(short error:new short[]{0,31,41,77,-1}) {
            var response=error==0?new AssignReplicasToDirsResponse(new AssignReplicasToDirsResponseData().setThrottleTimeMs(17)):
                (AssignReplicasToDirsResponse)original.getErrorResponse(17,Errors.forCode(error).exception());
            if(response.data().errorCode()!=error || !response.errorCounts().equals(Map.of(Errors.forCode(error),1)) || response.throttleTimeMs()!=17 || !response.data().directories().isEmpty())throw new AssertionError("reference error factory");
            write(out,count++,"response",response.data());
        }
        for(short top:new short[]{0,41,Short.MIN_VALUE,Short.MAX_VALUE}) {
            var response=new AssignReplicasToDirsResponseData().setThrottleTimeMs(Integer.MAX_VALUE).setErrorCode(top).setDirectories(List.of(
                new AssignReplicasToDirsResponseData.DirectoryData().setId(new Uuid(1,2)).setTopics(List.of(
                    new AssignReplicasToDirsResponseData.TopicData().setTopicId(new Uuid(3,4)).setPartitions(List.of(
                        new AssignReplicasToDirsResponseData.PartitionData().setPartitionIndex(-1).setErrorCode((short)100),
                        new AssignReplicasToDirsResponseData.PartitionData().setPartitionIndex(Integer.MAX_VALUE).setErrorCode((short)6)))))));
            if(!new AssignReplicasToDirsResponse(response).errorCounts().equals(Map.of(Errors.forCode(top),1)))throw new AssertionError("nested errors incorrectly counted");
            write(out,count++,"response",response);
            var opaque=response.duplicate();opaque.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1}));
            for(var d:opaque.directories()){d.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{2}));for(var t:d.topics()){t.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{3}));for(var p:t.partitions())p.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{4}));}}
            write(out,count++,"response",opaque);
        }
        write(out,count++,"response",new AssignReplicasToDirsResponseData());
        if(java.util.Arrays.stream(Admin.class.getMethods()).anyMatch(m->m.getName().equals("assignReplicasToDirs")))throw new AssertionError("standard Java Admin surface changed");
        System.out.println("{\"actual_sdk\":true,\"generated_bodies\":"+count+",\"public_Java_Admin_method\":false}");
    }
    private static void verify(Path candidate,Path golden)throws Exception {
        int count=0;
        try(var files=Files.list(golden)){for(Path reference:files.sorted().toList()) {
            String name=reference.getFileName().toString();if(!name.endsWith(".bin"))continue;
            Message expected=parse(name,Files.readAllBytes(reference));strip(expected);
            if(!parse(name,Files.readAllBytes(candidate.resolve(name))).equals(expected))throw new AssertionError("Rust known fields differ "+name);
            count++;
        }}
        if(count!=23)throw new AssertionError("incomplete reverse corpus");
        System.out.println("{\"independently_parsed_Rust_bodies\":"+count+"}");
    }
    private static String hex(Uuid id){return java.util.HexFormat.of().formatHex(ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());}
    private static Uuid unhex(String value){ByteBuffer b=ByteBuffer.wrap(java.util.HexFormat.of().parseHex(value));return new Uuid(b.getLong(),b.getLong());}
    private static void topic(String address,String name,boolean create,Path out)throws Exception {
        var config=new Properties();config.put("bootstrap.servers",address);config.put("request.timeout.ms","2500");config.put("default.api.timeout.ms","5000");
        Admin admin=Admin.create(config);
        try {
            if(create){admin.createTopics(List.of(new NewTopic(name,1,(short)1))).all().get(5,TimeUnit.SECONDS);var info=admin.describeTopics(List.of(name)).allTopicNames().get(5,TimeUnit.SECONDS).get(name);Files.writeString(out,hex(info.topicId())+"\n");}
            else admin.deleteTopics(List.of(name)).all().get(5,TimeUnit.SECONDS);
        }finally{admin.close(Duration.ofSeconds(1));}
    }
    private static void live(String address,long epoch,Uuid directory,Uuid topic,Path out)throws Exception {
        Files.createDirectory(out);String[] hp=address.split(":");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        try(Socket socket=new Socket()) {
            socket.connect(new InetSocketAddress(hp[0],Integer.parseInt(hp[1])),2000);socket.setTcpNoDelay(true);
            var input=new DataInputStream(socket.getInputStream());var output=new DataOutputStream(socket.getOutputStream());
            for(int i=0;i<9;i++) {
                var request=switch(i){case 1->new AssignReplicasToDirsRequestData().setBrokerId(99).setBrokerEpoch(0);case 2->new AssignReplicasToDirsRequestData().setBrokerId(1).setBrokerEpoch(-1);
                    case 3->nested(1,epoch,directory,Uuid.ZERO_UUID,0,-1,Integer.MAX_VALUE);case 4->new AssignReplicasToDirsRequestData().setBrokerId(1).setBrokerEpoch(epoch).setDirectories(List.of(new AssignReplicasToDirsRequestData.DirectoryData().setId(directory)));
                    case 5->nested(1,epoch,directory,Uuid.ZERO_UUID);case 6->nested(1,epoch,directory,topic,99);case 7->nested(1,epoch,directory,topic,0);case 8->nested(1,epoch,directory,topic,0,0);default->new AssignReplicasToDirsRequestData().setBrokerId(1).setBrokerEpoch(epoch);};
                var built=new AssignReplicasToDirsRequest.Builder(request).build((short)0);
                byte[] body=bytes(built.data(),(short)0);var header=new RequestHeaderData().setRequestApiKey((short)73).setRequestApiVersion((short)0).setCorrelationId(i+1).setClientId("assign-dirs-java");byte[] head=bytes(header,ApiKeys.ASSIGN_REPLICAS_TO_DIRS.requestHeaderVersion((short)0));
                output.writeInt(head.length+body.length);output.write(head);output.write(body);output.flush();Files.write(out.resolve("live-"+i+"-request.bin"),body);
                long remaining=deadline-System.nanoTime();if(remaining<=0)throw new AssertionError("original raw operation deadline");socket.setSoTimeout((int)Math.max(1,TimeUnit.NANOSECONDS.toMillis(remaining)));
                int size=input.readInt();if(size<5 || size>65536)throw new AssertionError("response frame bound");byte[] frame=new byte[size];input.readFully(frame);ByteBuffer buffer=ByteBuffer.wrap(frame);var accessor=new ByteBufferAccessor(buffer);
                var responseHeader=new ResponseHeaderData(accessor,ApiKeys.ASSIGN_REPLICAS_TO_DIRS.responseHeaderVersion((short)0));if(responseHeader.correlationId()!=i+1)throw new AssertionError("correlation");byte[] responseBody=new byte[buffer.remaining()];buffer.duplicate().get(responseBody);var response=new AssignReplicasToDirsResponseData(accessor,(short)0);if(buffer.hasRemaining())throw new AssertionError("trailing live response");
                Files.write(out.resolve("live-"+i+"-response.bin"),responseBody);
                short expected=(short)(i==1||i==2?77:0);if(response.errorCode()!=expected)throw new AssertionError("live top-level error "+i+" "+response);
                if(expected==0) {
                    if(response.directories().size()!=request.directories().size())throw new AssertionError("directory count");
                    for(int d=0;d<request.directories().size();d++){var rd=request.directories().get(d);var sd=response.directories().get(d);if(!rd.id().equals(sd.id())||rd.topics().size()!=sd.topics().size())throw new AssertionError("directory identity");
                        for(int t=0;t<rd.topics().size();t++){var rt=rd.topics().get(t);var st=sd.topics().get(t);if(!rt.topicId().equals(st.topicId())||rt.partitions().size()!=st.partitions().size())throw new AssertionError("topic identity");
                            for(int p=0;p<rt.partitions().size();p++)if(rt.partitions().get(p).partitionIndex()!=st.partitions().get(p).partitionIndex()||st.partitions().get(p).errorCode()!=(i==3?100:i==6?3:0))throw new AssertionError("partition outcome");}}
                }else if(!response.directories().isEmpty())throw new AssertionError("error response copied directories");
            }
        }
        Files.writeString(out.resolve("receipt.json"),"{\"actual_controller_requests\":9,\"socket_closed\":true,\"directory_assignment_scope\":\"owned online directory; no offline-directory movement or failover\"}\n");
    }
    private static void parseLive(Path out)throws Exception {int count=0;try(var files=Files.list(out)){for(Path file:files.sorted().toList())if(file.getFileName().toString().endsWith(".bin")){parse(file.getFileName().toString(),Files.readAllBytes(file));count++;}}if(count!=18)throw new AssertionError("missing live bodies");System.out.println("{\"independently_parsed_live_bodies\":18}");}
    public static void main(String[] args)throws Exception {
        switch(args[0]) {case "generate"->generate(Path.of(args[1]));case "verify"->verify(Path.of(args[1]),Path.of(args[2]));case "live"->live(args[1],Long.parseLong(args[2]),unhex(args[3]),unhex(args[4]),Path.of(args[5]));case "parse-live"->parseLive(Path.of(args[1]));case "create-topic"->topic(args[1],args[2],true,Path.of(args[3]));case "delete-topic"->topic(args[1],args[2],false,Path.of(args[3]));default->throw new IllegalArgumentException("mode");}
    }
}
