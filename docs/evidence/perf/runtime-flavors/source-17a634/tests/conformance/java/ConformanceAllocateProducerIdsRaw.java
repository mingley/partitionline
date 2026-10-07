import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;

/** Authentic generated bodies and bounded raw controller calls; no Java Admin API. */
public final class ConformanceAllocateProducerIdsRaw {
    private ConformanceAllocateProducerIdsRaw() { }
    private static byte[] bytes(Message message) {return bytes(message,(short)0);}
    private static byte[] bytes(Message message,short version) {
        ByteBuffer buffer=MessageUtil.toByteBufferAccessor(message,version).buffer();byte[] result=new byte[buffer.remaining()];buffer.get(result);return result;
    }
    private static Message parse(String name,byte[] bytes) {
        ByteBuffer buffer=ByteBuffer.wrap(bytes);var input=new ByteBufferAccessor(buffer);
        Message value=name.endsWith("request.bin")?new AllocateProducerIdsRequestData(input,(short)0):new AllocateProducerIdsResponseData(input,(short)0);
        if(buffer.hasRemaining())throw new AssertionError("trailing body "+name);return value;
    }
    private static void truncations(String name,byte[] body) {
        for(int size=0;size<body.length;size++) {
            boolean rejected=false;
            try {parse(name,java.util.Arrays.copyOf(body,size));}catch(RuntimeException expected){rejected=true;}
            if(!rejected)throw new AssertionError("accepted truncated SDK body "+name+" prefix"+size);
        }
    }
    private static void generate(Path out)throws Exception {
        Files.createDirectory(out);int n=0;
        for(int id:new int[]{0,2,Integer.MIN_VALUE,Integer.MAX_VALUE})for(long epoch:new long[]{0,123,Long.MIN_VALUE,Long.MAX_VALUE}) {
            var request=new AllocateProducerIdsRequest.Builder(new AllocateProducerIdsRequestData().setBrokerId(id).setBrokerEpoch(epoch)).build((short)0);
            byte[] body=bytes(request.data());if(!parse("request.bin",body).equals(request.data()))throw new AssertionError("SDK request parse");
            truncations("request.bin",body);Files.write(out.resolve("case-"+(n++)+"-request.bin"),body);
        }
        var original=new AllocateProducerIdsRequest.Builder(new AllocateProducerIdsRequestData().setBrokerId(2).setBrokerEpoch(123)).build((short)0);
        for(short error:new short[]{0,41,77,31,-1}) {
            AllocateProducerIdsResponse response=error==0?new AllocateProducerIdsResponse(new AllocateProducerIdsResponseData().setThrottleTimeMs(123).setErrorCode(error).setProducerIdStart(345).setProducerIdLen(234)):(AllocateProducerIdsResponse)original.getErrorResponse(17,Errors.forCode(error).exception());
            if(response.data().errorCode()!=error || !response.errorCounts().equals(Map.of(Errors.forCode(error),1)))throw new AssertionError("SDK error mapping");
            byte[] body=bytes(response.data());truncations("response.bin",body);Files.write(out.resolve("case-"+(n++)+"-response.bin"),body);
        }
        for(long start:new long[]{Long.MIN_VALUE,Long.MAX_VALUE})for(int length:new int[]{Integer.MIN_VALUE,Integer.MAX_VALUE}) {
            var response=new AllocateProducerIdsResponseData().setThrottleTimeMs(Integer.MAX_VALUE).setProducerIdStart(start).setProducerIdLen(length);
            Files.write(out.resolve("case-"+(n++)+"-response.bin"),bytes(response));
        }
        Files.write(out.resolve("case-"+(n++)+"-response.bin"),bytes(new AllocateProducerIdsResponseData()));
        var opaque=new AllocateProducerIdsRequestData().setBrokerId(2).setBrokerEpoch(123);
        opaque.unknownTaggedFields().add(new RawTaggedField(9,new byte[]{1,2,3}));
        byte[] tagged=bytes(opaque);truncations("request.bin",tagged);
        if(!parse("request.bin",tagged).equals(opaque))throw new AssertionError("SDK opaque tags");
        Files.write(out.resolve("opaque-request.tagged"),tagged);
        if(java.util.Arrays.stream(org.apache.kafka.clients.admin.Admin.class.getMethods()).anyMatch(method->method.getName().equals("allocateProducerIds")))throw new AssertionError("reference public surface changed");
        System.out.println("{\"actual_sdk\":true,\"generated_bodies\":"+n+"}");
    }
    private static void verify(Path candidate,Path golden)throws Exception {
        int n=0;try(var files=Files.list(golden)) {for(Path reference:files.sorted().toList()) {String name=reference.getFileName().toString();if(!name.endsWith(".bin"))continue;
            if(!parse(name,Files.readAllBytes(reference)).equals(parse(name,Files.readAllBytes(candidate.resolve(name)))))throw new AssertionError("Rust fields differ: "+name);n++;}}
        var opaque=(AllocateProducerIdsRequestData)parse("request.bin",Files.readAllBytes(golden.resolve("opaque-request.tagged")));
        opaque.unknownTaggedFields().clear();
        if(!parse("request.bin",Files.readAllBytes(candidate.resolve("opaque-request.normalized"))).equals(opaque))throw new AssertionError("Rust skipped opaque fields incorrectly");
        if(n!=26)throw new AssertionError("missing body");System.out.println("{\"independently_parsed_Rust_bodies\":"+(n+1)+"}");
    }
    private static void live(String address,long epoch,Path out)throws Exception {
        Files.createDirectory(out);String[] hostPort=address.split(":");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);long end=-1;int success=0;
        try(Socket socket=new Socket()) {
            socket.connect(new InetSocketAddress(hostPort[0],Integer.parseInt(hostPort[1])),2000);socket.setTcpNoDelay(true);
            var input=new DataInputStream(socket.getInputStream());var output=new DataOutputStream(socket.getOutputStream());
            for(int i=0;i<5;i++) {
                int id=i==3?99:1;long requestedEpoch=i==4?-1:i==3?0:epoch;
                var request=new AllocateProducerIdsRequest.Builder(new AllocateProducerIdsRequestData().setBrokerId(id).setBrokerEpoch(requestedEpoch)).build((short)0);
                var header=new RequestHeaderData().setRequestApiKey((short)67).setRequestApiVersion((short)0).setCorrelationId(i+1).setClientId("allocate-ids-java");
                byte[] head=bytes(header,ApiKeys.ALLOCATE_PRODUCER_IDS.requestHeaderVersion((short)0));byte[] body=bytes(request.data());
                output.writeInt(head.length+body.length);output.write(head);output.write(body);output.flush();Files.write(out.resolve("live-"+i+"-request.bin"),body);
                long remaining=deadline-System.nanoTime();if(remaining<=0)throw new AssertionError("original operation deadline");socket.setSoTimeout((int)Math.max(1,TimeUnit.NANOSECONDS.toMillis(remaining)));
                int size=input.readInt();if(size<5 || size>65536)throw new AssertionError("response frame bound");byte[] frame=new byte[size];input.readFully(frame);
                ByteBuffer buffer=ByteBuffer.wrap(frame);var accessor=new ByteBufferAccessor(buffer);var responseHeader=new ResponseHeaderData(accessor,ApiKeys.ALLOCATE_PRODUCER_IDS.responseHeaderVersion((short)0));
                if(responseHeader.correlationId()!=i+1)throw new AssertionError("correlation");byte[] responseBody=new byte[buffer.remaining()];buffer.duplicate().get(responseBody);
                var response=new AllocateProducerIdsResponseData(accessor,(short)0);if(buffer.hasRemaining())throw new AssertionError("trailing live response");Files.write(out.resolve("live-"+i+"-response.bin"),responseBody);
                if(i<3) {if(response.errorCode()!=0 || response.producerIdLen()!=1000 || response.producerIdStart()<end)throw new AssertionError("live block fields "+response);end=response.producerIdStart()+response.producerIdLen();success++;}
                else if(response.errorCode()!=77 || response.producerIdStart()!=0 || response.producerIdLen()!=0)throw new AssertionError("live stale/unknown epoch "+response);
            }
        }
        Files.writeString(out.resolve("receipt.json"),"{\"actual_controller_requests\":5,\"successful_blocks\":"+success+",\"stale_epoch_errors\":2,\"socket_closed\":true}\n");
    }
    private static void parseLive(Path out)throws Exception {
        int n=0;try(var files=Files.list(out)) {for(Path file:files.sorted().toList()) {if(file.getFileName().toString().endsWith(".bin")){parse(file.getFileName().toString(),Files.readAllBytes(file));n++;}}}
        if(n!=10)throw new AssertionError("missing live bodies");System.out.println("{\"independently_parsed_live_bodies\":10}");
    }
    public static void main(String[] args)throws Exception {
        switch(args[0]) {case "generate" -> generate(Path.of(args[1]));case "verify" -> verify(Path.of(args[1]),Path.of(args[2]));case "live" -> live(args[1],Long.parseLong(args[2]),Path.of(args[3]));case "parse-live" -> parseLive(Path.of(args[1]));default -> throw new IllegalArgumentException("mode");}
    }
}
