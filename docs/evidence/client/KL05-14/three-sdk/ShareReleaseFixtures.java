import java.lang.reflect.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.*;

public class ShareReleaseFixtures {
    static Path out;
    static final Uuid TOPIC = new Uuid(0x0001020304050607L, 0x08090a0b0c0d0e0fL);
    static Object create(String name) throws Exception {
        return Class.forName("org.apache.kafka.common.message." + name).getConstructor().newInstance();
    }
    static Object set(Object instance, String name, Object value) throws Exception {
        Method method = Arrays.stream(instance.getClass().getMethods()).filter(m -> m.getName().equals("set" + name) && m.getParameterCount() == 1).findFirst().orElseThrow(() -> new NoSuchMethodException(name));
        Class<?> target = method.getParameterTypes()[0];
        if (value instanceof List<?> values && !target.isInstance(value)) value = target.getConstructor(Iterator.class).newInstance(values.iterator());
        method.invoke(instance, value);
        return instance;
    }
    static Object fetch() throws Exception {
        Object r = create("ShareFetchRequestData");
        set(r,"GroupId","group"); set(r,"MemberId","AAECAwQFBgcICQoLDA0ODw"); set(r,"ShareSessionEpoch",7);
        set(r,"MaxWaitMs",99); set(r,"MinBytes",1); set(r,"MaxBytes",4096); set(r,"MaxRecords",2); set(r,"BatchSize",1);
        return r;
    }
    static Object ack() throws Exception {
        Object r=create("ShareAcknowledgeRequestData"); set(r,"GroupId","group"); set(r,"MemberId","AAECAwQFBgcICQoLDA0ODw"); set(r,"ShareSessionEpoch",8); return r;
    }
    static Object renew(boolean fetch) throws Exception {
        String base=fetch ? "ShareFetchRequestData" : "ShareAcknowledgeRequestData";
        Object r=fetch ? fetch() : ack(); set(r,"IsRenewAck",true);
        Object b=create(base+"$AcknowledgementBatch"); set(b,"FirstOffset",10L); set(b,"LastOffset",11L); set(b,"AcknowledgeTypes",List.of((byte)4));
        Object p=create(base+(fetch ? "$FetchPartition" : "$AcknowledgePartition")); set(p,"PartitionIndex",3); set(p,"AcknowledgementBatches",List.of(b));
        Object t=create(base+(fetch ? "$FetchTopic" : "$AcknowledgeTopic")); set(t,"TopicId",TOPIC); set(t,"Partitions",List.of(p)); set(r,"Topics",List.of(t)); return r;
    }
    static Object records() throws Exception {
        String prefix="org.apache.kafka.common.record.";
        try { Class.forName(prefix+"MemoryRecords"); } catch(ClassNotFoundException e) { prefix += "internal."; }
        Class<?> simple=Class.forName(prefix+"SimpleRecord"); Object list=Array.newInstance(simple,4);
        for(int i=0;i<4;i++) Array.set(list,i,simple.getConstructor(long.class,byte[].class,byte[].class).newInstance(500L+i,("k"+i).getBytes(),("v"+i).getBytes()));
        Class<?> compression=Class.forName("org.apache.kafka.common.compress.Compression"); Object none=compression.getField("NONE").get(null);
        return Class.forName(prefix+"MemoryRecords").getMethod("withRecords",compression,list.getClass()).invoke(null,none,list);
    }
    static Object fetched() throws Exception {
        Object range=create("ShareFetchResponseData$AcquiredRecords"); set(range,"FirstOffset",1L); set(range,"LastOffset",2L); set(range,"DeliveryCount",(short)3);
        Object p=create("ShareFetchResponseData$PartitionData"); set(p,"PartitionIndex",3); set(p,"Records",records()); set(p,"AcquiredRecords",List.of(range));
        Object t=create("ShareFetchResponseData$ShareFetchableTopicResponse"); set(t,"TopicId",TOPIC); set(t,"Partitions",List.of(p));
        Object r=create("ShareFetchResponseData"); set(r,"AcquisitionLockTimeoutMs",1200); set(r,"Responses",List.of(t)); return r;
    }
    static Object partial() throws Exception {
        Object p=create("ShareAcknowledgeResponseData$PartitionData"); set(p,"PartitionIndex",3);
        Object q=create("ShareAcknowledgeResponseData$PartitionData"); set(q,"PartitionIndex",4); set(q,"ErrorCode",(short)121); set(q,"ErrorMessage","expired");
        Object leader=create("ShareAcknowledgeResponseData$LeaderIdAndEpoch"); set(leader,"LeaderId",5); set(leader,"LeaderEpoch",8); set(q,"CurrentLeader",leader);
        Object t=create("ShareAcknowledgeResponseData$ShareAcknowledgeTopicResponse"); set(t,"TopicId",TOPIC); set(t,"Partitions",List.of(p,q));
        Object endpoint=create("ShareAcknowledgeResponseData$NodeEndpoint"); set(endpoint,"NodeId",5); set(endpoint,"Host","localhost"); set(endpoint,"Port",9092);
        Object r=create("ShareAcknowledgeResponseData"); set(r,"ThrottleTimeMs",9); set(r,"Responses",List.of(t)); set(r,"NodeEndpoints",List.of(endpoint));
        try { set(r,"AcquisitionLockTimeoutMs",1200); } catch(NoSuchMethodException absent) { System.out.println("ABSENT_FIELD ShareAcknowledgeResponseData AcquisitionLockTimeoutMs"); }
        return r;
    }
    interface Build { Object get() throws Exception; }
    static void save(String name,short version,Build build) throws Exception {
        try {
            Message m=(Message)build.get(); ObjectSerializationCache cache=new ObjectSerializationCache(); ByteBuffer bytes=ByteBuffer.allocate(m.size(cache,version)); m.write(new ByteBufferAccessor(bytes),cache,version); if(bytes.hasRemaining())throw new AssertionError();
            Files.write(out.resolve(name+".bin"),bytes.array());
            String classification = version < m.lowestSupportedVersion() || version > m.highestSupportedVersion() ? "RAW_OUTSIDE_ADVERTISED_RANGE" : "SUPPORTED";
            System.out.println(classification+" "+name+" v"+version+" "+HexFormat.of().formatHex(bytes.array()));
        } catch(NoSuchMethodException absent) { System.out.println("UNSUPPORTED_FIELD "+name+" "+absent.getMessage()); }
        catch(org.apache.kafka.common.errors.UnsupportedVersionException rejected) { System.out.println("UNSUPPORTED_VERSION "+name+" "+rejected.getMessage()); }
    }
    public static void main(String[] args) throws Exception {
        out=Path.of(args[0]); Files.createDirectories(out);
        Class<?> info=Class.forName("org.apache.kafka.common.utils.AppInfoParser"); System.out.println("SDK "+info.getMethod("getVersion").invoke(null)+" "+info.getMethod("getCommitId").invoke(null));
        for(String n:List.of("ShareFetchRequestData","ShareFetchResponseData","ShareAcknowledgeRequestData","ShareAcknowledgeResponseData")) { Message m=(Message)create(n); System.out.println("RANGE "+n+" "+m.lowestSupportedVersion()+" "+m.highestSupportedVersion()); }
        for(short v:new short[]{1,2}) {
            save("fetch_v"+v+"_defaults_request",v,ShareReleaseFixtures::fetch); save("ack_v"+v+"_defaults_request",v,ShareReleaseFixtures::ack);
            save("fetch_v"+v+"_acquired_subset_response",v,ShareReleaseFixtures::fetched); save("ack_v"+v+"_partial_response",v,ShareReleaseFixtures::partial);
        }
        save("fetch_v2_record_limit_request",(short)2,()->set(fetch(),"ShareAcquireMode",(byte)1));
        save("fetch_v2_renew_request",(short)2,()->renew(true)); save("ack_v2_renew_request",(short)2,()->renew(false));
        save("fetch_v2_session_error_response",(short)2,()->{Object r=create("ShareFetchResponseData");set(r,"ThrottleTimeMs",13);set(r,"ErrorCode",(short)122);set(r,"ErrorMessage","session lost");return r;});
        save("ack_v2_session_error_response",(short)2,()->{Object r=create("ShareAcknowledgeResponseData");set(r,"ThrottleTimeMs",11);set(r,"ErrorCode",(short)122);return r;});
        save("fetch_v1_ignorable_record_limit_request",(short)1,()->set(fetch(),"ShareAcquireMode",(byte)1));
        save("fetch_v1_renew_rejection",(short)1,()->renew(true)); save("ack_v1_renew_rejection",(short)1,()->renew(false));
    }
}
