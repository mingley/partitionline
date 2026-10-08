import java.nio.*;
import java.nio.file.*;
import java.util.*;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

public class ShareV2Fixtures {
 static Path out;
 static final Uuid TOPIC=new Uuid(0x0001020304050607L,0x08090a0b0c0d0e0fL);
 static byte[] bytes(Message m,short version){ObjectSerializationCache c=new ObjectSerializationCache();ByteBuffer b=ByteBuffer.allocate(m.size(c,version));m.write(new ByteBufferAccessor(b),c,version);if(b.hasRemaining())throw new AssertionError();return b.array();}
 static void save(String name,Message m,short version)throws Exception{byte[] b=bytes(m,version);Files.write(out.resolve(name+".bin"),b);System.out.println(name+" v"+version+" "+HexFormat.of().formatHex(b));}
 static ShareFetchRequestData fetch(){return new ShareFetchRequestData().setGroupId("group").setMemberId("AAECAwQFBgcICQoLDA0ODw").setShareSessionEpoch(7).setMaxWaitMs(99).setMinBytes(1).setMaxBytes(4096).setMaxRecords(2).setBatchSize(1);}
 static ShareFetchRequestData fetchRenew(){ShareFetchRequestData r=fetch().setIsRenewAck(true);ShareFetchRequestData.FetchTopic t=new ShareFetchRequestData.FetchTopic().setTopicId(TOPIC).setPartitions(new ShareFetchRequestData.FetchPartitionCollection(List.of(new ShareFetchRequestData.FetchPartition().setPartitionIndex(3).setAcknowledgementBatches(List.of(new ShareFetchRequestData.AcknowledgementBatch().setFirstOffset(10).setLastOffset(11).setAcknowledgeTypes(List.of((byte)4))))).iterator()));return r.setTopics(new ShareFetchRequestData.FetchTopicCollection(List.of(t).iterator()));}
 static ShareAcknowledgeRequestData ack(){return new ShareAcknowledgeRequestData().setGroupId("group").setMemberId("AAECAwQFBgcICQoLDA0ODw").setShareSessionEpoch(8);}
 static ShareAcknowledgeRequestData ackRenew(){return ack().setIsRenewAck(true).setTopics(new ShareAcknowledgeRequestData.AcknowledgeTopicCollection(List.of(new ShareAcknowledgeRequestData.AcknowledgeTopic().setTopicId(TOPIC).setPartitions(new ShareAcknowledgeRequestData.AcknowledgePartitionCollection(List.of(new ShareAcknowledgeRequestData.AcknowledgePartition().setPartitionIndex(3).setAcknowledgementBatches(List.of(new ShareAcknowledgeRequestData.AcknowledgementBatch().setFirstOffset(10).setLastOffset(11).setAcknowledgeTypes(List.of((byte)4))))).iterator()))).iterator()));}
 public static void main(String[]args)throws Exception{
  out=Path.of(args[0]);Files.createDirectories(out);
  save("fetch_v1_defaults_request",fetch(),(short)1);save("fetch_v2_defaults_request",fetch(),(short)2);save("fetch_v2_record_limit_request",fetch().setShareAcquireMode((byte)1),(short)2);save("fetch_v2_renew_request",fetchRenew(),(short)2);
  save("ack_v1_defaults_request",ack(),(short)1);save("ack_v2_defaults_request",ack(),(short)2);save("ack_v2_renew_request",ackRenew(),(short)2);
  ShareFetchResponseData.PartitionData part=new ShareFetchResponseData.PartitionData().setPartitionIndex(3).setRecords(MemoryRecords.withRecords(Compression.NONE,new SimpleRecord(500L,"k0".getBytes(),"v0".getBytes()),new SimpleRecord(501L,"k1".getBytes(),"v1".getBytes()),new SimpleRecord(502L,"k2".getBytes(),"v2".getBytes()),new SimpleRecord(503L,"k3".getBytes(),"v3".getBytes()))).setAcquiredRecords(List.of(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(1).setLastOffset(2).setDeliveryCount((short)3)));
  ShareFetchResponseData response=new ShareFetchResponseData().setAcquisitionLockTimeoutMs(1200).setResponses(new ShareFetchResponseData.ShareFetchableTopicResponseCollection(List.of(new ShareFetchResponseData.ShareFetchableTopicResponse().setTopicId(TOPIC).setPartitions(List.of(part))).iterator()));
  save("fetch_v1_acquired_subset_response",response,(short)1);save("fetch_v2_acquired_subset_response",response,(short)2);
  save("fetch_v2_session_error_response",new ShareFetchResponseData().setThrottleTimeMs(13).setErrorCode((short)122).setErrorMessage("session lost"),(short)2);
  ShareAcknowledgeResponseData ackResponse=new ShareAcknowledgeResponseData().setAcquisitionLockTimeoutMs(1200).setThrottleTimeMs(9).setResponses(new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponseCollection(List.of(new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse().setTopicId(TOPIC).setPartitions(List.of(new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(3),new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(4).setErrorCode((short)121).setErrorMessage("expired").setCurrentLeader(new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(5).setLeaderEpoch(8))))).iterator())).setNodeEndpoints(new ShareAcknowledgeResponseData.NodeEndpointCollection(List.of(new ShareAcknowledgeResponseData.NodeEndpoint().setNodeId(5).setHost("localhost").setPort(9092)).iterator()));
  save("ack_v1_partial_response",ackResponse,(short)1);save("ack_v2_partial_response",ackResponse,(short)2);
  save("ack_v2_session_error_response",new ShareAcknowledgeResponseData().setThrottleTimeMs(11).setErrorCode((short)122),(short)2);
  for(Message m:List.of(fetchRenew(),ackRenew())){try{bytes(m,(short)1);throw new AssertionError("renew silently downgraded");}catch(org.apache.kafka.common.errors.UnsupportedVersionException ok){System.out.println("EXPECTED_REJECTION "+m.getClass().getSimpleName()+" "+ok.getMessage());}}
  save("fetch_v1_ignorable_record_limit_request",fetch().setShareAcquireMode((byte)1),(short)1);
 }
}
