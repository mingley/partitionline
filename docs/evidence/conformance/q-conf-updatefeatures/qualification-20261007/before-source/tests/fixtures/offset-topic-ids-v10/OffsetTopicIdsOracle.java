import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

/** Genuine selected schemas and builder identity policy. */
public final class OffsetTopicIdsOracle {
    private OffsetTopicIdsOracle() { }
    private static byte[] bytes(Message message,short version) {
        ByteBuffer buffer=MessageUtil.toByteBufferAccessor(message,version).buffer();
        byte[] out=new byte[buffer.remaining()];buffer.get(out);return out;
    }
    private static OffsetCommitRequestData commit(short version,String mode) {
        var out=new OffsetCommitRequestData().setGroupId("group-κ").setGenerationIdOrMemberEpoch(7)
            .setMemberId("member-λ").setGroupInstanceId(mode.equals("null-empty")?"":null);
        List<OffsetCommitRequestData.OffsetCommitRequestTopic> topics=new ArrayList<>();
        if(!mode.equals("empty")) for(int i=0;i<2;i++) {
            var topic=new OffsetCommitRequestData.OffsetCommitRequestTopic();
            if(version==10)topic.setTopicId(new Uuid(i+1,i+17));else topic.setName("topic-κ-"+i);
            topic.setPartitions(List.of(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                .setPartitionIndex(i).setCommittedOffset(i==0?Long.MAX_VALUE:-1).setCommittedLeaderEpoch(i==0?Integer.MAX_VALUE:-1)
                .setCommittedMetadata(mode.equals("null-empty")?(i==0?null:""):"metadata-λ")));
            topics.add(topic);
        }
        return out.setTopics(topics);
    }
    private static OffsetCommitResponseData committed(short version,String mode) {
        var out=new OffsetCommitResponseData().setThrottleTimeMs(42);List<OffsetCommitResponseData.OffsetCommitResponseTopic> topics=new ArrayList<>();
        if(!mode.equals("empty"))for(int i=1;i>=0;i--) {
            var topic=new OffsetCommitResponseData.OffsetCommitResponseTopic();if(version==10)topic.setTopicId(new Uuid(i+1,i+17));else topic.setName("topic-κ-"+i);
            topic.setPartitions(List.of(new OffsetCommitResponseData.OffsetCommitResponsePartition().setPartitionIndex(i)
                .setErrorCode((short)(mode.equals("errors")?(i==0?100:16):0))));topics.add(topic);
        }
        return out.setTopics(topics);
    }
    private static OffsetFetchRequestData fetch(short version,String mode) {
        var out=new OffsetFetchRequestData().setRequireStable(true);List<OffsetFetchRequestData.OffsetFetchRequestGroup> groups=new ArrayList<>();
        for(int g=0;g<2;g++) {
            var group=new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("group-κ-"+g);
            if(version>=9)group.setMemberId(g==0?null:"").setMemberEpoch(g==0?-1:7);
            List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics=new ArrayList<>();
            if(!mode.equals("empty"))for(int i=0;i<2;i++) {
                var topic=new OffsetFetchRequestData.OffsetFetchRequestTopics();if(version==10)topic.setTopicId(new Uuid(i+1,i+17));else topic.setName("topic-κ-"+i);
                topic.setPartitionIndexes(List.of(i,Integer.MAX_VALUE));topics.add(topic);
            }
            group.setTopics(mode.equals("null-topics")?null:topics);groups.add(group);
        }
        return out.setGroups(groups);
    }
    private static OffsetFetchResponseData fetched(short version,String mode) {
        var out=new OffsetFetchResponseData().setThrottleTimeMs(42);List<OffsetFetchResponseData.OffsetFetchResponseGroup> groups=new ArrayList<>();
        for(int g=1;g>=0;g--) {
            var group=new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("group-κ-"+g).setErrorCode((short)(mode.equals("errors") && g==1?69:0));
            List<OffsetFetchResponseData.OffsetFetchResponseTopics> topics=new ArrayList<>();
            if(!mode.equals("empty"))for(int i=1;i>=0;i--) {
                var topic=new OffsetFetchResponseData.OffsetFetchResponseTopics();if(version==10)topic.setTopicId(new Uuid(i+1,i+17));else topic.setName("topic-κ-"+i);
                topic.setPartitions(List.of(new OffsetFetchResponseData.OffsetFetchResponsePartitions().setPartitionIndex(i)
                    .setCommittedOffset(i==0?Long.MAX_VALUE:-1).setCommittedLeaderEpoch(i==0?Integer.MAX_VALUE:-1)
                    .setMetadata(mode.equals("null-empty")?(i==0?null:""):"metadata-λ").setErrorCode((short)(mode.equals("errors") && i==0?100:0))));topics.add(topic);
            }
            group.setTopics(topics);groups.add(group);
        }
        return out.setGroups(groups);
    }
    private static OffsetCommitRequest.Builder commitBuilder(OffsetCommitRequestData data)throws Exception {
        try { return (OffsetCommitRequest.Builder)OffsetCommitRequest.Builder.class.getMethod("forTopicIdsOrNames",OffsetCommitRequestData.class,boolean.class).invoke(null,data,true); }
        catch(NoSuchMethodException stable) { return (OffsetCommitRequest.Builder)OffsetCommitRequest.Builder.class.getMethod("forTopicIdsOrNames",OffsetCommitRequestData.class).invoke(null,data); }
    }
    private static OffsetFetchRequest.Builder fetchBuilder(OffsetFetchRequestData data)throws Exception {
        try { return (OffsetFetchRequest.Builder)OffsetFetchRequest.Builder.class.getMethod("forTopicIdsOrNames",OffsetFetchRequestData.class,boolean.class,boolean.class).invoke(null,data,false,true); }
        catch(NoSuchMethodException stable) { return (OffsetFetchRequest.Builder)OffsetFetchRequest.Builder.class.getMethod("forTopicIdsOrNames",OffsetFetchRequestData.class,boolean.class).invoke(null,data,false); }
    }
    private static MetadataRequestData metadataRequest(String mode) {
        var out=new MetadataRequestData().setAllowAutoTopicCreation(false);
        if(mode.equals("all"))return out.setTopics(null);
        return out.setTopics(mode.equals("empty")?List.of():List.of(new MetadataRequestData.MetadataRequestTopic().setName("topic")));
    }
    private static MetadataResponseData metadataResponse(String mode) {
        var topic=new MetadataResponseData.MetadataResponseTopic().setName("topic")
            .setTopicId(new Uuid(0x0101010101010101L,0x0101010101010101L))
            .setTopicAuthorizedOperations(Integer.MIN_VALUE)
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition().setPartitionIndex(0)
                .setLeaderId(7).setLeaderEpoch(4).setReplicaNodes(List.of(7)).setIsrNodes(List.of(7))));
        return new MetadataResponseData().setClusterId("cluster").setControllerId(7)
            .setClusterAuthorizedOperations(Integer.MIN_VALUE)
            .setBrokers(new MetadataResponseData.MetadataResponseBrokerCollection(List.of(new MetadataResponseData.MetadataResponseBroker().setNodeId(7).setHost("127.0.0.1").setPort(9092)).iterator()))
            .setTopics(new MetadataResponseData.MetadataResponseTopicCollection((mode.equals("empty")?List.<MetadataResponseData.MetadataResponseTopic>of():List.of(topic)).iterator()));
    }
    private static void runtimeSeed(Path out,short version)throws Exception {
        Files.createDirectory(out);
        var identity=new Uuid(0x0101010101010101L,0x0101010101010101L);
        for(short code:new short[]{0,14,15,16,100}) {
            var ct=new OffsetCommitResponseData.OffsetCommitResponseTopic();
            var ft=new OffsetFetchResponseData.OffsetFetchResponseTopics();
            if(version==10) {ct.setTopicId(identity);ft.setTopicId(identity);}else {ct.setName("topic");ft.setName("topic");}
            ct.setPartitions(List.of(new OffsetCommitResponseData.OffsetCommitResponsePartition().setPartitionIndex(0).setErrorCode(code)));
            ft.setPartitions(List.of(new OffsetFetchResponseData.OffsetFetchResponsePartitions().setPartitionIndex(0).setCommittedOffset(123).setCommittedLeaderEpoch(4).setMetadata("").setErrorCode(code)));
            var cr=new OffsetCommitResponseData().setTopics(List.of(ct));
            var fr=new OffsetFetchResponseData().setGroups(List.of(new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("group").setTopics(List.of(ft))));
            Files.write(out.resolve("commit-"+code+".bin"),bytes(cr,version));Files.write(out.resolve("fetch-"+code+".bin"),bytes(fr,version));
            var batch=new OffsetFetchResponseData().setGroups(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("other").setTopics(List.of(ft)),
                new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("group").setTopics(List.of(ft))));
            Files.write(out.resolve("fetch-batch-"+code+".bin"),bytes(batch,version));
        }
        for(short v:new short[]{10,11,12,13})for(String mode:List.of("full","empty","all")) {
            Files.write(out.resolve("metadata-v"+v+"-"+mode+".request.bin"),bytes(metadataRequest(mode),v));
            Files.write(out.resolve("metadata-v"+v+"-"+mode+".response.bin"),bytes(metadataResponse(mode),v));
        }
        System.out.println("{\"runtime_offset_bodies\":15,\"runtime_offset_version\":"+version+",\"metadata_bodies\":24}");
    }
    private static void parseRuntime(Path directory,String driver)throws Exception {
        int commits=0,fetches=0,metadata=0,responses=0;
        try(var files=Files.list(directory)) {
            for(Path path:files.sorted().toList()) {
                String[] name=path.getFileName().toString().split("-");
                if(name.length!=4 || !name[3].endsWith(".frame"))continue;
                short api=Short.parseShort(name[1]),version=Short.parseShort(name[2]);
                if(api!=8 && api!=9 && api!=3 && api!=10)continue;
                ByteBuffer raw=ByteBuffer.wrap(Files.readAllBytes(path));
                if(raw.getInt()!=raw.remaining())throw new AssertionError("frame length differs");
                boolean request=name[0].equals("request");
                if(request) {
                    var header=RequestHeader.parse(raw);
                    if(header.apiKey().id!=api || header.apiVersion()!=version)throw new AssertionError("request header differs");
                } else ResponseHeader.parse(raw,(short)(api==10 && version<3?0:1));
                var reader=new ByteBufferAccessor(raw);
                if(request && api==8) {
                    var data=new OffsetCommitRequestData(reader,version);
                    int epoch=(driver.equals("rust-group") || driver.equals("rust-typed"))?7:-1;
                    String member=epoch==7?"member":"";
                    if(!data.groupId().equals("group") || data.generationIdOrMemberEpoch()!=epoch || !data.memberId().equals(member)
                        || data.topics().size()!=1)throw new AssertionError("actual commit membership differs");
                    var topic=data.topics().get(0);if(version==10?!topic.topicId().equals(new Uuid(0x0101010101010101L,0x0101010101010101L)):!topic.name().equals("topic"))throw new AssertionError("actual commit identity differs");
                    if(topic.partitions().size()!=1)throw new AssertionError("actual commit partitions differ");
                    var partition=topic.partitions().get(0);
                    String expectedMetadata=driver.equals("rust-typed") || version<10 && driver.startsWith("rust")?null:"";
                    if(partition.partitionIndex()!=0 || partition.committedOffset()!=123 || partition.committedLeaderEpoch()!=4
                        || !java.util.Objects.equals(partition.committedMetadata(),expectedMetadata))throw new AssertionError("actual commit fields differ: "+partition);
                    commits++;
                } else if(request && api==9) {
                    var data=new OffsetFetchRequestData(reader,version);
                    boolean batch=driver.endsWith("batch");
                    var groupIds=new java.util.HashSet<String>();
                    for(var group:data.groups())if(!groupIds.add(group.groupId()))throw new AssertionError("duplicate fetched group");
                    if(batch?!groupIds.equals(Set.of("group","other")):!groupIds.equals(Set.of("group")))throw new AssertionError("actual fetch group differs");
                    for(var group:data.groups()) {
                    if(version>=9) {
                        boolean member=driver.equals("rust-group") || driver.equals("rust-typed");
                        if(group.memberEpoch()!=(member?7:-1) || !java.util.Objects.equals(group.memberId(),member?"member":null))throw new AssertionError("actual fetch membership differs");
                    }
                    if(group.topics()!=null) for(var topic:group.topics()) {
                        if(version==10?!topic.topicId().equals(new Uuid(0x0101010101010101L,0x0101010101010101L)):!topic.name().equals("topic"))throw new AssertionError("actual fetch identity differs");
                        if(!topic.partitionIndexes().equals(List.of(0)))throw new AssertionError("actual fetched partitions differ");
                    }
                    }
                    fetches++;
                } else if(request && api==3) {
                    var data=new MetadataRequestData(reader,version);
                    if(data.allowAutoTopicCreation() && (driver.equals("rust-admin") || driver.equals("rust-batch")))throw new AssertionError("Rust enabled topic creation");metadata++;
                } else if(request && api==10) {
                    var data=new org.apache.kafka.common.message.FindCoordinatorRequestData(reader,version);
                    var keys=version>=4?data.coordinatorKeys():List.of(data.key());
                    if(data.keyType()!=0 || keys.isEmpty() || keys.size()>2 || new java.util.HashSet<>(keys).size()!=keys.size()
                        || keys.stream().anyMatch(key->!key.equals("group") && !(driver.endsWith("batch") && key.equals("other"))))throw new AssertionError("actual coordinator key differs");
                } else {
                    switch(api) {
                        case 8 -> new OffsetCommitResponseData(reader,version);
                        case 9 -> new OffsetFetchResponseData(reader,version);
                        case 3 -> new MetadataResponseData(reader,version);
                        case 10 -> new org.apache.kafka.common.message.FindCoordinatorResponseData(reader,version);
                        default -> throw new AssertionError(api);
                    };responses++;
                }
                if(raw.hasRemaining())throw new AssertionError("trailing actual frame bytes: "+path);
            }
        }
        System.out.println("{\"actual_commit_frames\":"+commits+",\"actual_fetch_frames\":"+fetches+",\"actual_metadata_frames\":"+metadata+",\"parsed_actual_responses\":"+responses+"}");
    }
    public static void main(String[] args)throws Exception {
        if(args.length==3 && args[0].equals("parse-runtime")) { parseRuntime(Path.of(args[1]),args[2]);return; }
        if(args.length==3 && args[0].equals("runtime-seed")) {
            runtimeSeed(Path.of(args[1]),Short.parseShort(args[2]));return;
        }
        if(args.length==2 && args[0].equals("verify")) {
            Path input=Path.of(args[1]);int count=0;
            for(short version:new short[]{8,9,10})for(String mode:List.of("full","empty","null-empty","null-topics","errors")) {
                String name="v"+version+"-"+mode;
                for(String kind:List.of("commit-request","commit-response","fetch-request","fetch-response")) {
                    ByteBuffer raw=ByteBuffer.wrap(Files.readAllBytes(input.resolve(name+"."+kind+".bin")));
                    var reader=new ByteBufferAccessor(raw);
                    Message parsed=switch(kind) {
                        case "commit-request" -> new OffsetCommitRequestData(reader,version);
                        case "commit-response" -> new OffsetCommitResponseData(reader,version);
                        case "fetch-request" -> new OffsetFetchRequestData(reader,version);
                        case "fetch-response" -> new OffsetFetchResponseData(reader,version);
                        default -> throw new AssertionError(kind);
                    };
                    Message expected=switch(kind) {
                        case "commit-request" -> commit(version,mode);
                        case "commit-response" -> committed(version,mode);
                        case "fetch-request" -> fetch(version,mode);
                        case "fetch-response" -> fetched(version,mode);
                        default -> throw new AssertionError(kind);
                    };
                    if(raw.hasRemaining() || !parsed.equals(expected))throw new AssertionError("Rust body differs: "+name+"."+kind);
                    count++;
                }
            }
            System.out.println("{\"independently_parsed_rust_bodies\":"+count+"}");return;
        }
        if(args.length!=1)throw new IllegalArgumentException("output directory, or verify input directory");
        Path out=Path.of(args[0]);Files.createDirectory(out);List<String> index=new ArrayList<>();
        for(short version:new short[]{8,9,10})for(String mode:List.of("full","empty","null-empty","null-topics","errors")) {
            var c=commit(version,mode);var cr=committed(version,mode);var f=fetch(version,mode);var fr=fetched(version,mode);
            byte[] cb=bytes(c,version),crb=bytes(cr,version),fb=bytes(f,version),frb=bytes(fr,version);
            if(!new OffsetCommitRequestData(new ByteBufferAccessor(ByteBuffer.wrap(cb)),version).equals(c)
                || !new OffsetCommitResponseData(new ByteBufferAccessor(ByteBuffer.wrap(crb)),version).equals(cr)
                || !new OffsetFetchRequestData(new ByteBufferAccessor(ByteBuffer.wrap(fb)),version).equals(f)
                || !new OffsetFetchResponseData(new ByteBufferAccessor(ByteBuffer.wrap(frb)),version).equals(fr))throw new AssertionError("SDK model/parser differs");
            String name="v"+version+"-"+mode;Files.write(out.resolve(name+".commit-request.bin"),cb);Files.write(out.resolve(name+".commit-response.bin"),crb);
            Files.write(out.resolve(name+".fetch-request.bin"),fb);Files.write(out.resolve(name+".fetch-response.bin"),frb);index.add(name+"\t"+version+"\n");
            if(!java.util.Arrays.equals(bytes(commitBuilder(c).build(version).data(),version),cb)
                || !java.util.Arrays.equals(bytes(fetchBuilder(f).build(version).data(),version),fb))throw new AssertionError("UUID-capable builder differs");
        }
        StringBuilder policy=new StringBuilder();
        var commitNames=OffsetCommitRequest.Builder.forTopicNames(commit((short)9,"full"));
        var fetchNames=OffsetFetchRequest.Builder.forTopicNames(fetch((short)9,"full"),false);
        policy.append("commit-name-factory-range\t").append(commitNames.oldestAllowedVersion()).append(':').append(commitNames.latestAllowedVersion()).append('\n');
        policy.append("fetch-name-factory-range\t").append(fetchNames.oldestAllowedVersion()).append(':').append(fetchNames.latestAllowedVersion()).append('\n');
        try { OffsetCommitRequest.Builder.forTopicNames(commit((short)10,"full")).build((short)10);policy.append("commit-name-v10\taccepted\n"); }
        catch(RuntimeException error) { policy.append("commit-name-v10\t").append(error.getClass().getName()).append('\n'); }
        try { OffsetFetchRequest.Builder.forTopicNames(fetch((short)10,"full"),false).build((short)10);policy.append("fetch-name-v10\taccepted\n"); }
        catch(RuntimeException error) { policy.append("fetch-name-v10\t").append(error.getClass().getName()).append('\n'); }
        Files.writeString(out.resolve("cases.tsv"),"name\tversion\n"+String.join("",index));Files.writeString(out.resolve("builder-policy.tsv"),policy);
        System.out.println("{\"independent_request_response_pairs\":30,\"generated_bodies\":60}");
    }
}
