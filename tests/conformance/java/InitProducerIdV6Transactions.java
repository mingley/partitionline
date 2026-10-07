/* SPDX-License-Identifier: Apache-2.0 */
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Public ordinary transaction history and independent captured field checks. */
public class InitProducerIdV6Transactions {
    public static void main(String[] args) throws Exception {
        if (args.length!=4) throw new IllegalArgumentException("bootstrap topic capture-dir pinned-jar");
        Path loaded=Path.of(InitProducerIdRequestData.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String hash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(loaded)));
        if (!Files.isSameFile(loaded,Path.of(args[3])) || !hash.equals("52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36"))
            throw new AssertionError("unmatched Apache4.3.1 loaded jar");
        var config=new Properties();config.put("bootstrap.servers",args[0]);
        config.put("transactional.id","java-init-v6-native");config.put("acks","all");
        config.put("key.serializer",ByteArraySerializer.class.getName());config.put("value.serializer",ByteArraySerializer.class.getName());
        config.put("max.block.ms","30000");config.put("request.timeout.ms","10000");config.put("delivery.timeout.ms","30000");
        try(var producer=new KafkaProducer<byte[],byte[]>(config)) {
            producer.initTransactions();producer.beginTransaction();
            for(String value:List.of("J0","J1")) producer.send(new ProducerRecord<>(args[1],0,null,value.getBytes(StandardCharsets.UTF_8))).get();
            producer.commitTransaction();producer.beginTransaction();
            producer.send(new ProducerRecord<>(args[1],0,null,"JA".getBytes(StandardCharsets.UTF_8))).get();
            producer.abortTransaction();
        }
        config=new Properties();config.put("bootstrap.servers",args[0]);config.put("enable.auto.commit","false");
        config.put("key.deserializer",ByteArrayDeserializer.class.getName());config.put("value.deserializer",ByteArrayDeserializer.class.getName());
        config.put("isolation.level","read_committed");config.put("default.api.timeout.ms","10000");
        var expected=Set.of("R0","R1","R2","J0","J1");var values=new HashSet<String>();
        try(var consumer=new KafkaConsumer<byte[],byte[]>(config)) {
            var tp=new TopicPartition(args[1],0);consumer.assign(List.of(tp));consumer.seekToBeginning(List.of(tp));
            long end=consumer.endOffsets(List.of(tp)).get(tp);long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();
            while(consumer.position(tp)<end && System.nanoTime()<deadline) {
                for(var record:consumer.poll(Duration.ofMillis(200))) {
                    String value=new String(record.value(),StandardCharsets.UTF_8);
                    if (!expected.contains(value) || !values.add(value)) throw new AssertionError("unexpected/duplicate committed value "+value);
                }
            }
            if (!values.equals(expected) || consumer.position(tp)<end) throw new AssertionError("missing committed records "+values);
        }
        int pairs=0,rust6=0,javaOrdinary=0;
        try(var paths=Files.list(Path.of(args[2]))) {
            for(Path request:paths.filter(p->p.getFileName().toString().endsWith("-request.bin")).sorted().toList()) {
                String name=request.getFileName().toString();short version=Short.parseShort(name.split("-v")[1].split("-")[0]);
                byte[] requestBytes=Files.readAllBytes(request),responseBytes=Files.readAllBytes(request.resolveSibling(name.replace("-request.bin","-response.bin")));
                if(requestBytes.length>65536 || responseBytes.length>65536) throw new AssertionError("capture bounds");
                var rb=ByteBuffer.wrap(requestBytes);var sb=ByteBuffer.wrap(responseBytes);
                var req=new InitProducerIdRequestData(new ByteBufferAccessor(rb),version);
                var resp=new InitProducerIdResponseData(new ByteBufferAccessor(sb),version);
                if(rb.hasRemaining() || sb.hasRemaining() || req.enable2Pc() || req.keepPreparedTxn()) throw new AssertionError("ordinary flags/input");
                if(resp.errorCode()==0 && (resp.producerId()<0 || resp.producerEpoch()<0)) throw new AssertionError("allocation identity");
                if(version==6 && (resp.ongoingTxnProducerId()!=-1 || resp.ongoingTxnProducerEpoch()!=-1)) throw new AssertionError("unexpected ongoing transaction");
                if(req.transactionalId().equals("partitionline-init-v6-native") && version==6 && resp.errorCode()==0) rust6++;
                if(req.transactionalId().equals("java-init-v6-native") && resp.errorCode()==0) javaOrdinary++;
                pairs++;
            }
        }
        if(rust6<1 || javaOrdinary<1) throw new AssertionError("missing actual successful client init captures");
        System.out.println("{\"status\":\"pass\",\"committed_records\":5,\"aborted_records_absent\":2,\"init_pairs\":"+pairs+",\"rust_v6_successes\":"+rust6+",\"java_ordinary_successes\":"+javaOrdinary+",\"producer_closed\":true,\"consumer_closed\":true}");
    }
}
