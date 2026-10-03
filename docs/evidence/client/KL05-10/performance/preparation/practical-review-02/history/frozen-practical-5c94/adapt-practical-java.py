from pathlib import Path
import difflib
root=Path(__file__).parent
p=root/'benchmarks/sticky-partitioner/StickyBenchmark.java'
s=p.read_text()
s=s.replace('String topic, long seed, boolean keyed, long nextId, int phase, long epoch)', 'String topic, long seed, boolean keyed, long nextId, int phase, long epoch, boolean qualification)')
s=s.replace('long started = System.nanoTime(), duration = (phase == 0 ? 15L : 60L)*1_000_000_000L;', 'long started = System.nanoTime(), duration = (qualification ? 0L : phase == 0 ? 15L : 60L)*1_000_000_000L;')
s=s.replace('long minimum = phase == 0 ? 10_000L : 1_000_000L, submitted=0, acknowledged=0;', 'long minimum = qualification ? phase == 0 ? 8192L : 16384L : phase == 0 ? 10_000L : 1_000_000L, submitted=0, acknowledged=0;\n        long maxRecords=qualification ? 24576L : MAX_RECORDS, phaseDeadlineSeconds=qualification ? 90L : 300L;')
s=s.replace('> 300L*1_000_000_000L', '> phaseDeadlineSeconds*1_000_000_000L')
s=s.replace('if (nextId>=MAX_RECORDS)', 'if (nextId>=maxRecords)')
s=s.replace('"+(acknowledged/(elapsed/1e9))+",\\"partition_counts\\":', '"+(qualification ? "null" : Double.toString(acknowledged/(elapsed/1e9)))+",\\"partition_counts\\":')
# Explicit purpose on every phase; no qualification rate/ranking claim.
needle='String json="{\\"phase\\":"+phase+'
replacement='String json="{\\"purpose\\":"+quote(qualification?"qualification":"ranking")+",\\"performance_qualified\\":false,\\"phase\\":"+phase+'
assert needle in s;s=s.replace(needle,replacement)
start=s.index('        if(!Files.readString(Path.of("/proc/self/status"))')
end=s.index('        if(!profile.equals(',start)
s=s[:start]+'''        if(args.length!=6)throw new IllegalArgumentException("qualify|rank PROFILE BOOTSTRAP TOPIC OUT SEED");
        if(!args[0].equals("qualify")&&!args[0].equals("rank"))throw new IllegalArgumentException("exact purpose required");
        boolean qualification=args[0].equals("qualify");
        String expectedCPU=qualification?"Cpus_allowed_list:\\t0-1":"Cpus_allowed_list:\\t3";
        if(!Files.readString(Path.of("/proc/self/status")).lines().anyMatch(line->line.equals(expectedCPU)))throw new IllegalArgumentException("producer CPU differs from qualification or ranking lease");
        String profile=args[1],bootstrap=args[2],topic=args[3];Path out=Path.of(args[4]);long seed=Long.parseUnsignedLong(args[5]);
''' +s[end:]
s=s.replace('properties.setProperty("request.timeout.ms","30000")', 'properties.setProperty("request.timeout.ms",qualification?"10000":"30000")')
s=s.replace('properties.setProperty("delivery.timeout.ms","120000")', 'properties.setProperty("delivery.timeout.ms",qualification?"30000":"120000")')
s=s.replace('properties.setProperty("max.block.ms","60000")', 'properties.setProperty("max.block.ms",qualification?"5000":"60000")')
s=s.replace('receipt(out,"configuration","{\\"profile\\":"+quote(profile)+', 'receipt(out,"configuration","{\\"purpose\\":"+quote(qualification?"qualification":"ranking")+",\\"performance_qualified\\":false,\\"profile\\":"+quote(profile)+')
s=s.replace('",\\"max_records\\":10000000,\\"producer_policy\\":', '",\\"max_records\\":"+(qualification?24576:10000000)+",\\"producer_policy\\":')
s=s.replace('try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(properties);DataOutputStream journal=', 'KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(properties);\n        try(DataOutputStream journal=')
s=s.replace('phase(producer,out,journal,topic,seed,keyed,0,0,epoch)', 'phase(producer,out,journal,topic,seed,keyed,0,0,epoch,qualification)')
s=s.replace('phase(producer,out,journal,topic,seed,keyed,warmup.nextId,1,epoch)', 'phase(producer,out,journal,topic,seed,keyed,warmup.nextId,1,epoch,qualification)')
s=s.replace('            throw error;\n        }\n    }\n}', '            throw error;\n        } finally {\n            producer.close(Duration.ofSeconds(qualification?30:120));\n        }\n    }\n}')
p.write_text(s)
(root/'java-qualification.patch').write_text(''.join(difflib.unified_diff(
 Path('/workspace/work/client-sticky-performance-source-04f6bc29/benchmarks/sticky-partitioner/StickyBenchmark.java').read_text().splitlines(True),s.splitlines(True),
 fromfile='frozen-ebb7/StickyBenchmark.java',tofile='practical-497f/StickyBenchmark.java')))
print('Java source adapted only; javac/JVM remain unexecuted')
