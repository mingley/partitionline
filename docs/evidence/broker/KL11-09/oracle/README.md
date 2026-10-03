This independent probe executes actual Apache Kafka 4.1.2, 4.2.1 and 4.3.1
`LogSegment`, `OffsetIndex`, `TimeIndex` and `FileRecords` methods. Official
client JAR and distribution archive hashes, source excerpts, LICENSE and NOTICE
are retained under the existing sibling `KL11-68/upstream-pins.json` and
`KL11-68/upstream/` evidence. The driver checks those exact distribution/client
hashes before extracting only four named, bounded dependency JARs.

Three manually created segments contain twelve assigned records, including
equal and regressing timestamps across batch and segment boundaries. Each run
compares 160 first-match timestamp queries to a separate linear input scan,
captures 63 actual offset/time index hints and 21 actual reads, then repeats all
queries after close/reopen. Exact log payload bytes and on-disk file hashes are
retained. Fresh repeated executions and all three release outputs must agree.
One deliberately wrong expected timestamp offset per release must compile and
fail at the intended assertion; those sources/classes and failures are retained.

This proves component behavior on the stated bounded inputs. It does not execute
Rust, Apache automatic rolling, a wire broker, retention, compaction, replication
or a performance workload. Apache validates a 1 MiB segment target minimum;
manual boundaries keep actual files below 4 KiB. Its bare component can return a
partial batch slice with `minOneMessage=false`; that is recorded as component
behavior, rather than promoted to a Kafka wire Fetch result. Negative timestamp
queries are component searches, not ListOffsets special-sentinel dispatch.

`development/` retains candidate setup failures and passing candidate runs.
Final qualification requires executing the exact pushed driver/Java sources in
a fresh directory; implementation qualification remains owned by KL11-09.

Run from the repository root with existing pinned official inputs:

```sh
python3 docs/evidence/broker/KL11-09/oracle/probe-segments.py \
  --jars /workspace/work/broker-wire/jars \
  --distributions /workspace/work/broker-wire/releases \
  --scratch /workspace/work/segments-assessment/apache-final-UNIQUE \
  --output docs/evidence/broker/KL11-09/oracle/final-UNIQUE
```
