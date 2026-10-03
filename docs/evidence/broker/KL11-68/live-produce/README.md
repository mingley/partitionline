# Accepted ordinary Produce runtime portion

`validation.json` binds the accepted Produce portion to exact handler source
`c34bdfd3fbba65492ea49b3804dd0ae5311364b0`. The entire archived source matches
7,884 Git blob bytes. The first independent 1,074-case wire corpus is retained
unchanged in its earlier source commit; this runtime portion does not close the
remaining Fetch/native work in KL11-68.

Stable Rust 1.99.0 and Rust 1.85.0 ran actual Apache Java 4.1.2, 4.2.1 and
4.3.1 producers with idempotence disabled, no transaction ID, no compression,
zero retries and one request in flight. Actual producers exercised acks 1, -1
and 0. Forced TCP peers exercised every Produce version 3–13, sequential
assigned offsets, protected CRC rejection, acks-0 same-channel continuation and
acks-0 error EOF. There are 9,138 runtime assertions over 24 successful Java
executions. Ninety independently parsed wire responses expose exactly five
APIs: Produce 3–13, Metadata 0–13, ApiVersions 0–4, CreateTopics 2–4 and
DeleteTopics 1–6. Fetch/ListOffsets are absent.

Both toolchains restart the same journals, retain 24 topic identities across
the two lanes, and assign SDK checkpoint offsets 12 and raw checkpoint offsets
33. All six Rust server processes exit with status 0. The first stable server
completed its documented 180-second harness deadline; its actual Linux kernel
exit status was captured before the zombie was reaped. Subsequent supervisors
capture process status with `wait()` and snapshot complete journal bytes.

Independent compressed requests reuse the exact Apache-built rich gzip,
Snappy, LZ4 and Zstd fixtures and JNI classpath pins from KL11-67. Default
builds reject all 264 known-codec requests with error 76 and consume no offsets.
Codec-enabled builds admit 240 requests, each containing three records, while
all 24 Zstd requests below Produce 7 still return 76. Plain marker writes
independently confirm the next offset after those errors and admissions.

The actual Rust journal format is checked using independent bounded parsing
and bitwise Castagnoli CRC32C. All three pinned Apache readers then decode
every stored payload. They compare all 1,248 distinct stored records over 42
partition journals, yielding 3,744 exact record SHA-256/offset/order/timestamp/
key/value/null/empty/header comparisons. Every admitted compressed batch is
stored as ordinary uncompressed magic 2 with the original decoded history.
This is offline reverse byte interoperability; it is not a Kafka Fetch result.

The first compressed-default attempt retains a connection-refused exception:
the successful harness expired during peer preparation, before that attempt
sent a request. Its same-journal rerun passed. The first reverse decoder strict
compile retains the Apache 4.3 CompressionType package mismatch; adapting that
import to the actual pinned record package corrected it. Earlier source-batch
builder/metrics setup failures remain retained separately. No Rust runtime
failure was observed in this accepted portion.

The peer sources and command runners are in the parent evidence directory.
Use a fresh numbered evidence/state directory for each attempt. For example:

```sh
python3 docs/evidence/broker/KL11-68/run-produce-live.py --source /workspace/work/broker-log-live-produce/source-c34bdfd3 --source-sha c34bdfd3fbba65492ea49b3804dd0ae5311364b0 --target /workspace/work/broker-log-live-produce/target-msrv --toolchain 1.85.0 --evidence /absolute/fresh/attempt --state /absolute/fresh/state --peer-state /absolute/fresh/attempt/clients --phases append,compressed-default
python3 docs/evidence/broker/KL11-68/run-produce-live.py --source /workspace/work/broker-log-live-produce/source-c34bdfd3 --source-sha c34bdfd3fbba65492ea49b3804dd0ae5311364b0 --target /workspace/work/broker-log-live-produce/target-msrv --toolchain 1.85.0 --evidence /absolute/fresh/restart-attempt --state /absolute/existing/state --peer-state /absolute/existing/clients --restart --phases restart-append
python3 docs/evidence/broker/KL11-68/run-produce-live.py --source /workspace/work/broker-log-live-produce/source-c34bdfd3 --source-sha c34bdfd3fbba65492ea49b3804dd0ae5311364b0 --target /workspace/work/broker-log-live-produce/target-msrv --toolchain 1.85.0 --features codecs --evidence /absolute/fresh/codec-attempt --state /absolute/existing/state --peer-state /absolute/existing/clients --restart --phases compressed-enabled
python3 docs/evidence/broker/KL11-68/decode-produce-journals.py --journal-state /absolute/existing/state --peer-state /absolute/existing/clients --source-sha c34bdfd3fbba65492ea49b3804dd0ae5311364b0 --output /absolute/fresh/decode-attempt
```

The consumer group, coordinator, transaction, idempotent sequence, replication
and native producer qualifications remain outside this accepted portion.
Acks -1 covers only the declared single-node ISR and local sync. Acks 0 has no
client durable receipt; its persistence is observed independently after
shutdown/restart and byte decoding. Native magic-2 production waits for the real
Fetch handler and truthful advertisement.
