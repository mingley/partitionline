# rust-rdkafka comparison peer

This independent `[workspace]` pins `rdkafka=0.39.0` and
`rdkafka-sys=4.10.0+2.12.1`, with dynamic linking to the existing independently
pinned **librdkafka 2.15.0** KL04-03 installation. The sys crate's `2.12.1`
suffix is its binding/header baseline, not this driver's runtime native version.
The executable reports all three and rejects a wrong runtime version or a
mapped native library whose SHA256 differs from the checked build manifest.
The loader check requires Linux `/proc/self/maps`; other platforms are not
validated. None of these dependencies belongs to the partitionline workspace,
lockfile, package, or runtime graph.

The measured API is `BaseProducer::send` with explicit polling and delivery
callbacks, not `FutureProducer` or a Tokio executor. Keep its bar separate from
the C-only peer: safe-wrapper calls, opaque allocations, SHA256 generation,
and delivery history/sample I/O are included in this instrumented driver.
It provides no estimate of wrapper overhead alone without a separately matched
paired experiment. CPU/RSS cover the producer child's whole lifetime, including
warmup and native inspection; Java receipt audit is outside producer timing.
Allocation counts, average RSS, broker resources, and isolated steady-state CPU
are unmeasured. No performance or production-readiness claim follows from an
adapter correctness run. Suite HOLD remains active.

Build the existing C peer first using
[`../librdkafka/build.sh`](../librdkafka/build.sh), or reuse its exact recorded
installation. The builder verifies its native source commit/tree and installed
library hash, supplies isolated pkg-config metadata, and creates a binary with
pinned-library RPATH. It never builds bundled C 2.12.1. Use unique output paths
to preserve all failed build/check logs:

```sh
taskset -c 0-2,4 python3 benchmarks/peers/rust/build.py \
  --native-build /workspace/work/c-peer \
  --out /workspace/work/rust-peer-build \
  --target-dir /workspace/work/target-rust-peer --check
```

`--check` runs the card's `cargo test --locked --manifest-path
benchmarks/peers/rust/Cargo.toml`, strict formatting/Clippy/rustdoc, and a release
build with the isolated pkg-config/RPATH environment. `--toolchain 1.85.0`
validates MSRV; a separate target avoids mixing compiler artifacts. Check
`commands.json` and retained logs, then:

```sh
python3 -m unittest discover -s benchmarks/peers/rust -p test_adapter.py
python3 benchmarks/peers/rust/run.py emit-config \
  --binary /workspace/work/rust-peer-build/rust-peer
```

The settings parser and result-unit/sample helpers deliberately reuse the C
adapter. It validates positive bounded counts/deadlines, prohibits implicit
idempotence adjustments, sets queues/batches/compression/socket options
explicitly, and allowlists **post-creation** native configuration. The dump
contains no credentials/private-key paths. Enqueue acceptance is never counted
as broker acknowledgement. Only successful delivery callbacks with `acks!=0`
count; acks=0 remains unknown, timeouts and failures stay recorded. Queue-full
retries retain the same logical ID. Each measured run has independent absolute
offer, flush and audit deadlines. Timeout purge uses the C peer's additional
bounded five-second drain. Inspection calls each have ten-second deadlines;
this is a sequential bounded adapter, not a real-time global-deadline service.

Roundtrip writes measured-phase high-watermark fences after warmup. Records use
the C/franz-go splitmix64 scheme: `be64(index)||be64(mix(seed^index))` keys and
a deterministic value stream, explicit `index % partitions` placement. Warmup
reuses IDs and is excluded by fences. The 16-byte ID keys are a shared integrity
instrumentation format; they do not prove RFC UUID shape/uniform UUID-key
distribution in the named frozen scenario. Future qualification must reconcile
that frozen key requirement across peers; no settings are changed here.

An independent Apache Java **kafka-clients 3.9.1** consumer directly assigns
the fences and emits actual key/value bytes and partition offsets. It never
regenerates expectations. Python separately derives bytes and checks every ID,
hash, partition, offset coverage and order with `scripts/check-record-history.py`.
The inspected distribution's jar, source and compiled auditor are hashed.
This audit currently accepts PLAINTEXT only; secure cells remain unqualified.
All output paths are exclusive; retries must use new names. Driver raw output,
callbacks, receipts, history verdict, effective settings and failures are retained.

For a **separately named exploratory integrity smoke**, not the frozen campaign:

```sh
COUNT=256 WARMUP=32 ACKS=-1 IDEMPOTENT=1 ISOLATION=read_committed \
BATCH_BYTES=1048576 KAFKA_BOOTSTRAP=127.0.0.1:19094 \
KAFKA_TOPIC=your-precreated-six-partition-rf1-minisr1-topic \
BROKER_IMAGE=apache-kafka-3.9.1-distribution BROKER_VERSION=3.9.1 \
SCENARIO_ID=rust-rdkafka-matched-settings-integrity-smoke \
taskset -c 0-2,4 python3 benchmarks/peers/rust/run.py roundtrip \
  --binary /workspace/work/rust-peer-build/rust-peer \
  --kafka-home /workspace/work/c-peer/kafka_2.13-3.9.1 --result work/fresh-smoke.json
```

The frozen named scenario retains **8,000,000 records, 10,000 warmup, five
repetitions**, its endpoint/topic and every other setting. This adapter does
not run that campaign as part of bounded validation. `not-run` files its
configuration under that exact scenario ID with zero measurements and an
explicit reason. `unsupported` files an actual unsupported-cell artifact;
neither disposition can be counted as a win:

`TIER=required|exploratory` selects registry metadata and defaults to exploratory;
invalid values fail before traffic. For the frozen named `not-run` filing use
`TIER=required RTT_MS=0.1`, alongside its unchanged counts and other settings.
Tier selection never supplies measurements, sample floors or qualification.

```sh
PROFILE=group/share SCENARIO_ID=share-exp-kip932-concurrency \
python3 benchmarks/peers/rust/run.py unsupported \
  --binary /workspace/work/rust-peer-build/rust-peer --result work/fresh-unsupported.json \
  --reason 'Wrapper/sys bindings and this adapter lack a share lifecycle; native 2.15.0 has preview KIP-932; Kafka 3.9.1 fixture is ineligible'
```

The driver does not implement scheduled open-loop latency, standalone fetch,
transactions, KIP-848 group lifecycles or share consumption. Native library
support is distinct from driver support: native 2.15.0 supports traditional
groups, KIP-848, transactions and preview KIP-932. The pinned wrapper/sys binding
API and this adapter expose no share lifecycle; the Kafka 3.9.1 validation
fixture also cannot run the share cell. The adapter does not exercise group or
transaction lifecycles. Generic
producer settings include TLS/mTLS and PLAIN/SCRAM, but frozen TLS cipher/version,
rehandshake and SCRAM semantics have no adapter validation. OAuth/GSSAPI and
custom TLS server-name override are rejected. Null-key/constant payload produce
is available for compatibility, but unique-ID roundtrip verification cannot
pass such a cell. No sequence/retry-fencing or transactional isolation claim
follows solely from enabling idempotence/read_committed.

[`pure-rust-candidates.json`](pure-rust-candidates.json) pins and audits a
separate `rskafka 0.6.0` candidate and `kafka 0.10.0` alternative. Neither
matches any unchanged frozen cell. Selection remains **blocked**, eligibility
is empty and there is no runnable additional peer or comparison standing.
In particular, rskafka fixes acks=-1, lacks idempotence/transactions/groups/share,
fixes the Fetch wire isolation setting without verified abort filtering, and
lacks the required TCP_NODELAY knob. Its source does implement linger/batching,
optional TLS and four SASL mechanisms; those capabilities do not remove the
cell mismatches. Stock default native codecs must not be described as no-native.
The kafka alternative lacks TCP_NODELAY and a total Fetch byte limit, as well
as modern idempotence/transaction/group/share semantics. These exclusions keep
the all-peer suite blocked; they never create a comparative victory.
