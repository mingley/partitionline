# KL04-07 record-history gate evidence

The frozen implementation at `c9a2e8c12dd12e056902b8b9410e1407f798fad6`
passes the card's correctness requirements. These are small diagnostic runs on a
shared host, using dev binaries and an isolated, owned Apache Kafka 3.9.1 broker.
They do not qualify throughput or satisfy the full benchmark result schema.
Suite HOLD remains active.

| Artifact | Workload and result |
| --- | --- |
| `native-final/` | Final source, 128 records of 100 bytes, two partitions, acks=1, zero warmup. Independent receipt verifies every unique ID, deterministic SHA-256 payload, key and partition order. The additional nontransactional high-watermark delta is 128. |
| `acks0-native-final/` | Final source, 16 records, acks=0. All 16 received independently. Producer reports `acked=0`, `acked_rec_s=null`, and 16 local completions. The verdict explicitly excludes acknowledged throughput. |
| `control-native/` | Kafka Java client commits a real three-record transaction. End offset is 4 because the commit marker occupies an offset. Rust read-committed fetch verifies three application IDs and hashes; the marker is not an application record. |
| `matching-count-corruption/` | Declared synthetic mutation of the earlier real 128-record trace replaces ID 3 with another ID 1 while preserving record totals and offsets. The checker exits 1 with `SYNTHETIC_SWAP`, 128 attempted and consumed, and `performance_claims_invalidated=true`. Raw and normalized failed artifacts are retained. |
| `native-run/`, `acks0-native/` | Earlier successful iterations retained. The producer's stdout calculation for partial local completions was subsequently corrected; see source provenance below. |

`rust-tests.log` records three Rust regression tests, `python-tests.log` records
16 adapter and harness tests, and `shared-history-tests.log` records the existing
45 shared-checker tests. The adapter fixtures cover independent payload
generation, matching-total loss/duplicates, corrupt hashes, ordering, controls,
acks=0, malformed and partial journals, refusing overwritten artifacts, and
retention after a failed harness run. `settings-validation.json` records 24
invalid example configurations rejected before connection. `build.log`,
`clippy.log` and `fmt.log` record the final build and source checks.

Initial development compiler diagnostics remain in `check.log`. Two Java
fixture compilation attempts also remain visible: the `javac` executable was
absent, and a direct compiler-module attempt used an unexpanded wildcard
classpath. `compile-java-explicit.*` records the successful compiler-module run
using the explicit Kafka client jar. Both failures happened before transactional
records were attempted; they are not erased or described as successful runs.

`provenance.json` binds final source snapshots and binary hashes to the pushed
source commit. The files were compiled while dirty, before that commit; the
commit supplies a later byte-for-byte content binding. The full precompile
working-tree status and earlier binary hashes were not retained. This limitation
is explicit. `working-tree-status-at-provenance.txt` is the actual later captured
status, including unrelated concurrent work. `compile-input-hashes.json` records
the 35 core/Cargo inputs, verified equal to the source commit. The earlier
producer snapshot is reconstructed by reversing the two-line stdout correction;
its diff and method are retained under `source-snapshots/pre-stdout-correction/`.
Fetch/helper content did not change between the retained native runs.

`ControlRecords.java` and its compiled class preserve the independent producer
fixture. `replay-scripts/` preserve the commands/settings used for native setup
and direct auxiliary runs; their original paths and exclusive output directory
requirements are intentional. Allocate new ports/topics/output directories to
replay a live run. `broker/` retains the owned server configuration, metadata and
complete startup/shutdown log. `broker-cleanup.json` records verification of the
owned PID/config before SIGTERM and confirms the listener closed.

Offline verification can use the retained raw journals without starting Kafka:

```sh
python3 scripts/bench-record-history.py \
  --producer docs/evidence/perf/KL04-07/native-final/run-1/producer.jsonl \
  --consumer docs/evidence/perf/KL04-07/native-final/run-1/consumer.jsonl \
  --history /tmp/kl04-07-replay-history.json \
  --output /tmp/kl04-07-replay-verdict.json
python3 scripts/check-record-history.py --json \
  docs/evidence/perf/KL04-07/matching-count-corruption/history.json
```

Choose unused replay output paths; the adapter refuses replacements. The first
command succeeds, and the deliberately corrupted second history exits 1. The
top-level `SHA256SUMS` covers all retained files except itself, with paths
relative to this directory.
