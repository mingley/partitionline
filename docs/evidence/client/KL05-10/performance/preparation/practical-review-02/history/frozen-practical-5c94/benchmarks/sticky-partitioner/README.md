# Sticky partitioner matched profiles (prepared, unexecuted)

The additive small qualification mode sends exactly 8,192 warmup records and
16,384 exercise records per profile (24,576 total) using the same 1 MiB batch and
5 ms linger settings. It runs under a future CPU 0 and 1 correctness lease, emits
purpose=qualification and performance_qualified=false, and keeps throughput
rates null. This mode can check actual API, admission, ack, delivery and journal
correctness; it cannot satisfy the separate 60-second/1M/five-paired ranking gate.
Use qualify-produce/qualify-verify for Rust, qualify as the Java first argument,
--qualification on run-cell.py and on audit.py. No command has been executed.

Qualification uses bounded 10-second requests, 30-second delivery and 5-second
admission waits. Rust joins and flush share the remaining absolute 90-second
phase deadline. Java checks 90 seconds between calls; its synchronous send can
spend the separately configured 5 seconds and its close is bounded at 30 seconds.
The host hard-stops a producer at 360 seconds and an entire qualification cell at
1,000 seconds, reserving its final 90 seconds for bounded source/input guards and
owned process closure. Small failure receipts may use the stop margin while
retaining the actual 350 MiB floor. Rust close is 30 seconds in qualification.
Source guards and process cleanup remain part of that host budget.

The qualification forecast requires 620,298,256 free bytes (about 592 MiB) before
one cell, including the 350 MiB floor, 16 MiB stop margin, 128 MiB shared-growth
reserve, 3.74 MB two journals, a 16 MiB SQLite cap, bounded logs and a 64 MiB ready
broker append allowance. Six cells recheck real free space; worst-case cumulative
allocation can prevent the sixth and is not claimed to fit automatically.
No image pull, broker cold start, new topic/index preallocation or compile cache
is silently counted as zero: a broker and fresh six-partition topic must already
be ready with actual physical-allocation receipt before this runtime check.
The original 8.4 GB ranking forecast remains byte-for-byte in resource-forecast.json.

Cargo.lock here is a manual WORK graph candidate, not a Cargo-resolved artifact.
It preserves all 80 root-lock bindings, adds seven pinned serde_json-closure
packages from the existing peer lock and one local driver. All 86 registry
archives were read and SHA256-checked against lock checksums; no archive is
missing. No Cargo metadata, lock generation or compile command was run.
First resolve/validate offline in a root-authorized shallow WORK adopter using
the exact immutable client. Preserve any resolver difference and failed-first
output, freeze/push the actual lock correction, then run final offline+locked
checks on the new actual immutable pin. Never reuse an old client package
fingerprint solely because archived source mtimes compare older.

This source is a separate public-API adopter. It does not add producer queue,
in-flight, batch, latency or partitioner instrumentation. No Cargo, JVM or
measurement run has executed this source. No performance result exists.

The six profiles are Rust default round-robin/null plus murmur2/keyed, Rust opt-in
uniform/null plus murmur2/keyed, and genuine Java 4.3.1 builtin uniform/adaptive=false
for null plus murmur2/keyed. The public Rust and Java producers never specify a
partition. The existing RECORD_HISTORY benchmark specifies a partition and cannot
qualify these unkeyed profiles.

Rust's selected named policy consumes successful admissions using conservative
record upper bounds and 61 bytes per real cohort. Java consumes its own accumulator
history. Their compression/packing histories and production random draws are not
claimed identical. The same 100-byte deterministic public IDs and 16-byte non-null
keys (or genuinely null keys) are used within each paired block. Rust sticky uses
the block seed; Java production RNG is not externally seedable.

Each driver pipelines at most 8,192 record sends. Every public ack records ID,
phase, partition, offset, actual invocation/completion times, SHA256 of key
presence plus key/value, and conservative record bound in a 76-byte binary row.
Warmup lasts at least 15 seconds and 10,000 acks; measurement lasts at least 60 seconds
and 1,000,000 acks. Timing includes input generation, public admission,
driver/journal work, ack drain and flush. A separate Rust consumer checks every
ID/value/key/hash and reconciles the public ack's partition/offset against actual
delivery, rejects duplicates and offset holes, and records a fixed broker fence.
The offline SQLite audit checks both actual journals, an independent Python
generator/hash, per-phase times/counts, skew and missing/duplicate IDs.

The source caps a whole cell at 10,000,000 warmup+measurement records. Reaching that
cap before both time/count minima is a failed, unqualified run. It never pauses
traffic to manufacture a 60-second denominator. Any cap change needs a new frozen
source and resource forecast. Outstanding sends on cancellation are ambiguous;
failures and partial journals are retained. This is a closed-loop throughput
workload. Its invocation-to-ack samples do not establish coordinated-omission
corrected or open-loop latency percentiles.

Use profiles.json for five paired randomized blocks (30 complete cells). Paired
keyed and null comparisons remain separate. A future report needs paired
uncertainty (95% bootstrap interval and MAD), all actual configurations and
failures, and every public ID/hash/offset capture. No global fastest-client claim
follows from this task.

run-cell.py is prepared to bind the entire actual immutable source/origin receipt,
accepted source-bound ELF (debug allowed only for qualification; release required
for ranking), genuine SDK classes/JAR, broker/image/config receipt, exact
full modes and actual runtime command. It captures output through bounded pipes,
monitors the physical 350 MiB floor plus a 16 MiB stop margin every 0.2 seconds,
confines only the measurement producer to future exclusive CPU 3, confines guards
and the independent verifier to CPUs 0 and 1, and closes only its own process group.
It starts no containers, pulls no images, compiles no source and performs no
cleanup. Every failed cell and audit database remains available for review.

Future compilation is still pending. This standalone crate intentionally has its
own workspace and path-depends on the actual immutable client. The manual lock
candidate requires real offline resolver validation in an authorized preparation
tree, review and a pushed source pin before a locked immutable build. Force the client/adopter package to recompile when
reusing dependency caches; bind the resulting executable to the exact source pin.
Build receipts consumed by the runner must be actual records:

- Rust: source_commit, an actual boolean release (true required for ranking;
  verified debug allowed only for qualification), driver_source_sha256,
  driver_binary_identity (sha256/bytes/full_mode), and
  package_recompiled_for_exact_source_pin=true.
- Java: source_commit, caller_sha256, and the exact actual compiled_classes map
  for StickyBenchmark.class plus its Ack and Phase classes. Pin the genuine
  kafka-clients-4.3.1.jar SHA and slf4j JAR in the actual build receipt.
- Broker: actual image_digest, cpu_set excluding CPU 3, fresh topic, partitions=6,
  replication_factor=min_isr=1, retention_ms=-1, security=PLAINTEXT, ready=true.
  Qualification additionally binds completed topic creation, actual post-readiness
  physical free bytes and no broker/image provisioning inside the cell.
  Broker ack is not fsync durability proof.

Actual source/build/runtime identity and host hardware/governor/JIT/process
receipts are still required. The current environment's approximately 1 GB free is
below the conservative max-cap forecast; no benchmark is authorized to launch.
The worst case includes journals, broker data, bounded SQLite database and
lossless archive transients. Plan verified per-cell archival and own-topic
lifecycle before allocating another cell. The source does not remove data or
ask the user for a new approval flow.

Requests per second, wire bytes and batch-size distributions are unobserved by
these public APIs. Conservative per-record sums do not add the unobserved actual
cohort overhead. A separately prepared framed capture could supply those
observations later; this driver does not invent them.

Java metrics push is explicitly disabled to avoid unmatched background telemetry.
Java metadata snapshots bind partition IDs and leader IDs; the public PartitionInfo
API does not expose the leader epoch. Rust snapshots also bind its public leader
epoch. Both require exactly six available leaders and a fresh zero-offset topic.
The 450 MiB cold development graph forecast plus 40 MiB retained executable/log
reserve, 16 MiB metadata, 16 MiB stop margin and 350 MiB floor requires
914,358,272 free bytes before an authorized cold compilation. It is a forecast,
not an observed build; actual cache growth can still stop the cell.
