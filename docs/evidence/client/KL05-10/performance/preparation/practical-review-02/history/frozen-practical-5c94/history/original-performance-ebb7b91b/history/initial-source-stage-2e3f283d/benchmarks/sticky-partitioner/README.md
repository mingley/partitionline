# Sticky partitioner matched profiles (prepared, unexecuted)

This source is a separate public-API adopter. It does not add producer queue,
in-flight, batch, latency or partitioner instrumentation. No Cargo, JVM or
measurement run has executed this source. No performance result exists.

The six profiles are Rust default round-robin/null plus murmur2/keyed, Rust opt-in
uniform/null plus murmur2/keyed, and genuine Java4.3.1 builtin uniform/adaptive=false
for null plus murmur2/keyed. The public Rust and Java producers never specify a
partition. The existing RECORD_HISTORY benchmark specifies a partition and cannot
qualify these unkeyed profiles.

Rust's selected named policy consumes successful admissions using conservative
record upper bounds and61bytes per real cohort. Java consumes its own accumulator
history. Their compression/packing histories and production random draws are not
claimed identical. The same100byte deterministic public IDs and16byte non-null
keys (or genuinely null keys) are used within each paired block. Rust sticky uses
the block seed; Java production RNG is not externally seedable.

Each driver pipelines at most8,192record sends. Every public ack records ID,
phase, partition, offset, actual invocation/completion times, SHA256 of key
presence plus key/value, and conservative record bound in a76byte binary row.
Warmup lasts at least15seconds and10,000acks; measurement lasts at least60seconds
and1,000,000acks. Timing includes input generation, public admission,
driver/journal work, ack drain and flush. A separate Rust consumer checks every
ID/value/key/hash and reconciles the public ack's partition/offset against actual
delivery, rejects duplicates and offset holes, and records a fixed broker fence.
The offline SQLite audit checks both actual journals, an independent Python
generator/hash, per-phase times/counts, skew and missing/duplicate IDs.

The source caps a whole cell at10,000,000warmup+measurement records. Reaching that
cap before both time/count minima is a failed, unqualified run. It never pauses
traffic to manufacture a60second denominator. Any cap change needs a new frozen
source and resource forecast. Outstanding sends on cancellation are ambiguous;
failures and partial journals are retained. This is a closed-loop throughput
workload. Its invocation-to-ack samples do not establish coordinated-omission
corrected or open-loop latency percentiles.

Use profiles.json for five paired randomized blocks (30complete cells). Paired
keyed and null comparisons remain separate. A future report needs paired
uncertainty (95%bootstrap interval and MAD), all actual configurations and
failures, and every public ID/hash/offset capture. No global fastest-client claim
follows from this task.

run-cell.py is prepared to bind the entire actual immutable source/origin receipt,
accepted release ELF, genuine SDK classes/JAR, broker/image/config receipt, exact
full modes and actual runtime command. It captures output through bounded pipes,
monitors the physical350MiB floor plus a16MiB stop margin every0.2seconds,
confines only the measurement producer to future exclusiveCPU3, confines guards
and the independent verifier toCPU0,1, and closes only its own process group.
It starts no containers, pulls no images, compiles no source and performs no
cleanup. Every failed cell and audit database remains available for review.

Future compilation is still pending. This standalone crate intentionally has its
own workspace and path-depends on the actual immutable client. Cargo.lock must
first be generated in an authorized preparation tree, reviewed and pushed before
a locked immutable build. Force the client/adopter package to recompile when
reusing dependency caches; bind the resulting executable to the exact source pin.
Build receipts consumed by the runner must be actual records:

- Rust: source_commit, release=true, driver_source_sha256,
  driver_binary_identity (sha256/bytes/full_mode), and
  package_recompiled_for_exact_source_pin=true.
- Java: source_commit, caller_sha256, and the exact actual compiled_classes map
  for StickyBenchmark.class plus its Ack and Phase classes. Pin the genuine
  kafka-clients-4.3.1.jar SHA and slf4j JAR in the actual build receipt.
- Broker: actual image_digest, cpu_set excluding3, fresh topic, partitions=6,
  replication_factor=min_isr=1, retention_ms=-1, security=PLAINTEXT, ready=true.
  Broker ack is not fsync durability proof.

Actual source/build/runtime identity and host hardware/governor/JIT/process
receipts are still required. The current environment's approximately1GBfree is
below the conservative max-cap forecast; no benchmark is authorized to launch.
The worst case includes journals, broker data, bounded SQLite database and
lossless archive transients. Plan verified per-cell archival and own-topic
lifecycle before allocating another cell. The source does not remove data or
ask the user for a new approval flow.

Requests per second, wire bytes and batch-size distributions are unobserved by
these public APIs. Conservative per-record sums do not add the unobserved actual
cohort overhead. A separately prepared framed capture could supply those
observations later; this driver does not invent them.
