# Pinned librdkafka behavioral adapter (KL01-13)

This adapter selects **`main_0125_immediate_flush`** and its
`do_test_flush_overrides_linger_ms_time` assertion from librdkafka **v2.15.0**,
commit `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`.
[Immutable upstream source](https://github.com/confluentinc/librdkafka/blob/9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab/tests/0125-immediate_flush.c).
`upstream/0125-immediate_flush.c` is byte-identical to that file, with the original
license retained; `pin.json` fixes its SHA-256.

The regression checks an observable delivery behavior: **flush bypasses linger**.
A producer with `linger.ms=10000` enqueues 50 records without flushing. Their
natural delivery must take 10000–15000 ms. It enqueues another 50 records and
calls `flush(timeout=2000)`: success and elapsed time 0–2500 ms are required. All
100 records must then be consumed. The original C function and timing thresholds
are unchanged; they are not benchmark speed comparisons.

## API and harness adaptations

The native binary compiles the original selected function against a small
`shim/test.h` and `shim/helpers.c`, using the existing KL04-04 native shared
library. It does not introduce another native dependency stack. The shim replaces
only upstream test-suite infrastructure, not the C producer implementation:

- The orchestrator creates a fresh one-partition RF1/minISR1 topic before each
  run. The upstream topic helper verifies its metadata instead of creating it
  again; the topic name comes from the orchestrator's unique namespace.
- Both clients use explicit acks 1, no idempotence or compression, one connection
  and one in-flight request, 1 MB / 32768-record batch caps, 32 MiB queue byte cap,
  plaintext and the same 10000-ms linger. The C producer also has a dormant
  1000000-record queue cap. Payloads are 50 bytes and batches stay far below caps.
- Upstream helper arguments reuse testid 0 and message index 0 for both halves.
  The adapter assigns unique IDs `m-000`…`m-099` across phases and identical
  ASCII payloads (`000:` plus 46 `x` bytes, etc.). This additive fixture identity
  replaces the upstream helper's payload format and enables independent
  ID/hash/order checking. All selected counts and scheduling calls are preserved.
- C observes delivery through the real `rd_kafka_poll` callbacks. Rust uses
  spawned `Producer::send` futures with a per-record enqueue barrier to preserve
  the same input order. Its first-phase delivery observation uses producer
  metrics at the upstream one-second poll cadence; futures supply actual
  acknowledged offsets. The second phase uses `flush_timeout(2000)`.
- The C consumer directly assigns offset 0, independently checks every key,
  payload byte, partition and offset, and queries broker ListOffsets. It audits
  both the native C producer and the Rust producer. The shared KL03-18 history
  checker then verifies unique IDs, payload hashes and partition order.
- Timing failures are retained as assertion events and cause exit 1 after
  collecting receipt evidence. This changes the harness's failure-reporting
  shape, while preserving the original assertion and failed verdict.

The sibling `main_0125_immediate_flush_mock` remains in the copied source but is
excluded from the executable via function-section garbage collection. Its
three-broker metadata assertion is **not run**. Other C/C++ suite cases and the
upstream test-runner infrastructure are not qualified by this selected case.

## Build and execute

Provide the existing isolated KL04-04 build and a running isolated Kafka broker.
The Rust adapter is a standalone workspace with a locked graph and a path
dependency on partitionline; it adds no dependency to the main crate.

```bash
# cargo/rustc must be on PATH; use an available offline Cargo cache.
C_PEER_BUILD_DIR=/tmp/partitionline-c-peer CASE_BUILD_DIR=/tmp/c-0125 \
  tests/conformance/librdkafka/build.sh
python3 tests/conformance/librdkafka/run.py \
  --c-binary /tmp/partitionline-c-peer/0125-peer \
  --rust-binary /tmp/c-0125/rust-target/debug/partitionline-librdkafka-0125 \
  --build-manifest /tmp/c-0125/behavior-build-manifest.json \
  --kafka-home /path/to/kafka_2.13-3.9.1 \
  --bootstrap 127.0.0.1:19104 --output /tmp/c-0125-run-01
python3 -m unittest discover -s tests/conformance/librdkafka -p test_runner.py -v
```

The build freezes actual core and adapter source hashes before/after compilation
and refuses concurrent source changes. Its manifest records the source revision,
cleanliness, compiler versions, binary hashes, shim hashes and native library pin.
The runner verifies those hashes before execution. Use a new output directory for
every attempt; files are exclusively created and no prior attempt is overwritten.

## Deliberately failing controls

The runner executes both clients normally and with **omit-flush**. For the
mutation, the native shim intercepts the selected flush call and waits for natural
delivery; the Rust adapter omits its flush and does the same. Both preserve the
full input and receipt history, but must fail the unchanged ≤2500-ms flush
assertion at approximately 10000 ms. This is an intentional adapter mutation,
not a defect claim about either client.

A separate synthetic receipt mutation replaces the last receipt with the prior
receipt while keeping count 100. The history checker must reject the duplicate /
missing IDs with a minimal counterexample. Successful receipt histories therefore
do not conceal a timing failure, and equal record counts do not conceal lost IDs.

The scoped conformance aggregate includes every normal and mutated attempt and
must exit 1 because the deliberate failures remain recorded. A successful control
test demonstrates that rejection; it does not turn a failed attempt green.
Checked-in evidence is in `docs/evidence/conformance/KL01-13/`. It covers this
selected assertion on an isolated native Kafka 3.9.1 broker, not the entire
librdkafka suite or general performance/compatibility certification.
