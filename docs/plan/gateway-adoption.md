# KL10: close the gaps a gateway-like workload exposes

**Goal:** make `partitionline` win on a target synthetic profile modeled on
Kafka gateways and proxies: about 1 KiB records, gzip, mTLS, `acks=all`,
ordered delivery and one acknowledgment per record. A same-settings
comparison against librdkafka on 2026-09-30 found the gaps below. KL10
fixes them and gives the [KL09 claim gate](performance-leadership.md) cells
that cover this profile.

[tasks.json](tasks.json) owns KL10 status and dependencies, like every other
lane. Work proceeds one card per session under the
[session guide](README.md). Optimization cards follow the KL09
[worker protocol](performance-leadership.md#6-worker-protocol-for-kl09-cards)
and [hot-file locks](performance-leadership.md#7-claims-hot-file-locks-and-the-ready-query).

Nothing here is a performance claim. Section 2 is dated, local, unsigned
evidence from single runs on one host. **Suite HOLD remains.**

## 0. The gateway profile and its cells

A gateway accepts records from many upstream callers, publishes them through
one long-lived client, and tells each caller whether its record was
acknowledged. Its consumer side reads assigned partitions from explicit
offsets.

| Knob | Value |
|---|---|
| Records | ~1 KiB values of seeded, JSON-like metrics text (gzip ratio about 7); one 8-byte header |
| Partitions | 8, with an explicit partition per record |
| Codec | gzip, which many existing producers use; lz4 as a comparison cell; zstd once KL05-04 lands |
| Durability and ordering | `acks=all`, max in flight 1, linger 100 ms, 1 MiB batches |
| Security | TLS 1.3 with client-certificate authentication (mTLS) |
| Acknowledgment | One completion per record, consumed by the caller |
| Consume | Manual assignment from an explicit offset, over a backlog of gzip batches written by another client |

The KL09 contract cells use 100-byte records and require only lz4 and zstd
compressed cells, so none of them exercises this path. These exploratory,
local cells are the canonical definitions. KL10-02 and KL10-14 build them
and register them in
[performance-leadership.md section 4](performance-leadership.md#4-local-measurement-cells):

| Cell | Built by | Workload | Metrics |
|---|---|---|---|
| `lb-gateway-produce-gzip` | KL10-02 | The profile above at an open-loop 25k records/s, and unpaced; fire-and-forget, with every record confirmed by a timed `flush` | Acknowledged rec/s, CPU per record, peak RSS |
| `lb-gateway-produce-lz4` | KL10-02 | As gzip, with lz4 | As gzip |
| `lb-gateway-produce-ack` | KL10-02 | As gzip, with one completion per record; each client's acknowledgment API is named | As gzip, plus p50/p99/p99.9 acknowledgment latency |
| `lb-gateway-fetch-gzip` | KL10-14 | 8 partitions, manual assignment, over a seeded gzip backlog written once by a pinned peer | Verified rec/s, CPU per record, peak RSS |

## 1. Why a separate lane

KL09 optimizes measured hotspots of its own contract cells. The gaps below
sit outside those cells:

- a red CI on `main`
- the gzip backend and the way records are fed to it
- the gzip decode path
- a missing owned delivery API
- the consumer's single decode thread

KL10 adds the cells, fixes the gaps and hands its cells to the KL09/KL04
claim process. It publishes no claims.

## 2. Evidence (dated 2026-09-30; local, unsigned)

| Item | Detail |
|---|---|
| Source | `partitionline` `origin/main` `290adda5` |
| Peer | rust-rdkafka 0.39.0 with librdkafka 2.15.1, dynamically linked, system zlib 1.2.12 |
| Host | Apple M4 Pro (14 cores), macOS 26.7, rustc 1.98.1, release profile |
| Broker | One local broker on the same host, SSL with client authentication |

Each row is a single run, so expect ±10% noise. Outcomes were counted as
follows:

- **librdkafka and `send()`:** per-record completions.
- **`try_send`:** a successful `flush()` at the end of the run, inside the
  timed window, confirmed every record. Per-record errors were not
  observable.

High watermarks were not checked.

### Produce, gateway profile at a fixed 25k records/s (20 s)

| Client | CPU per record | Cores | p50 / p99 ack |
|---|---|---|---|
| librdkafka (`FutureProducer::send_result`) | **17.6 µs** | 0.44 | 57 / 107 ms |
| partitionline, `try_send` plus `flush` | 24.3 µs (+38%) | 0.61 | n/a |
| partitionline, `send()` in one spawned task per record | 30.6 µs (+74%) | 0.76 | 63 / 119 ms |

**Profile of the `try_send` run:** 80% of client CPU is in
`gzip_compress_into` (miniz_oxide `compress_inner`). The protocol, batching
and TLS path is only a few percent.

### Produce, unpaced (15 s)

| Client | Confirmed rec/s | Cores | CPU per record | p99 ack |
|---|---|---|---|---|
| librdkafka | 81.5k | 1.39 | 17.1 µs | 4.85 s |
| partitionline, `try_send` plus `flush` | **477k** | 6.71 | 14.1 µs | n/a |
| partitionline, `send()` per spawned task | 379k | 8.50 | 22.4 µs | 594 ms |

partitionline compresses on its per-connection workers in parallel. With max
in flight 1, librdkafka compresses on a single broker thread. This is a
real strength, but the clients used very different numbers of cores, so it
is not a per-core comparison.

### Consume a gzip backlog (8 partitions, manual assignment)

| Client | Records/s | CPU per record | Cores | Max RSS |
|---|---|---|---|---|
| librdkafka (`BaseConsumer`) | **220k** | **2.2 µs** | 0.48 | 193 MB |
| partitionline (`Consumer::fetch`) | 29k | 16.9 µs (7.8×) | 0.50 | 1,426 MB |

**Profile:** 84% of partitionline's CPU is in `gzip_decompress`, all of it on
the caller's task. `read_bounded` streams `GzDecoder` output through an
8 KiB buffer, so miniz copies matches through its window; `transfer` alone
is 36% of samples. The effective inflate rate is about 60 MB/s. The same
backend reaches about 1 GB/s when it writes into a large buffer.

### Codec microbenchmark (same payload, 16 KiB and 256 KiB batches, gzip level 6)

| Codec and backend | Compress | Decompress | Ratio |
|---|---|---|---|
| gzip, miniz_oxide 0.9.1 (current) | 101–122 MB/s | 1.06–1.10 GB/s | 6.7–7.2 |
| gzip, system zlib 1.2.12 (librdkafka) | 92–122 MB/s | 1.82–2.15 GB/s | 6.7–7.3 |
| gzip, zlib-rs 0.6.8 (pure Rust) | **178–218 MB/s** | 1.12–1.43 GB/s | 6.8–7.2 |
| lz4, lz4_flex (current) | 1.6–2.2 GB/s | 7.8–8.3 GB/s | 3.9–4.2 |
| lz4, C liblz4 | 1.7–2.1 GB/s | 7.6–7.9 GB/s | 3.9–4.2 |
| zstd level 3, C libzstd 1.5.7 (reference only) | 714–842 MB/s | 2.3–3.1 GB/s | 7.7–7.8 |

**Reading:**

- lz4_flex is already at parity with C.
- miniz compresses at the same speed as librdkafka's zlib. Yet the client
  spends about 18 µs per record in deflate, against about 10 µs when the
  same backend compresses the same bytes in one call. That feed overhead
  needs explaining (KL10-04).
- zlib-rs compresses 1.8× faster. It is pure Rust but uses `unsafe` and SIMD
  internally, so adopting it is a policy decision (KL10-05).
- zstd is a potential migration path for workloads that can change codec,
  subject to end-to-end consumer compatibility (KL05-02..05).

### Repository health

- **Red main CI.** Every CI run on `main` since `848e58c` (KL01-09,
  2026-09-26) fails `test (1.85)`, `test (stable)` and `features`.
  `tests/verifiable_contract.rs` runs `target/debug/examples/verifiable_*`,
  but `cargo test --all-targets` never builds those files under that name,
  so the tests panic with "missing example binary".
- **Build times.** On the same host with dependencies prefetched: clean
  debug 12.4 s, clean release 21.7 s, release examples 46 s, incremental
  debug 1.8 s. CI wall time is about 8.5 minutes per push, dominated by
  broker smoke on Kafka 4.1.

### Adoption notes from writing the comparison

- `send()` is an `async fn` that borrows the producer and enqueues when
  first polled.
  - A caller that wants one completion per record can spawn a task per
    record, which costs 6.3 µs per record more than `try_send` in the
    table above.
  - It can instead poll a bounded set of borrowed futures in one task, an
    option not measured here.
  - Or it can give up per-record outcomes and use `try_send`.
- None of these options admits the record at call time, fixes order by
  call order, or returns an owned completion that can cross task
  boundaries. librdkafka's `send_result` and the Java client's `send` do
  all three (KL10-07).
- `TlsConfig::client_identity` with a PKCS#8 PEM key worked against a
  client-authenticating broker on the first try. Metadata, explicit
  partitions, headers and manual assignment behaved as documented.

## 3. Findings and cards

| # | Finding | Fix | Card |
|---|---|---|---|
| F1 | Main CI is red, so evidence cannot gate | Find or build the example binaries the contract tests run | **KL10-01 (P0)** |
| F2 | No cells cover the gateway profile | Produce cells and a fetch cell against a same-settings librdkafka peer | KL10-02, KL10-14 |
| F3 | Codec microbenchmarks lack a realistic 1 KiB text family | JSON-like 1 KiB payloads at two batch sizes, plus a raw-backend baseline | KL10-03 |
| F4 | In-client deflate costs almost 2× deflating the same bytes in one call | Measure batch sizes, write granularity, encoder re-initialization and level; split the fixes | KL10-04 |
| F5 | miniz deflate is no faster than C zlib; zlib-rs is 1.8× faster | Backend policy decision, then an opt-in backend | KL10-05, KL10-06, KL10-15 |
| F6 | gzip decode uses 7.8× librdkafka's CPU per record | Decode-then-discard (re-probe 2026-10-01): decompress only applied batches, then keep the rest instead of re-fetching; pre-sized output (rejected) and off-caller decode | KL10-16, KL10-17, **KL09-34**, KL10-09 |
| F7 | No owned, eagerly admitted per-record completion | Specify, then implement, an additive delivery API | KL10-07, KL10-08 |
| F8 | Consumer peak RSS is 1.4 GB, against 193 MB for the peer | Explain it against the configured budgets; fix the default or the documentation | KL10-10 |
| F9 | The claim gate has no gzip or ~1 KiB cells | Propose a contract amendment adding gateway cells | KL10-11 |
| F10 | zstd is missing | The existing decision and implementation chain | **KL05-02..05** |
| F11 | Adopters need a gateway recipe | An example and a migration-guide section | KL10-13 |

KL10-12 re-measures every gateway cell after the first wave and publishes a
dated local verdict.

## 4. Order of work

| Step | Cards | Why |
|---|---|---|
| 1 | **KL10-01** | Restores a green `main`. KL10's implementation cards and its verdict depend on it. |
| 2 | **KL09-34**, **KL05-02** (both ready now) | The largest measured loss (gzip decode), and the decision on the codec that would let some workloads avoid gzip |
| 3 | KL10-03, KL10-07 (ready now); KL10-02, KL10-14 after KL04-05/06/07 | Cells that make every later change measurable, and the API specification |
| 4 | KL10-04, KL10-05, KL10-10 | Analysis and policy, with no hot-file edits |
| 5 | KL10-06, KL10-08, KL10-09 | One hot file each: `records.rs`, `producer.rs`, `consumer.rs` |
| 6 | KL10-11, KL10-12, KL10-13 | Contract proposal, dated verdict, adoption documentation |

KL10-06, KL10-08 and KL10-09 hold different hot files, so they can run in
parallel under the KL09 section 7 locks. A KL10-05 decision to keep
miniz_oxide, or a rejected KL10-07 specification, blocks only its own
implementation card. KL10-12 still runs and lists the blocked card with its
reason.

## 5. Card index

<!-- KL10-INDEX:START -->
| ID | P | Kind | Title | Depends on | Hot files |
|---|---|---|---|---|---|
| KL10-01 | P0 | ci | Restore green main CI for the verifiable contract tests | - | - |
| KL10-02 | P1 | benchmark | Build the gateway produce cells with a same-settings librdkafka peer | KL04-05, KL04-06, KL04-07, KL09-01 | - |
| KL10-03 | P1 | benchmark | Add a 1 KiB JSON-like payload family to the codec microbenchmarks | KL09-04, KL09-05 | - |
| KL10-04 | P1 | analysis | Explain the in-client gzip deflate overhead | KL10-02, KL10-03 | - |
| KL10-05 | P1 | decision | Decide the gzip backend policy | KL10-03, KL05-01 | - |
| KL10-06 | P1 | implementation | Add the approved gzip backend as an opt-in feature | KL10-01, KL10-05, KL10-02 | records |
| KL10-07 | P1 | specification | Specify an owned, eagerly enqueued per-record delivery API | KL07-10 | - |
| KL10-08 | P1 | implementation | Implement the owned delivery API | KL10-01, KL10-07, KL10-02 | producer |
| KL10-09 | P1 | implementation | Decode compressed fetch batches off the caller task | KL10-01, KL09-34, KL10-14 | consumer, fetch |
| KL10-10 | P1 | analysis | Explain consumer peak RSS on the gateway fetch cell | KL10-14 | - |
| KL10-11 | P1 | specification | Propose gzip and gateway cells for the benchmark contract | KL10-02, KL10-14 | - |
| KL10-12 | P1 | analysis | Re-measure the gateway cells and publish a dated local verdict | KL10-01, KL10-02, KL10-14, KL10-04, KL10-05, KL10-07, KL10-09, KL09-34 | - |
| KL10-13 | P2 | documentation | Document the gateway adoption recipe | KL10-08 | - |
| KL10-14 | P1 | benchmark | Build the gateway fetch cell with a same-settings librdkafka peer | KL04-05, KL04-07, KL09-01 | - |
| KL10-15 | P1 | baseline-change | Re-baseline the gzip allocation cells for the zlib-rs default | KL10-05 | - |
| KL10-16 | P1 | implementation | Decompress fetch batches only when they are applied | KL02-08, KL09-10 | consumer, fetch, records |
| KL10-17 | P1 | implementation | Keep fetched batches the budget cannot hold instead of re-fetching them | KL10-16 | consumer |
<!-- KL10-INDEX:END -->

## 6. Boundaries

Everything in the [KL09 boundaries](performance-leadership.md#9-boundaries-unchanged-unless-the-maintainer-decides-otherwise)
applies. In addition:

- **Native peers stay in benchmark crates.** The librdkafka peer lives in
  workspace-excluded crates under `benchmarks/`. It never enters the
  `partitionline` dependency graph or package.
- **Backend changes need approval.** No gzip or zstd backend becomes a
  default, and no backend that uses `unsafe` internally is added, without
  the maintainer approval recorded by KL10-05 or KL05-02.
- **Equal settings or no comparison.** Gateway cells compare the clients at
  the same codec, level, acks, max in flight, linger, batch size and TLS
  configuration, and name each client's acknowledgment mode.
  - An enqueue rate is never reported as acknowledged throughput.
  - A fire-and-forget result is never compared with a per-record
    acknowledgment result.
- **Public, synthetic workloads only.** Cells use seeded synthetic payloads.
  Do not commit private workload names, hosts, topics or traces.

## 7. Done when

- `main` CI is green and stays green for every KL10 change.
- On the gateway cells, partitionline's CPU per record is at most
  librdkafka's for gzip produce and gzip consume at matched settings and
  load, with p99 no worse.
- Per-record acknowledgment costs no more than 10% over `try_send` on
  `lb-gateway-produce-ack`.
- KL10-12 has published its dated per-cell verdict, and the maintainer has
  accepted or rejected KL10-11's contract proposal.

Until then, the measured statement is: **"higher throughput than librdkafka
at saturation on this profile; more CPU per record at a fixed rate, and
much more on gzip consume."**
