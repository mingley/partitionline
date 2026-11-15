# Data request buffer capacity

The current scoped candidate is `a72fbd02af1eae4027f58fcaeb9b372457dfcc83`, based on `f092d69d43ebb300c756cca006f4964c9820e491`. Its five changed Rust source blobs and inventory blob are identical to the previously measured `fea406c9b12e28e322cea966ebb34336fca48c81` candidate based on `4409633fa989effe1083e623f6e59fcc0d179eee`. This does not establish a process RSS bound or production readiness.

## What was observed

On the earlier source `6298f6bfbed2ab98189d291ebebd2190a2ee75b6`, the public `Handler::exchange_request` accepted a valid 31-byte Fetch and an 8,192-byte spare allocation with one 4,160-byte logical data slot. In [run 42457484148](https://github.com/mingley/partitionline/actions/runs/42457484148), the actor directly observed that spare both before and after caller cancellation while its permit remained held. A separate control showed the permit remained held for an unconsumed reply, then the 8,192-byte response allocation transferred to the caller when the reply was consumed. These are different ownership phases.

## Candidate behavior

The candidate keeps the existing logical admission budget. Before enqueue it compacts an input Vec whose capacity exceeds the configured request maximum, and it discards scratch whose capacity exceeds the response maximum. It rechecks the actual replacement capacity after reservation. Input bytes are preserved; inability to allocate within the limit returns the existing protocol allocation error before enqueue.

Copies above 256 KiB yield between chunks. The admission timestamp is captured first, so this time counts toward the Produce request timeout and Fetch wait where that timestamp is used. Stopping is checked at each yield, and cancellation or shutdown drops the original and replacement Vecs before the permit. A suspended normalization future can retain both allocations until normalization finishes or the future is dropped. Allocator reservation and deallocation may still block, and yielding does not guarantee when another task will run.

A non-immediate Fetch keeps a response allocation for the next attempt only after checking its actual capacity. Parked response and scan caches are separately checked against their configured response and scan limits. A completed response can still have capacity above its logical limit because a builder or allocator may over-reserve. The completion path preserves that result instead of adding a potentially failing allocation after a durable Produce or DeleteRecords operation. Pre-reserving and checking the response allocation before mutation remains open work.

The request limit accepts at most 64 MiB and defaults to 8 MiB. While normalization is in progress, the caller-supplied original Vec can have arbitrary capacity and overlap a replacement of up to the request limit. Parser structures, other actor caches, metadata requests, allocator overhead, active builders, transport buffers and caller-owned memory are not covered by the logical budget. Consuming a reply transfers its Vecs to the caller and releases the permit.

## Controls

[Run 42459725831](https://github.com/mingley/partitionline/actions/runs/42459725831) exercised the combined source before the test-lint repair in default and all-feature profiles. Controls include a forced allocation failure and a test-only over-reservation that actually expands the replacement beyond the cap; both reject before enqueue and drop buffers before the permit. A valid oversized input is then accepted with identical bytes and a valid Fetch response. Another synthetic actor supplies an over-capacity response to verify it is discarded before a long-poll wait. A 1 MiB copy is canceled and stopped at each of its three yield points, with drop probes verifying both copy buffers drop while the permit is still held. The response-completion control retains an oversized result unchanged, while discarding oversized scratch, so it does not add a late output allocation.

The exact combined source `fea406c9b12e28e322cea966ebb34336fca48c81` is tested in [run 42459876440](https://github.com/mingley/partitionline/actions/runs/42459876440). The default all-target suite passed 774 tests with 3 ignored across 36 targets. Formatting, all-feature Clippy, all-feature Rustdoc, both doctest profiles and the inventory check with 55 negative controls passed. The all-feature library and 23 further targets passed 774 tests with 3 ignored; the next target, `raft_runtime`, passed 10 tests and failed one. The five-peer observer timed out after 15 seconds at `observer-addition-all`: node 4 reported term 125, Candidate, commit 3; the other nodes reported term 33 and commit 4. Its cause is unknown. Cargo therefore did not run `raft_snapshot`, `records`, `retention`, `sasl`, `sasl_credentials`, `sasl_sessions`, `segments`, `tls`, `transport`, `upgrade_campaign`, or the `oidc_live_probe` example in that profile.

The earlier candidate `042ef205e95215e2de8af06d47dd3074c08b6002` had [failed full gates](https://github.com/mingley/partitionline/actions/runs/42459565532): strict Clippy identified two test-only issues, the default library run failed four Raft tests with one `Deadline` and three `Node(NotLeader)` results (observer-prefix-proposal and fault-after-process-exit-proposal phases). The all-feature Raft integration target failed four tests with `Node(NotLeader)` at observer-addition and fault-delayed-old-reply-proposal. The Clippy issues were repaired in `fea406c9`. The failures are retained; they are not attributed to the buffer change. Both full behavioral profiles passed on the intermediate `429504941b4f28f98fcf8f9fe547a84d54f798c3` in [run 42459733449](https://github.com/mingley/partitionline/actions/runs/42459733449), whose quality job still had the old lint failures. In [run 42459910043](https://github.com/mingley/partitionline/actions/runs/42459910043), the unmodified `4409633` baseline passed all 36 targets in both profiles: 764 passed and 3 ignored by default; 912 passed and 3 ignored with all features. The baseline pass does not explain the earlier failures.

## x86 timing

In [run 42458735198](https://github.com/mingley/partitionline/actions/runs/42458735198), synchronous candidate `9114b10` and baseline `1036a39` ran eight alternating pairs on a shared AMD EPYC runner. Each pair measured 5,000 single-client in-budget Fetch exchanges and 1,000 oversized-capacity exchanges, validating response bytes. Candidate/baseline elapsed time per exchange had medians and ranges of 0.984 (0.939–1.032) in budget and 1.013 (0.972–1.035) for 31-byte inputs with 8 KiB capacities. A separately timed 64 MiB normalization took 28.573–45.310 ms; the median of pair medians was 29.678 ms. Timing includes reservation, copy and old-source drop, with source setup excluded.

A separate [manual-poll witness](https://github.com/mingley/partitionline/actions/runs/42459114862) on an Intel Xeon 8370C evaluated cooperative chunks without Tokio. For 64 MiB copied in 256 KiB chunks, 2,560 polls across 10 trials had a 116 µs median, 150 µs p99 and 2.153 ms maximum; median total time was 32.110 ms. This is not a hard poll-time bound, and the different host and executor prevent a direct comparison with the synchronous result.

[Run 42459578231](https://github.com/mingley/partitionline/actions/runs/42459578231) compared cooperative source `042ef205` to `4409633` on a shared AMD EPYC 7763 runner. The candidate/baseline elapsed medians and ranges were 1.006 (0.988–1.018) for in-budget exchanges and 1.017 (0.999–1.048) for 31-byte oversized exchanges. Its 64 MiB direct normalization took 39.706–54.387 ms across samples; the median of pair medians was 41.120 ms and includes yields. The paired harness, added adapters, sources and binaries are hashed in that run. These measurements cover one small workload on shared x86 hosts and do not establish general throughput or relative Kafka performance.

The exact `fea406c9` benchmark is [run 42459886564](https://github.com/mingley/partitionline/actions/runs/42459886564). Eight alternating pairs completed on a shared AMD EPYC 7763 runner with rustc 1.99.1 and CPU affinity 0,1. Candidate/baseline elapsed time per exchange had a median of 1.004 and range 0.994–1.046 for the in-budget fixture, and median 1.004 and range 0.971–1.024 for the oversized fixture. Every response and copied input was checked. Source buffers for the oversized fixture were initialized before timing; the exchange timer includes the actor dispatch, normalization and response checks.

| Pair | In-budget baseline ns/op | In-budget candidate ns/op | Oversized baseline ns/op | Oversized candidate ns/op |
| --- | ---: | ---: | ---: | ---: |
| 1 | 22,988 | 23,056 | 22,870 | 23,425 |
| 2 | 22,540 | 22,758 | 23,360 | 23,322 |
| 3 | 22,291 | 23,317 | 23,074 | 23,522 |
| 4 | 22,637 | 22,707 | 23,101 | 23,133 |
| 5 | 22,893 | 22,771 | 23,830 | 23,537 |
| 6 | 22,649 | 22,520 | 23,150 | 23,315 |
| 7 | 22,513 | 22,835 | 23,167 | 23,418 |
| 8 | 22,880 | 22,974 | 24,067 | 23,361 |

The separately timed cooperative normalizer included reservation, copying, scheduler yields and original-source drop; its pre-initialized source setup and returned-Vec drop were outside timing. For 31 B, 4 KiB, 1 MiB, 8 MiB and 64 MiB, the medians of the eight pair medians were 90 ns, 1.463 µs, 790.419 µs, 6.466 ms and 40.630 ms respectively. For 64 MiB the observed individual range was 38.517–47.208 ms. Whole-operation time is not uninterrupted poll time.

The benchmark-only harness SHA-256 is `bb771565dcf5d8d06293cac8db1cfd22b228227e7a940d97f6dab3c9b40b15d2`; its workflow is `402db64b8fb3b966c8f184bc57118230a71cf0e2d3c386de588570b825b2853f`. Both builds received a test Peer adapter; the candidate additionally exposed its actual normalizer and the baseline adapter returned the input unchanged. The release binaries were SHA-256 `1adf2bb191afa847b517c92e17d94fe9c60377cc8a4deb55aa0959b5c6b01a91` and `2775e05d425d5508e5568242860b6a841393235db6b2f1db1553406108ccfb6d` for baseline and candidate. Raw samples, instrumented-source hashes and build logs are retained in the run artifact.

## Source pins and main-tree preservation

The candidate Git blobs are:

| File | Git blob SHA | SHA-256 |
| --- | --- | --- |
| `metadata.rs` | `59ead6db6c2189ebbb2b18c819078358012dc701` | `e9491aab041deec700db162db42bcf891470c7562c6e0247979e603c5b1cb564` |
| `fetch.rs` | `c079df490c0f7edd7af019c213814be229ae0a34` | `9419715abfd8f9f45e7cfca1bb2867350b1ab7215af915c3a30105d0a42b28fd` |
| `produce.rs` | `dcc49f5bc93502cebcca15af64fb9b1a684f75fb` | `cd61db2623164f22e114aa8dcb74dabdcb64b62271a8bd346a62f415c258dc2d` |
| `protocol.rs` | `d3272262c0e8aad49829633829bda6ad766064d4` | `c4ad923c1283bf3bcb7bd6c25e1be8a4a425c0131db0a811917df1a01b983f99` |
| `transport.rs` | `63769fdd3c8f40d9e46ff626d7da66497c40ad2b` | `7c4e52412d9963c3c0bcf478efdad3e6ca9d2e0a30ed6b912995b1399ee132d8` |

`transport.rs` is byte-for-byte the `f092d69` version plus one test-only Peer constructor. The broker manifest is byte-identical to that base. The observer diagnostic `tests/common/raft_runtime.rs` has Git blob `5a9610523130bf3d381b00cf04ebd36bd539fb05` in both `f092d69` and `a72fbd02`, preserving the landed deadline controls, test configuration and observer diagnostics.

## Combined observer diagnostic

The exact `a72fbd02` combined source is under [full qualification](https://github.com/mingley/partitionline/actions/runs/42460268120). Formatting, strict all-feature Clippy, Rustdoc, both doctest profiles and the inventory plus 55 negative controls passed. The default library target had 485 passed, 1 failed and 3 ignored: the five-node minority-partition test received `Node(NotLeader)` on node 2 at `fault-initial-proposal`. Its cause is unknown. Cargo stopped before the operator binary, all 33 integration targets and the `oidc_live_probe` example. The all-feature run passed 775 tests with 3 ignored across 24 targets, then the `raft_runtime` target passed 8 and failed 4. The three- and five-node minority-partition tests returned `Node(NotLeader)` at `fault-initial-proposal`; the three- and five-peer observer tests returned it at `observer-prefix-proposal`. The subsequent `raft_snapshot`, `records`, `retention`, `sasl`, `sasl_credentials`, `sasl_sessions`, `segments`, `tls`, `transport` and `upgrade_campaign` targets, and the `oidc_live_probe` example, were not reached. The post-failure node snapshots were gathered separately and are not an atomic cluster observation; the failures' causes remain unknown. The diagnostic first lines were:

- Default five-node fault case: node 0 `4 Follower 1 0 true`, node 1 `4 Candidate 1 0 true`, node 2 `4 Follower 1 1 true`, node 3 `4 Follower 1 0 true`, node 4 `4 Follower 0 0 true`.
- All-feature three-node fault case: node 0 `4 Candidate 1 1 true`, node 1 `2 Follower 1 1 true`, node 2 `2 Leader 1 1 true`.
- All-feature three-peer observer case: node 0 `6 Candidate 1 1 true`, node 1 `2 Leader 1 1 true`, node 2 `1 Follower 0 0 true`.
- All-feature five-node fault case: node 0 `4 Candidate 1 1 true`, node 1 `4 Follower 1 1 true`, node 2 `4 Candidate 1 1 true`, node 3 `2 Leader 1 1 true`, node 4 `4 Candidate 1 1 true`.
- All-feature five-peer observer case: node 0 `9 Candidate 1 1 true`, node 1 `8 Follower 1 0 true`, node 2 `5 Follower 1 1 true`, node 3 `8 Candidate 1 1 true`, node 4 `1 Follower 0 0 true`.

No retry is being used to erase these results.

The [matched benchmark against `f092d69`](https://github.com/mingley/partitionline/actions/runs/42460277400) passed its checks over eight alternating pairs on a shared AMD EPYC 9V45 runner. Candidate/baseline elapsed medians and ranges were 0.995 (0.929–1.073) for in-budget exchanges and 1.003 (0.960–1.034) for oversized 31-byte exchanges. The median of pair medians for direct 64 MiB cooperative normalization was 27.399 ms, with individual range 25.658–29.429 ms; it includes yields and is not uninterrupted poll time. The release binary hashes were identical to those listed above for the `fea406c9` run. The host differs from that run, so the absolute timings are not a direct host-controlled comparison.

A separate [public Router poll witness](https://github.com/mingley/partitionline/actions/runs/42460490528) used the actual `Handler::exchange_request` futures on a current-thread Tokio runtime. It ran six alternating pairs on a shared AMD EPYC 9V74 host with rustc 1.99.1, using CPUs 0 and 1. Three trials per arm and size initialized and faulted in the caller's spare before timing its first poll:

| Spare length, capacity at least | Baseline first-poll median (range) | Candidate first-poll median (range) |
| --- | ---: | ---: |
| 64 MiB | 14.743 µs (9.263–18.207 µs) | 3.800 ms (3.605–4.370 ms) |
| 256 MiB | 16.700 µs (11.807–26.319 µs) | 16.134 ms (14.760–17.359 ms) |

There were 18 samples per arm and size. The baseline transferred the spare to the actor, so its first poll did not measure the later disposal; the candidate dropped it on that poll. This is an ownership and scheduling shift, not an end-to-end cost comparison.

The same witness timed each actual public-path poll for an initialized 64 MiB input with capacity above the 64 MiB limit. Its Fetch key/version prefix was valid, but the body was intentionally padded; the fixture measures normalization and admission, not Kafka processing. The candidate performed 256 polls per trial. Across the 18 trials, the median of per-trial poll medians was 126.924 µs and the median of per-trial p99s was 150.163 µs. The final poll, which included the final copy, original-source deallocation and enqueue, had a 4.321 ms median and 3.734–4.663 ms range. The baseline's single first poll had a 14.061 µs median and 8.933–30.455 µs range because it did not normalize the input.

Input/spare initialization, startup and cleanup were outside the measured polls. The harness manually yielded between polls, and scheduler and host conditions varied; these measurements are not a latency bound. The witness harness SHA-256 is `b0bfbb508d2a41478cd7b918ff96b529960042ec663ce47d2a26e1e5548b9c5a`, the workflow SHA-256 is `59f68aea6d634a00cc3f8615161ce26300c2984f2c95c16a007d8a3b7b6bbaa7`, and the baseline and candidate binary SHA-256 values are `4a7ba79aee78d8956026f509a5a48dae64bf554991f49f1db5e841c38bdb7b8a` and `4dd95bf249cc795dc17f4be56385b86236386c34253fdc8759de29aa2be59bf5`. Raw poll samples and build/source hashes are retained in the run artifact.

The newly synchronous drop of an arbitrary caller-supplied spare can materially stall a current-thread runtime; large allocation and final source deallocation also remain synchronous. The candidate remains isolated while the Raft failures and an ownership-preserving off-runtime disposal design are investigated.
