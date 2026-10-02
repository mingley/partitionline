# KL04-05 Rust wrapper benchmark adapter evidence

The independent `benchmarks/peers/rust` workspace builds and runs the pinned safe Rust `BaseProducer` wrapper against the independently pinned C installation. This evidence validates the adapter, its accounting, and its independent receipt auditor. It does not qualify any frozen performance cell or establish a performance win. Suite HOLD remains active.

## Source and dependency pins

- Reviewed metadata source: `6b4d3306fd517decbc01839ae25835c695de7572`.
- Compiled source and complete behavioral gates: `9c6a9b3539bd0bf3eae8dc851d18186029a656b8`.
- Claim/tested base: `e054ce0cdcc68a46e9688ca56f2698d45de6595e`.
- Earlier source `18194a1a3635ac82b6eaeb4958fd44a318911d32` and its results are retained, with the registry tier/RTT mismatch explicitly superseded.
- `rdkafka` wrapper `0.39.0`, MIT, declared MSRV 1.74, crate source commit `598ac4ba1f714852bdf4e5685fe10cf5a66e947c`; `rdkafka-sys` `4.10.0+2.12.1`, MIT, declared MSRV 1.74, crate source commit `47a17d8de72b8bfa89589a84a4a0c600c54df1e0`. Both disable default features and enable dynamic linking.
- Actual loaded C library `2.15.0`, source commit `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`, tree `429a83d1ba5350c0846aeaee4efe821b463f6b85`; library SHA256 `8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc`. The sys header baseline `2.12.1` is reported separately from the actual loaded native version. Runtime verifies both the native version and the mapped library SHA256 using Linux procfs.

`dependency-proof.json` records all 44 resolved peer packages (43 registry dependencies), licenses, declared MSRV, features, checksums, and source SHA256 hashes. It proves the client Cargo lock and graph sections are unchanged from the claim base, the peer has its own workspace, and no native Kafka dependency enters the client graph. The peer Cargo graph has no Tokio, libz-sys, zstd-sys, or openssl-sys; C/OpenSSL/zlib/zstd remain explicit external benchmark installation dependencies. The retained build manifests pin the existing native compiler/configuration as well as wrapper/binding/runtime versions.

The final source changed only peer README facts and scoped registry metadata relative to the compiled source. `final-6b4d3306/result-validation.json` hashes every unchanged peer input, records the original binary and build-manifest hashes, and explicitly identifies reused compilation. Shared helper hashes are pinned too. No rebuilt binary is claimed at the metadata-only source.

## Gates and live correctness controls

`final-9c6a9b35/commands.json` records 25 orchestration commands with actual and expected exit codes, affinity, settings, and hashed logs. Its two build steps contain ten Rust gates: release build, locked tests, format check, all-target Clippy with warnings denied, and rustdoc with warnings denied on each lane. Stable is rustc `1.99.0 (b940084d7 2026-09-28)`; MSRV is `1.85.0 (4d91de4e4 2025-02-17)`. Each lane passed seven Rust tests and eleven Python integrity tests: 36 named test executions across both lanes. Cargo doctests also passed with zero doctests. Builds used one job, no incremental compilation, debug information disabled for development/test profiles, and CPU affinity `0-2,4`.

The bounded Kafka 3.9.1 KRaft fixture used six partitions, RF=1, loopback ports 19094/19095, a 256 MiB heap, and unique control topics. Two separate exploratory 256-record receipt smokes and a 32-record queue-full smoke produced 544 successful measured acknowledgements and 544 independently observed records, with exact IDs, keys, values, SHA256 hashes, partitions, offsets, and fence deltas matching. The two normal smokes also acknowledged 64 warmup records, excluded by the measured fences. Queue-full retries retain the same record IDs and produce a clean history. These small exploratory workloads are not frozen scenarios.

The independent auditor uses pinned Apache Kafka Java client `3.9.1`, build commit `f745dfdcee2b9851`, jar SHA256 `25c5e4eb059c35766f645c0e0bd2fe623a1ebdc18250957506b1edbf476d1272`, on JDK 21.0.12.1. It directly assigns bounded partition fences and emits the actual key/value bytes and offsets; it does not regenerate expected records. Python independently regenerates expected records and invokes the shared record-history gate. Two altered histories are rejected with counterexamples. Java compilation uses the installed `jdk.compiler` module; the initial missing `javac` launcher failure remains retained.

Other retained controls distinguish local enqueue acceptance from broker acknowledgement:

- The oversized-record control accepted 32 records locally but received 32 callback failures, zero successful ACKs, zero independently consumed records, and zero fence delta. Its failed artifact passes JSON Schema and is rejected by the clean report gate.
- The ACKS=0 control accepted eight records locally and retained eight unknown outcomes, zero successful ACKs, and zero reported acknowledged throughput. A nonzero offset delta is not converted into successful acknowledgements. Its failed artifact is rejected by the clean report gate.
- Invalid idempotence settings and an invalid tier fail before workload traffic. A same-version native library with a different SHA256 is rejected before client configuration/traffic.

`final-9c6a9b35/result-validation.json` pins JSON Schema validator 4.26.0, source cleanliness, artifact SHA256 hashes, and clean-report results. Nonzero controls remain failed artifacts; they are never replaced by clean scores.

`final-6b4d3306/commands.json` records six additional successful commands: both lanes file the exact required frozen scenario as `not_run`, file share as `unsupported`, and independently recheck runtime/effective configuration. Four corrected artifacts pass schema, registry assertions, and the clean report validator. The frozen filing uses required tier, RTT 0.1 ms, 8,000,000 measured records, 10,000 warmup records, and five repetitions; no broker workload is attempted. The schema-required positive duration is the actual metadata-filing wall duration, explicitly labelled with `measured_phase_present=false`. All throughput/acknowledged/consumed counts remain zero.

## Native share support and pure Rust exclusions

`native-share-audit/` preserves the exact pinned native header and CHANGELOG, their Git/source hashes, and 28 installed `rd_kafka_share_*` dynamic exports. Native C 2.15.0 supplies preview KIP-932 support. The earlier inherited claim of native absence was false and is explicitly retained in `development-failures.json`; the corrected source and filings supersede that wording. The pinned wrapper/sys APIs and current Rust/C benchmark drivers have no implemented share lifecycle, and the Kafka 3.9.1 fixture is ineligible. No share behavioral validation or qualification was performed.

`candidate-sources/` preserves independently verified crate archives for `rskafka` 0.6.0 and `kafka` 0.10.0; `pure-rust-candidates.json` in the adapter pins their commits and inspected source hashes. No pure Rust peer is eligible for the unchanged frozen cells. In particular, rskafka has fixed ACKS=-1, no idempotent/transaction/group/share implementation, fixed read-committed fetch/list-offset requests, and no TCP_NODELAY control; the legacy kafka candidate also lacks the needed modern semantics and uses per-partition rather than the frozen total fetch-byte cap. The audit recognizes rskafka's actual batch/linger, optional Rustls, and SASL implementations rather than inferring absence from stale prose. Candidates are excluded for precise setting/semantic gaps, not installed as client dependencies, and not presented as wins. Selection remains blocked.

## Limits and retained failures

The safe wrapper/API bar is distinct from the C-only bar. Its producer, callback, hashing, allocation, and artifact work do not isolate wrapper overhead. The current adapter does not implement open-loop scheduling, standalone fetch, transactions, KIP-848 groups, or share lifecycle. TLS/SCRAM producer settings are generic configuration support; the independent receipt audit is plaintext-only, and no frozen security cell is qualified. Linux procfs runtime verification is required. Offer/flush/audit and each inspection call have bounds; this is not a global real-time service guarantee. No idempotence-sequence or transaction-visibility claim is made.

The shared peer generator deterministically matches IDs/keys/values with the existing peers, but its ID-bearing 16-byte keys do not establish the frozen UUID key shape/distribution. This remains a campaign qualification blocker. Frozen profiles, named knobs, prior peer configurations, and suite hold are proven unchanged in `dependency-proof.json`. The full 8M/10k/five-repetition campaign is unexecuted, and every unsupported/not-run cell remains excluded from scoring.

`development/`, `development-failures.json`, `final/`, and the supersession records preserve successful and unsuccessful development attempts, including strict-lint failures, history exception handling, missing Java compiler launcher, interrupted first broker startup, zero-duration schema failures, incorrect evidence-script literals, the earlier tier/RTT filing mismatch, and the corrected native share claim. No successful development run replaces a failed artifact.

`broker-lifecycle.json`, `broker-validation.properties`, and `broker-validation.log` preserve the owned fixture configuration and graceful shutdown. Only owned PID 193959 was sent SIGTERM after validation; the foreground session returned exit 143, with completed shutdown in the log. Shared broker data was not reset, topics were not deleted, and the original native installation/data/logs remain intact. `checksums.json` seals the retained evidence files.
