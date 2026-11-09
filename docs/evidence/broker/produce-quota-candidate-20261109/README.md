# Produce quota draft checkpoint

Status: isolated draft, not qualified for merge or release. This is an opt-in,
router-wide, in-memory token bucket, not Kafka's sampled per-user or per-client
quota. Restart resets debt. Produce request header and body bytes are charged;
the four-byte length prefix is excluded. The displayed delay is capped at 60 s
while excess debt remains. A capped per-connection mute and shared debt do not
provide instantaneous rate enforcement or fairness across concurrent connections.

The actor samples time after processing the request or a grouped Produce batch.
Reply delivery and response writes consume the resulting absolute interval, so
the remaining mute after the response can be shorter than its throttle field.
A quota accounting or response-control error can occur after an append has
committed. Its delivery outcome is ambiguous; retries may duplicate records
without an applicable idempotence guarantee. The supplied transport must honor
the deadline even for acks=0, and a custom dispatcher must enforce it, its
authentication lifetime, and shutdown itself.

## Sources

- Baseline: `a7b6edc1645e3a4789cf699a3b9e7656c55a3d10`.
- Clean isolated source commit: `50b5de969a14f11f52df6da36bf2037d4f61de60`,
  branch `qualification/produce-quota-timing-clean-20261109`, based on the
  baseline. It contains only the six production/test files. Broker tree
  `4870c22abcbaf381df615be34af2e4d4180d8721` matches draft
  `2396d443029274de9afd9fc90f7c7680f1a2e697`.
- Production blobs: `src/metadata.rs`
  `8a74915548731d2cbfcfb5af326bff7e2287b3b1`; `src/produce.rs`
  `5338acb770c23094c4bd7418835e10ce8ec42904`; `src/transport.rs`
  `ab9ec08cea10495f44a5abfd9e87a04a683ce8c8`;
  `src/metadata/bandwidth.rs` `93057aa5e51a217ae780eacd33ed21bcf53e9f9d`.
- Test blobs: `tests/produce_quota.rs`
  `001c1824705a329c9082188f4ed8d099a22bed37`;
  `tests/sasl_sessions.rs` `24924aca1f4d7d014fd011d228ce2f59583c727c`.
  Broker `Cargo.lock` is `19652acd96b929402b1e4a9394a33d12a7e69449`.

## Checks

On stable Rust 1.99.1, x86_64, [run 42685986082](https://github.com/mingley/partitionline/actions/runs/42685986082)
passed both default and all-feature focused controls on the preceding,
unformatted test revision `56aa28c20616c5d2df9568a4adfc3ce14efec2bc`.
They cover all 33 combinations of Produce versions 3–13 and acks
-1/0/1 through independent Apache request fixtures, versioned response
fixtures where a response exists, acks=0 socket pauses for versions 3/10/13,
and a SCRAM-SHA-256 authenticated socket with both normal resumption and
authority expiry during mute. Other controls cover the exact deadline boundary,
stale deadlines, overflow, shutdown, OIDC lease revocation, actor-owned grouped
charging, dropped replies and work rejected before admission. A deliberate
post-append response-field corruption yields a failed quota mark while the
append remains visible; this is not an externally induced socket error.

The full-layout run [42683068504](https://github.com/mingley/partitionline/actions/runs/42683068504)
on earlier source `ad49b981c3318647d0ce734c32795166713a8ea8` passed both
generic all-target behavioral suites, Rustdoc and doctests, but failed format
and strict Clippy. The next full-layout run
[42684875590](https://github.com/mingley/partitionline/actions/runs/42684875590)
on `ddcd3c0184772ade7d5aad91ca4477c2a54ea70c` passed format, the all-feature
behavioral suite, Rustdoc and both doctest steps, but its default behavioral
step and strict Clippy failed. The default failure's cause is not established.
The two reported quota-test lint diagnostics were addressed in the expanded
revision; this is not yet a passing strict result.

Formatting run [42686571124](https://github.com/mingley/partitionline/actions/runs/42686571124)
passed after formatting the touched files. Focused run
[42686910954](https://github.com/mingley/partitionline/actions/runs/42686910954)
and full-layout run [42687057356](https://github.com/mingley/partitionline/actions/runs/42687057356)
both passed on `97c59436a17b4886eadee0e6a0839c7ea1668d3b`: the latter passed
format, both all-target behavioral suites, strict all-feature Clippy and
Rustdoc, and default/all-feature doctests. The preceding `eb359bdfd8973a727bb2df1f109ef9ab3d0480a8` adds
fairness/rate documentation; exact-source focused run
[42688066532](https://github.com/mingley/partitionline/actions/runs/42688066532)
and full-layout run [42688205776](https://github.com/mingley/partitionline/actions/runs/42688205776)
both passed. The full run passed format, both all-target behavioral suites,
strict all-feature Clippy and Rustdoc, and both doctest steps. The current
version-timing revision has additional documentation and socket controls;
its exact-source focused run [42689370992](https://github.com/mingley/partitionline/actions/runs/42689370992)
passed, including v3/v5/v6 socket timing. Full-layout run
[42689508416](https://github.com/mingley/partitionline/actions/runs/42689508416)
passed format, the default behavioral suite, strict all-feature Clippy and
Rustdoc, and both doctest steps. Its generic all-feature behavioral step failed;
one target reported 1 passed and 1 failed. None of the specifically named new
quota controls was reported failed. The failure was not investigated and its
cause is not established. The earlier default failure remains unresolved.
A downstream legacy Handler/ExchangeOutcome sample
passed on baseline and candidate in
[42684875746](https://github.com/mingley/partitionline/actions/runs/42684875746);
it does not cover every downstream implementation or method-name collision.

## Version-dependent response timing

Apache Kafka 4.3.1 resolves to commit
`26b251a451ce941d3d7a55e6487bcb7f16b5ad48`. Its
[ProduceResponse client check](https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java#L294-L297)
enables client-side throttle only for Produce version 6 and later.
[AbstractResponse](https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/clients/src/main/java/org/apache/kafka/common/requests/AbstractResponse.java#L300-L306)
relates that decision to brokers returning responses before throttling. Thus
v3–v5 clients do not independently delay sending based on the response; their
historical interpretation differs from v6+.

The pinned broker's [Produce success callback](https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/core/src/main/scala/kafka/server/KafkaApis.scala#L487-L527)
mutes the channel and sends the response immediately without a version branch.
[KIP-219](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/74685726/KIP-219%2B-%2BImprove%2Bquota%2Bcommunication)
describes older clients sending their next request while the broker channel is
muted; the KIP page is a mutable design record. Source review therefore does not
establish that a Kafka 4.3.1 broker delays v3–v5 responses.

The draft writes the response before waiting for its absolute pause at every
supported Produce version. The response-byte fixtures, the draft's socket
timing controls, and source review are separate evidence. No live Kafka
broker/client timing comparison has been run, so timing or quota parity is not
established, particularly for v3–v5 clients. The current revision's v3/v5/v6 draft socket controls passed in
[42689370992](https://github.com/mingley/partitionline/actions/runs/42689370992).

## Paired no-quota samples

All samples below are elapsed seconds for matched baseline and candidate runs
on one hosted Intel Xeon Platinum 8370C, x86_64, Rust 1.99.1. The unknown-topic
workload uses 20,000 sequential Produce dispatches per sample
([42684875746](https://github.com/mingley/partitionline/actions/runs/42684875746)).
The successful existing-topic workloads use 320 measured requests after 32
warmups ([42686379854](https://github.com/mingley/partitionline/actions/runs/42686379854)).
Each successful sample checked response success and all 352 distinct offsets.
The burst workload submitted requests eight at a time; actual actor group
formation was not instrumented. These are short, noisy hosted-runner samples,
not a controlled regression bound, Kafka comparison, or fastest-performance
evidence.

| Pair | Unknown baseline | Unknown candidate | Append baseline | Append candidate | Burst baseline | Burst candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.405908684 | 0.426555662 | 0.084897923 | 0.087784212 | 0.014139642 | 0.016241244 |
| 2 | 0.412771840 | 0.418325557 | 0.094917945 | 0.091453216 | 0.014908036 | 0.014667280 |
| 3 | 0.412539125 | 0.407280735 | 0.081222044 | 0.084916162 | 0.015577170 | 0.015057725 |
| 4 | 0.435967305 | 0.431298877 | 0.064155124 | 0.069384641 | 0.015588365 | 0.014892054 |
| 5 | 0.337814258 | 0.421072658 | 0.068362298 | 0.066300068 | 0.014597457 | 0.014239540 |
| 6 | 0.417730834 | 0.408743722 | 0.073227384 | 0.071026174 | 0.014571250 | 0.014535674 |

The benchmarked production logic matches the clean commit; its broker tree
predates later test revisions and documentation. Compared with measured
`ddcd3c0184772ade7d5aad91ca4477c2a54ea70c`, the current `metadata.rs`,
`produce.rs`, and `transport.rs` blobs are identical. The only changed
production file is `metadata/bandwidth.rs`: stripping `///` documentation
lines from measured blob `8e4176496062b2f1295ed73a37011d6055ca83e6` and
current blob `93057aa5e51a217ae780eacd33ed21bcf53e9f9d` leaves identical
bytes. Performance results remain tied to their originally pinned trees. Independent
reproduction, sustained load, latency distributions, real Kafka peers,
TLS/OIDC authenticated quota traffic, quota fairness and production
fault/restart campaigns remain unqualified.

## Retained failed and unavailable attempts

Three early draft attempts [42671569976](https://github.com/mingley/partitionline/actions/runs/42671569976),
[42672008026](https://github.com/mingley/partitionline/actions/runs/42672008026)
and [42672908995](https://github.com/mingley/partitionline/actions/runs/42672908995)
failed with no jobs and no source result. The same is true for workflow
attempts [42682559098](https://github.com/mingley/partitionline/actions/runs/42682559098)
and the unintended duplicate [42683064167](https://github.com/mingley/partitionline/actions/runs/42683064167);
their causes are not established.

[42678458491](https://github.com/mingley/partitionline/actions/runs/42678458491)
passed the all-feature library check but unit-test compilation failed on a
changed Produce helper return type. [42678923098](https://github.com/mingley/partitionline/actions/runs/42678923098)
compiled and ran controls but failed a socket-readiness assertion.
[42679655526](https://github.com/mingley/partitionline/actions/runs/42679655526)
did not compile its new diagnostic test, and
[42679940311](https://github.com/mingley/partitionline/actions/runs/42679940311)
showed readiness with no available data in that observation. The revised
control reads an actual byte through the deadline and checks handler admission;
that later pass does not erase the earlier outcomes.

[42682562407](https://github.com/mingley/partitionline/actions/runs/42682562407)
used an incomplete isolated layout, so the generic behavioral tests did not
compile and its Clippy configuration was incomplete.
[42682743740](https://github.com/mingley/partitionline/actions/runs/42682743740)
passed the compatibility sample but its temporary benchmark binary failed to
compile because it lacked required crate documentation. No performance result
came from that attempt.
