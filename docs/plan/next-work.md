# Next work

Updated 19 November 2026. Use the latest stable Rust. The
[task registry](tasks.json) tracks status, dependencies and acceptance checks.
The [status page](../STATUS.md) records the source tested by each run.

## Client

- Kafka 4.3.1 verified mixed-topic receipts, flush, close and buffer release
  with fixed partitions on a healthy broker.
  [Live producer check](../evidence/client/producer-live-receipts-20261119/README.md)
- Extend the producer proof to sticky selection, callback outcomes, idempotent
  retry and broker faults. Ambiguous writes still need an ownership and
  producer-epoch policy. Mixed-topic
  reservation now counts partition visits toward the per-poll budget.
  Check the scheduling and throughput of wide batches against Kafka.
  [Reservation trials](../evidence/client/producer-group-reservation-20261119/slot-replacement.md)
- Expand the registered wire and live case matrix. Check negotiated versions,
  authentication renewal, group churn, transaction recovery and the independent
  consumer histories. The support matrix describes what is exercised today.
  [Support matrix](../support.md) · [Client capabilities](../gaps.md)
- Finish the 24-hour resource and fault campaign and seven-day continuity run.
  Keep failed attempts and actual process closure records.

## Broker

- Complete replicated partition data, high-watermark and min-ISR semantics,
  native KRaft, transaction marker propagation and Kafka wire quotas.
- Verify process-wide bounds with real contention, overload, graceful shutdown,
  mixed-version upgrades, backup and restore. The benchmark plan depends on the
  same durability and security behavior for each peer.
  [Broker plan](broker-implementation.md)

## Schema Registry

- Trusted HTTPS lookup passed on a pinned Redpanda registry.
  [TLS check](../evidence/schema/registry-tls-20261119/README.md)
- Complete a live Kafka round trip and credential checks. Qualify the
  remaining formats and measure memory and time for large synchronous JSON
  parses before publication.
  [Companion guide](../schema-companion.md)

## Performance and release

The [x86_64 codec gate](../evidence/perf/codec-instructions-20261119/README.md)
checks ten instruction counts. Resolve the LZ4 decode instruction miss
recorded on `51a4d86`, then repeat the codec checks on the release candidate.
[LZ4 trace](../evidence/client/lz4-main-decode-20261119/linked-code.md)

Run matched client and broker cases on a
controlled x86_64 host, with equal durability and security settings, raw
measurements, all failures and independent record verification. Repeat the
comparison on the same host or equivalent hardware. Use `--artifact-root` to
check the reported bundle lengths and digests. A profile claim requires
a win in every required case. Suite HOLD remains active.
[Benchmark contract](../benchmark-contract.md)

Run the applicable correctness, lint, docs, packaging and release rehearsal
checks on the exact release candidate. Preserve failed, skipped and ignored
checks in the evidence. Publish once the candidate meets its release gates.
[Release guide](../RELEASE.md)

The [previous handoff](https://github.com/mingley/partitionline/blob/2496b978fdaf0d3919b766dbe3401d6d7d3d9739/docs/plan/next-work.md)
retains the earlier experiments and detailed evidence.
