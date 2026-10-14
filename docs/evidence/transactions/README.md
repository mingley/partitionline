# KL03-20 transactional crash and fencing campaign

Frozen plan: `campaign-freeze.json`. Consume-transform-produce on native
Apache Kafka 4.1.0 RF=3 / min.ISR=2.

Cases crash before/after produce ack, after offset staging, before EndTxn,
and during EndTxn, plus explicit abort and replacement-producer fencing.
A `read_committed` partitionline consumer and the pinned Java
`kafka-console-consumer --isolation-level read_committed` must agree.
Transaction timeout (15s) is separate from leader-recovery (20s).

No exactly-once claim for external application effects. Live run: 8/8
histories valid on `0f350cc5`, Java `read_committed` agree, `fence_replacement`
replayed on a fresh topology. `crash_during_endtxn` committed atomically.
Replacement fencing surfaced as `INVALID_PRODUCER_EPOCH` (47). See
`summary.json`, `campaign-report.json`, and `docs/plan/evidence/KL03-20.json`.
