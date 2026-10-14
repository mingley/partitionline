# KL03-20 transactional crash and fencing campaign

Frozen plan: `campaign-freeze.json`. Consume-transform-produce on native
Apache Kafka 4.1.0 RF=3 / min.ISR=2.

Cases crash before/after produce ack, after offset staging, before EndTxn,
and during EndTxn, plus explicit abort and replacement-producer fencing.
A `read_committed` partitionline consumer and the pinned Java
`kafka-console-consumer --isolation-level read_committed` must agree.
Transaction timeout (15s) is separate from leader-recovery (20s).

No exactly-once claim for external application effects. See
`docs/plan/evidence/KL03-20.json` after the live run.
