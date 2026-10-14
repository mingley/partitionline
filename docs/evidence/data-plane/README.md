# KL03-19 nontransactional data-plane fault campaign

Frozen plan: `campaign-freeze.json`. One profile, 1,000 seeds, Apache Kafka
4.1.0 three-broker KRaft (`tests/cluster/topology.json`, RF=3 / min.ISR=2).

Each seed produces three idempotent `acks=all` records on a pinned partition,
injects one fault after the first ack, then a manual consumer reads the
acked records. Histories are checked with `scripts/check-record-history.py`.

Faults: leader move, lost ack (leader disconnect during subsequent produce),
stale metadata (leader move then produce), broker disconnect, broker restart.

Five-second restoration is a proposed controlled-restoration target, not a
client timer promise. Failed attempts stay under `failed-attempts/`.

See `docs/plan/evidence/KL03-19.json` after the live run.
