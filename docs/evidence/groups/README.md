# KL03-21 group-protocol churn and recovery

Frozen plan: `campaign-freeze.json`. Separate classic, cooperative,
KIP-848 and share-group histories on native Apache Kafka 4.1.0.

Each protocol/profile cell is one resumable job. Churn kinds: member
loss, coordinator loss, pause, max-poll expiry and SASL PLAIN
reconnect. Timer budgets (session, heartbeat, max-poll, share lock,
pause, coordinator recovery) stay distinct. Kafka 3.9.1 KIP-848/share
cells stay explicitly unsupported. Share cells produce after the first
armed fetch because broker `share.auto.offset.reset` defaults to latest.

Live 20/20 on native Kafka 4.1.0 is in `campaign-report.json` and
`summary.json`. Each assembled history passed `check-record-history.py`.
`classic/member_loss` was replayed on a fresh topology.

See `docs/plan/evidence/KL03-21.json`.
