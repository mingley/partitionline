# KL03-21 group-protocol churn and recovery

Frozen plan: `campaign-freeze.json`. Separate classic, cooperative,
KIP-848 and share-group histories on native Apache Kafka 4.1.0.

Each protocol/profile cell is one resumable job. Churn kinds: member
loss, coordinator loss, pause, max-poll expiry and SASL PLAIN
reconnect. Timer budgets (session, heartbeat, max-poll, share lock,
pause, coordinator recovery) stay distinct. Kafka 3.9.1 KIP-848/share
cells stay explicitly unsupported.

See `docs/plan/evidence/KL03-21.json` after the live run.
