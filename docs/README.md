# Docs

Start at the [root README](../README.md). These pages are the rest.

| Page | What's on it |
|---|---|
| [producer.md](producer.md) | send, batches, idempotence, transactions |
| [consumer.md](consumer.md) | assign, fetch, seek, pause, offsets |
| [groups.md](groups.md) | classic, KIP-848, share groups |
| [admin.md](admin.md) | admin client, grouped by area |
| [config.md](config.md) | builders, TLS, SASL, defaults vs Java |
| [design.md](design.md) | how produce/fetch/TLS work |
| [protocol.md](protocol.md) | API versions spoken, wire gotchas |
| [gaps.md](gaps.md) | missing vs librdkafka / Java Admin |
| [STATUS.md](STATUS.md) | unsigned benches, mock e2e, closed APIs |
| [benchmark.md](benchmark.md) | how to run benches, summary tables |
| [benchmark-runs.md](benchmark-runs.md) | per-run numbers |

API details and Java method names live in rustdoc (`cargo doc --open`).
