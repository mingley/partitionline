# Documentation map

One map, one authoritative location per kind of information. Start from
the goal table; each row names where that kind of information lives.

## I want to ...

| Goal | Authoritative location |
|---|---|
| Install the crate and run a first produce/fetch | [README quickstart](../README.md) (dependency snippet, Produce/Fetch/Groups/Transactions/Admin), then the [operator guide](guide.md) |
| Learn the guarantees (delivery, memory, outcomes) | [resource contract](resource-contract.md); API promises in [api-stability.md](api-stability.md); supported platforms in [support.md](support.md) |
| Configure clients (builders, defaults, TLS/SASL) | [operator guide](guide.md) ([TLS and SASL](guide.md#tls-and-sasl), [defaults](guide.md#defaults-that-differ-from-java)) plus crate rustdoc for `ProducerConfig` / `ConsumerConfig` / `AdminConfig` |
| Troubleshoot a failure | [operator guide troubleshooting](guide.md#troubleshooting) |
| Check protocol and capability support | [gaps vs librdkafka](gaps.md) (human-readable) and `tests/conformance/features.json` ([KL05-01 matrix](../tests/conformance/features.json), machine-readable) |
| Reproduce a benchmark | [benchmark.md](benchmark.md) (Reproduce sections) under the rules in [benchmark-contract.md](benchmark-contract.md) |

## Lanes

- **Tutorial** (learn by doing, in order): [README](../README.md)
  quickstart, then [guide](guide.md) from Produce through Admin, running
  the `examples/` programs against a local broker.
- **Recipes** (do one task): [guide recipes](guide.md#recipes) (backpressure,
  buffer ownership, cancellation/shutdown, leave/close, fetch budget,
  rebalance, exactly-once), [troubleshooting](guide.md#troubleshooting),
  [migrate from rust-rdkafka](migrate-from-rdkafka.md) for porting, and the
  [adoption pilot checklist](ADOPTION.md) for rollout.
- **Reference** (exact claims): [api-stability.md](api-stability.md),
  [support.md](support.md), [gaps.md](gaps.md), [security.md](security.md),
  [auth-refresh.md](auth-refresh.md), [resource-contract.md](resource-contract.md),
  [RELEASE.md](RELEASE.md), and crate rustdoc. Java client/method names
  appear here only as porting cross-references; Rust behavior and
  ownership explanations are authoritative.
- **Architecture** (how it works): [design.md](design.md) (producer/consumer
  pipelines, wire-format notes, compression, TLS) and
  [resource-contract.md](resource-contract.md) for byte ownership and
  outcome classification.

## Plan and history

- Task status lives **only** in [plan/tasks.json](plan/tasks.json); the
  [session guide](plan/README.md), [ROADMAP](ROADMAP.md) and
  [TODO](../TODO.md) are navigation and context. Never duplicate or reset
  task statuses in prose.
- [STATUS.md](STATUS.md) is the **historical** Suite HOLD log: dated
  entries are historical evidence, not a current signoff.
- [CIVILIZATION.md](CIVILIZATION.md) is the **historical** foundation plan,
  superseded by the session plan and the
  [2026-09-21 audit](audits/2026-09-21.md).
- Blank adopter run template: [adopter-exercise.md](adopter-exercise.md)
  (**unfilled**, not evidence). Companion designs:
  [schema-companion.md](schema-companion.md),
  [zstd-spike.md](zstd-spike.md).

## Claim-linking rule

Every capability, support, or default claim in tutorial/recipe text
links to its authoritative reference above instead of restating it.
When a claim and its reference disagree, the reference wins.
