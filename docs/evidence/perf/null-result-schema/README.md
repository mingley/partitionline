# Null-broker result files

KL09-73 is complete. The report CLI now applies the entire JSON schema and keeps
its outcome, unit, integrity and finite-number checks. Missing schema files or
validator dependencies are errors.

The compiler source is `39bb8fa3c6531113c109acc682f24b00c83f104c`, built with
stable Rust 1.99.0. The controller and validator source is
`dda2076d1b33b49c1f97c16c0ad8f079ca79a87f`. The Rust source stayed unchanged
between those commits; Python validation gained the finite-number check.

Three actual jobs passed both the full schema and report CLI:

| Job | Offered | Acknowledged | Consumed |
| --- | ---: | ---: | ---: |
| nb-send-seq | 1,000 | 1,000 | 0 |
| nb-fetch-bulk | 20,000 | 0 | 20,000 |
| nb-connect | 6 | 6 | 0 |

Each effective-configuration sidecar matches its hash and the result settings.
All broker endpoints rebound after the waited jobs, which left no child process
groups. These are format checks, not performance comparisons.

A retained original native result passes the new schema and report CLI. A
retained original incomplete null result fails both. Their bytes are copied in
original-inputs; the earlier successful report receipts and schema failures stay
in the immutable build-profiles evidence. Neither original capture was changed.

Twenty changed-result controls ran the actual report CLI and returned exit one.
Twelve violate schema fields: missing config paths, wrong broker modes and wrong
phase labels. Eight test semantic accounting or non-finite numbers. All actual
failed attempts remain, including omitted sparse-checkout inputs, an absent
standalone producer binary, and the first Python diagnostic compatibility test.
Later passing runs use separate receipts.

The harness passes 26 Rust runtime tests, formatting and strict Clippy. All 42
Python benchmark tests pass, including 27 report tests. Four settings tests use
a separately identified retained benchmark executable. Existing UpdateFeatures
checks pass 16 selected tests plus two safety regressions per build on default
and all-feature configurations; this does not finish KL01-28.

Schema v1 remains strict for Kafka runs. Null schema v2 is restricted to
exploratory fixtures. It records absent durability, Kafka cluster identity,
warmup, steady-state/campaign duration and independent sequence audits honestly;
fetch producer options and produce isolation are not applicable. The user guide
is [benchmark-results.md](../../../benchmark-results.md).

The manifest maps retained commands, logs, source snapshots and executables to
original paths and hashes. Large files use deterministic gzip. Both stored and
decoded hashes were verified. No library defaults, previous measurements,
scenario qualification or ranking changed.
