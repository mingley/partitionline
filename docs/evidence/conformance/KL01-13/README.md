# KL01-13 selected librdkafka regression evidence

The selected normative source is librdkafka **v2.15.0** commit
`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`,
`tests/0125-immediate_flush.c`, SHA-256
`1528ed781bf3aae5f1ea0a503e0059bd6c6de5c7f529279cbdda2445f423eb8e`.
The original `main_0125_immediate_flush` function ran unchanged with an explicit
helper shim. Its Rust equivalent ran the same 50-record natural-linger phase and
50-record explicit-flush phase. API/harness adaptations are documented in
`tests/conformance/librdkafka/README.md`. The sibling brokerless metadata case and
the remaining C/C++ suite were not run.

A native Apache Kafka **3.9.1** KRaft broker listened only on
`127.0.0.1:19104` (controller `19105`), using a separate scratch data directory.
Every client/variant received a fresh unique topic with one partition, RF1 and
minISR1. Clients used acks1, plaintext, 10000-ms linger and identical unique keys
and 50-byte values; inputs/fault schedules and independent receipts are retained
in every history. The C consumer independently verified IDs, exact payload bytes,
partition/offset order and broker HW100 for both producers. KL03-18's shared
checker accepted all actual 100-record histories, including those with timing
failures. This prevents timing/receipt correctness from being conflated.

`run-01/` retains the first complete observed run. It records then-current main
HEADs and `source_clean=false` during parallel work, plus adapter and binary
hashes. `run-02/` repeats the selected case after freezing build inputs: its
`behavior-build-manifest.json` contains actual core and adapter source hashes,
source revision/cleanliness, compiler versions, native pin and binary hashes.
The source digest was identical before and after compilation. Later main commits
or worktree edits do not change that frozen binary's provenance.

Final `run-02` observations, with unchanged upstream bounds:

| Client / variant | Natural delivery (10000–15000 ms) | Flush (0–2500 ms) | Behavior / history |
|---|---:|---:|---|
| librdkafka normal | 10005.037 ms | 1.049 ms | pass / pass |
| partitionline normal | 10013.230 ms | 2.189 ms | pass / pass |
| librdkafka omit-flush | 10003.967 ms | 10003.938 ms | **fail** / pass |
| partitionline omit-flush | 10012.621 ms | 10014.150 ms | **fail** / pass |

All four runs acknowledged and independently consumed 100 unique IDs with
identical payloads, ordered offsets 0–99 and HW100. The omit-flush mutants exit 1
because they violate the original second-phase bound; their successful receipt
histories do not erase that failed behavioral assertion. This deliberate adapter
mutation is not reported as a client defect. Durations above assess the selected
behavioral bound, not client ranking or performance qualification.

The separate `synthetic-duplicate-loss.history.json` retains count100 while
replacing the last receipt with its predecessor. The checker exits 1 with a
minimal counterexample naming the lost `m-099`; both the missing and duplicate
IDs remain visible. This synthetic negative is evidence about checker rejection,
not actual client loss. Each run's `conformance-attempts.json` includes every normal
and mutant result. Its selected-case aggregate exits 1 and remains failed because
the two deliberately failed attempts are preserved.

`validation.json` re-evaluates the measured durations against the fixed original
bounds and verifies every recorded raw/history/result companion checksum and
length. Four artifact guards pass, including rejection of widened bounds, forged
pass flags and incomplete receipt evidence. All 45 history-checker tests and all
29 conformance-report tests pass. Source/C compiler checks, Rust offline locked
build, shell syntax and `git diff --check` pass.

Run the executable scenario as documented in the adapter README. Every new
invocation requires a new output directory, retains logs and per-record events,
and never overwrites an earlier attempt. Source provenance for the baseline C
library is inherited from KL04-04; no second native library stack or main-crate
runtime dependency was introduced. These checks establish only this selected
assertion. They do not confer a full C/C++ suite pass, a broader compatibility
claim or a Suite HOLD lift.
