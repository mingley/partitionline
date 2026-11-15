# Producer ownership baseline on 731eb97

These controls reproduce known producer histories on a newer baseline. They
do not repair the producer or qualify delivery, Kafka interoperability or
performance. The tests use yield instrumentation and a mock broker, which
change scheduling and do not establish an uninstrumented or real-Kafka
history.

## Source and runs

- Main baseline:
  [`731eb97a08445a18da0b043986ae192397ba176a`](https://github.com/mingley/partitionline/tree/731eb97a08445a18da0b043986ae192397ba176a),
  uninstrumented `src/producer.rs` blob
  `426dbdb5c5e27fdf655930a76682c5c18fb07e89`.
- [Control source commit](https://github.com/mingley/partitionline/commit/fff9bd075e51cd60af4d18beed16ad848e511143)
  and [corrected workflow commit](https://github.com/mingley/partitionline/commit/a831cf72886887373e6e8f58a4eb85f4e7289627)
  are on the isolated branch
  [`qualification/producer-ownership-baseline-731eb97`](https://github.com/mingley/partitionline/tree/qualification/producer-ownership-baseline-731eb97).
  The correction changed only the workflow; source and tests stayed identical.
- The producer has the same cfg-gated gate patch as the retained
  [2026-11-08 controls](../producer-ownership-regression-controls-20261108/README.md):
  Git blob `537f165d886b76742a5a855ce2ef2689bf2e4662`, SHA-256
  `07f306cf93669d5caedd7e27859e9f14a776a185c5ed010e9bf2fc3df4a7872a`.
  The uninstrumented `retry_one`, `requeue_pendings` and
  `stamp_sequences` regions were byte-identical between the original
  control baseline and this main baseline before adding the gates.
- The four integration test files were copied without changes from
  `7f020b5e860546178707a1f087ff0f0793acde71`. Their Git blobs are:
  `sequence_ownership_public.rs` `28f1ef21833f7467b38c41029f8f3c1f0a675668`,
  `sequence_retry_existing_yield_public.rs` `b1e8f1bce5b4a1da38f2229135153fc54dea036c`,
  `producer_ownership_regression_public.rs` `2bbf01576be3aa4d65c577bc893389d1ae6b885b`,
  and `producer_retry_regression_public.rs` `dc64b722426ad37b27805366cbb65208f5d04bbe`.
  The mock `tests/common/mod.rs` was unchanged on current main and has blob
  `4afd104e0b5e10227167d0d38328fe03dda07a0a`.
- Both runs used hosted x86_64 Linux on an AMD EPYC 7763 runner, stable rustc
  1.99.1 and LLVM 23.1.1. No performance measurement was made.

The first [run 42681345332](https://github.com/mingley/partitionline/actions/runs/42681345332)
failed because the workflow inherited Bash `-e`. The first desired negative
test exited 101 and terminated the step before the harness captured that
status. It nevertheless ran all four default observation controls and the
default abort/mixed desired control. Its remaining default desired controls
and all all-feature controls were unrun. Its retained
[artifact 13702791743](https://github.com/mingley/partitionline/actions/runs/42681345332/artifacts/13702791743)
has SHA-256 `7ce50705a7dd613e7b8799c3c1e66fd07693a559ceba3cc02b9bc9489316b80b`.

The [corrected run 42682087832](https://github.com/mingley/partitionline/actions/runs/42682087832)
disabled `-e` and ran only those previously unrun cases. It succeeded
because it required the expected negative assertions and exit 101, as well
as passing observation controls. The green workflow therefore means the
instrumented failures were reproduced; it is not a producer pass. Its
[artifact 13703770470](https://github.com/mingley/partitionline/actions/runs/42682087832/artifacts/13703770470)
has SHA-256 `a693388906cf004642718e356633288cdebcfa0570f2240d145fc124edf6a8de`.
Both artifacts were configured for 30-day retention.

## Worker cancellation and close

The gate held sequence stamping after the first 8,192 of 8,193 target
records. The mock contained only one warmup record at that point. The public
control either aborted the worker or called `close_timeout(150ms)`; it
tested one retained `send` receipt plus 8,192 `try_send` admissions
(`mixed`) and 8,193 retained `send` receipts (`all`).

In both default and all-feature profiles, the desired control failed its
terminal-callback and buffer-release assertion for all four combinations:

| Worker action | Admission | Receipts: success / error | Target callbacks: ack / error | Target bytes retained |
| --- | --- | --- | --- | ---: |
| Abort | Mixed | 0 / 1 | 0 / 0 | 65,544 |
| Abort | All | 0 / 8,193 | 0 / 0 | 65,544 |
| Timed close | Mixed | 0 / 1 | 0 / 0 | 65,544 |
| Timed close | All | 0 / 8,193 | 0 / 0 | 65,544 |

The first run supplied default abort/mixed; the corrected run supplied the
other seven combinations. The controls counted receipt errors without
identifying every variant. Callback counts are aggregate and cannot prove
exactly one invocation for each individual record. Timed close returned
`Err(Timeout)`. The callback/accounting observation was limited to four
seconds; the result does not establish a permanent leak.

The retained observation control separately recorded a closed receipt,
8,193 missing target callbacks and 65,544 retained bytes on abort or timed
close. After abort, a replacement worker attempted base 8,194 and the mock
returned error 45. With the gate released instead, the same fixture observed
8,193 target acknowledgements, no retained bytes and a whole batch at base 1.

## Retry and flush

In both profiles the released-gate control observed the original and retry as
whole 8,193-record batches at base 1, receipts at offsets 1 and 8,193,
8,193 target callbacks, and a single copy of the target data in the mock log.

With the existing retry handoff yield held and public `flush_timeout`
running, both desired controls failed their whole-retry assertion: the
original was `(1000, 0, 1, 8193)` and the retry was
`(1000, 0, 1, 8192)` (producer ID, epoch, base, count). The retained
observation fixture also recorded a later one-record retry at base 8,193 and
mock error 45 for the split attempts; the mock log stayed unchanged. The
fixture observes for finite intervals and cannot exclude a later write.

The controls use
`--cfg sequence_owner_witness --check-cfg=cfg(sequence_owner_witness)`
and
`--cfg sequence_retry_yield_witness --check-cfg=cfg(sequence_retry_yield_witness)`.
They hold existing cooperative yields but add snapshots and synchronization;
they are not a byte-identical uninstrumented producer. The successful
observation tests assert the documented history, including failures, and
should not be counted as successful delivery tests. No full suite, Clippy,
format or real-Kafka qualification was run here.
