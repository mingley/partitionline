# Producer ownership regression controls

These are isolated public-API controls for two retained producer histories on
main at `cad4257335d98233f567ae3928a109232dce5eaf`. They do not change the
production producer or establish a fix. Both profiles reproduced the defects
under controlled yields and the repository's mock broker. The controls are
intentionally red against this baseline.

## Sources and run

- Uninstrumented baseline `src/producer.rs`: Git blob
  `dcd012b3f86a8b9d4254f88d7f48946ce8f9e922`, SHA-256
  `60a3fa0985358198e81757e6080af8b58ab52cb00edc1f34e2c97ed033e90b78`.
- [Test source commit](https://github.com/mingley/partitionline/commit/7f020b5e860546178707a1f087ff0f0793acde71):
  `7f020b5e860546178707a1f087ff0f0793acde71`. Its parent is the baseline.
  The test build enables the existing yield witnesses, so it does not execute
  a byte-identical, uninstrumented producer.
- [Qualification workflow commit](https://github.com/mingley/partitionline/commit/fb963982e8f291deb47890658de22fc181e71804):
  `fb963982e8f291deb47890658de22fc181e71804`, with the test commit as
  parent. Its workflow blob is
  `da89672dc323ee3626a81a90d70d94db960e0fbf`.
- [Actions run 42497282310](https://github.com/mingley/partitionline/actions/runs/42497282310):
  default job `128782242556`; all-features job `128782242868`. Both used
  stable Rust 1.99.1 and LLVM 23.1.1 on hosted x86-64 Linux. The default job
  reported an AMD EPYC 7763 and the all-features job an Intel Xeon Platinum
  8573C. These runs were not performance measurements.
- The workflow passed because it required exit 101 and the expected assertion
  text for each red control. Its success does **not** mean that the producer
  passed those controls.
- Preserved logs: [default artifact 13624047608](https://github.com/mingley/partitionline/actions/runs/42497282310/artifacts/13624047608),
  SHA-256 `b107431887f279a62c95cc1561e2f48e88ba96577063083cf80a8ea729185779`;
  [all-features artifact 13623059784](https://github.com/mingley/partitionline/actions/runs/42497282310/artifacts/13623059784),
  SHA-256 `b4a4cd515f9ce9202c270c99a6fa8cf09fc7abc11ffc8b193ab822cf7ca6777f`.
  Artifact retention expires 2026-12-08.

The exact tested files are pinned below; SHA-256 values are from the job's
source-identity step.

| File | Git blob | SHA-256 |
| --- | --- | --- |
| `src/producer.rs` (instrumented) | `5d5a056a18ab867acd09ba3d320c2768fd22bcca` | `d8c43834b100edebaa1a7d9cfff0a331d2675092459e0931448c81c0864abddf` |
| `tests/sequence_ownership_public.rs` | `28f1ef21833f7467b38c41029f8f3c1f0a675668` | `e2042537efa7dc72821adb7a0ca0da2a53908a52894198f065cb9e618189cf5f` |
| `tests/sequence_retry_existing_yield_public.rs` | `b1e8f1bce5b4a1da38f2229135153fc54dea036c` | `66f9cdf32b229401b9167a0261624e15371fcdfc5024574663b2c4cc1e1d7c2b` |
| `tests/producer_ownership_regression_public.rs` | `2bbf01576be3aa4d65c577bc893389d1ae6b885b` | `7425c03b5212ed6fe1996ae44afa4ef674f0525af0fe7acf5eeb9d3c7b23cb54` |
| `tests/producer_retry_regression_public.rs` | `dc64b722426ad37b27805366cbb65208f5d04bbe` | `d958fbb1f52b1806b1e116bc5900ce709c1b51cb8b9b972af00614db0df45fe8` |
| `tests/common/mod.rs` | `4afd104e0b5e10227167d0d38328fe03dda07a0a` | `45921a161ac3d1e310558d2ec37a9f5c7b4f8b5e7ed1813b34f6ccf468c27559` |
| `Cargo.lock` | `96e364d34121fe90183f87f4f49fd92b883bc447` | `a6702ab5ac47b734ddda21c83502ce4810423662731494365b8e1432a486814a` |

## Worker cancellation and close

The setup admits 8,193 eight-byte target records to one topic-partition after
one acknowledged warmup record, with `buffer_memory(0)` and one configured
connection slot. On a current-thread runtime, the existing
`sequence_owner_witness` gate holds sequence stamping after the first 8,192
records. It scans and validates the stamped prefix and unstamped tail. At this
point the mock contains only the warmup; the target batch has not been written.
The test either aborts the captured worker or calls public
`close_timeout(150ms)`. Each case runs with all 8,193 `send` receipts retained
or with one retained `send` receipt and 8,192 `try_send` admissions.

The desired control accepts success or explicit failure for each retained
receipt. In this controlled, responsive mock it requires accepted records'
callbacks to settle and the buffered-byte count to return to zero. It checks
successful receipt offsets and values against the mock log; in the abort case it
also checks that a later send does not report out-of-order sequence error 45.
The latter checks were **not reached** by these failing baseline cases. Receipt
completion is bounded at ten seconds and the later callback/accounting
observation at four seconds: these are test limits for this schedule, not an
API-wide latency guarantee. A correct implementation that deliberately retains
or fences work beyond these limits can also fail this progress check. The
observations alone do not establish that the accounting is permanently stuck.

Both feature profiles produced the same counts:

| Worker action | Admission | Retained receipts: success / error | Target callbacks: ack / error | Target bytes still buffered |
| --- | --- | --- | --- | --- |
| Abort | One `send` + 8,192 `try_send` | 0 / 1 | 0 / 0 | 65,544 |
| Abort | 8,193 `send` | 0 / 8,193 | 0 / 0 | 65,544 |
| Timed close | One `send` + 8,192 `try_send` | 0 / 1 | 0 / 0 | 65,544 |
| Timed close | 8,193 `send` | 0 / 8,193 | 0 / 0 | 65,544 |

Both public timed closes returned `Err(Timeout)`. Each case failed the
assertion that accepted records reach terminal callbacks and release bytes.
The new control records receipt error counts, not each error variant; it does
not show that every error was `Closed`. Callback counts are aggregate:
`ProducerInterceptor::on_error` does not identify the individual record, so
counts alone do not prove exactly one callback per record.

The retained observation fixture also ran in both profiles. With the gate
released it observed 8,193 target callbacks, no retained bytes and a whole
batch at base 1. Its abort history observed a `Closed` receipt, 8,193 missing
callbacks, 65,544 retained bytes, and a subsequent replacement worker attempt
at base 8,194 rejected by the mock with error 45. Its close history observed
timeout, a `Closed` receipt, 8,193 missing callbacks and retained bytes. These
are observation tests that pass when they reproduce the documented history;
their green status is not evidence of correct producer behavior.

## Retry handoff and flush

The retry witness uses the actual cooperative yield after 8,192 per-record
retry offers, while instrumentation records the offers and holds that yield.
The mock has appended the original 8,193-record group and dropped its response.
The public `flush_timeout` begins while the gate is held. Any subsequent
Produce observed by the desired control must keep the original producer ID,
epoch, base sequence and count. It permits no subsequent write or a pending or
fenced outcome; it does not require an ambiguous write to be replayed.

In both profiles the released-gate control observed the full original and full
retry at base 1/count 8,193, receipts at offsets 1 and 8,193, 8,193 target
callbacks, and one copy in the mock log. With the gate held and flush running,
the desired control instead observed original `(1000, 0, 1, 8193)` and retry
`(1000, 0, 1, 8192)`, then failed its whole-retry assertion. The retained
observation test additionally recorded the later one-record tail at base
8,193 and mock error 45 on the split attempts, with the mock log unchanged.

The held control observes for up to five seconds, releases the gate, then waits
up to five more seconds for flush before its final check. A pass cannot rule out
a later split outside those windows. The gate adds observation work and
changes scheduling even though it holds a yield that already exists in the
baseline path. A batch-owned implementation may remove that per-record yield;
the witness must then move to an equivalent rooted ownership-transfer boundary
before the repaired implementation can be compared.

## Reuse and qualification limits

The workflow sets:

```text
RUSTFLAGS='--cfg sequence_owner_witness --check-cfg=cfg(sequence_owner_witness) --cfg sequence_retry_yield_witness --check-cfg=cfg(sequence_retry_yield_witness)'
```

It runs the two retained observation fixtures and the new
`accepted_records_settle_after_worker_cancellation` and
`retained_retry_batch_is_not_split_by_flush` tests with `--locked`, once
with default features and once with `--all-features`. The four ownership
combinations use `PL_OWNER_CASE=abort|close` and
`PL_ADMISSION=mixed|all`. The workflow is a baseline-negative harness: after
a repair, run the individual controls expecting success rather than reusing
its requirement that they fail.

Strict `cargo fmt --all -- --check` reported differences in both retained
fixture files and the new controls. The workflow made formatting diagnostic
only, so formatting did **not** pass. No full client suite or Clippy
qualification was run here. The mock, one topic-partition, single connection
slot, controlled yields and finite observation windows do not establish a
real-broker outcome, production readiness or performance. Original evidence
remains unchanged; no production repair is claimed.
