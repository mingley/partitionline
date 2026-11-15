# Producer sequence ownership at worker cancellation

A public producer history against the repository's mock shows that accepted
records can lose completion and byte accounting when a worker stops during a
large idempotent batch. This is an instrumented test of one cancellation
boundary, not a repair or a real-Kafka compatibility result.

## Setup

[Witness commit c64b906][witness] is based on main
`262d2544a4352d52538f19ef9ab0c8c5700774d3`, whose producer source Git blob is
`1e7733c136e54b532cdc8bddabd1e0c828e50ed2` (SHA-256
`3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`).
The witness adds a cfg-only gate immediately before the existing yield after
8,192 stamps in `stamp_sequences`. Its gate scans the group synchronously to
check the stamped prefix and unstamped tail; this extra work and held yield
change scheduling and are not present in ordinary builds.

Each test warms one explicit partition with one acknowledged record, then
admits 8,193 eight-byte records without yielding to the worker: one public
`send` with a retained delivery future and 8,192 public `try_send` calls.
The same-partition batch has base sequence 1 and length 8,193. At the gate,
the first 8,192 records have the expected sequence, batch base and length;
the final record has no sequence or batch base. The producer id and epoch
match the warmup, and the mock has seen no target Produce. The test uses one
worker, unlimited byte capacity, and generous delivery and request deadlines.

## Observations

[Run 42466660114][final-run] used stable Rust 1.99.1 and LLVM 23.1.1 on
hosted x86_64. Each history ran once with default features and once with all
features. A successful job means these observations and assertions were
reproduced; the cancellation results describe a defect, not a passing producer
qualification.

| History | Observed result in each profile |
| --- | --- |
| Release the gate | The retained `send` returned its acknowledgement; all 8,193 batch acknowledgements reached the callback, the ordered mock log contained every submitted value, and `bytes_buffered` returned to zero. The batch used base 1 and count 8,193. |
| Abort the gated worker through the test task handle | The captured worker finished. The retained `send` returned `Closed`; no batch acknowledgement or error callback was observed, and `bytes_buffered` remained 65,544. A later public send was driven while its mock response was delayed. A different live worker task sent it using the same producer id and epoch with base 8,194; the mock's sequence check returned error 45 and did not append it. |
| Call public `close_timeout(150 ms)` while the worker is gated | Close returned `Timeout` and the captured worker finished. The retained `send` returned `Closed`; no batch acknowledgement or error callback was observed, and `bytes_buffered` remained 65,544. New admission returned `Closed`. |

The other 8,192 records use `try_send` and have no per-record receipt.
Callback totals and the mock log do not establish an individual delivery
result for them. The retained byte count is an accounting observation, not a
measurement of allocated memory. The close history stops before the batch is
written; the guide's specific in-flight timeout row does not directly describe
that stage. The guide does describe visible byte reservations as lasting until
acknowledgement or permanent failure, while the observed receipt was closed
and the count remained.

The mock enforces sequence continuity for its own idempotent-produce model.
Its error 45 is a mock observation, not evidence of a real broker's response.
The gate, exact worker abort and delayed mock response are controlled fixtures;
they do not prove that the same interleaving occurred naturally. Only this
single worker, partition and sequence boundary were exercised.

## Earlier attempts and source pins

[Run 42466269195][initial-run] passed the released and forced-abort histories
on the first witness. It observed base 8,194 and mock error 45 after the
abort, but did not prove a distinct replacement task. The stronger
[run 42466391264][v2-run] passed its released control but failed both abort
arms after the observer waited 30 seconds: its later `send` future had been
polled only once and was not driven while the mock was inspected. The
[run 42466553024][v3-run] repeated that undriven-send failure in both abort
arms; its separate released and public close histories passed. These failed
arms are not included as successful replacement-worker evidence. V4 drives
the later future concurrently with observation, then checks a different live
task before the delayed reply.

The V4 instrumented producer Git blob is
`82695cbf414c12f7dd96dc8ff6776166fb8f37b6` (SHA-256
`322ec7138cef1a1d0bc6d4f5eef81fab4f36a14837db4dd7c9cbb39832a6be21`).
The witness test blob is `28f1ef21833f7467b38c41029f8f3c1f0a675668`
(SHA-256 `e2042537efa7dc72821adb7a0ca0da2a53908a52894198f065cb9e618189cf5f`).
The mock fixture SHA-256 is
`b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`;
the lockfile SHA-256 is
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
The witness is not part of production builds.

A recovery design must retain the original producer identity, epoch, base and
length across this point or fence that identity before another write. Re-running
the current assignment on the partially stamped group is insufficient: it
reserves another range while leaving already stamped entries unchanged. A
complete repair also needs rooted ownership across the other asynchronous
batch, retry, callback and shutdown boundaries. This witness does not establish
those properties or measure performance.

[witness]: https://github.com/mingley/partitionline/commit/c64b906d7edadfccc7b0c780bdb2fbd3426d4219
[final-run]: https://github.com/mingley/partitionline/actions/runs/42466660114
[initial-run]: https://github.com/mingley/partitionline/actions/runs/42466269195
[v2-run]: https://github.com/mingley/partitionline/actions/runs/42466391264
[v3-run]: https://github.com/mingley/partitionline/actions/runs/42466553024
