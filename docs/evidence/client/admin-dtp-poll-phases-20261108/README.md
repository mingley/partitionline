# DescribeTopicPartitions Ready and Pending polls

The [row checkpoint](../admin-dtp-row-checkpoint-20261108/README.md) is on
main. A public Admin mock comparison still showed a long poll for 500,000
rows. This diagnostic separates the poll that completed the call from earlier
polls that returned `Pending`; it does not identify the work inside either.

[Run 42484154153](https://github.com/mingley/partitionline/actions/runs/42484154153)
passed all 40 cases, with five separate process samples at each of 32, 8,192,
100,000 and 500,000 rows for both empty successful topics and
`TOPIC_AUTHORIZATION_FAILED` topics. The current-thread test measured each
poll of the public Admin future against one TCP mock peer. The peer verified
all requested names and returned the same names in order; the test checked
every returned name, topic error, empty partition list and omitted
authorization flag. The table gives the range of each metric after the peer
finished writing. [Raw samples](samples.csv) contain all timings.

| Rows | Row kind | Longest Pending poll | Final Ready poll |
| ---: | --- | ---: | ---: |
| 32 | empty | none after write | 0.034–0.046 ms |
| 32 | error | none after write | 0.037–0.055 ms |
| 8,192 | empty | 1.921–2.199 ms | 0.385–0.415 ms |
| 8,192 | error | 1.935–1.979 ms | 0.377–0.403 ms |
| 100,000 | empty | 5.387–5.820 ms | 8.578–9.954 ms |
| 100,000 | error | 5.137–5.465 ms | 8.458–9.531 ms |
| 500,000 | empty | 70.836–82.303 ms | 59.379–65.661 ms |
| 500,000 | error | 68.861–90.570 ms | 59.960–64.219 ms |

In the CSV, zero for the 32-row maximum Pending poll means none was observed
after the peer's write, not that a Pending poll had zero cost. All 40 Ready
polls began after the write marker. A measured poll can include reads,
decoding, row conversion, merging and cleanup. In particular, the data do not
establish that hash-map growth or destruction caused either 500,000-row
pause. The body for that case was 20,000,009 bytes; this is a mock, not a
real Kafka performance or cancellation test. The output collection remains
owned by the test after the Ready poll, so its eventual destruction is not
part of `ready_poll_ns`.

The production source is blob
`23cb280eee5e91a64a9a6f58347f707524163d07`, SHA-256
`519f56c39da3cd24a045c8d6d10587d412121e51e0b51c028e96f1b4d98ff412`.
The test-only measurement fixture is blob
`e66cec513a747e03d430705537abbac315ab1f1e`, SHA-256
`8780fb1fd250ff52d35d683a0254e5f4a9a0611353c713314331076be173b134`,
from [commit 4da4c9e](https://github.com/mingley/partitionline/commit/4da4c9e599513dc3f91e0ff0a1dc56ae2dc09b8c).
It adds a timer around each poll and tracks Pending and Ready separately;
there are no source hooks in the production blob.

The GitHub-hosted Ubuntu 24.04 runner reported AMD EPYC 9V74, stable Rust
1.99.1 (`58cf8f9c3`), LLVM 23.1.1 and an unavailable `cpu.max`. The test
binary used the release profile, default features, thin LTO and one codegen
unit. Its SHA-256 was
`e785338c2c57db883c6ed20655c71aa1aa989b4298435e16712dfb1f8ac85016`;
the binary itself was not retained. The run artifact retains the build,
machine details and raw output for 30 days. The prior paired comparison used
a different runner, so these timings are not a matched before/after
comparison with it.

A separate instrumented experiment is needed to examine individual index
growth and destruction, including whether those operations overlap the slow
polls. Cooperative checkpoints do not establish a hard deadline or bounded
cancellation drop.
