# Combined producer qualification

The homogeneous-route `send_all` optimization remains unapplied. The exact
combined tree did **not** pass its full mock-producer integration target, so
the successful narrower controls do not qualify it for integration or release.

## Exact combined result

[Run 42474072730](https://github.com/mingley/partitionline/actions/runs/42474072730)
tested qualification commit
[`05832f006a2602c14c8c3bb33ff61b681c8c9d36`](https://github.com/mingley/partitionline/commit/05832f006a2602c14c8c3bb33ff61b681c8c9d36)
on the `e487f729` tree with all four intended source and test blobs:

| File | Git blob |
| --- | --- |
| `src/producer.rs` | `31bdaf849244026b3d64334b1429f676d8d4e755` |
| `tests/producer_send_all_regression.rs` | `d43afd11fc9d6eb42430710ebbf8889abbf88b1e` |
| `tests/common/mod.rs` | `30c2069ae40ff81c893acb202ee3d370ee75780f` |
| `tests/mock_produce.rs` | `4fd9a5a940086a97037bb45192ed84224a1542c6` |

The qualification-only workflow is not an intended main change. Both default
and all-feature jobs had the same outcome:

| Check | Result per profile |
| --- | --- |
| `producer_send_all_regression` | 5 passed |
| complete `mock_produce` target | **28 passed, 17 failed** |
| `produce_cancel` | 23 passed |
| `producer_retained_backing` | 9 passed, 1 ignored |

Stable Rustfmt and strict `cargo clippy --locked --all-targets --all-features
-- -D warnings` passed. They do not make the two test jobs pass. The renamed
redial discovery-handshake test was among the 28 passing mock tests.

Both profiles failed the same 17 tests. The exact failure output is in the
[default job](https://github.com/mingley/partitionline/actions/runs/42474072730/job/128711346787)
and [all-feature job](https://github.com/mingley/partitionline/actions/runs/42474072730/job/128711346856):

```text
commit_reuses_a_produce_handshake_for_the_moved_coordinator
commit_reuses_bootstrap_when_the_coordinator_returns
epoch_bump_reuses_a_produce_handshake
idle_metadata_reuses_learned_api_versions
idle_produce_leader_reuses_learned_api_versions
multi_bootstrap_commit_reuses_known_coordinator_handshakes
multi_bootstrap_send_offsets_reuses_known_handshakes
multi_bootstrap_transaction_reuses_the_other_bootstrap_handshake
produce_reuses_bootstrap_api_versions
produce_reuses_the_transaction_coordinator_handshake
produce_reuses_a_finished_bootstrap_handshake
retry_refreshes_waiting_topics_together
send_offsets_reuses_bootstrap_api_versions
send_offsets_negotiates_a_new_group_coordinator
send_offsets_repeats_a_remote_coordinator_without_another_handshake
transactional_rediscover_skips_a_second_bootstrap_handshake
transactional_startup_skips_a_second_bootstrap_handshake
```

The failures include expectations about connection negotiation counts or
timing and one metadata-request count. They have not been attributed to the
optimization or shown to reproduce on the unchanged baseline; the combined
failure remains open.

## Narrower evidence

The corrected producer and permanent test alone passed
[42473814706](https://github.com/mingley/partitionline/actions/runs/42473814706):
5/5 tests in both profiles, Rustfmt and strict Clippy. The tests verify
ordered receipts, mock records and accounting at 31, 32 and 8,191 records,
and check two pre-admission polls at 8,192 without draining that slow
fallback. They also check mixed partitions, an interceptor rewrite and sticky
routing. The black-box tests do not prove shortcut selection or detect a
performance regression; their
[separate evidence](https://github.com/mingley/partitionline/blob/dc4f2c9259936f4a9fb44aa63f5addeb6ce4940f/docs/evidence/client/send-all-durable-tests-20261108/README.md)
retains the initial Rustfmt and Clippy failures and the earlier source runs.

The corrected producer source passed the producer-only Apache Kafka 3.9.1
history in [42473919585](https://github.com/mingley/partitionline/actions/runs/42473919585).
With all features, idempotence off and on each verified 22,561 receipt offsets,
the broker end offset, and all values using Kafka's CLI. The verifier rejected
missing, duplicate, changed and reordered values. This used one partition on
one broker and does not establish failover, throughput or production readiness.

The earlier matched mock benchmark
[42471868128](https://github.com/mingley/partitionline/actions/runs/42471868128)
and long-topic run
[42472036756](https://github.com/mingley/partitionline/actions/runs/42472036756)
measured producer blob `650e34cca0cfa0bee9825ffc7d7da86514e28a31`, not
the corrected blob above. The correction moves a doc comment; those samples
remain pinned to their actual source. Their
[122 samples](https://github.com/mingley/partitionline/blob/45fcf7520667a2e3425e4ff0bbf12d3f5974d503/docs/evidence/perf/sendall-no-yield-20261108/samples.csv)
include a 17.250 ms eligible poll on a 249-byte topic and the multi-second
8,192-record fallback. The corrected-source rerun
[42473834473](https://github.com/mingley/partitionline/actions/runs/42473834473)
is separate; its outcome must be recorded independently.

Earlier instrumented
[route controls](https://github.com/mingley/partitionline/blob/501e1f75a2dac18e6cb8ec92c8936f6963119552/docs/evidence/client/send-all-route-controls-20261108/README.md)
and uninstrumented
[cancellation controls](https://github.com/mingley/partitionline/blob/87ed8994a802c36497f5587334d36b5262e3cbb3/docs/evidence/perf/sendall-cancel-v5-20261108/README.md)
used the prior producer blob. Removing an explicit yield does not establish
cancellation-boundary equivalence: faster admission can change when an
existing buffer or channel wait becomes pending.

The old redial test's 450 ms assertion failed 12/12 matched baseline and
12/12 candidate trials in [42471756660](https://github.com/mingley/partitionline/actions/runs/42471756660).
The separate redial test now on main checks concurrent discovery handshakes
with a gate and rejects a serialized negative control. It does not claim the
old end-to-end latency bound or assert old-socket closure; its
[history](https://github.com/mingley/partitionline/blob/c80aa9850880e9fb7ad5984447fc4c42b9ec60e3/docs/evidence/client/sendall-redial-gate-20261108/README.md)
retains the original failures and test limits.

At main `b51f645d`, the producer was still baseline blob
`1e7733c136e54b532cdc8bddabd1e0c828e50ed2`; the redial test and common mock
already matched the combined blobs. The full combined result above is red
regardless of that partial match.
