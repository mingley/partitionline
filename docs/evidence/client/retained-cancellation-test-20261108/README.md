# Retained producer cancellation test

The replacement test on [branch commit f792d89][replacement] is under
qualification. It changes only test code and a mock log accessor. The
producer Git blob remains `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`.
No production fix or successful replacement qualification is claimed here.

## Original test observations

In [matched run 42460753994][matched], the original
`cancelled_channel_admission_releases_only_the_unenqueued_reservation` test
failed its 4,096-versus-4,097 byte assertion in all 12 invocations: three each
for base `4409633` and draft `bc45ba4`, under both default and all features.
The collection job succeeded because it recorded every result; the tests did
not pass. This establishes that the observed assertion also occurs on that
base, not its cause.

The snapshot-only test in [run 42462075980][snapshot-one] kept the original
assertion and added counters. The default arm recorded:

| Snapshot | Queued | Acked | Errors | Buffered bytes | Mock log length |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before abort | 4,098 | 1 | 0 | 4,098 | 1 |
| After abort/await | 4,098 | 1 | 1 | 4,096 | 1 |

The original assertion then failed with 4,096 versus 4,097. One additional
accounted error and a two-byte decrease are consistent with an accepted record
failing while the canceled unqueued reservation is released; the snapshots do
not independently identify the released records or prove a leak. The
all-feature arm returned `Timeout` after the warmup snapshot, before either
before/after snapshot.

The fixed collection [run 42462409199][snapshot-two] repeated the same default
before/after values and assertion failure. Its all-feature arm again returned
`Timeout` before the two snapshots; collection marked that arm incomplete.
The collection jobs' success does not mean the tests passed.

## Replacement controls

The proposed replacement keeps both cancellation shapes:

- A current-thread test fills the single worker channel without yielding, polls
  a one-record `send_all` containing a distinct 113-byte value once, observes
  Pending, and drops it before yielding. It checks the unchanged queued and outcome
  counts and the exact byte change.
- A two-worker runtime parks the real worker in a warmup acknowledgement
  callback using an externally released latch. One spawned `send_all` admits
  4,096 distinct eight-byte records, then holds the unqueued 113-byte record.
  The test aborts and awaits that same task while the worker is held. After
  release, it checks the accepted acknowledgement offsets and the mock's
  ordered record values. An RAII guard releases the latch on early exit.

Both controls depend on a warmed, fresh metadata route, an unlimited byte
budget and the configured 4,096-entry channel. The first depends on Tokio's
current-thread scheduling and no await between its snapshots; the second uses
a deliberately blocking test callback and a two-worker runtime. The mock uses
this repository's codec; these are not real-Kafka histories.

The expected held total is `4096 * 8 + 113 = 32881` bytes and the correct
post-cancellation total is `32768`. For a fault confined to the 113-byte
reservation, omitting release should leave `32881`; releasing twice should
leave `32655`. Those are expected negative-control outcomes, not results.

[replacement]: https://github.com/mingley/partitionline/blob/f792d8986b98127e29777e882c295977741eda8f/tests/producer_retained_backing.rs
[matched]: https://github.com/mingley/partitionline/actions/runs/42460753994
[snapshot-one]: https://github.com/mingley/partitionline/actions/runs/42462075980
[snapshot-two]: https://github.com/mingley/partitionline/actions/runs/42462409199
