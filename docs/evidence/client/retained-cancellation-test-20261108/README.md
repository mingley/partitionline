# Retained producer cancellation test

The [replacement test][replacement] and mock log accessor isolate the
cancellation of a pending producer admission. The candidate changes only test
code. It does not fix or qualify the producer's outstanding flush and ownership
issues.

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

The replacement keeps both cancellation shapes:

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
leave `32655`. Both values were observed in qualification-only fault
controls, as described below.

## Qualification

The [candidate commit][candidate] is based on main
`36424a580913e3d77571beed65b91d6751d7b108`. Its relevant Git blobs and
SHA-256 digests are:

| File | Git blob | SHA-256 |
| --- | --- | --- |
| `src/producer.rs` | `1e7733c136e54b532cdc8bddabd1e0c828e50ed2` | `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f` |
| `tests/producer_retained_backing.rs` | `cfed2ebbee2bc5594d79d4b80b72d93f40c1ebc0` | `9234838d6741b8ca8a4a16b21428ad5932127beab20ff3f64271cbf8b39864c1` |
| `tests/common/mod.rs` | `4917e00407f79e764909ddd37816e096303e5900` | `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1` |

The non-sticky producer path creates the reservation after the byte-budget
wait and reaches the worker send without another await. In the gated fixture,
a live worker is held in its callback, a public `try_send` probe reports the
channel full, and the next public `send_all` poll returns Pending with 113
additional reserved bytes and no additional queued record. Together with the
fresh route and unlimited byte budget, these observations locate the wait at
the channel send in this fixture. The warmup ack count is an accounting
snapshot taken before the gated callback returns; it does not show that the
warmup receipt had completed.

In [run 42464534091][revised] the revised test source passed in default and
all-feature builds on hosted x86_64 with stable Rust 1.99.1. Each profile ran
the two channel tests three times, the full retained-backing test file with
9 passed and 1 ignored native test, and `produce_cancel` with 23 passed.
Stable formatting and strict all-target, all-feature Clippy also passed.
[Run 42464647153][combined] repeated these checks on the candidate rebased onto
main `36424a5`, with the same per-profile counts and successful lint results.

The positive arms retain the unmodified producer source. After the positive
checks, each profile's throwaway checkout injects a fault only for the
113-byte `BufferReservation` and uses a separate Cargo target directory: once
it omits release and once it releases twice. Both
faults failed the intended byte assertion in both profiles, with exit 101 and
the expected values `32881` and `32655`, respectively, against
`32768`. The mutated producer's SHA-256 was
`dd6dbd96e9ddd5d8fa91a7aa25d5c21274182dbc8f0a8b9b1ce39933acc2315e`.
The injection and its scripts belong only to the qualification branch and are
not part of the candidate. The test checks the accepted callbacks and mock log
after cancellation; the canceled task's receipt receivers have been dropped.

An earlier [run 42463193194][lint-failure] passed the behavioral and injected
fault checks but failed strict Clippy for fixture mutexes, a manual no-op waker
and unfulfilled lint expectations. The candidate uses `parking_lot`,
`Waker::noop()`, and takes the callback's receiver out of the mutex before
blocking on the external latch. The earlier failure remains part of the
record. The original flaky test's assertion failures and incomplete all-feature
snapshots above are also not reclassified as passes.

The tests use an in-repository mock broker and one controlled route. They do
not establish real Kafka compatibility, producer shutdown or flush
correctness, performance, or production readiness.

[candidate]: https://github.com/mingley/partitionline/commit/84639ab6700e8eb12e949d60caa5f4edf138b009
[revised]: https://github.com/mingley/partitionline/actions/runs/42464534091
[combined]: https://github.com/mingley/partitionline/actions/runs/42464647153
[lint-failure]: https://github.com/mingley/partitionline/actions/runs/42463193194

[replacement]: https://github.com/mingley/partitionline/blob/84639ab6700e8eb12e949d60caa5f4edf138b009/tests/producer_retained_backing.rs
[matched]: https://github.com/mingley/partitionline/actions/runs/42460753994
[snapshot-one]: https://github.com/mingley/partitionline/actions/runs/42462075980
[snapshot-two]: https://github.com/mingley/partitionline/actions/runs/42462409199
