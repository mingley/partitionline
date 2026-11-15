# Bounded broker transport read-ahead

This change fixes a transport pipelining defect. It does not implement Kafka
bandwidth quotas, establish production readiness, or measure comparative
performance.

## Behavior

On the pinned baseline `133d2413217cee4130bca2267e4e36b7225ff86f`, the
disconnect watcher could read past its one-frame budget while the current
handler was pending. Two individually legal maximum-size frames could then
close the connection as an invalid length and cancel the handler. The
[baseline controls](https://github.com/mingley/partitionline/blob/81ffc703049deffd5eaafe15872544b859c13224/partitionline-broker/tests/quota_transport_boundaries.rs) observed that
behavior; a small legal pipeline remained ordered. Both controls passed with
default and all features in
[42663188859](https://github.com/mingley/partitionline/actions/runs/42663188859).
That run also passed the existing grouped Produce unconsumed-reply and
no-response pipeline controls.

The candidate limits pending read-ahead to `max_request_bytes + 4` across
the handler watcher, fragmented buffered reads, and response prefetch. It
leaves later bytes in the socket rather than rejecting a valid pipeline.
A full buffer means EOF or a malformed following prefix may be detected only
after the current handler completes and reads resume. The handler deadline
and transport shutdown remain effective. This changes disconnect-detection
latency; it does not guarantee an immediate close when unread data fills the
buffer. Transport metrics can change accordingly: a valid combined pipeline
is no longer counted as an invalid length.

The tests cover a fragmented pipeline with multiple maximum-sized frames,
in-order responses, full-buffer EOF and eventual close, a malformed next prefix,
handler deadline, shutdown and canceled shutdown/retry. Unit controls check
buffered-byte limits and that a full watcher neither keeps reading nor
self-wakes. Buffer allocation capacity and operating-system or TLS buffers
are not asserted to equal the pending-byte limit.

## Sources

- Candidate: `b718c5c4335acdbc7bb5cce61530394562ae2d49`, parent
  `6ae6c675e150bd99a132a764f7763fc3eddfbd94`.
- `partitionline-broker/src/transport.rs`: blob
  `7d43efab6f3e929611ed2df1def5f2edb96f475a`.
- `partitionline-broker/tests/transport_backpressure.rs`: blob
  `2fc5613de332cd47396847f1778f69d33a5897a4`.
- `docs/evidence/broker/quota-produce-design-20261108/README.md`: blob
  `f9e54eeb200878bed16344303ca745b6d5134eb1`.
- Qualification workflow-only child:
  `8cfa6a4152ec1c420ae1bbba13240c9ace591905`.
  [42666712563](https://github.com/mingley/partitionline/actions/runs/42666712563)
  tests those source blobs; the workflow is not part of the candidate.

## Previous attempts

- [42664751943](https://github.com/mingley/partitionline/actions/runs/42664751943)
  failed before scheduling a job because the temporary workflow contained an
  unquoted filter. No code check ran.
- [42665094042](https://github.com/mingley/partitionline/actions/runs/42665094042)
  passed three internal controls, four socket controls and the 16 existing
  transport integration tests. Its transport unit suite had 46 passes and one
  failure: the new full-buffer deadline test accepted only clean EOF, and
  did not print the actual read result. The revised assertion accepts normal
  reset as well as EOF and reports other outcomes; the old result does not
  establish which outcome occurred.
- [42665815517](https://github.com/mingley/partitionline/actions/runs/42665815517)
  passed default controls, five socket controls, 16 transport integration
  tests and 47 transport unit tests; all-feature socket controls also passed.
  The all-feature unit target did not compile because a new test compared
  the SASL `Frame` wrapper directly with `Vec<u8>`. The candidate compares
  its bytes instead. Clippy did not run in that attempt.

## Exact-source qualification

On x86_64 GitHub-hosted Ubuntu with stable rustc 1.99.1, the focused job
passed on the exact source: three internal controls, five socket controls,
47 default transport unit tests and 16 existing transport integration tests;
all-feature socket controls passed 5/5 and transport unit tests passed 51/51.
The default internal controls are included in the default unit total.

The full default broker profile passed 814 tests, with four ignored, across
39 test executables. Formatting, all-target/all-feature Clippy, rustdoc with
warnings denied, and all-feature doc tests passed; there were no doc tests.

The full all-feature broker profile passed 962 tests, with four ignored,
across 39 test executables. All four jobs in the exact-source
[run](https://github.com/mingley/partitionline/actions/runs/42666712563)
passed.
