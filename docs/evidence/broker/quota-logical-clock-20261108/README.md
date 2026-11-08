# Quota-window clock ordering

This change makes the in-memory broker quota window use the latest time it has
observed when a time-bearing operation receives an older timestamp. The
caller must supply times from one clock; future timestamps remain trusted and
can advance the window. The change does not connect the window to Kafka wire
throttling, provide durable quota administration, or establish server-wide
overload fairness.

Before the change, a window could observe time 10,000 ms on one bucket, admit
a new identity at an older supplied time, then grant that identity another
full burst at the already-observed time. A stale `set_rates_at` could also
apply the new rate to an interval that had already elapsed at the old rate.
The candidate clamps `connect_at`, `admit` and `set_rates_at` to the
window's latest observed time. The untimed `set_rates` retains its existing
documented behavior: the new rate applies to the next refill's whole elapsed
interval.

The existing validation order is preserved. A valid client name passed to
`connect_at` observes time even if a later connection cap refuses it. A
nonzero `admit` that fits the in-flight byte cap observes time even if a
later work-ID, identity or quota check rejects it; rejected charges do not
spend tokens. Invalid connection names, zero or over-cap admission, and
invalid timed rates return before observing time.

## Source and controls

The [candidate](https://github.com/mingley/partitionline/commit/06a39aa3149d9cdee7607c4b1b30da24e5189c8f)
uses `partitionline-broker/src/quotas.rs` Git blob
`e873fc35cb8c2ae88884e4175d81d08844413c77` (SHA-256
`772970ffaab9d99017e3475990adaa7299e06dfbc040909c74a74fefdf5ad1ed`).
The baseline source is blob `92428c4b3d15917aab5168afc4b225afad883662`
(SHA-256 `d5a9adaa2f24c1e92b02ca2749a65dcda977a8b7f1b62c249a583dacf2176036`).
The public test file is blob `2b39b2f086180d6d709e541cc13ec932c578ffd9`
(SHA-256 `0fc350ee6a216909e10678326b39a1b72e21072c330a588dc4802a1f80908643`).

The separately compiled [default](https://github.com/mingley/partitionline/actions/runs/42487793820)
and [all-feature](https://github.com/mingley/partitionline/actions/runs/42488501381)
runs each recorded 4 passes and 9 expected failures with the baseline source,
then 13/13 public controls and 10/10 existing quota unit tests with the
candidate. Baseline and candidate used identical test and lockfile bytes,
separate Cargo target directories and distinct test-executable hashes. The
tests cover stale first-use and existing-client charges, replication, a timed
rate update, rejection timing, an idle identity with debt and retained
accepted work, forward clocks, untimed rates, a `u64::MAX` edge, and a
specific two-thread serialized schedule. They are not an exhaustive
concurrency or overload test.

In [run 42487414681](https://github.com/mingley/partitionline/actions/runs/42487414681),
stable rustfmt, strict all-target/all-feature Clippy, and all-feature Rustdoc
with warnings denied passed on the candidate source and test blobs above.
Three default-feature mutations independently restored the old admission,
connection, and timed-rate clock handling. Each corresponding named test
failed as expected; the job restored and checked the source blob afterward.

The first qualification [42487081987](https://github.com/mingley/partitionline/actions/runs/42487081987)
failed formatting on the new test layout and strict Clippy on its
`std::sync::Mutex` test fixture. Both baseline profiles showed the expected
4/9 result, but their jobs failed before recognizing the expected Cargo 101
because the shell still had errexit enabled. The corrected test uses a Tokio
mutex for the synchronous-thread schedule.

In the second run, the baseline and candidate behavior jobs shared a Cargo
target directory. The candidate step reused the baseline executable and
reported the same 4/9 result; those candidate results are invalid. Formatting,
Clippy, Rustdoc, and the three independently restored mutations passed.
The [third run](https://github.com/mingley/partitionline/actions/runs/42487793820)
used separate targets: its default profile passed, but its all-feature
baseline check failed because parallel `--nocapture` output interleaved one
test name and failure marker. The [fourth run](https://github.com/mingley/partitionline/actions/runs/42488188215)
serialized output, but `--nocapture` still put panic text between the
name and marker; its all-feature baseline again showed 4/9 and the harness
failed before candidate execution. The successful all-feature run captures
the baseline output and uses separate targets.

The standard locked all-target broker behavioral suites passed in
[42488837344](https://github.com/mingley/partitionline/actions/runs/42488837344):
794 passed and four ignored across 37 executables with default features, and
942 passed and four ignored across 37 executables with all features. These
runs used the exact candidate source and permanent test blobs above. They do
not explain earlier intermittent Raft failures or qualify production use.

## Local timing

[Run 42487414681](https://github.com/mingley/partitionline/actions/runs/42487414681)
used the same source pair, separate release targets and the identical
benchmark fixture in both arms. It ran on one shared x86-64 GitHub-hosted AMD
EPYC 9V74, pinned to one allowed CPU, with stable Rust 1.99.1 and LLVM 23.1.1.
Each shape had one warmup per arm and 12 alternating pairs.
[All 96 samples](paired-timings.csv) preserve execution order.

| In-memory shape | Baseline / candidate median | Median paired ratio | Ratio range | Candidate slower |
| --- | ---: | ---: | ---: | ---: |
| 300,000 client admit/complete calls | 12.578 / 12.447 ms | 0.9876 | 0.6723–1.0227 | 3/12 |
| 300,000 replication admit/complete calls | 8.285 / 8.211 ms | 0.9925 | 0.9857–1.0030 | 1/12 |
| 300,000 connect/disconnect calls | 7.361 / 7.299 ms | 0.9911 | 0.9851–1.0006 | 1/12 |
| 500 timed rate updates across 1,024 identities | 3.272 / 3.180 ms | 0.9731 | 0.9674–0.9870 | 0/12 |

The fixture uses generous limits and monotonically increasing timestamps. It
does not measure Kafka requests, network throughput, allocation counts, or
tail latency, and these short shared-host timings do not establish a speedup.
The baseline and candidate test binaries had SHA-256
`c7fbe2627014caba2d275f1ca85e4899ad7a0e0132d1f062fd84d07d9c6b50fd`
and `11523c9f286620b04f11704d164e39f92eb54a5b66023674d5deb7ce23f729e8`.
The binaries themselves were not retained. The benchmark fixture is Git blob
`ab75cabecb8b7943de946bbee60c2534cedaec5d`.

The first run also had a separate-target comparison, but no warmups; its
[96 samples](initial-paired-timings.csv) are retained as exploratory data.
A broad performance ranking or fastest-server claim is not supported.

## Remaining limits

This is an in-memory `Window`, not a Kafka protocol throttle implementation.
The existing global in-flight-byte cap can still constrain unrelated clients,
and reclaiming idle identities or changing all rates can scan client state
synchronously. Bounded client identity count and accepted-work accounting are
not a bound on total process memory or poll latency. The test schedule does
not establish cluster behavior, durable administration, disconnect behavior
in a live listener, or real broker fairness.
