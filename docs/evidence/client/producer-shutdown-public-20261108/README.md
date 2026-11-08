# Public producer shutdown histories

These isolated histories test an experimental close-admission seal. They use a
local mock broker and test-only observers. They do not establish complete
shutdown semantics, production readiness or performance.

## Results

[Run 42462037131][final-run] passed three separate tests in each of four arms:
the `bc45ba4` baseline and `b7c1178` sealed experiment, each with default
and all features. The formatting check passed.

- With a previously accepted record held at the broker, a clone started a new
  send after the close marker. The baseline accepted and acknowledged the new
  record. The experiment rejected it with `Error::Closed` after its observed
  admission seal. The held record completed and close succeeded in both.
  After the held reply was released, the test polled close and the first
  record's receipt together, with close polled first. The receipt won in all
  four arms and the test observed its callback before close succeeded. The
  alternative close-first assertion was not exercised in these runs.
- When the first close future was dropped after its marker, the baseline
  accepted and acknowledged the next send. The experiment still rejected that
  send: dropping the close future had not undone its seal. The held record
  completed and a second close succeeded. This is an observation of the draft,
  not a complete contract for canceling close.
- A 600-byte record filled most of a 1,024-byte buffer while its reply was held.
  A second 600-byte send had entered the interceptor and reached a failed
  buffer reservation before close. With the close future paused at its marker
  and the first reply still held, that waiter remained pending for a 200 ms
  Tokio timeout in every arm. After the first record released its bytes, the
  baseline admitted and acknowledged the waiter; the experiment returned
  `Error::Closed`. This shows a bounded delay in this fixture, not a general
  upper or lower latency bound.

The controls check record receipts, callback identities and counts, mock log
contents, and released bytes. The draft's buffer waiter checks the shutdown flag
rather than the new seal while waiting for bytes; the observation above should
not be read as prompt notification on sealing.

In the separate [b7 focused run][focused-run], 14 admission and ledger controls
and 23 existing producer-cancellation controls passed in each feature mode.
The retained-backing suite then reported 7 passed, 1 failed and 1 ignored in
each mode: `cancelled_channel_admission_releases_only_the_unenqueued_reservation`
observed 4096 buffered bytes where the test expected 4097. Later mock-retry and
formatting steps were skipped. The failure requires a matched baseline analysis
before assigning cause. The earlier [flush history evidence][flush-evidence]
also records a separate canceled-waiter error-loss limitation in the preceding
completion draft.

## Boundaries and provenance

The mock holds an actual Produce response after preparing it; it uses the
repository's own Kafka codec and is not an independent Kafka broker. The
qualification-only close counter is placed before flushing on the baseline,
where it marks close entry rather than a seal. On the experiment it runs just
after the completion-domain seal. A second counter records a failed buffer
reservation. Neither counter changes producer state or exists in ordinary
builds. The response gate deliberately extends a scheduling window.

| Item | Baseline | Sealed experiment |
| --- | --- | --- |
| Uninstrumented source commit | `bc45ba4f65f126c406ff88a57fcfe08007d35b29` | `b7c11784d79b05a36ca624b3de17a952603d4931` |
| Qualification commit | `f813df0bdfcf242a4478409cb7bc44be034ea6c7` | `65a0fa66f64af9fe6862a344d33ef1595b66199e` |
| Original `src/producer.rs` blob | `8c0e6754f7bdd4500e8e9c5c4af8009cad86a671` | `0f4affa57ce20eb33b94b2b9754f2d85a75fad7c` |
| Instrumented `src/producer.rs` blob | `c80dca539d304b316d78f9654032de427536a1ea` | `beb1d1ef51cc97004017d63c08e170832106cf35` |
| Completion-domain blob | `2fd3dbe9f8069e1e97526ed10d54118d0dac68a3` | `758164ed462f3e26f59a639540d9374928d06795` |

Both arms used test blob `36d8c3313785953fcc1cc68c1142b5d35a14ed23`,
mock fixture blob `9d14334cf587b774701692547fdbdfd64bd399a1`, and
`Cargo.lock` blob `d1cb698aaaf7554fe904ba02d671c4a8ab4975b3`. They ran
on the same hosted x86_64 Linux worker with stable Rust 1.99.1 and separate
source and feature target directories. The [retained artifact][artifact]
contains source and mock patches, hashes of the four test binaries and the
individual logs; its SHA-256 is
`0ca2c21b359f4f9e6fa4b443610d96dcf4ebc9f244dfe325756b494ef789eba2`.

[Run 42461163765][format-run] passed its 12 semantic controls but failed its
formatting check. [Run 42461487424][observer-run] passed with an explicit
buffer-wait observer. [Run 42461750015][wait-run] passed after adding the
200 ms waiter observation. The final run added the concurrent close/receipt
oracle and passed. All these runs and the initial format failure are retained.

[final-run]: https://github.com/mingley/partitionline/actions/runs/42462037131
[focused-run]: https://github.com/mingley/partitionline/actions/runs/42460603408
[flush-evidence]: ../producer-flush-barrier-public-20261108/README.md
[artifact]: https://github.com/mingley/partitionline/actions/runs/42462037131/artifacts/13610102502
[format-run]: https://github.com/mingley/partitionline/actions/runs/42461163765
[observer-run]: https://github.com/mingley/partitionline/actions/runs/42461487424
[wait-run]: https://github.com/mingley/partitionline/actions/runs/42461750015
