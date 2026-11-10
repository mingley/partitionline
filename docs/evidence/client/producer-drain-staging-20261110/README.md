# Producer drain-staging experiment

This is an isolated candidate for cancellation inside `Worker::drain_pending`.
It is not applied to main and does not fix cancellation in the subsequent
grouping or delivery phases. The baseline producer blob is
`426dbdb5c5e27fdf655930a76682c5c18fb07e89`; the
[drain design and baseline witness](../producer-grouping-cancellation-design-20261110/README.md)
remain open.

The candidate reserves staging capacity before removing records, keeps the
selected batch in the worker's spare vector and uses a guard to restore it to
the front of the original queue if the drain future is dropped. Estimate and
deadline changes commit after the yielding scans. An independent source review
found no concrete defect under the stated capacity and exclusive-borrow
invariants. Restoration moves records synchronously in proportion to the queue
length; no time bound, allocator-abort recovery or process-abort guarantee is
established. Once drain returns, later cancellation boundaries remain open.

In [run 42886272921](https://github.com/mingley/partitionline/actions/runs/42886272921)
on `751a8f92cf2c050b1f70364c0c532ad69c781ee5`, ten focused controls passed
and one diagnostic was ignored in each feature profile. The pinned baseline
failed six named controls in each profile as expected. Running the later
grouping diagnostic separately failed as expected with 8,193 closed senders
and 8,193 seeded bytes still charged at observation. These are constructed
private states, not public admission or Kafka delivery histories. That
candidate job failed formatting and strict test Clippy.

The clean [ed2f7dc candidate](https://github.com/mingley/partitionline/commit/ed2f7dc2ff07af1da5d647f944f008ebab1e4300)
removed the intentionally failing ignored diagnostic from the source. Its
producer source blob is `c989706f500c7063417d8998b08d0434772ced1b`, with
production-prefix blob `ac545b081e38d3f20f07289d158f1fdde126c2df`.
[Run 42887339170](https://github.com/mingley/partitionline/actions/runs/42887339170)
passed 11 focused controls in both profiles, strict test Clippy and rustdoc,
but failed formatting. Its broader producer unit subset reported 132 passed
and one failed in each profile; failure identities and causes were not
inspected. The baseline producer subset was still running when this note was
recorded. The candidate has additional tests, so raw totals have different
denominators.

A [format-only successor](https://github.com/mingley/partitionline/commit/e7fe196d4b9f6dc02166a4819e6dafbd1652d586)
changed one test line's formatting and passed
[run 42887516702](https://github.com/mingley/partitionline/actions/runs/42887516702).
Its full focused and broad tests were not run in that workflow. No paired
normal-path timing or large-queue cancellation timing is available for this
source. Open-worker and closed/full-retry controls use synthetic channels,
nonblocking callbacks and manually seeded records; they do not exercise public
admission, a retry actor, sticky-tag bookkeeping, arbitrary callbacks or Kafka.
The candidate remains unqualified.
