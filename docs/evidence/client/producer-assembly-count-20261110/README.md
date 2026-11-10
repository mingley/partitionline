# Producer assembly count experiment

This is an isolated producer request-assembly candidate. It is not applied to
main and does not repair producer delivery, receipt ownership or cancellation.

The existing mixed-batch reservation walk already visits the topic groups. The
candidate adds their group counts there and passes the result to assembly,
removing a later full topic hash-lookup pass. Normal reservation and assembly
yield points remain unchanged. A fallback that adds a new topic updates the
hint; an unexpected fallback that adds a group to an existing topic can
under-reserve and make the output vector grow.

The output vector still performs a synchronous allocation. Its destruction and
the walks over empty or missing topics can still run without a bounded yield.
A yielding future does not guarantee another task will run. No wall-time bound
is established.

## Controls

The [instrumented baseline](https://github.com/mingley/partitionline/commit/317af2efc2e6484a95f252e4b77a767c3f528f15)
added debug-test-only lookup observations to source `5cd53d3c84eb23671b1d74f2c488d95ba69208e5`.
In [run 42866126362](https://github.com/mingley/partitionline/actions/runs/42866126362),
the first direct poll of 8,193 synthetic one-partition topics performed 16,385
topic lookups: the pre-count and the first 8,192 removals. Dropping that
directly controlled future at its existing yield closed 8,192 receipt
channels, with one topic still in scratch. Both feature profiles observed
these counts. This is a seeded private-state control, not a public-client or
real-Kafka loss history, and it does not establish whether a receipt would
recover later in a different worker history.

An earlier [yield-during-count draft](https://github.com/mingley/partitionline/commit/5f2dbd0b5f0e5a307234f2e503aa0e101aff0934)
passed its three focused controls in each profile
([run 42864841941](https://github.com/mingley/partitionline/actions/runs/42864841941))
but added a new suspension point. A later cancellation still closed 8,192
receipt channels and retained 8,193 topics. Formatting and Clippy failed.
That draft is superseded and unapplied.

A later [instrumented count-reuse revision](https://github.com/mingley/partitionline/commit/2ed232500456a48d063ca81e09ad638225fa43be)
passed seven selected controls per feature profile, formatting and Clippy
([run 42868667760](https://github.com/mingley/partitionline/actions/runs/42868667760)).
Its first direct poll performed 8,192 observed topic lookups. Two separately
run diagnostics still failed: dropping at the existing assembly yield closed
8,192 receipt channels, and an empty-topic walk did not yield. The workflow
marked those steps `continue-on-error`; its success does not make either
diagnostic pass. A synthetic stale-scratch fixture retained 1,024 stale topics
while the four-group output capacity remained four; this is not a natural
worker history.

The [clean candidate](https://github.com/mingley/partitionline/commit/561d3235471dddd6ac87d048f586b53c47dd9430)
removes the debug lookup hooks and the isolated diagnostics.
[Run 42869430328](https://github.com/mingley/partitionline/actions/runs/42869430328)
passed five assembly controls, one existing large-assembly test and five
grouping tests in each feature profile, plus formatting, all-target/all-feature
Clippy and strict rustdoc. The sparse-tail controls hold any completed output
while checking its receipt. They do not prove pending-assembly cancellation
safety. The initial revision [12c9229](https://github.com/mingley/partitionline/actions/runs/42866731201)
did not compile its tests because a new `usize` result was unused, and also
failed formatting and Clippy. That failure was corrected in the later source.

## Measurements and further checks

Three preliminary paired benchmark workflows,
[42867330546](https://github.com/mingley/partitionline/actions/runs/42867330546),
[42868849526](https://github.com/mingley/partitionline/actions/runs/42868849526), and
[42870207763](https://github.com/mingley/partitionline/actions/runs/42870207763)
completed successfully but are **invalid**: their worktrees shared one Cargo
target directory and the candidate invocation did not recompile after the
baseline build. They could reuse the baseline executable, so none of their
measurements should be used. The third run verified different source prefixes,
which does not establish that it ran different binaries.

The first measurement workflow
[42867269317](https://github.com/mingley/partitionline/actions/runs/42867269317) failed
before starting a job because it used a runner context at job level. A later
[source-marker attempt](https://github.com/mingley/partitionline/actions/runs/42870131054)
failed before measurement. The corrected
[measurement run](https://github.com/mingley/partitionline/actions/runs/42870370271)
uses separate Cargo target directories, checks production-source prefixes
against the baseline and clean candidate, records executable hashes and refuses
identical executables. It was still running when this note was written. Its
release test binaries add benchmark-only code, so even a completed result
would be an in-process diagnostic rather than the shipped binary or a Kafka
ranking. It does not use socket sampling or explicitly access `/proc`.

The [broad client and Kafka run](https://github.com/mingley/partitionline/actions/runs/42870880409)
uses separate Cargo targets for the baseline and clean candidate. Its Kafka
job passed on a pinned, plaintext, single-broker Apache Kafka 4.3.1 instance.
Each arm acknowledged 256 records across four fresh single-partition topics;
the returned offsets were 0–63 for each topic. A separate Java CLI reader
observed the first 64 values in order in each topic. The baseline and candidate
producer source SHA-256 values were
`0a2807aa920d2d9ac9eee5d05d45e3a58a99e9633d2e3d0097537376420d9fa0` and
`7fe720f54497f1b99a70b37c7794b0d378593ad5f7750ad580a801cbef6c42c6`;
the shared smoke example was
`8553578f832b30e77053775111e03c2713e7435dcc86be453737900d647244bc`.
The reader stops after 64 values per topic, so it does not rule out extra
records after those reads. The smoke does not establish fault behavior,
durability modes, mixed-request batching, or production readiness. Its broad
client job was still running when this note was updated.

The [clean patch](clean.patch), [instrumented patch](instrumented.patch),
[focused workflow](focused-workflow.yml.txt), [broad/live workflow](broad-live-workflow.yml.txt),
[measurement workflow](measurement-workflow.yml.txt), and
[live producer example](producer-smoke.rs.txt) are retained as evidence. In
[run 42871918359](https://github.com/mingley/partitionline/actions/runs/42871918359),
both patches applied to the exact baseline and reproduced their recorded
producer source Git blob hashes.
