# DescribeTopicPartitions index growth diagnostic

The [Ready/Pending poll probe](../admin-dtp-poll-phases-20261108/README.md)
found long polls both before and at completion for a 500,000-row Admin mock
response. The experiment here instruments an isolated test build to see
whether index growth and completion cleanup overlap those polls. It does not
change main or propose production preallocation.

## Persistent-peer control

[Run 42485887797](https://github.com/mingley/partitionline/actions/runs/42485887797)
passed 60 cases on a single runner, five alternating ordinary/reserved pairs
per size and row kind. It used one test-instrumented release binary for both
arms. The peer kept its socket and response buffer alive until after the
client's Ready-poll events were captured. The fixture checked every returned
name, all 16 topic-ID bytes, internal flag, error, empty partition list and
omitted authorization flag. The caller retained the result through event
capture.

At 500,000 rows, all ten ordinary samples had an observed capacity increase
from 458,752 to 917,504 during the longest post-write Pending poll, at the
insertion of entry 458,753. The timed insertion intervals were 47.99–59.72 ms.
All ten reserved controls recorded no growth; their longest Pending polls
were 9.53–12.47 ms. The test-only reserve took 0.347–0.400 ms and allocated a
table with capacity 917,504 before conversion. These are wall-clock timings
in the instrumented fixture. Reservation changes allocation timing, table
layout, cache and working set; independent hash states and unequal numbers
of trace events add variation. The full poll difference cannot be assigned
solely to resizing.

Each accepted one-page case had exactly one response-drop event followed by
one index-drop event, both inside the Ready poll. At 500,000 rows the
ordinary arm recorded 6.22–6.65 ms around the response and 20.55–29.04 ms
around the index; the reserved arm recorded 6.21–6.97 ms and
27.28–32.52 ms respectively. Markers include TLS and scheduling effects and
do not measure when memory is returned to the system. The output collection
was not dropped in these intervals.

The test ran 16 empty-marker pairs for each drop kind in every process:
1,920 [calibration samples](calibration.csv), ranging from 10 to 40 ns in
this run. The sample storage was reserved at capacity 64 and did not grow.
Calibration is not subtracted from measured intervals. There were 60 distinct
64-bit fingerprints of a fixed-string hash, one per process. A fingerprint is
not the seed and does not make the arms seed-matched.

[Summaries](validated-samples.csv) and [all recorded events](validated-events.csv)
retain the samples. Only the longest post-write Pending poll and Ready poll
have event ranges; events are not mapped to every individual poll. The write
marker does not mean the client has finished reading or decoding. The mock
peer and client share a process and allocator. The test-build insertion timer
excludes cloning the name but includes the insertion and post-insert checks.
It records capacity changes only when the pre-insert length equals capacity;
this is not an exhaustive trace of internal table work.

The source and fixture are test-instrumented blobs
`81d47fe6addd0c0f97d459497b7bcafc2b4a5118` and
`de32bd06a510572efc98a9d09dbb819b600ab5e0`, from
[commit feae6ae](https://github.com/mingley/partitionline/commit/feae6ae08b5007531cd78e40fbfcb5c92d1f8894).
The runner reported AMD EPYC 9V45, stable Rust 1.99.1
(`58cf8f9c3`), LLVM 23.1.1 and an unavailable `cpu.max`.
The release test binary SHA-256 was
`e425cdf77bd5ecaec8fcc89855df328d9015701e7b01bda712d363ecceb229da`;
the binary was not retained. The run artifact retains its raw output for 30
days.

## Preliminary control

[Run 42485152243](https://github.com/mingley/partitionline/actions/runs/42485152243)
passed 60 public-call cases in one process per sample, alternating an ordinary
index and a test-only control that reserves for all response rows before
conversion. Both arms use the same instrumented binary. There were five
pairs per row kind at 8,192, 100,000 and 500,000 rows. The fixture used a
current-thread runtime and a local peer that checked requested names; it
checked returned names, topic errors, empty partitions and omitted
authorization. It did not check every topic-ID byte or `is_internal`.
[Sample summaries](preliminary-samples.csv) and
[observed events](preliminary-events.csv) retain this first run.

In all ten ordinary 500,000-row cases, the slowest post-write Pending poll
contained an observed map capacity increase from 458,752 to 917,504, at entry
458,753. The timed insertion interval was 78.47–88.32 ms. The pre-reserved
control observed no map growth; its longest Pending polls were 11.73–13.13 ms.
That control changed table size and allocation timing. Hash states were
independent; its reduction cannot be attributed solely to resizing.

The preliminary one-page response and index drop markers recorded
6.28–6.90 ms and 31.69–35.59 ms respectively during the Ready poll. The
returned descriptions were still held by the caller. These are instrumented
wall-clock intervals, not isolated allocator CPU time or a bound on
cancellation. The local peer closed its socket and freed its encoded response
after writing, which can affect the observed schedule and allocator state.

This initial fixture did not verify exact marker order, calibrate the markers,
or keep the peer alive through event capture. A stronger persistent-peer control is recorded above.
No conclusion depends on the initial fixture proving those properties.

The preliminary source and fixture are test-only blobs
`8ae4f6eecad9701af2c340a7b9fa7f7923f39fda` and
`a9d2e458588967e067478d2565e0903780079100`, from
[commit 30bd6b9](https://github.com/mingley/partitionline/commit/30bd6b9b425f923e538181624f825c1806d258fb).
The runner reported AMD EPYC 9V74, stable Rust 1.99.1
(`58cf8f9c3`), LLVM 23.1.1 and an unavailable `cpu.max`.
The release test binary SHA-256 was
`365db42665fd9496fb69d4544a8d16209c3df111586c0880570b6671cdbfe8dd`;
the binary was not retained. The run artifact contains the raw output for 30
days.

The instrumented build adds work to insertions and changes its async state
layout. The peer and client run in the same process, so peer request parsing
also affects allocator state. Polls beginning after the peer's write can
still include socket reads and decoding. Only the longest Pending poll and
the Ready poll retain event ranges. These data do not establish real Kafka
latency, production allocation counts or a hard deadline. Reserving from an
untrusted response length could also allocate much more than the number of
distinct topics; memory use, cancellation and errors need separate controls
before considering any implementation.
