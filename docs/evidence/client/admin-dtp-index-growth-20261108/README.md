# DescribeTopicPartitions index growth diagnostic

The [Ready/Pending poll probe](../admin-dtp-poll-phases-20261108/README.md)
found long polls both before and at completion for a 500,000-row Admin mock
response. The experiment here instruments an isolated test build to see
whether index growth and completion cleanup overlap those polls. It does not
change main or propose production preallocation.

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
or keep the peer alive through event capture. A stronger control is running.
No conclusion depends on the initial fixture proving those properties.

The preliminary source and fixture are test-only blobs
`8ae4f6eecad9701af2c340a7b9fa7f7923f39fda` and
`a9d2e458588967e067478d2565e0903780079100`, from
[commit 30bd6b9](https://github.com/mingley/partitionline/commit/30bd6b9b425f923e538181624f825c1806d258fb).
The runner reported AMD EPYC 9V74, stable Rust 1.99.1
(`58cf8f9c3`), LLVM 23.1.1 and an unavailable `cpu.max`.
The release test binary SHA-256 was
`365db42665fd9496fb69d4544ead85445fed811b36b7c8f6ee50df9902704543`;
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
