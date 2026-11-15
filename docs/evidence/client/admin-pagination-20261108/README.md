# Admin topic pagination deadline

The [constant-space change][candidate] is applied on main. Previously the
`describe_topics` path checked its deadline between pages but gave each
DescribeTopicPartitions request the original full timeout. It also returned
the accumulated result when a broker repeated the immediately preceding
cursor.

The candidate carries one checked deadline through cooperative request
encoding, all page exchanges, response decoding and aggregation. An
immediately repeated cursor returns a protocol error. A longer cycle can
continue until the deadline and then returns `Timeout`; no ordering of topic
names is assumed. The raw `describe_topic_partitions_timeout` method still
returns a single page and its next cursor, and its timeout now also covers
cooperative encoding and decoding.

For the raw method, an unsupported broker still returns `Unsupported`
before a zero or oversized timeout is examined. With support, zero and
unrepresentable timeouts return `Timeout`; an empty aggregate request is
still a no-op. The checked deadline is now established before encoding, so
an expired or unrepresentable timeout can take precedence over an
unencodable request where the old raw path encoded first.

## Focused and live checks

[Run 42469640650][focused] passed ten loopback controls in both default and
all-feature profiles, plus strict all-target/all-feature Clippy. Controls
cover a complete two-page result, immediate cursor repetition, an A→B→A
cycle that issues another request and times out, timeout overflow, unsupported
API precedence, zero timeout, empty input, and the raw single-page cursor.

A deadline control holds the first response for 1.2 seconds of a two-second
budget and withholds the second. The candidate returns `Timeout` inside
the test's 1.4-second wait after that request and marks the in-flight
connection closed. After the peer writes a late response to the old socket,
the next request performs a new ApiVersions handshake and reads a distinct
response on the replacement connection. Canceling during preparation before
a write keeps the original socket usable; canceling after the peer receives
a request marks it closed. These controlled local TCP histories do not prove
every cancellation or reconnection path.

The [Apache Kafka 4.1.2 smoke][live] passed in both profiles on a fresh
digest-pinned broker. Kafka's CLI created and independently described four
partitions. With a limit of one, the client received four raw pages with
indexes 0–3, and its aggregate call returned all four. This is one local
broker and one topic, not general compatibility qualification.

An [active A→B cycle control][active-cycle] compared the alternative
history-map candidate with the constant-space candidate on separate shared
runners. The history version returned `Protocol` after three requests; the
constant version made eight requests and returned `Timeout` after
250.84 ms with a 250 ms budget. Each arm ran once with normal server TCP
settings. These observations do not bound request counts or retained memory
for faster peers.

The candidate source Git blob is `8b65e1bbe24586a6c5074bbf4fe34a7668960cd1`
(SHA-256 `0e6132b7e78a02cc813053f4af5779b36b52b04d82bc0811fb07fed6c663138c`);
its formatted test blob is `8e35bcb0dfc4e1387bb41dd6f58b36aee758afdb`
(SHA-256 `cb19e0db9291dd8a1daf9eff49ea5bd039a6d8f8d72cf04af3c1cca0798e5114`).
The checked lockfile SHA-256 was
`bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.

## Existing Admin suite

The [exact-source broad run][broad] passed the ten new controls in each
profile. Default passed all 604 selected existing Admin library tests,
formatting and strict all-target/all-feature Clippy. The all-feature suite
had 602 passes and two failures: the description-extension and
producer-indexing tests observed 86.008040 ms and 85.124694 ms,
respectively, against 80 ms thresholds. The all-feature job stopped there.
The causes are unknown, and the overall run is not green.

An earlier [history-map candidate run][history-broad] had 601 passes and
three failures in default, and 602 passes and two failures with all features.
The default measurements were 85.792914 ms for description extension,
83.200723 ms for coordinator indexing, and 83.441912 ms for producer
indexing. All features measured 84.884409 ms and 81.221322 ms for extension
and producer indexing; coordinator indexing passed. Each failing threshold
was 80 ms.

A separate [same-host baseline/history control][fairness] retained the
thresholds and printed elapsed time after measurement. Across four alternating
pairs per profile, description extension failed 4/4 baseline and 4/4
history samples in each profile. Producer indexing failed 2/4 baseline and
0/4 history samples in default, and 2/4 baseline and 1/4 history samples
with all features. The [raw observations](fairness.csv) retain every result.
These instrumented samples on one shared x86 EPYC 7763 runner show that the
unchanged baseline can also fail; they do not explain the earlier failures.

The initial [history qualification attempt][compile-failure] failed to
compile in both profiles because the workspace's `unused_results` lint
rejected three discarded `Duration` values and one test-read `usize`.
No controls ran in that attempt. A later [focused run][history-focused]
passed ten history-candidate controls in each profile and strict Clippy,
and retained four expected baseline failures: partial `Ok` on an immediate
cursor repeat, EOF after the baseline sent another request in the A→B→A
fixture, an instant-overflow panic, and a held second page that outlasted
the test's observation window.

## Measurements and limits

The [three-way comparison][threeway] built separate baseline, history-map and
constant-space release binaries on one shared x86 AMD EPYC 9V74 runner with
Rust 1.99.1 and LLVM 23.1.1. Each of the six arm orders ran twice for each
shape, with one pinned logical CPU. The test fixture and lockfile matched
across arms. Ratios compare candidate to baseline elapsed time:

| Workload | History median (range; slower) | Constant median (range; slower) |
| --- | --- | --- |
| 4,000 single-page calls | 1.00996 (0.99021–1.01603; 11/12) | 1.01145 (0.99743–1.08702; 11/12) |
| 250 sixteen-page calls | 1.01218 (0.83652–1.07672; 11/12) | 1.00551 (0.83280–1.04393; 11/12) |

[All 72 samples](threeway.csv) include execution order. Baseline, history
and constant binary SHA-256 values were
`72fb3cd455c25a511ed11b6f640ce4cfa1c034f59c9bee11bbd81b61f64463d6`,
`cda5683b52472aa121cc966204060e8b070c37355991ea08daafff0aafe86516`
and `91832a7022d30a87001a23727136f70e695fc046d4cab0ff66e9dd3d8be52500`.
These short local TCP/mock measurements do not rank Kafka implementations.

An earlier [two-arm comparison][perf] on a shared EPYC 9V45 runner produced
history/baseline median ratios of 1.00746 for single-page calls and 1.00849
for sixteen-page calls; [all 32 samples](paired.csv) are retained. Runner and
binary differences prevent treating it as an exact repeat of the three-way
run.

The constant-space candidate does not retain cursor history proportional to
pages, but accumulated topic descriptions can still grow until the deadline.
The discarded history-map alternative retained one integer per prior cursor
and one key per distinct topic. An [allocator diagnostic][memory] measured
655,690 bytes of live requested allocation for 100,000 cursors from one topic
and 14,368,272 bytes for 100,000 distinct topics, excluding allocator metadata
and RSS. Neither measurement is a process-wide memory bound.

The outer deadline starts after the public method collects topic names.
Cooperative cancellation cannot preempt synchronous work or a large destructor,
so it is not a hard wall-clock bound. The Metadata fallback is unchanged.
The DescribeTopicPartitions decoder still reserves vectors from peer-declared
counts after the common remaining-wire-byte check; its separate
[initial-reservation candidate](../admin-dtp-reservation-20261108/README.md)
remains unapplied after valid-response timing regressions. This work does not
qualify the client for production or a release.

[candidate]: https://github.com/mingley/partitionline/commit/762940f9bbee0142939c1f71d003dad3651b2650
[focused]: https://github.com/mingley/partitionline/actions/runs/42469640650
[live]: https://github.com/mingley/partitionline/actions/runs/42469878503
[active-cycle]: https://github.com/mingley/partitionline/actions/runs/42470273823
[broad]: https://github.com/mingley/partitionline/actions/runs/42469879254
[history-broad]: https://github.com/mingley/partitionline/actions/runs/42468906570
[fairness]: https://github.com/mingley/partitionline/actions/runs/42469318221
[compile-failure]: https://github.com/mingley/partitionline/actions/runs/42468217803
[history-focused]: https://github.com/mingley/partitionline/actions/runs/42468474000
[threeway]: https://github.com/mingley/partitionline/actions/runs/42469685628
[perf]: https://github.com/mingley/partitionline/actions/runs/42468616379
[memory]: https://github.com/mingley/partitionline/actions/runs/42468271462
