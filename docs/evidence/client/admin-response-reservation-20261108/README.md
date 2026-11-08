# Admin response initial reservations

The Admin response decoders previously reserved result vectors directly from
peer-declared counts. The common array-length check requires each count to be
no larger than the remaining wire bytes; it does not account for the larger
in-memory element type. A padded malformed response can therefore cause a
large allocation request before its first element is validated.

The applied change initially asks `Vec` for at most 16 KiB worth of element
capacity, or one element when that element is larger. `Vec` and its allocator
may provide more capacity, and this request does not bound allocator overhead
or RSS. Vectors can continue growing as valid elements are decoded. The
changed synchronous and cooperative response decoders cover CreateTopics,
DeleteTopics, DescribeConfigs, CreatePartitions,
AlterConfigs, IncrementalAlterConfigs and DeleteRecords, including their
changed nested vectors. Other decoders and request paths are not covered.
The change does not impose a final response limit, total process memory limit,
per-connection budget or fallible allocation policy.

## Source and focused checks

Baseline commit: `de152e78ca6d85e6cd4ea5915058b2bb6141cf42`.
Candidate commit: `28c779958274adf7101312be911bbe2f9b9e2574`.
Baseline `src/protocol/admin.rs` SHA-256:
`65d3259a6314418295838534293e2e6b5f36545ac52a99383e3ee852e0060418`.
Candidate source SHA-256:
`3d6aead72b5f4ca189c681165c969d8d46a8404ec1ab42444d58054e5f96e513`.
Candidate test `tests/admin_response_reservation.rs` SHA-256:
`4799ef1f8181dd6403cbc1bfc567b22ce81d2de76eabf12988f61038d5dd74f6`.

[Run 42454848699](https://github.com/mingley/partitionline/actions/runs/42454848699)
passed the new response test and 472 selected Admin unit tests in each of
the default and all-feature profiles on x86 Linux with Rust 1.99.1. Formatting and strict
all-target/all-feature Clippy passed. The earlier
[run 42454670542](https://github.com/mingley/partitionline/actions/runs/42454670542)
failed at formatting before any tests ran; the test formatting was corrected.
A later [combined-tree run 42456288775](https://github.com/mingley/partitionline/actions/runs/42456288775)
also failed formatting because its initial test blob was unformatted. The
static job stopped before Clippy; the corrected tree restores the exact test
blob used in the passing focused and integration runs. The corrected
[combined run 42456533805](https://github.com/mingley/partitionline/actions/runs/42456533805)
passed the response test and 472 Admin unit tests in each feature profile,
formatting, and strict all-target/all-feature Clippy. It checked the same
client source, buffer comment and test bytes reported here; this paragraph is
a later report-only update.

The new test compares synchronous and cooperative output and cursor position
for valid and truncated responses, including fragmented buffers. It exercises
CreateTopics versions 0, 4, 5, 7; DeleteTopics 0, 3, 4, 6;
CreatePartitions 0–3; DescribeConfigs 0–4; AlterConfigs 0–2;
IncrementalAlterConfigs 0–1; and DeleteRecords 0–2. Larger fixtures include
320 topic results, 320 configuration entries, 340 synonyms and 400 deleted
record partitions; these are not all-version or live-broker qualification.

[Run 42455384163](https://github.com/mingley/partitionline/actions/runs/42455384163)
passed 23 specifically named existing Admin mock integration tests in each
feature profile. The cases cover controller routing, retries, timeouts,
configurations and deleted records. Version checks include CreateTopics
4, 5 and 7; DeleteTopics 3–6; CreatePartitions 1–3; DescribeConfigs 1
and 3; AlterConfigs 1–2; IncrementalAlterConfigs 0–1; and DeleteRecords
1–2. The run verifies the candidate source and test SHA-256 values above.
It does not cover the complete client suite or a real Kafka broker.

## Malformed input and an initial performance comparison

[Run 42454942487](https://github.com/mingley/partitionline/actions/runs/42454942487)
compared baseline and candidate release builds on one shared x86 AMD EPYC 7763
host, pinned to a single logical CPU, with Rust 1.99.1. The standalone probe
uses an instrumented system allocator. It measures allocation requests, not
RSS or process-wide memory. Its source SHA-256 was
`c452921a8fdc4e10c80eb200cb88c39cd798f1a7841420357a7417f1d809a367`.

For 11 malformed, padded flexible responses declaring 100,000 elements,
the baseline's largest individual allocation request ranged from 1,600,000 to
10,400,000 bytes; the candidate's ranged from 16,320 to 16,384 bytes. This
includes nested CreateTopics configs, DescribeConfigs entries and synonyms,
and DeleteRecords partitions. The first attempted element failed, so these
numbers do not establish an aggregate bound after many valid elements.
Synchronous and cooperative variants passed in each feature profile, and their
errors and remaining cursor bytes matched baseline. A differential corpus
compared full decoded debug output, errors and remaining bytes for valid input,
every truncated prefix and one-byte mutations of small response fixtures
across the versions listed above. The corpus included fragmented cooperative
buffers.

Eight paired, alternating-order timing samples used valid, memory-resident
CreateTopics and DescribeConfigs responses. Candidate/baseline median elapsed
ratios were 0.9817, 1.0111 and 1.0622 for 10, 320 and 20,000 CreateTopics
results; the 20,000-result paired range was 1.0584–1.1008. DescribeConfigs
ratios were 0.9916, 0.9871 and 1.0205 for 10, 320 and 10,000 entries; the
10,000-entry range was 1.0021–1.0411. Small-response ranges included outliers.
These short samples are not Kafka throughput results. The initial run resolved
standalone probe dependencies independently in separate project lockfiles,
so it did not establish identical dependency graphs. The growth follow-up
below uses a common lockfile.

## Same-lock allocation and growth comparison

[Run 42455741181](https://github.com/mingley/partitionline/actions/runs/42455741181)
compared baseline, the 16 KiB initial-cap candidate, and an isolated 4× growth
experiment on a shared Intel Xeon Platinum 8573C host with Rust and Cargo
1.99.1. The three standalone probes used the same generated dependency
lockfile. The source SHA-256 values were the baseline and candidate hashes
above and `985683df85d3668947609dcc6a8b5c19544602ae162b1165de8bfe250a6b15f6`
for the experiment. The probe SHA-256 was
`64e607cbf38f43cf985eecc06befc2e352df075cd6a488ce365ea674c8346f6c`.

The experiment also initially asks for at most 16 KiB worth of slots per
vector (or one oversized element), then requests up to four times the current
capacity only when that vector is full of validated
elements, capped at the declared count. It is **not applied**. It passed the
same first-invalid controls and small differential corpus, but its formatting,
full focused suites and malformed-input controls around growth boundaries have
not been qualified.

Each timing below is the median candidate/baseline elapsed ratio of eight
three-arm samples; the parenthesized numbers are the range of paired ratios.
The arm order rotated across samples, pinned to one logical CPU. These are
short, in-memory decoder measurements, not Kafka throughput or a comparison
against another implementation.

| Valid response | 16 KiB initial cap / baseline | 4× experiment / baseline |
| --- | --- | --- |
| CreateTopics, 10 results | 1.0268 (0.9232–1.1991) | 1.0421 (0.9379–1.1102) |
| CreateTopics, 320 results | 1.0448 (0.9332–1.2954) | 1.0681 (0.9474–1.1540) |
| CreateTopics, 20,000 results | 1.0604 (0.9667–1.7003) | 1.1053 (0.9930–1.1755) |
| DescribeConfigs, 10 entries | 1.0027 (0.9871–1.3458) | 1.0293 (1.0215–1.0414) |
| DescribeConfigs, 320 entries | 1.0104 (0.8897–1.0264) | 1.0017 (0.7737–1.0139) |
| DescribeConfigs, 10,000 entries | 0.9976 (0.9917–1.0640) | 0.9879 (0.9805–0.9962) |

The 20,000-result CreateTopics candidate was slower in the median in both
this noisy run and the initial AMD comparison; all eight paired ratios in
the AMD run were above baseline. The 4× experiment reduced some allocation
requests but did not improve CreateTopics timing enough to justify replacing
the simpler candidate.

The probe tracked allocation requests made during synchronous decoding.
The peak is the greatest sum of tracked live requested sizes observed after
allocator calls; it excludes the preexisting wire frame, allocator metadata,
other process allocations and any temporary copy within `realloc`. It is not
RSS or a process-wide bound. Values are bytes.

| Valid response | Baseline largest / peak | Initial cap largest / peak | 4× largest / peak |
| --- | --- | --- | --- |
| CreateTopics, 10 results | 960 / 1,030 | 960 / 1,030 | 960 / 1,030 |
| CreateTopics, 320 results | 30,720 / 33,490 | 32,640 / 35,410 | 30,720 / 33,490 |
| CreateTopics, 20,000 results | 1,920,000 / 2,128,890 | 2,088,960 / 2,297,850 | 1,920,000 / 2,128,890 |
| DescribeConfigs, 10 entries | 1,040 / 3,588 | 1,040 / 1,908 | 1,040 / 1,908 |
| DescribeConfigs, 320 entries | 33,280 / 112,618 | 65,312 / 90,890 | 33,280 / 58,858 |
| DescribeConfigs, 10,000 entries | 1,040,000 / 3,528,978 | 1,044,992 / 1,853,970 | 1,040,000 / 1,848,978 |
| DeleteRecords, 20,000 partitions | 320,000 / 320,053 | 524,288 / 524,341 | 320,000 / 320,053 |

For CreateTopics with 20,000 results, allocation/reallocation call counts
were 20,001 / 20,008 / 20,005 for baseline, initial cap and 4×; cumulative
requested bytes were 2,128,890 / 4,370,490 / 3,516,090. For
DescribeConfigs with 10,000 entries, the respective counts were
50,003 / 50,009 / 50,006 and cumulative requested bytes were
3,528,978 / 2,882,634 / 2,191,866. The candidate allocates fewer synonym
slots in the one-synonym fixture because the old `Vec::reserve` could
round up from one to four. Cooperative results were also recorded; a
current-thread yield adds one small allocation in the largest fixtures.

The 22 first-invalid controls, 24 valid allocation cases and small
differential corpus passed for both candidates in this default-feature run.
The [measurement archive](https://github.com/mingley/partitionline/actions/runs/42455741181/artifacts/13607453773)
has GitHub-reported SHA-256
`5ade3f6d6f76857e254ab116669f210ec26fd8e0a71792330c1232d2ff9fc255`;
the archive was not independently inspected here.

Neither set of focused controls closes the known client CI failures or
qualifies a release. Broader allocation accounting, additional Admin response
families, valid large response performance, concurrency and broker
interoperability remain open.
