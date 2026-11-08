# Producer timestamp candidate

Status: the narrow timestamp correction is applied. The broad producer suites
failed and the cancellation-safe batch-ownership repair remains open. This
does not establish production readiness or a general performance claim.

## Candidate and boundary

The comparison starts at [`7dfbe4735d1c4268a1b8a7185bb34f1121f0db13`](https://github.com/mingley/partitionline/commit/7dfbe4735d1c4268a1b8a7185bb34f1121f0db13).
The source-only and permanent-test candidate is
[`1fece7daf7e98d00c3e24629f3c89725365dce0c`](https://github.com/mingley/partitionline/commit/1fece7daf7e98d00c3e24629f3c89725365dce0c):

| File | Git blob | SHA-256 |
| --- | --- | --- |
| `src/producer.rs` | `dcd012b3f86a8b9d4254f88d7f48946ce8f9e922` | `60a3fa0985358198e81757e6080af8b58ab52cb00edc1f34e2c97ed033e90b78` |
| `tests/common/mod.rs` | `4afd104e0b5e10227167d0d38328fe03dda07a0a` | `45921a161ac3d1e310558d2ec37a9f5c7b4f8b5e7ed1813b34f6ccf468c27559` |
| `tests/producer_timestamp_contract_public.rs` | `d8f1f45285c15d1edd563898754d128045551886` | `26f4e495860ea6af32158264a57e62acedcdafb6d3dbaeeec3d22f3909d4e908` |

The two encoders save an implicit record timestamp in the existing
`Pending` while encoding. Later encodes and receipt metadata can reuse it.
Explicit timestamps are preserved; a nonnegative broker-provided
log-append timestamp still takes precedence in receipt metadata. The mock's
log-append override changes the response only and does not model a broker
rewriting the log.

The unchanged behavior and the timestamp mismatch on the mock are documented
in [producer retry timestamps](../producer-retry-timestamp-20261108/README.md).
The separate [batch ownership controls](../producer-batch-ownership-controls-20261108/README.md)
and [retry handoff witness](../producer-retry-flush-split-20261108/README.md)
describe unresolved loss, splitting and ambiguous-write risks. Freezing
timestamps does not fix sequence ownership, worker cancellation, callbacks,
per-record deadlines or ambiguous outcomes.

## Focused and broad checks

[Final-pin run 42485743433](https://github.com/mingley/partitionline/actions/runs/42485743433)
used workflow commit `cf223c6eecec24869212a2b5745e56219ab776f5`.
Formatting and `cargo clippy --locked --all-targets --all-features -- -D warnings`
passed. In both default and all-feature profiles, the permanent public mock
timestamp contract passed and the 23 `produce_cancel` tests passed.
The contract exercises an ordinary delayed acknowledgement, mixed implicit
and explicit records, a dropped-response retry, `acks=0`, a mock positive
log-append response override, classic Produce v3 and flexible v9/v13, and
uncompressed, gzip, snappy, lz4, and feature-enabled zstd paths.

The broader checks on the same final pins did not pass. In the default
profile, the producer library filter passed 121 and failed 1
(`replacing_an_idle_metadata_socket_lets_the_broker_leave`); in all features,
120 passed and 2 failed (the same test plus
`assembling_many_produce_topics_does_not_hold_the_caller`). The
`mock_produce` suite passed 28 and failed 17 in each profile. The
[default job](https://github.com/mingley/partitionline/actions/runs/42485743433/job/128747090425)
and [all-feature job](https://github.com/mingley/partitionline/actions/runs/42485743433/job/128747090205)
retain their full failures. They are not a passing qualification and cannot
be attributed to the candidate from this run alone.

An earlier formatting check on the diagnostic fixture failed
[run 42484591240](https://github.com/mingley/partitionline/actions/runs/42484591240).
The subsequent final-production-source run
[42485440275](https://github.com/mingley/partitionline/actions/runs/42485440275)
passed formatting but failed strict Clippy with 29 fixture indexing and
`expect` lint errors. The final test helpers contain scoped fixture lint
expectations; the later strict pass above is for the corrected final pins.

## Cancellation control

The candidate's private in-memory control drops the encoder future at its
existing yields for uncompressed and gzip batches of 8,193. A drop at the
sizing yield leaves all timestamps unset; a drop after 8,192 writes leaves
that prefix stamped and the last record unset. The fixture itself retains
the group, then resumes with a different clock; the prefix remains stamped
and the tail receives a timestamp. This is not a production retention or
cancellation test: the current enclosing worker can lose the detached group
when canceled. A future rooted batch owner must preserve the complete batch,
including a stable batch clock before first encode, with its deadlines and
callbacks through every retry, flush, close and failure path.

The instrumented prior ownership histories were run on candidate-derived
source in [42486320533](https://github.com/mingley/partitionline/actions/runs/42486320533)
(default and all features). Their assertions confirm the known callback-loss
and partial-handoff mock histories remain observable; they are not fixes or
real-broker outcomes. Their instrumented source also failed diagnostic
formatting, and that run is not a strict qualification.

## Additional checks

[Matched run 42486544658](https://github.com/mingley/partitionline/actions/runs/42486544658)
used workflow commit `93521012a9520f2d264428ea2ee7c2338cee8453`, the same
mock fixture for both arms, separate targets, and rustc 1.99.1 on each
profile's runner. The baseline source blob was
`1e7733c136e54b532cdc8bddabd1e0c828e50ed2`; the candidate was the
`dcd012b3...` blob above. The candidate has one extra library test.
In the default library filter, baseline was 117 pass/4 fail and candidate
119 pass/3 fail; all three candidate failures also appeared in baseline,
and `encoding_many_produce_partitions_does_not_hold_the_caller` failed only
on baseline. In all features, baseline was 119 pass/2 fail and candidate
120 pass/2 fail, with the same two failures. For `mock_produce`, both arms
were 28 pass/17 fail in each profile, with identical failing test names.
The comparison job completed because it recorded failures as data; the
underlying suites failed. These single sequential runs do not determine the
causes of the failures or prove timing stability. Full logs and comparisons
are artifacts [default 13619841653](https://github.com/mingley/partitionline/actions/runs/42486544658/artifacts/13619841653)
(SHA-256 `115195f956c20d3786eca2db27382aeaa96567200bdc05eaea80af260e9be364`)
and [all 13619713114](https://github.com/mingley/partitionline/actions/runs/42486544658/artifacts/13619713114)
(SHA-256 `97d3998cc9ec1577e1bdb0295af40492787a67a846aa0d697cab7a2c5c79a8d4`).

[Real Kafka smoke 42486752702](https://github.com/mingley/partitionline/actions/runs/42486752702)
used workflow commit `3fb45631694da7f670b4dc06b35a57541ba620bb`, separate
source-pinned release binaries and Apache Kafka 4.1.0 CreateTime topics.
The image digest was
`sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f`.
A local proxy captured Produce v13 frames and delayed responses; Kafka's
Java client decoded one request/response per arm. The verifier required each
Produce response delay to be at least 150 ms, successful responses with
offset 0 and `log_append=-1`, and matching decoded record and batch times.

| Arm | Produce wire time | Receipt time |
| --- | ---: | ---: |
| Baseline | 1794146172780 | 1794146172991 |
| Candidate | 1794146173803 | 1794146173803 |

This covers one ordinary acknowledgement per arm. It does not read broker
storage, exercise a retry or test a LogAppendTime topic. The Kafka Java
client jar SHA-256 was
`180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb`;
binary SHA-256 was
`1147ea6c4848c9531d7d30f77e1b0fd11fda75b261ee7ff1b2f21aa1fd957a52`
for baseline and
`a58b654a83631a050d8a6da59ee65fa95f1ceb1af053ef65627c86671e058692`
for the candidate. Captured frames, proxy/broker logs and the comparison are
in [artifact 13619992489](https://github.com/mingley/partitionline/actions/runs/42486752702/artifacts/13619992489)
(SHA-256 `a20a19231df568f2e201fcd158f803be68400b53566feee611ed42f37166572e`).

The supplementary 4,096-record timing run
[42486437941](https://github.com/mingley/partitionline/actions/runs/42486437941)
used workflow commit `e5d41361ebde78eaf3198ab8895f03200667c36b`,
rustc 1.99.1 and an x86 AMD EPYC 9V74 hosted runner. It ran eight
alternating pairs per mode after calibrating each arm for at least 250 ms.
In the mock producer path, elapsed ranges for 524,288 records were
485.7–524.4 ms baseline versus 478.8–521.1 ms candidate with implicit
timestamps, and 496.8–559.5 ms versus 491.3–532.6 ms with explicit ones.
In the private encoder alone, elapsed ranges for 8,388,608 records were
352.3–391.1 ms versus 350.5–357.2 ms implicit, and 355.0–362.9 ms versus
354.2–390.0 ms explicit. Each implicit encoder iteration reset its records
before timing; encoded lengths and CRCs matched across arms. This earlier
fixture did not decode and verify every timestamp, so these observations
are supplementary. The raw samples and distinct binary and source hashes
are in [artifact 13620022750](https://github.com/mingley/partitionline/actions/runs/42486437941/artifacts/13620022750)
(SHA-256 `8e1414b169dbfe9c8816717d66854cd66da0480bbdf0b82b643d49d217d41bd3`).
These shared-host microbenchmarks do not establish Kafka throughput or a
general performance improvement.

The expanded run [42486671927](https://github.com/mingley/partitionline/actions/runs/42486671927)
used workflow commit `95385bc32326b2008493be95d4a829627e344ffc`,
separate targets and distinct binary and source hashes. It completed
eight alternating mock-producer pairs at each size and mode, with fresh
records and decoded Produce timestamp checks:

| Batch | Timestamp | Records per sample | Baseline range (ms) | Candidate range (ms) |
| ---: | --- | ---: | ---: | ---: |
| 1 | implicit | 16,384 | 419.3–427.4 | 417.8–423.8 |
| 1 | explicit | 16,384 | 419.0–429.4 | 417.4–424.9 |
| 4,096 | implicit | 524,288 | 487.5–507.1 | 485.2–517.9 |
| 4,096 | explicit | 524,288 | 489.2–512.1 | 489.8–510.7 |

The run **failed** before collecting paired encoder samples. The first
batch-1 baseline implicit calibration reached its 1,048,576-iteration cap
at 195.1 ms, below the required 250 ms. The job did not produce its final
JSON report, but the successful producer samples, calibration rows, build
output and hashes are in [artifact 13620520060](https://github.com/mingley/partitionline/actions/runs/42486671927/artifacts/13620520060)
(SHA-256 `e4d063af04e1026dfd60f697802c204885965c97f20aa898e2454144c70378d1`).
The expanded fixture source SHA-256 was
`99de9a45691353628f3b7c60687cedd13abda4d85e0a1a490e8b1daf3d9f43d1`.
Its instrumented producer binaries were
`c931ccc87fb4015df2409083b9d3ac8a3f20c037b369f5929e073bf6c32513f1`
baseline and
`53bd34b3b389cfc847025e83d030a269e18bbaa90c88a2792e330631db3bba24`
candidate. The instrumented source hashes are in the artifact. The
shared-host ranges do not establish a general speed difference.

The encoder iteration cap was increased without lowering the calibration
threshold in diagnostic commit `441514cf41bd56d933c432a9300f25142286d003`.
[Run 42487803923](https://github.com/mingley/partitionline/actions/runs/42487803923)
then completed on one shared virtualized AMD EPYC 7763 x86 runner with
rustc 1.99.1. It used distinct source and binary hashes, separate targets,
calibration of each arm/mode to at least 250 ms, and eight alternating
pairs per combination. The following values are elapsed milliseconds per
sample: minimum / median / maximum. Paired deltas compare candidate elapsed
time to baseline within each pair; positive means candidate took longer.
Both paths use uncompressed eight-byte values; the direct encoder uses
Produce v9. The producer path sends to the in-process mock and flushes
each batch.

| Path | Batch | Timestamp | Records/sample | Baseline ms | Candidate ms | Paired delta range |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| mock producer | 1 | implicit | 8,192 | 291.15 / 293.44 / 297.06 | 291.32 / 296.31 / 300.49 | -1.22% to 2.35% |
| mock producer | 1 | explicit | 8,192 | 293.74 / 295.93 / 303.06 | 295.45 / 298.74 / 304.05 | -2.18% to 2.95% |
| mock producer | 4,096 | implicit | 262,144 | 272.46 / 276.58 / 278.03 | 274.55 / 278.06 / 284.19 | -0.74% to 4.30% |
| mock producer | 4,096 | explicit | 262,144 | 272.81 / 278.38 / 281.83 | 272.71 / 276.15 / 281.77 | -2.45% to 1.83% |
| encoder | 1 | implicit | 1,048,576 | 256.59 / 257.25 / 257.62 | 261.70 / 262.53 / 269.47 | 1.84% to 4.74% |
| encoder | 1 | explicit | 1,048,576 | 256.65 / 257.49 / 258.08 | 261.38 / 261.68 / 263.06 | 1.46% to 2.04% |
| encoder | 4,096 | implicit | 4,194,304 | 256.83 / 257.53 / 259.43 | 259.05 / 260.69 / 261.96 | 0.55% to 1.87% |
| encoder | 4,096 | explicit | 4,194,304 | 259.14 / 259.85 / 261.52 | 259.30 / 260.10 / 262.47 | -0.75% to 0.92% |

This run observed a small encoder cost: every batch-1 implicit pair took
1.84%–4.74% longer on the candidate, and every batch-4,096 implicit pair
took 0.55%–1.87% longer. The batch-1 explicit control also took longer in
every pair. The mock-producer ranges overlapped. Each private-encoder
iteration resets implicit timestamps outside the timed region; explicit
values remain set. It verifies decoded payloads and timestamps, and the
arms' encoded lengths and CRC32C values match. A CRC match is not byte-for-byte
proof. The mock producer uses fresh records and checks mock-observed
timestamps. Per-encode clock reads and a shared virtualized host can affect
these results, particularly for the one-record batch. These tests do not
measure a real broker or establish a general speed difference.

The instrumented source SHA-256 values were
`a1556577d889aa6d536cc55456ed5820fc9547fff9175b812575cd1420147508`
baseline and
`6f0e86d0f0ec7d6f270b2fe78ac33e87e96cf2d4704981f20db404350709e5ab`
candidate. Encoder binary SHA-256 values were
`ea19246d08dfa6e8419f843ea2eee751b907c6d20b03b724f2d98e32768702cd`
and
`c9de132b10302ba9e089d87af2611a8c4f64daa32380caf05f8cfdbeb327e5d1`.
Mock-producer binaries were
`c931ccc87fb4015df2409083b9d3ac8a3f20c037b369f5929e073bf6c32513f1`
and
`53bd34b3b389cfc847025e83d030a269e18bbaa90c88a2792e330631db3bba24`.
The identical fixture paths, original source pins and build output are in
[artifact 13620677451](https://github.com/mingley/partitionline/actions/runs/42487803923/artifacts/13620677451)
(SHA-256 `f436e925eb7e5be8bc71a9c8547026f979777ed29aa843f133a3bec40cec59c0`).
The [raw measured samples](paired-timestamp-samples.csv) reproduce all
128 sample rows from that run's log; the CSV pair/order columns are derived
from the driver's alternating order.

A separate [first-encode byte comparison](https://github.com/mingley/partitionline/actions/runs/42488853551)
passed on workflow commit `b9c31898792eeb81bb74dcc10ea78e87e3e95e56`.
It validated the original baseline and candidate source SHA-256 values
above, injected the same test-only fixture into each, and used separate
all-feature release targets. For a mixed implicit/explicit three-record
batch, both arms decoded and checked the timestamps and payloads. Python
then compared each complete serialized body byte for byte: all 14 cases
matched. The matrix covers Produce v3/v9/v13 with no compression, gzip,
snappy and lz4, plus zstd on v9/v13. This is a first-encode control; the
retry change is checked separately by the public mock contract.

The fixture SHA-256 was
`d9d499102dcb120c69d54e2bd517cbdb87823a7b32004d685415ff1502971a35`.
Instrumented source SHA-256 values were
`e06c67e1207f5720cab455b29927d7440b0d737672a5c1df17195d3b5aa4d3d1`
baseline and
`7355ca8e351b17343a76b4cffa0471bfeb752932896b127b2c89f0acaf797f5d`
candidate; test binaries were
`79c378d3790930de55062231c876caed8bda085c4a79ab851ffdd49917573164`
and
`6c86c551c8523eb916aa5d5cc5890273709061b8c5c75c6a33081cd6e6710f82`.
The [body index](first-encode-body-hashes.csv) records each body's size
and SHA-256. The actual paired byte files, build outputs and comparison
are in [artifact 13620747415](https://github.com/mingley/partitionline/actions/runs/42488853551/artifacts/13620747415)
(SHA-256 `75077f433521d1f1e72e7748763d95b8ae40aa1858fe014f2db0565aab3e651f`).
The first matched run [42485996176](https://github.com/mingley/partitionline/actions/runs/42485996176)
stopped after a baseline library failure because the shell retained `-e`;
it did not run the candidate. Its artifact `13619816493` has SHA-256
`4cd9fb792234b743d0c01334cd3efdfd7e78aeed0ed146558afb9254114f3b88`.

The first real-Kafka setup [42486073240](https://github.com/mingley/partitionline/actions/runs/42486073240)
reached Kafka 4.1.0 and built the producers but the Java wire observer failed
to compile because `RecordBatch` has no `baseTimestamp()` method. It
collected no producer comparison. Artifact `13620145744` has SHA-256
`ef5143b5fddd63d47740b26ad1d4396f949c81f632b2d4b88331fa82caae835a`.

The first timing setup [42486098207](https://github.com/mingley/partitionline/actions/runs/42486098207)
failed before collecting any sample: the baseline test fixture could not
infer the `Pending` vector type at `pending.rec.timestamp = None`.
Artifact `13619318797` has SHA-256
`84dc45b7d22ce6ca6b47f91df86cc1cc04346ff2b1183a38e6d2947b3629c9f6`.
The diagnostic was extracted in
[42486393813](https://github.com/mingley/partitionline/actions/runs/42486393813).
