# Admin topic aggregation slot index

The applied change stores randomized hashes and output-vector slots in
the DescribeTopicPartitions merge index. It checks the full stored hash and
topic name before using a slot. It keeps each known topic ID with its slot and
uses the cached hash when the table grows. The change avoids a second owned
name per distinct indexed topic, and adds a direct dependency on
`hashbrown 0.17.1` with default features disabled. The index uses
`std::collections::hash_map::RandomState`.

The production source is Git blob
`8c2f94353e04c2925b1bfaf8478285190dd317d7` and the final test blob is
`a69d5cceff56a7d1c52f321b5766d695423871aa`, in
[commit 31c3ec4](https://github.com/mingley/partitionline/commit/31c3ec4e4ea852ffc7d6b6ec388fe7b4cb56ed0a).
Its base was main `97a0757a3e5ab8777b147876305ab854b1faf675`, whose
Admin source and test blobs are `3125c234259c87d1bc4a89d0cd0734d124529bea`
and `f18e87db6a6eab6d33a05288fe1d39817b9c2388`. Main
`f5edca564b5481eb9d66c105ea4b13b3877e092c` retained those Admin and Cargo
blobs. The candidate manifest and lock blobs are
`4a90b08ab3fb886676231310b1f7132e04e2cfbb` and
`96e364d34121fe90183f87f4f49fd92b883bc447`.

## Correctness and release checks

[Final tests](https://github.com/mingley/partitionline/actions/runs/42488776128)
passed six slot controls and all 72 pagination tests in each default and
all-feature profile. The controls cover forced equal hashes, distinct full
hashes with the same table tag, output order and topic ID conflicts, and a
duplicate lookup when the table is full. A 128-name collision test observed
capacity growth in both the output vector and the table after each already
held entries, followed by reverse lookups. It does not establish that the
vector moved to a different address. The public cross-page test sends 8,191
matching rows on its first page and a matching row on the next page, then
checks success, conflicting IDs, unrequested names and later null-name
precedence. It checks same-socket reuse; it does not independently observe
the row-checkpoint suspension.

[Combined gate 42487883745](https://github.com/mingley/partitionline/actions/runs/42487883745)
used the same production source with earlier test blob
`c47052f9405a1261ac79fb1965ec86d46edc2265`. It passed five slot controls,
71 pagination tests and two existing index controls in each profile,
formatting, strict all-target/all-feature Clippy, strict Rustdoc and
doctests. A mutant that ignored the name equality check failed the forced
collision test in both profiles. A preliminary build
[42486800818](https://github.com/mingley/partitionline/actions/runs/42486800818)
failed with four E0308 test-adaptation errors in each profile; no tests ran
there.

[Final fixture gate 42488535699](https://github.com/mingley/partitionline/actions/runs/42488535699)
passed the four-case public cross-page history in each profile, formatting,
strict Clippy, cargo-deny, RustSec audit, package verification, the
extracted-package consumer checks and a publish dry run. No crate was
published. Cargo-deny reported duplicate `getrandom`/`syn` versions and an
unused license allowance as warnings. The RustSec audit examined 82 lockfile
dependencies against advisory database commit `309392b18f6b17022f82c701f300433126901068`
and found no vulnerabilities or informational warnings; the audit action
also emitted a Node.js deprecation warning. The earlier fixture gate
[42488266939](https://github.com/mingley/partitionline/actions/runs/42488266939)
failed strict Clippy on two test-only `unreachable!` calls; its public
histories, audit, deny and package jobs passed. The corrected fixture uses
explicit test panics and adds the observed-capacity assertions.

The [Kafka 4.1.2 smoke](https://github.com/mingley/partitionline/actions/runs/42489331652)
passed one public test per feature profile using image
`apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`
and fixture blob `5aefcf29a857f49317d94cbdef56bf60ae787f7e`. Four raw
pages covered seven partitions; the aggregate matched raw response fields in
both authorization modes. This is a single-broker happy-path smoke.

[Cancellation controls](https://github.com/mingley/partitionline/actions/runs/42488911458)
passed two public cancellation/timeout tests in each profile using
test-instrumented source `38c1ffcebf039777539e10bc37b0fc6728d7b97c` and
test blob `c7790f0479c6275796020f57598cf1bcc673fa17`. Before/after
markers identify the existing merge checkpoint; suppressing its yield made
the cancellation test fail with the checkpoint-specific assertion in both
profiles. The controlled cancellation and timeout histories retained use of
the socket. Instrumentation changes the test build, and does not bound the
work required to grow or destroy an index.

An independent source review found no concrete collision, name-validation,
order or error-precedence regression. A duplicate lookup does not call the
table insertion method, although merging partitions can allocate. As in the
baseline, a new entry is placed in the private index before pushing its
output row, with no await between the operations. An unwind there unwinds the
enclosing aggregate; ordinary infallible allocation failure normally
aborts. This is source analysis, not a reproduced public allocation failure.

## Matched large-response mock

[Run 42488154421](https://github.com/mingley/partitionline/actions/runs/42488154421)
passed 384 public calls in 192 alternating pairs on one shared AMD EPYC 7763
x86 host using stable Rust 1.99.1 (`58cf8f9c3`) and LLVM 23.1.1. The
runner could not read `cpu.max`. Builds used separate target directories;
the baseline and candidate release binary SHA-256 values were
`8c42a6bc0cec63df2b7d89dbcb254082dd1eed15de56dcfca2375a73ff2579b1`
and `17103bf438c7c3783471ead48f7548ad39b0d49167bc201682536a540f0dd309`.
The binaries were not retained. Both arms used the identical public mock
fixture Git blob `ea89fb501223e09a0ce8ea4bda7e4cb9b98954f6` (SHA-256
`1ac9875720f294baca2daca629502d11f1211dccb1d1b30b2342521d4d11415d`).
The benchmark used the earlier candidate test blob `c47052f9` plus that
fixture, not the final test blob; its production source is the candidate
blob above.

The peer checked every requested name and returned a single mock page. The
client checked every returned name, all topic-ID bytes, internal flag,
error, empty partitions and omitted authorization. The peer and client
shared a process and allocator. Calls were separately executed processes with
independent randomized hash states; only the 32-row success shape was warmed
twice per arm before sampling. Sizes were tested in fixed order, with
alternating baseline/candidate order within each shape.

| Mock shape | Pairs | Baseline median | Candidate median |
| --- | ---: | ---: | ---: |
| 1 distinct success row | 21 | 139.28 µs | 139.42 µs |
| 32 distinct success rows | 21 | 169.32 µs | 163.87 µs |
| 256 distinct success rows | 21 | 355.08 µs | 321.02 µs |
| 8,192 distinct success rows | 5 | 6.490 ms | 5.159 ms |
| 100,000 distinct success rows | 5 | 77.887 ms | 63.406 ms |
| 500,000 distinct success rows | 3 | 578.943 ms | 380.785 ms |
| 8,192 success rows, 32 distinct | 5 | 2.893 ms | 2.873 ms |
| 8,192 error rows, 32 distinct | 5 | 2.831 ms | 2.854 ms |

For 100,000 distinct success rows, the five paired candidate/baseline
ratios ranged from 0.788 to 0.821. For 32 distinct success rows the 21
ratios ranged from 0.769 to 1.166. At 500,000 distinct success rows the
median longest post-write Pending poll was 74.57 ms baseline and 23.92 ms
candidate; the candidate still has a long observed poll and the result is
not a deadline bound. Median peak process RSS at that size was 303,640 KiB
and 295,720 KiB respectively. RSS includes the peer, inputs, outputs and
allocator; it does not count index allocations or isolate index memory.

[All 384 samples](paired-samples.csv) were recovered by
[readout 42489599414](https://github.com/mingley/partitionline/actions/runs/42489599414)
from artifact `13620353243`, digest
`sha256:7ce6c0e4954891bb54ab1d2fa680088d8d1f09190fb61734180d4b74f8a563e0`.
The readout verified the artifact identity, binary hashes and 192 pairs.
The first run [42488059220](https://github.com/mingley/partitionline/actions/runs/42488059220)
collected 384 calls but failed its summary because the workflow mistakenly
expected 404; it is not a passing run. It retained a separate artifact and
is not mixed into this CSV.

This loopback experiment does not establish Kafka throughput, a global
ranking, allocation counts or behavior on every workload. Hash-table growth
and destruction still execute synchronously, and independent random seeds,
scheduler effects, small sample counts and mock-peer work affect timings.

## Additional checks

[Full Admin run 42489700720](https://github.com/mingley/partitionline/actions/runs/42489700720)
passed 679 Admin-filtered unit tests and the Admin response integration test
in each feature profile, with no failures or ignored tests. It used the final
production and test blobs above. Because both candidate suites passed, the
conditional baseline reruns did not run.

[Lexical-name comparison 42490023473](https://github.com/mingley/partitionline/actions/runs/42490023473)
passed on a shared AMD EPYC 9V45 x86 host using stable Rust 1.99.1 and LLVM
23.1.1. The runner pinned each process to CPU 0; `cpu.max` was unavailable.
It compared baseline Admin blob `3125c234259c87d1bc4a89d0cd0734d124529bea`
against the candidate production blob `8c2f94353e04c2925b1bfaf8478285190dd317d7`.
Separate target directories produced baseline and candidate binary SHA-256
values `b667177f5f2cb72fbb623e7a018b6d49f5d1988b4b8a1ad0a33e2d927d4b4884`
and `8f22c5f5c1e7230dbde53d1d920f2b65d34b9aac979931878370eb879d3c2aea`.
Those binaries were not retained.

Both arms used the unchanged 14-shape benchmark fixture Git blob
`dae07e157e9b6687cca044b366d6aa4f5b673ab0` (SHA-256
`4ca812ef0d29cd3df23de0126ffb844384ef063d3ec205907f99dceb12be9883`),
which replaced the pagination test module in the benchmark builds. This is
the same fixture as the earlier requested-name qualification. It checks the
peer's requested names and order, and each result's count plus error code or
partition count. It does not independently check returned names, topic IDs
or per-topic partition mappings during the timed calls. Separate untimed
controls with appended test blob `632486c76c79ee234eac53cc3a70b913cddeb8f1`
passed for both source arms and checked those fields and order for 32 and 256
ordered and lexical success and error rows.

The comparison warmed each shape once per arm and measured 12 alternating
pairs per shape. Each 32-name sample performs 1,000 public calls; each
256-name sample performs 200. The single-name samples perform 4,000
one-page calls or 250 sixteen-page calls. The following ratios are paired
candidate/baseline elapsed time; a value above one is slower.

| Mock shape | Paired median | Pair range | Candidate slower |
| --- | ---: | ---: | ---: |
| 1 name, one page | 0.9986 | 0.9589–1.0648 | 4/12 |
| 1 name, 16 pages | 0.9985 | 0.9822–1.0238 | 5/12 |
| 32 ordered error rows | 0.9300 | 0.9230–1.2470 | 1/12 |
| 256 ordered error rows | 0.8234 | 0.8165–0.8314 | 0/12 |
| 32 reversed error rows | 0.9342 | 0.9154–0.9445 | 0/12 |
| 256 reversed error rows | 0.8498 | 0.7926–0.8759 | 0/12 |
| 32 lexical error rows | 0.9339 | 0.8856–0.9397 | 0/12 |
| 256 lexical error rows | 0.8452 | 0.6419–0.8538 | 0/12 |
| 32 ordered success rows | 0.9427 | 0.9354–0.9484 | 0/12 |
| 256 ordered success rows | 0.8575 | 0.7012–0.8691 | 0/12 |
| 32 lexical success rows | 0.9443 | 0.9312–0.9496 | 0/12 |
| 256 lexical success rows | 0.8762 | 0.8617–0.8916 | 0/12 |

[All 336 observations](lexical-samples.csv), including the other two shapes,
retain the measured execution order. The run's artifact is `13621307842`
with digest
`sha256:540eaee2a6c61631c1a0709e04c23749500eea2b25a00490bb404a91538f222b`.
The fixture uses numeric request names with rows returned in request,
reverse or lexicographic order. The result is a comparison against
the validation-enabled baseline on one shared host. It does not directly
compare with the older pre-validation source, establish real Kafka latency
or establish a global ranking.

The isolated [combined tree ab8f91f](https://github.com/mingley/partitionline/commit/ab8f91f7c56a019fceb39c30b09bc1a9166a6e74)
starts at main `342e7b90ee63d6824be2652b65d71be483ea6d16`, changes only the
four candidate Admin/Cargo blobs, and retains producer source blob
`dcd012b3f86a8b9d4254f88d7f48946ce8f9e922`. The
[combined Admin and producer gate](https://github.com/mingley/partitionline/actions/runs/42491179180)
failed in both profiles on wall-time assertions in the Admin unit suite.
The same-runner baseline ran later using the identical public cross-page
fixture, with separate target directories. The baseline does not contain the
candidate's five slot-specific tests.

| Profile and arm | Admin passed / failed | Timing assertion failures |
| --- | ---: | --- |
| Default candidate | 677 / 2 | Named results 80.191042 ms; coordinators 83.626273 ms |
| Default baseline | 674 / 0 | None |
| All-feature candidate | 677 / 2 | Named results 86.851513 ms; coordinators 84.580543 ms |
| All-feature baseline | 673 / 1 | Named results 82.724592 ms |

The named-results failure was
`grouping_many_named_results_does_not_hold_the_caller`; the coordinators
failure was `indexing_many_coordinators_does_not_hold_the_caller`.
The Admin response integration and producer timestamp public targets passed
1/1 in each arm and profile. Formatting, strict all-target/all-feature
Clippy, strict Rustdoc and doctests passed. The observed timing failures
remain unresolved; these runs do not establish their cause. The earlier
independent full Admin run passed, but does not turn this combined gate
into a pass.
