# Admin topic aggregation slot-index experiment

This isolated candidate stores randomized hashes and output-vector slots in
the DescribeTopicPartitions merge index. It checks the full stored hash and
topic name before using a slot. It keeps each known topic ID with its slot and
uses the cached hash when the table grows. The change avoids a second owned
name per distinct indexed topic, and adds a direct dependency on
`hashbrown 0.17.1` with default features disabled. The index uses
`std::collections::hash_map::RandomState`. It has not been applied to main.

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

PENDING_RESULTS
