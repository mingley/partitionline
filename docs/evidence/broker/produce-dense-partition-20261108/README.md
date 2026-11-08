# Dense Produce partition lookup candidate

The candidate is **not applied to main**. It replaces the map-based duplicate check for unordered, dense lists of more than 16 Produce partitions with a bounded array of first-owner positions. Negative, sparse, oversized, or allocation-declined inputs use the original map path. It preserves first-occurrence output and duplicate/error precedence in the focused controls. The additional scan can change timing near deadlines; it does not add cancellation checkpoints or a hard bound on poll time or allocation latency.

## Source and checks

- Baseline: `a3748caecae60a893de96c0700e1e5ab04854443`, Produce Git blob `28ac0a2eb00c4994cc3ebc8d3fe4fa035e6bdc4a`.
- Isolated candidate: [36dfa7caf07142d6468e66a383ce26d5b98059e8](https://github.com/mingley/partitionline/commit/36dfa7caf07142d6468e66a383ce26d5b98059e8), Produce blob `6897be1463187c7cd123066729f8dd98ea3e100f`, unit-control blob `5522262296bd98855d4dcb09f967606346557c34`, public fixture blob `82a4045662ec3ced99383cc002625fa63fcfa005`. The common broker manifest blob is `4e23c42f8d9ac722005098a4b58e1046b711e1a3` and lockfile blob is `19652acd96b929402b1e4a9394a33d12a7e69449`.
- [Final focused gate 42495509367](https://github.com/mingley/partitionline/actions/runs/42495509367), workflow commit `b50f1fbb9ae840b2f5df62bb906936f2f554d28a`: stable Rust 1.99.1; rustfmt, strict all-target/all-feature Clippy and warning-denied Rustdoc passed. In each of default and all-feature profiles, four classifier controls, one public router/byte/readback control, and one Apache Produce golden test passed; the same public fixture also passed once on baseline. The full broker suite has **not** run on this candidate.
- Unit controls cover the 16/17 path selection, dense/duplicate classifications through 4,096, sparse/negative fallbacks, synthetic `u16` owner boundaries, and a simulated allocation-decline fallback. The simulation forces the fallback before `try_reserve_exact`; it is not an actual allocator failure. The existing flags and fallback map also use infallible allocations, so this does not establish recovery from out-of-memory conditions.
- The public fixture independently constructs expected response bytes for ordered, unordered, duplicate, sparse and invalid requests and checks precedence. It sends valid records through the in-process router and reads expected offsets and payload bytes back through the same broker implementation after clean shutdown. This is not a networked Kafka peer, an independent record decoder, or crash/power-loss durability evidence.
- The cancellation control cancels at the third active check after the first valid partition's stage attempt, then checks an empty fetch and a successful retry. It does **not** directly observe a successful `Stage::Sync` before cancellation, and its later `store.slot` call can create the slot. It establishes that tested cancel/retry outcome, not independently that a staged write was rolled back. An independent source review found no concrete duplicate, bounds, ordering, precedence or fallback defect but identified this limitation.

Earlier behavior runs [42494455921](https://github.com/mingley/partitionline/actions/runs/42494455921) and [42494704777](https://github.com/mingley/partitionline/actions/runs/42494704777) passed their focused default/all-feature behavior controls and strict Clippy/Rustdoc, but each failed the overall gate because new test files failed rustfmt. The exact later formatter run [42495052514](https://github.com/mingley/partitionline/actions/runs/42495052514) succeeded; the final gate above checks the resulting permanent files.

## Matched in-process measurements

[Run 42495696463](https://github.com/mingley/partitionline/actions/runs/42495696463), measurement commit `239dc3388f310e4fbf19465b0fd68b1afdad795f`, built the baseline and exact candidate Produce blobs in separate release target directories and copied identical timing fixtures to both. Their SHA-256 values are `fd4ed4e6c5547ecdd00eae9bec543686bf142a6333159f132de38d96abd65a42` for the router fixture (Git blob `72a0ba73820f1734331f89c49cc77be139aa9de7`) and `6648e904feb3f07bc193a3ba751f240dc6ab410214142affef17aae5a6c74d14` for the append fixture (Git blob `c45d4b2e8d2db34093f0c4b59c913f7941e57764`). These timing fixtures and their driver are qualification-only; they are not the permanent public-test blob above.

The x86_64 hosted runner reported an AMD EPYC 7763 under Microsoft virtualization, four visible CPUs, and Rust `1.99.1 (58cf8f9c3 2026-10-13)`, LLVM 23.1.1 and Cargo 1.99.1. Each subprocess was pinned to CPU 0. The `cpu.max` file was unavailable; no CPU quota is inferred. The workflow recorded an ext4 filesystem for `RUNNER_TEMP`, but the fixture uses `std::env::temp_dir()`; it did not establish that these paths share a mount. Do not attribute the append timings to the reported mount.

The four release executable SHA-256 values were:

| Arm | Fixture | SHA-256 |
| --- | --- | --- |
| baseline | router | `95853c7ba9531a5b1c9882454725de36a287e9257c2539641526b27b7d06b343` |
| candidate | router | `14d244f96103af0b3ddc7038e4aec13af46c923422cdfc94982d14cd1e0ba07c` |
| baseline | append | `70dd08c5a4578ffb3e17c36349ce110b2abdaa4a6b07d44c5a6aabf24bd8a88c` |
| candidate | append | `b4f3e896c7e62a0058e3cba9e896cd9828ae5c867366f2bdbda29bc20d242e0a` |

The binary digests were logged, but the binaries were not retained. The run alternated baseline/candidate order over 12 pairs per shape after an arm-specific warmup, on one shared host. The [raw samples](matched-samples.csv) (Git blob `1f9eae7efa79caccf40290fb5159100f9f3863b3`) contain all 504 timed arm samples. Each elapsed value is the total for the stated operation count, not a per-request latency distribution. The following microsecond values are separate arm medians divided by operation count; the percentage and range are computed from candidate/baseline ratios within each pair, so they need not equal a ratio of the two displayed medians.

| Path | Partitions and shape | Operations per sample | Baseline median µs/op | Candidate median µs/op | Median paired change | Paired range |
| --- | --- | --- | --- | --- | --- | --- |
| router | 1 ascending | 2000 | 6.612 | 6.714 | 0.6% | -2.3% to 18.4% |
| router | 16 reversed | 1000 | 7.284 | 7.508 | 2.3% | -14.4% to 6.4% |
| router | 17 reversed | 1000 | 8.868 | 7.576 | -16.3% | -26.5% to 11.0% |
| router | 32 ascending | 1000 | 8.090 | 8.092 | 0.5% | -24.0% to 40.4% |
| router | 32 reversed | 1000 | 10.366 | 8.572 | -17.3% | -18.3% to -16.2% |
| router | 256 ascending | 200 | 14.856 | 15.048 | 1.3% | -0.3% to 4.4% |
| router | 256 reversed | 200 | 29.335 | 15.823 | -46.4% | -58.9% to -45.5% |
| router | 256 rotate | 200 | 29.342 | 15.551 | -47.0% | -48.7% to -46.1% |
| router | 256 duplicates | 200 | 12.728 | 9.641 | -24.2% | -24.9% to -21.9% |
| router | 256 sparse_last | 200 | 29.026 | 29.233 | 0.9% | -2.2% to 4.3% |
| router | 256 negative_last | 200 | 29.003 | 29.281 | 0.9% | -2.4% to 2.6% |
| router | 1024 reversed | 50 | 94.310 | 40.653 | -57.0% | -59.5% to -56.2% |
| router | 1024 sparse_last | 50 | 94.574 | 95.363 | 0.7% | -24.9% to 2.1% |
| router | 4096 reversed | 15 | 597.584 | 309.400 | -48.2% | -48.7% to -45.7% |
| router | 4096 sparse_last | 15 | 599.060 | 603.974 | 0.6% | -35.0% to 58.0% |
| durable | 32 ascending | 10 | 315.662 | 309.557 | 1.5% | -42.8% to 9.9% |
| durable | 32 reversed | 10 | 327.412 | 313.208 | 0.3% | -44.9% to 10.1% |
| durable | 32 rotate | 10 | 305.694 | 307.806 | -0.2% | -47.6% to 15.1% |
| durable | 256 ascending | 10 | 322.533 | 317.182 | 1.5% | -59.1% to 19.0% |
| durable | 256 reversed | 10 | 354.538 | 345.217 | -2.2% | -50.4% to 70.1% |
| durable | 256 rotate | 10 | 345.098 | 312.819 | -4.2% | -24.3% to 70.6% |

`router` sends sequential in-process requests to a missing topic with null records; every reply is independently byte-checked and no records are appended. `durable` also uses sequential in-process dispatch: only partitions 0 and 1 exist, so each 32- or 256-partition request appends two records and returns errors for the others. Every response is byte-checked; after clean shutdown the same broker decoder checks 22 records per arm/sample (two partitions, ten timed records and one warmup record each). Setup and readback are outside the measured interval. The append-pair ranges are wide and do not establish a reliable append improvement.

These are scoped shared-host mock-router results, not a real Kafka workload, concurrent throughput, independent durability verification, a fastest-broker comparison, or a production-readiness qualification. Hash seeds and hosted scheduling were not controlled. The sorted and sparse cases include small median increases; the 16-entry path is unchanged by the candidate, making host noise a plausible but unproven contributor.

Two earlier measurement runs used the same source and fixture pins but a start-of-line output parser. Both failed on their first baseline warmup because libtest prefixed the measurement with `test ...`: [42495612451](https://github.com/mingley/partitionline/actions/runs/42495612451) observed 18,702,927 ns for 2,000 operations, and [42495632174](https://github.com/mingley/partitionline/actions/runs/42495632174) observed 13,487,422 ns. Neither collected a candidate sample or a valid pair. The successful run corrected only the driver to find the unique marker within the line; the two failed runs are not pooled with it.

## All-valid partition writes

[Run 42496577865](https://github.com/mingley/partitionline/actions/runs/42496577865) compared the same baseline and candidate Produce blobs using a separate, all-success workload. It found **no useful supported improvement** for the tested writes; the candidate remains unapplied.

Each request contains 17 or 32 real catalog partitions, all with valid one-record Kafka batches. Keys vary by partition and values by iteration; their batch CRC32C values are computed before timing. A full warmup request creates and writes every partition before the timed loop. Each timed sample then sends 10 sequential requests through the in-process router and checks every response byte, including requested partition order, success code and offset. The timed interval includes request cloning, dispatch, the normal monolithic staged synchronization path and response comparison. There are no concurrent producers, network transport, replication or Kafka peer. The configuration sets monolithic storage and permits four concurrent sync workers, which share the pinned CPU; sync syscall counts were not independently traced.

After clean shutdown, a separate test parser reads every raw journal file directly. It checks file and entry magic, exact file length, offset and record count, header and payload CRC32C, and every complete partition/iteration-specific batch against an independently constructed expected payload. The software CRC32C implementation checks the standard `123456789` vector and the original batch's checksum. Each 17-partition fixture execution verifies 187 records, and each 32-partition execution verifies 352 records, including that execution's warmup. Across all 156 successful fixture executions the raw checker verified 42,042 records. This is independent of the broker's journal decoder, but it is a clean-shutdown filesystem read, not a power-loss test.

For each of six shapes the driver ran one separate warmup execution per arm and 12 alternating matched pairs. The [raw samples](all-valid-samples.csv) (Git blob `70096da9436bd4d5eca52e74da6a89f02017d953`) retain all 156 arm executions; `block=-1` marks the 12 warmup executions and is excluded from the paired table. Every other block has one baseline and one candidate. Times are for 10 requests per arm. The two arm medians are calculated separately; the percentage and range are candidate/baseline changes within each pair and therefore need not equal a ratio of the displayed medians.

| Partitions and order | Baseline median ms/10 requests | Candidate median ms/10 requests | Median paired change | Paired range | Candidate faster pairs |
| --- | --- | --- | --- | --- | --- |
| 17 ascending | 9.847 | 10.194 | 3.14% | -20.19% to 36.76% | 4/12 |
| 17 reversed | 9.996 | 9.906 | -0.29% | -5.89% to 7.47% | 7/12 |
| 17 rotate | 9.897 | 9.891 | -0.70% | -4.44% to 2.33% | 7/12 |
| 32 ascending | 16.312 | 16.232 | -1.70% | -5.79% to 2.47% | 8/12 |
| 32 reversed | 16.350 | 16.519 | -0.82% | -2.11% to 2.79% | 7/12 |
| 32 rotate | 16.870 | 16.724 | 0.00% | -4.62% to 21.68% | 6/12 |

Setup includes catalog/router creation and one fully checked warmup; shutdown, independent raw verification, and file cleanup are outside the timed interval. Median overheads below are baseline / candidate milliseconds per fixture execution. Input and expected-response construction is outside all listed phases.

| Partitions and order | Setup + warmup | Shutdown | Raw verification | Cleanup |
| --- | --- | --- | --- | --- |
| 17 ascending | 7.487 / 7.543 | 0.036 / 0.040 | 0.248 / 0.249 | 2.197 / 2.300 |
| 17 reversed | 7.449 / 7.725 | 0.032 / 0.033 | 0.240 / 0.243 | 2.291 / 2.344 |
| 17 rotate | 7.332 / 7.556 | 0.032 / 0.033 | 0.243 / 0.250 | 2.281 / 2.220 |
| 32 ascending | 11.347 / 11.327 | 0.053 / 0.055 | 0.452 / 0.480 | 4.047 / 4.045 |
| 32 reversed | 11.497 / 11.494 | 0.058 / 0.056 | 0.476 / 0.458 | 3.989 / 3.888 |
| 32 rotate | 11.750 / 11.580 | 0.071 / 0.074 | 0.479 / 0.482 | 4.017 / 3.969 |

The same x86_64 hosted runner compared both arms on CPU 0, with separate release target directories, Rust `1.99.1 (58cf8f9c3 2026-10-13)`, LLVM 23.1.1, Cargo 1.99.1, and an AMD EPYC 9V45 under Microsoft virtualization. Four CPUs were visible; `cpu.max` was unavailable, so no quota is inferred. `TMPDIR` was set to `/home/runner/work/_temp/dense-success-temp`; each fixture asserted that Rust's actual temporary directory resolved to that location. `df -T` reported `/dev/root` as ext4, while `stat -f` reported the ext2/ext3 filesystem family. Scheduling, filesystem cache and other host activity were uncontrolled. These results come from a different host/run than the earlier rejected-request measurements and are not pooled with them.

The measurement branch is pinned at `2278ad87da7b60f132f4400667174a3f7f85ae89`: all-success fixture Git blob `964529e5d7ef1481febd8220fb79e307f216830e` (SHA-256 `fdbe3263d4c1eeddacc6c593057ae7fce05bc7e9933f54bb50d7b27e8a10a7fa`), copied helper blob `72a0ba73820f1734331f89c49cc77be139aa9de7` (SHA-256 `fd4ed4e6c5547ecdd00eae9bec543686bf142a6333159f132de38d96abd65a42`), driver blob `fb7e43b65de7a73d1097505c8e614cea617ce289`, and workflow blob `b980d8d4f3a08cdf450a9d7bf3842e7a743a8d50`. Both arms used the same original batch fixture blob `9e8276e77b37abb5979d98c4aaaac25d7279d1dd` (SHA-256 `431810cfa21b3c669ede109ac7d40c53a63acf07e27f4087a1e834dd5cc00c28`) and identical broker manifest and lockfile blobs listed above. The baseline and candidate source SHA-256 values were `ace0982025aac1d2f66846865fe4e9083b578a2dfccb6fba16af1f1d5183ff11` and `c3173ad3f56304836b2fc2a239e090b1e5d537ecbc9380f95230fdc5a26f3cc4`, respectively. Release executable SHA-256 values were `e4fe50aae678d5c248212f9fbfb48700999911b0a76f32da2e7976e1e32d3b3c` for baseline and `d42e8370d6c61fda7bebf045185c33a2ebbd98eb0e9c01673908b9ab0c386b31` for candidate; the binaries were not retained. The timing fixtures are qualification-only and do not modify the candidate's permanent tests.

Every paired range crosses zero, including the unchanged ascending path. A batch total over 10 sequential requests cannot establish per-request tail latency, concurrent throughput, production readiness, or a comparison against other Kafka servers. The full broker suite remains unrun on the candidate, and the cancellation-stage reachability limitation described above remains open.
