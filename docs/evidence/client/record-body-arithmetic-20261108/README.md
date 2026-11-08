# Record body length experiment

The candidate is **not applied**. The corrected x86 comparison found
slower single-record batch encoding: median candidate/baseline elapsed time
was 1.0609 for an empty record and 1.0398 for a keyed 100-byte record.
All nine pairs were slower in both cases. This single shared-host
microbenchmark does not establish an end-to-end performance result.

The candidate grouped nonnegative record body length additions after
calculating the key, value and header widths. Kafka varint widths total at
most 31 bytes including the attributes byte. Key and value lengths,
header processing, the final `i32` bound, and buffer rollback retained
their existing order. The change is isolated at
[`b4c0899`](https://github.com/mingley/partitionline/tree/b4c08995515af680f2f7099aaba1d455c9fa2f77)
and was compared against
[`f092d69`](https://github.com/mingley/partitionline/commit/f092d69d43ebb300c756cca006f4964c9820e491).
It is not proposed for integration.

## Corrected comparison

[Run 42461723892](https://github.com/mingley/partitionline/actions/runs/42461723892)
used separate Cargo target directories, verified the source hashes against
both commits, checked identical fixture and manifests, and confirmed both
production crates compiled. Its recorded SHA-256 values were:

| Input | SHA-256 |
| --- | --- |
| Baseline `src/protocol/records.rs` | `a0d5b6ff6e1bb76b48057278654f4f43535fd6bf27075badda88b993c4f535f4` |
| Candidate `src/protocol/records.rs` | `e7ea6a1d246146ad1b7d8c099c353a080eb6bc92532452c69aaa02e257418230` |
| Baseline release binary | `59bcc709bdc4a0079dbee9d671ad8c834573b61b597c30aeedb13cd4dfd67ed0` |
| Candidate release binary | `b71a003d1b1dc31fbcd3d0915fe0a9384e0f8823e3d815b95cff23fe95299a15` |

The run compared the complete encoded bytes of 1,012 batches. The corpus
covered record counts 1, 7 and 64; payload sizes 0, 1, 63, 64, 127, 128 and
1,024 bytes; zero, one, three and nine headers; null, empty and 16-byte keys;
and uncompressed, gzip, Snappy and LZ4 encoding. Four additional cases mixed
null and variable-length values. Uncompressed cases also compared the
synchronous and cooperative encoders. Both corpus files had SHA-256
`4a62b72711cbdb071ecaab2ee2d7d622450e5fedaa6406d41431e5d5eb3701e0`.

Twelve error cases covered count mismatch, offset overflow and timestamp
overflow in all four codec modes, comparing error strings and buffer rollback;
the uncompressed cases also compared cooperative failures. Each generated
batch was decoded and compared with the input using **this repository's**
record decoder; it was not an external Kafka decoder. A synthetic test
compared the old and new size calculations over 16,384 width and numeric
boundary combinations. The formatted candidate passed that test and 79
focused default-feature record tests. All-feature tests and strict Clippy
were not run because the measured regressions rejected the candidate.

The shared Ubuntu 24.04 x86 runner reported four logical CPUs on an AMD EPYC
7763. Measurements pinned the fixture to CPU 0 and used stable rustc 1.99.1,
LLVM 23.1.1, the locked default-feature release profile, thin LTO and one
codegen unit. Each shape used nine alternating baseline/candidate pairs,
with randomized shape order and identical baseline-calibrated iteration
counts. Each arm lasted 123.7–242.3 ms. Output buffers and compression
scratch were reused. The synchronous shapes encode complete batches;
`async_*` shapes exercise the cooperative uncompressed writer. These are
local codec measurements, not a client or broker ranking.

Values below are candidate/baseline time ratios; higher is slower. The
pair ratios are in trial order. The baseline and candidate columns are
individual medians, so their quotient need not equal the median paired
ratio. Full integer elapsed times, iteration counts, trial order, source
and build logs, complete corpora and host information are in
[artifact 13610530521](https://github.com/mingley/partitionline/actions/runs/42461723892/artifacts/13610530521)
(SHA-256 `aa80195916e7a847e5fdf10e45e02dbe5f3f7253585a6476feeafc82e33f46bf`,
expiry 2026-12-08). Binary hashes were recorded but the binaries were not
retained for rehashing.

| Shape | Baseline ns/batch | Candidate ns/batch | Median ratio | Pair ratios, trials 1–9 |
| --- | ---: | ---: | ---: | --- |
| `one_empty` | 80.7 | 85.7 | 1.06091 | 1.0611, 1.0629, 1.0590, 1.0609, 1.0905, 1.0578, 1.0604, 1.0609, 1.0774 |
| `one_100` | 118.4 | 123.3 | 1.03978 | 1.0343, 1.0434, 1.0434, 1.0592, 1.0389, 1.0296, 1.0449, 1.0346, 1.0398 |
| `f01_8_plain` | 619.2 | 621.7 | 1.00669 | 1.0009, 1.0047, 0.9984, 1.0067, 1.0179, 0.9987, 1.0617, 1.0091, 1.0128 |
| `f02_8_headers` | 1094.6 | 1101.3 | 1.00586 | 1.0044, 1.0104, 1.0074, 1.0042, 1.0142, 1.0059, 1.0163, 0.9953, 0.9919 |
| `f03_500_plain` | 35383.1 | 35573.8 | 1.00634 | 1.0101, 0.9997, 1.0169, 1.0129, 1.0085, 1.0040, 1.0055, 1.0063, 0.9947 |
| `f04_32_headers` | 9628.0 | 9623.1 | 0.99804 | 0.9998, 0.9946, 1.0003, 1.0077, 0.9962, 0.9980, 0.9959, 1.0059, 0.9890 |
| `many_null_key` | 32642.8 | 32725.0 | 0.99997 | 0.9889, 1.0000, 0.9949, 1.0105, 0.9997, 1.0167, 1.0023, 0.9980, 1.0129 |
| `many_empty_key` | 34575.4 | 34875.7 | 1.00891 | 1.0079, 1.0102, 1.0086, 1.0089, 1.0085, 1.0222, 1.0095, 1.0016, 1.0215 |
| `header_heavy` | 70760.7 | 71342.2 | 1.00755 | 1.0136, 0.9820, 1.0010, 0.9936, 1.0218, 1.0111, 1.0085, 1.0076, 1.0013 |
| `boundaries` | 13024.3 | 13105.1 | 1.00823 | 1.0047, 1.0082, 1.0096, 1.0098, 1.0159, 1.0011, 1.0011, 0.9877, 1.0092 |
| `large_values` | 225718.2 | 225666.9 | 1.00232 | 1.0023, 1.0163, 0.9849, 1.0137, 0.9874, 1.0119, 0.9823, 1.0080, 0.9843 |
| `gzip_128` | 249676.4 | 249004.3 | 0.99694 | 0.9552, 1.0000, 1.1175, 0.9991, 1.0128, 0.9969, 0.9952, 0.9901, 0.9942 |
| `snappy_128` | 13608.5 | 13876.8 | 1.02030 | 1.0109, 1.0333, 1.0252, 1.0006, 1.0225, 0.9960, 1.0203, 0.9761, 1.0223 |
| `lz4_128` | 13843.5 | 13813.3 | 0.99993 | 0.9963, 1.0004, 1.0023, 1.0068, 1.0015, 0.9999, 0.9949, 0.9999, 0.9984 |
| `async_f01_8` | 642.2 | 646.0 | 1.00637 | 1.0070, 1.0058, 0.9886, 1.0029, 1.0040, 1.0064, 1.0078, 1.0458, 1.0073 |
| `async_f03_500` | 36007.4 | 36252.6 | 1.00968 | 0.9982, 1.0058, 1.0223, 1.0743, 1.0070, 0.9968, 1.0239, 1.0098, 1.0097 |
| `async_headers` | 71551.0 | 70872.6 | 0.99179 | 0.9918, 0.9879, 0.9968, 0.9984, 1.0155, 0.9995, 0.9874, 0.9801, 0.9903 |
| `async_large` | 225835.1 | 225628.7 | 0.99741 | 0.9869, 1.0111, 0.9862, 1.0276, 0.9810, 1.0122, 0.9974, 1.0515, 0.9936 |

Twelve shapes had slower median paired ratios; the table retains all of them.
The one-empty and one-100 regressions appeared in every pair. The other
changes are small relative to some pair ranges and should not be treated as
settled speedups or regressions without further controlled measurement.

## Optimized code and repeat

[Run 42462618286](https://github.com/mingley/partitionline/actions/runs/42462618286)
rebuilt the same pinned sources and fixture in separate target directories on
a shared four-logical-CPU AMD EPYC 7763 x86 runner, again pinned to CPU 0.
The compiler was stable rustc 1.99.1 with LLVM 23.1.1. The two executable
SHA-256 hashes matched the corresponding baseline and candidate hashes in
the first corrected run, and the 1,012 batches and 12 error cases matched
again. Nine alternating pairs were run per shape; individual arms lasted
123.9–238.8 ms.

The repeated median paired ratios were 1.06629 for the empty record and
1.03915 for the keyed 100-byte record. All nine empty-record pairs were
slower; eight of nine 100-byte pairs were slower. Seventeen of the 18
shapes had slower median ratios, although many differences are small or
overlap the observed pair ranges. One `many_empty_key` pair was an outlier
at 1.4766; all nine paired values, elapsed nanoseconds and iteration counts
remain in the artifact. These codec results do not establish Kafka client
or broker performance.

| Shape | Baseline ns/batch | Candidate ns/batch | Median ratio | Pair ratio range |
| --- | ---: | ---: | ---: | ---: |
| `one_empty` | 80.7 | 86.0 | 1.06629 | 1.0474–1.0759 |
| `one_100` | 118.7 | 123.4 | 1.03915 | 0.9929–1.0477 |
| `f01_8_plain` | 611.5 | 621.1 | 1.01515 | 1.0130–1.0224 |
| `f02_8_headers` | 1095.7 | 1099.1 | 1.00182 | 0.9952–1.0220 |
| `f03_500_plain` | 35479.8 | 35688.1 | 1.00905 | 0.9972–1.0140 |
| `f04_32_headers` | 9615.7 | 9627.8 | 1.00368 | 0.9925–1.0084 |
| `many_null_key` | 32615.7 | 32588.6 | 1.00156 | 0.9907–1.0067 |
| `many_empty_key` | 34709.7 | 35244.9 | 1.01767 | 1.0087–1.4766 |
| `header_heavy` | 70501.5 | 70892.4 | 1.00508 | 0.9974–1.0163 |
| `boundaries` | 13074.0 | 13128.0 | 1.00578 | 0.9822–1.0254 |
| `large_values` | 231016.0 | 231103.8 | 1.00097 | 0.9928–1.0058 |
| `gzip_128` | 248508.0 | 248250.6 | 0.99905 | 0.9841–1.0072 |
| `snappy_128` | 13572.8 | 13682.1 | 1.00309 | 0.9828–1.0421 |
| `lz4_128` | 13780.6 | 13903.8 | 1.00857 | 1.0005–1.0196 |
| `async_f01_8` | 640.4 | 647.3 | 1.01141 | 0.9964–1.0248 |
| `async_f03_500` | 35929.0 | 36304.6 | 1.01153 | 1.0025–1.0194 |
| `async_headers` | 70923.1 | 71139.1 | 1.00588 | 0.9981–1.0254 |
| `async_large` | 230832.6 | 230683.6 | 1.00073 | 0.9914–1.0125 |

The final linked x86 disassembly shows that `encode_record` remained a
shared function and grew from 2,625 to 2,693 bytes; the selected disassembly
contained 621 versus 619 instructions. The arithmetic helper was inlined
on the successful path; there is no new helper call on that path.

For a successful body length, the baseline generates the zigzag value with
one `add` and then computes its width without an explicit minimum clamp.
The candidate emits `lea`, `sar` and `xor` for the same zigzag and adds
`cmp`, `mov` and `cmov` for the width clamp. The preceding bounds check,
register allocation and placement of cold error blocks also differ. In the
linked binaries the baseline success branch is a short jump from offset
`0x49f` to `0x4e7`; the candidate uses a longer jump from `0x4c2` to
`0x638`. Those instructions are consistent with range information being
less useful to the optimizer at the candidate's use site, but this
disassembly does not reveal LLVM's internal reasoning.

The extra prefix instructions and changed layout are plausible contributors
to the repeated regression, not an isolated causal measurement. The readout
runner did not have Valgrind installed; `perf stat` reported no accessible
events with `perf_event_paranoid=4`. No privileged counter settings were
changed. The baseline also calls the generic varint writer for several
one-byte fields; whether specializing that path helps remains unmeasured.

[Readout 42463137001](https://github.com/mingley/partitionline/actions/runs/42463137001)
verified the artifact identity and digest before displaying the relevant
instruction ranges. Complete disassembly, symbols, corpus, raw paired
measurements and host/build details are in
[artifact 13610647012](https://github.com/mingley/partitionline/actions/runs/42462618286/artifacts/13610647012)
(SHA-256 `5fda2ae97ea3df622dcf94fc30408231f882126d67ba3ac09029c0bfe39bca96`,
expiry 2026-12-08). The artifact contains binary hashes, not the binaries.

## Earlier attempts

- [Run 42460658543](https://github.com/mingley/partitionline/actions/runs/42460658543)
  failed rustfmt before tests or measurements. Its retained
  [artifact 13608599348](https://github.com/mingley/partitionline/actions/runs/42460658543/artifacts/13608599348)
  has SHA-256 `85c1d064f15a20c7b2019694374c87e37457da2cec426b7ab65d2514af83a6a8`.
- [Run 42460867058](https://github.com/mingley/partitionline/actions/runs/42460867058)
  passed the formatted default-feature record tests but failed building the
  baseline benchmark example: four `black_box` return values violated the
  denied unused-results lint. Its retained
  [artifact 13609702050](https://github.com/mingley/partitionline/actions/runs/42460867058/artifacts/13609702050)
  has SHA-256 `4ef1f11f7fe4a52f52551c0cbb2bae258b476a4275c665cec677c3ae6d2963647`.
- [Run 42461167659](https://github.com/mingley/partitionline/actions/runs/42461167659)
  completed, but its comparison is **invalid**: the baseline and candidate
  shared one Cargo target directory, the candidate build finished in 0.03 s,
  and both binaries had SHA-256
  `197b721525b3770c31bc09634b742d4cfe14d38d1dc74086d41d07e5515c3831`.
  Its candidate/baseline corpus and timing do not establish equivalence or
  performance. Its retained
  [artifact 13609537526](https://github.com/mingley/partitionline/actions/runs/42461167659/artifacts/13609537526)
  has SHA-256 `bce2b8899d12dbd6f031060fb15c9334451c7addb0370dea71e33e9b20a1e9d5`.
