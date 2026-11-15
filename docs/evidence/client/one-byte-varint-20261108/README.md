# One-byte varint encoding experiment

The two candidates are **not applied**. The inline shortcut produced mixed results, including slower single-record batches in the second run. Outlining its multi-byte fallback improved the header-heavy cases in that run but slowed several plain and multi-byte cases. These are encoding measurements on shared x86 runners; they do not establish Kafka client or server throughput, a world ranking, or production readiness.

The first candidate adds a direct one-byte write to the existing varint and varlong width wrappers. It masks the value to seven bits, as the original helper did even for a noncanonical value with width one. Other widths use the original writer. The second candidate keeps that shortcut and adds `#[inline(never)]` to the shared multi-byte fallback. Both are isolated at [`272aa95`](https://github.com/mingley/partitionline/commit/272aa9561c6dc719a221d74d65c32e263432e68a) and [`e5f2f74`](https://github.com/mingley/partitionline/commit/e5f2f74f94e4e68e060468c85568e4ea57843094), respectively. The production baseline is [`36424a5`](https://github.com/mingley/partitionline/commit/36424a580913e3d77571beed65b91d6751d7b108). The branches also contain benchmark and workflow files.

In the three-way run, outlined/baseline median elapsed ratios were **0.9264** for 128 records with 16 headers each, **1.0133** for 500 plain records, and **1.0274** for 512 records with null keys. All 12 pairs were slower for each plain case. The constant-input primitive cases gave **1.0505** for a two-byte u64 and **1.1673** for a ten-byte u64; all 12 pairs were slower for each. The inline candidate's single 100-byte record median changed from **0.9351** in the first run to **1.0489** in the second, with all pairs favoring opposite sides. The runner CPU and benchmark binary changed between runs, so this does not isolate the cause.

## Checks and limits

Both runs passed formatting and focused default-feature tests: the new width test, 34 buffer tests, and 78 record tests. The width test compares the shortcut to the original helper for widths 0 through 12 and `usize::MAX`, boundary values including values above 127 with width one, and 16,384 deterministic generated values. The three-way run built each arm from a separate verified source tree and Cargo target directory, checked the manifests and other client source files, and required different binary hashes. It compared complete encoded bytes and exact error messages for 1,012 batches and 12 error/rollback cases. The corpus includes null and empty keys, headers, compressed and uncompressed output, cooperative encoding, and count, offset, and timestamp failures. The decoder is from this repository, not an independent Kafka implementation. These checks do not cover every input, all features, or end-to-end behavior.

Both runs used Rust 1.99.1, release builds, a shared four-vCPU GitHub Actions Ubuntu 24.04 x86 runner, and one pinned logical CPU within each run. The first runner reported AMD EPYC 9V74; the second reported AMD EPYC 9V45. Each shape used the same iteration count across its arms. The first run used nine alternating pairs; the second used 12 blocks with all six arm orders repeated twice and a shuffled shape order. Timings are elapsed nanoseconds. The first fixture capped calibration at 16,777,216 iterations, leaving one-byte primitive samples around 55 ms; the second raised the cap to 67,108,864. This also changed the benchmark binary, so the runs are not exact binary repetitions.

The 18 whole-batch shapes include synchronous encoding, gzip, snappy, LZ4, and cooperative uncompressed encoding. The six standalone varint shapes hold one value constant for the loop and black-box it only before the loop; LLVM can hoist value-dependent work. They are diagnostics for these constant inputs, not general varint throughput measurements. Whole-batch loops black-box their record input per iteration. The raw elapsed samples and execution order are preserved in [first-run.csv](first-run.csv) and [three-way.csv](three-way.csv), including outliers.

## Measurements

Each cell gives the median candidate/baseline elapsed ratio, its full minimum–maximum range, and the number of slower pairs. A ratio below one is faster. The first column has nine pairs; the other two have 12. Detailed workload definitions are in the [fixture](https://github.com/mingley/partitionline/blob/e5f2f74f94e4e68e060468c85568e4ea57843094/examples/bench_record_body.rs).

| Shape | First inline / baseline (range; slower) | Three-way inline / baseline (range; slower) | Three-way outlined / baseline (range; slower) |
| --- | --- | --- | --- |
| one_empty | 0.9640 (0.9526–1.0199; 1) | 1.0603 (0.9998–1.1847; 11) | 0.9108 (0.8755–1.0907; 1) |
| one_100 | 0.9351 (0.6687–0.9400; 0) | 1.0489 (1.0219–1.2052; 12) | 0.9752 (0.9634–1.0178; 1) |
| f01_8_plain | 0.9573 (0.6621–1.0051; 1) | 0.9933 (0.9474–1.0204; 5) | 0.9934 (0.9655–1.0308; 4) |
| f02_8_headers | 0.9952 (0.9719–1.0146; 2) | 0.9862 (0.9554–1.0062; 3) | 0.9318 (0.9002–1.0212; 1) |
| f03_500_plain | 0.9919 (0.9832–1.0099; 3) | 0.9783 (0.9575–1.0058; 1) | 1.0133 (1.0007–1.1168; 12) |
| f04_32_headers | 0.9824 (0.8916–1.0088; 1) | 0.9933 (0.9827–1.0758; 3) | 0.9801 (0.9663–1.0392; 3) |
| many_null_key | 0.9995 (0.9717–1.0185; 3) | 0.9843 (0.9697–1.0715; 1) | 1.0274 (1.0138–1.0587; 12) |
| many_empty_key | 0.9907 (0.9782–1.0041; 1) | 0.9650 (0.9080–1.1355; 1) | 1.0115 (0.9610–1.0314; 10) |
| header_heavy | 1.0122 (0.9997–1.0237; 8) | 1.0012 (0.9745–1.0126; 7) | 0.9264 (0.9124–0.9653; 0) |
| boundaries | 0.9887 (0.9596–1.0055; 1) | 0.9778 (0.7961–1.0173; 1) | 0.9514 (0.8360–0.9744; 0) |
| large_values | 0.9999 (0.9981–1.0373; 4) | 0.9988 (0.9327–1.0107; 5) | 1.0007 (0.9846–1.1242; 7) |
| gzip_128 | 1.0061 (0.9646–1.0153; 7) | 0.9917 (0.9635–1.0179; 2) | 1.0100 (0.9752–1.0383; 9) |
| snappy_128 | 0.9838 (0.9460–0.9985; 0) | 0.9438 (0.8902–0.9516; 0) | 1.0036 (0.9538–1.0374; 9) |
| lz4_128 | 0.9907 (0.9744–1.0050; 3) | 0.9821 (0.8637–1.0508; 2) | 1.0024 (0.9034–1.2561; 8) |
| async_f01_8 | 0.9730 (0.9420–0.9835; 0) | 0.9930 (0.9666–1.0459; 5) | 0.9892 (0.9463–1.0179; 4) |
| async_f03_500 | 0.9922 (0.9820–1.0109; 2) | 0.9809 (0.8970–0.9966; 0) | 1.0242 (0.8719–1.1034; 10) |
| async_headers | 0.9954 (0.9808–1.0073; 1) | 0.9947 (0.9794–1.0075; 2) | 0.9257 (0.9094–0.9352; 0) |
| async_large | 1.0001 (0.9953–1.0030; 5) | 1.0003 (0.9938–1.0464; 6) | 1.0002 (0.9821–1.0336; 6) |
| varint_u32_1 | 0.7517 (0.7497–1.4656; 1) | 0.6009 (0.5872–0.7900; 0) | 0.1808 (0.1791–0.1831; 0) |
| varint_u32_2 | 0.9641 (0.9605–0.9752; 0) | 1.0015 (0.9787–1.0114; 8) | 1.0006 (0.9911–1.0678; 8) |
| varint_u32_5 | 0.9642 (0.9541–0.9658; 0) | 0.9542 (0.9313–0.9704; 0) | 0.9538 (0.9307–0.9637; 0) |
| varint_u64_1 | 0.7515 (0.7405–0.7637; 0) | 0.5981 (0.5789–0.6519; 0) | 0.1831 (0.1784–0.2539; 0) |
| varint_u64_2 | 0.9639 (0.9623–0.9651; 0) | 0.9945 (0.9851–1.0232; 4) | 1.0505 (1.0375–1.0573; 12) |
| varint_u64_10 | 0.9690 (0.9457–0.9769; 0) | 0.9292 (0.8962–0.9534; 0) | 1.1673 (1.1173–1.2450; 12) |

## Source, binary and artifacts

SHA-256 source hashes for `src/protocol/buf.rs` were `35f02e9e3410003879a743f55edf30189e57f17be24bdc6e59701e18e8d3f101` baseline, `8053a993f4adb7df84526e83c5b90630c052deea18644c6fac74a4b7af9bc85e` inline, and `2656a73c900b17924e1241ebc89006c151c56cdb8fa2a9c18fb6359dd9e2cbad` outlined. At main `262d254`, the baseline versions of `src/protocol/buf.rs`, `src/protocol/records.rs`, `Cargo.toml`, and `Cargo.lock` had identical Git blob hashes to `36424a5`.

| Run | Baseline binary SHA-256 | Inline binary SHA-256 | Outlined binary SHA-256 |
| --- | --- | --- | --- |
| [42464945788](https://github.com/mingley/partitionline/actions/runs/42464945788) | `f82698bf6a9f701aa98e56d056f5ef3b4aadfde69715f885a48d8ff1a1abc88f` | `c26bc739ce5cf794e392f2908cdf563e08b6188281958bc763532af60b0b5cfd` | not built |
| [42465717828](https://github.com/mingley/partitionline/actions/runs/42465717828) | `13a819b948d8227ec9c7d2eeccba9e77bec80da6e1bab5e35504668879e571ad` | `de2212a993ad13aac194f83d71a3eca887d01f872df1935678703e328101392c` | `114f2f24b0c3354ee8bfb9dfdf4b2614eb3c69818a35bbb32410335af5576c26` |

The complete corpus was byte-identical across arms in both runs, with SHA-256 `4a62b72711cbdb071ecaab2ee2d7d622450e5fedaa6406d41431e5d5eb3701e0`. The [first artifact](https://github.com/mingley/partitionline/actions/runs/42464945788/artifacts/13610968973) has SHA-256 `6eebf5a63e3911273d5c33deee272833b7669d406414e910c431a71eefc79888`; the [three-way artifact](https://github.com/mingley/partitionline/actions/runs/42465717828/artifacts/13610859096) has SHA-256 `99c6284ddfeed47a411c833094bef5a9e301353391701bdab85bf34588577913`. They include inputs, logs, raw measurements, and disassembly and are retained for 30 days from upload.

The optimized three-way binaries had `encode_record` symbol sizes of 2,625 bytes baseline, 3,076 bytes inline, and 2,997 bytes outlined. The baseline helper was 406 bytes, the inline binary had no separate helper symbol, and the outlined helper was 458 bytes. These observations show different generated code, but do not identify the cause of the measured differences.

## Earlier invalid measurement

The separate [record body arithmetic experiment](../record-body-arithmetic-20261108/README.md) had two setup failures and an invalid initial measurement. [Run 42460658543](https://github.com/mingley/partitionline/actions/runs/42460658543) stopped at formatting; [42460867058](https://github.com/mingley/partitionline/actions/runs/42460867058) passed its tests but failed to compile the benchmark. [42461167659](https://github.com/mingley/partitionline/actions/runs/42461167659) reused a Cargo target directory and produced identical binary hashes, so its timings are invalid. The subsequent corrected arithmetic comparisons used separate target directories and rejected that candidate. This invalid run was a different experiment; both one-byte runs above used separate targets and distinct binary hashes.
