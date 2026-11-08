# Batch header write experiments

The two candidates are **not applied**. Building the fixed 61-byte header on the stack improved cold-buffer cases but regressed a reused-buffer, one-record case in both runs. Reserving the header capacity before the existing writes improved the cold-buffer cases, but the three-way run also showed a slower cooperative eight-record case. These are local codec measurements and do not establish end-to-end Kafka throughput or production readiness.

The baseline is [`60caa74`](https://github.com/mingley/partitionline/commit/60caa746c3654c89d01557e35e1100cdc2a84b74). The [stack candidate](https://github.com/mingley/partitionline/commit/f14eb8849db06c1f34725a94073445cc71206384) builds the header in a fixed array and appends it once. The [reserve candidate](https://github.com/mingley/partitionline/commit/0101be5465aa7dddf29b17fe9623b5933484a021) calls `reserve(61)` and retains the baseline field writes. The isolated commits also contain the test fixture and workflows.

For a reused buffer with one keyed 100-byte record, the stack/baseline median elapsed ratio was **1.0381** in the two-arm run (12 of 12 pairs slower) and **1.0680** in the three-way run (11 of 12 slower). The three-way stack/baseline ratio for LZ4 was **1.0119** (12 of 12 slower). The reserve/baseline ratio for cooperative encoding of eight plain records was **1.0118** (11 of 12 slower; range 0.9641–1.0550). With a new output buffer per batch, reserve/baseline ratios were **0.6843** for one empty record and **0.7735** for one keyed 100-byte record, with all 12 pairs faster. Those allocation-sensitive controls are not an end-to-end producer benchmark.

## Correctness and measurement

The candidate test compares the original header writer with each implementation across signed field and count boundaries, several existing prefixes, and returned patch positions. An additional golden test checks the exact 61 bytes of a v2 header and positions after a prefix. Each measured run passed those two tests and 80 focused record tests in both default and all-feature configurations.

Both runs built from verified, separate source trees and Cargo target directories, checked identical manifests and benchmark fixtures, and required distinct binaries. The default-feature benchmark corpus compared **1,016** full encoded batches and **12** error/rollback cases, including empty batches, null and empty keys, headers, compressed and uncompressed output, cooperative uncompressed encoding, and count, offset, and timestamp errors. Complete output and error strings matched; its decoder is from this repository, not an independent Kafka implementation. This does not cover every input or establish feature-wide correctness.

Each run used Rust 1.99.1 and a shared four-vCPU GitHub Actions Ubuntu 24.04 x86 runner reporting AMD EPYC 7763. Each arm in a run was pinned to the same logical CPU. The 21 workloads include warm reused output buffers, two fresh-buffer cases, varying record counts and headers, gzip, snappy, LZ4, and cooperative encoding. Zstd and a real broker were not measured. All arms for a shape use the same iteration count. The first run used 12 alternating pairs; the second used 12 blocks with all six arm orders repeated twice and shuffled shape order. Samples are elapsed nanoseconds and include outliers.

Raw samples, iteration counts, and execution order are in [paired.csv](paired.csv) and [three-way.csv](three-way.csv). The benchmark's complete [fixture](https://github.com/mingley/partitionline/blob/0101be5465aa7dddf29b17fe9623b5933484a021/examples/bench_record_body.rs) and scripts are pinned to the isolated commit. The faster large-batch observations should not be assigned to the header write alone: code layout and runner effects were not isolated.

## Results

Each cell gives median candidate/baseline elapsed ratio, the complete minimum–maximum range, and the number of slower pairs out of 12. A ratio below one is faster.

| Shape | Stack, paired run (range; slower) | Stack, three-way run (range; slower) | Reserve, three-way run (range; slower) |
| --- | --- | --- | --- |
| one_empty | 0.9443 (0.8651–0.9463; 0) | 0.9463 (0.9427–0.9657; 0) | 1.0030 (0.9983–1.0285; 11) |
| one_100 | 1.0381 (1.0241–1.0414; 12) | 1.0680 (0.9834–1.0830; 11) | 1.0000 (0.9258–1.0438; 6) |
| f01_8_plain | 0.9836 (0.9549–0.9933; 0) | 0.9860 (0.9818–0.9947; 0) | 0.9899 (0.9861–1.0036; 1) |
| f02_8_headers | 0.9731 (0.9691–1.0809; 2) | 0.9772 (0.5938–1.0048; 1) | 1.0011 (0.6140–1.0264; 8) |
| f03_500_plain | 0.9780 (0.9716–0.9867; 0) | 0.9750 (0.9600–0.9884; 0) | 1.0002 (0.9837–1.0118; 6) |
| f04_32_headers | 0.9574 (0.9468–0.9789; 0) | 0.9566 (0.9400–0.9664; 0) | 1.0005 (0.9906–1.0086; 7) |
| many_null_key | 0.9812 (0.9655–0.9936; 0) | 0.9799 (0.9625–0.9969; 0) | 0.9993 (0.9815–1.0107; 4) |
| many_empty_key | 0.9825 (0.9696–0.9978; 0) | 0.9929 (0.9833–1.0049; 2) | 0.9988 (0.9961–1.0074; 4) |
| header_heavy | 0.9554 (0.9339–0.9701; 0) | 0.9638 (0.9470–0.9785; 0) | 1.0000 (0.9775–1.0085; 6) |
| boundaries | 0.9787 (0.8147–0.9945; 0) | 0.9798 (0.9558–0.9951; 0) | 0.9982 (0.9765–1.0219; 5) |
| large_values | 0.9448 (0.9294–0.9558; 0) | 0.9416 (0.9180–0.9715; 0) | 0.9993 (0.9758–1.0233; 5) |
| gzip_128 | 1.0032 (0.9479–1.0237; 8) | 1.0046 (0.8716–1.2633; 7) | 0.9836 (0.8069–1.0343; 2) |
| snappy_128 | 0.9794 (0.9607–1.0175; 2) | 0.9849 (0.9575–1.0123; 1) | 1.0036 (0.9747–1.0667; 6) |
| lz4_128 | 1.0079 (0.9991–1.0804; 10) | 1.0119 (1.0007–1.0257; 12) | 1.0010 (0.9887–1.0115; 6) |
| async_f01_8 | 0.9893 (0.9807–1.0429; 2) | 0.9887 (0.9434–1.0087; 1) | 1.0118 (0.9641–1.0550; 11) |
| async_f03_500 | 0.9752 (0.9631–0.9930; 0) | 0.9739 (0.9582–0.9800; 0) | 0.9983 (0.9858–1.0114; 4) |
| async_headers | 0.9611 (0.9476–0.9841; 0) | 0.9637 (0.9575–0.9931; 0) | 0.9983 (0.9918–1.0168; 4) |
| async_large | 0.9430 (0.9284–0.9534; 0) | 0.9420 (0.9174–0.9662; 0) | 1.0003 (0.9763–1.4650; 6) |
| zero_records | 0.9126 (0.8949–0.9246; 0) | 0.9114 (0.8954–0.9684; 0) | 0.9992 (0.9798–1.0196; 4) |
| cold_one_empty | 0.6796 (0.6663–0.7124; 0) | 0.6767 (0.6702–0.6863; 0) | 0.6843 (0.6740–0.6908; 0) |
| cold_one_100 | 0.7774 (0.7670–0.7888; 0) | 0.7771 (0.7019–0.8203; 0) | 0.7735 (0.6912–0.8191; 0) |

## Source, binaries, and artifacts

SHA-256 hashes for `src/protocol/records.rs` were `a0d5b6ff6e1bb76b48057278654f4f43535fd6bf27075badda88b993c4f535f4` baseline, `c7f35eba8839802b61d8f6153ee8552fb72f71dd15ff07840cbafe53febdea61` stack, and `76460cced2297de59debe8f77345d340abc910cb557c45867fd4d735dca81ee6` reserve. The source Git blobs were `bf77f06a753a9341d5f7528816b9f9891a526c4f`, `5ebe2a484d99c30343e634ad344e846a329e7628`, and `cda45639e35c09e94a20414280907a88c116a704`, respectively.

| Run | Baseline binary SHA-256 | Stack binary SHA-256 | Reserve binary SHA-256 |
| --- | --- | --- | --- |
| [42467693810](https://github.com/mingley/partitionline/actions/runs/42467693810) | `9a256d29e553767b4aaae2fafff583ee0dc23dea9517ec426d82d5b3aae11fdf` | `99c97b467792dbedbcd5cc4510f6b1bc163eb7980c41e123f279df6ef7c769aa` | not built |
| [42468477850](https://github.com/mingley/partitionline/actions/runs/42468477850) | `9a256d29e553767b4aaae2fafff583ee0dc23dea9517ec426d82d5b3aae11fdf` | `99c97b467792dbedbcd5cc4510f6b1bc163eb7980c41e123f279df6ef7c769aa` | `d94ba3d7d71d5b86bc50e958c949d07ce49aba4a88d44562a2aade1b7d10d81c` |

The compared corpus SHA-256 was `a3e86998ca5d7d75e1d6af68e575e03c35ee862ea1d02c5ab5e353ebaa4e9a10`. The [paired-run artifact](https://github.com/mingley/partitionline/actions/runs/42467693810/artifacts/13612114146) SHA-256 is `60c4fc2777b6148b497dd0a1818c4ccb229d1c9c80c10549614664921f047d4d`; the [three-way artifact](https://github.com/mingley/partitionline/actions/runs/42468477850/artifacts/13612349844) SHA-256 is `e2e271d3c045054acfc4a46d2d83a42793930e6d038b438b2c0aca5eb252560b`. They contain logs, input hashes, raw data, and disassembly, with 30-day retention.

The optimized `start_record_batch` symbol sizes were 1,331 bytes baseline, 387 stack, and 1,378 reserve. These differences do not establish why a workload changed.

## Setup failure

[Run 42467537375](https://github.com/mingley/partitionline/actions/runs/42467537375) stopped at rustfmt before tests or timing because of two formatting differences. Its [artifact](https://github.com/mingley/partitionline/actions/runs/42467537375/artifacts/13612152734) has SHA-256 `0bd2af0f3554a1a3535ab179d5ecc2c90f4ff0248b3c2ec4b9f8765416589d7f`. The following stack candidate used the exact formatted source and was measured in both successful runs above.
