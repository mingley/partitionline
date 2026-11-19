# Producer small-group classification

The producer classifies the first bounded slice directly from its input
vector. A small single-partition batch returns that vector as its group.
Other batches move into worker storage before the first yield. Classification
still yields after 8,192 records examined after the first record. The five
cancellation and retry FIFO controls cover classification, single-topic
scatter, mixed scatter, assembly, and retry order.

## Verification

Rust 1.100.0 with LLVM 23.1.5 passed formatting, strict all-features Clippy, and
129 producer tests with each of the default, all, and no-default feature
sets. All five controls passed in each set.
The five controls failed on the prior source because dropping grouping
closed pending receipts before the worker could settle them.
[Baseline controls](https://github.com/mingley/partitionline/actions/runs/44612558287/job/135872694760) ·
[Qualification run](https://github.com/mingley/partitionline/actions/runs/44615262134)

The baseline `713c8a3d861a843bc1e6e229eecf53ce5632b3bf` and candidate
`f37992486d47ebb03432be59e233e7172a821c93` were built separately on the same hosted x86_64 runner.
The timing process was pinned to one available logical CPU. Eight pairs
alternated order. Each timed interval covers grouping and observation of
its output. Input preparation and output validation are outside the interval.
The workflow checks Git blobs and both production prefixes before building.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44615262134/job/135882114637)

| Shape | Baseline median ns/group | Candidate median ns/group | Median paired elapsed change | Paired range |
| --- | ---: | ---: | ---: | ---: |
| `single-1` | 46.16 | 45.25 | −2.00% | −2.94% to −0.45% |
| `single-64` | 191.53 | 97.03 | −49.44% | −53.35% to −41.29% |
| `single-8193` | 34558.15 | 21537.95 | −38.55% | −41.36% to −34.58% |
| `partitioned-64` | 2917.72 | 2721.01 | −6.84% | −11.33% to +1.04% |
| `mixed-512` | 40529.75 | 37264.65 | −8.46% | −14.68% to −3.28% |
| `mixed-scatter-8193` | 667986.78 | 594037.97 | −11.03% | −14.74% to −7.66% |
| `mixed-assembly-2051` | 339169.19 | 316040.17 | −6.82% | −8.28% to −5.93% |

The candidate reduced median grouping time in all seven shapes. One
partitioned-64 pair was 1.04% slower. The prior ownership fix had taken
34.62% longer on the one-record shape and 5.06% longer on the homogeneous
64-record shape in its own paired run. These shared-runner diagnostics
cover grouping. Kafka throughput and latency require end-to-end evidence.
[Prior paired samples](ownership-first-pairs.jsonl) ·
[Prior timing job](https://github.com/mingley/partitionline/actions/runs/44612172989/job/135871361718)

The 112 arm measurements retain their original execution order and
iteration counts.
[Raw pairs](raw-pairs.jsonl)
[Qualification workflow](https://github.com/mingley/partitionline/blob/83f5e59f870c72a3b49da9d7a556d1e3181e0cf1/.github/workflows/producer-small-group-qualification.yml)

## Build provenance

```text
GROUPING_SOURCE arm=baseline blob=47c4fc0c06c57a906d16af32637ebb69c7194511 instrumented_sha256=ce1e0f11553aeecfc6d37ef36ff8bdc2f1f58767ec18b4613ef599747b493d79 production_sha256=4cf4ae3d91fe16be398663ff50c451781efa5267b2cce425d6d6ad27523141b5
GROUPING_SOURCE arm=candidate blob=3bd9548b1114464030a01348c6885b6a68bf6de7 instrumented_sha256=dfab58d8b49856031f0d33a73357d9364ea1c77eafd9c8a61df5374c565b8dc7 production_sha256=eab1df6256fa9f50bfba9c0e84e05ea50c0e0928138d0585c35e4cc359c89f88
GROUPING_BUILD_FILE Cargo.toml sha256=cd14d7fa421e960e14e8dd86dde33b7f4dae5510fbcfca5ab2798ec062d2b677
GROUPING_BUILD_FILE Cargo.lock sha256=a3873c6f1009d2742501943132c1dc17c3b7fbf4eee2bd8f2d907a9251146e4d
GROUPING_HOST arch=x86_64 cpu=0 available=4
GROUPING_BINARY baseline=6d73d4dc09a786f4ea3ed79178e1dc0dbbdc8c1c6724fe7f42e14456927ea61e candidate=8dfae45f925d387b037be58966cfcec1783d34712b25d3086d78f51b1cd43f84
```
