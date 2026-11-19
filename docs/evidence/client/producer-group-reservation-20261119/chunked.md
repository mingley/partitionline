# Mixed-topic reservation with contiguous chunks

This experiment reserves the partition lists in contiguous chunks and yields
when it reaches the 8,192-unit budget. A present partition count is read
and replaced through one map lookup. The existing insertion fallback handles
a missing count. The same checkpoint tests confirm the number of completed
reservations inside a wide topic and across topic boundaries.
[First variant and original failure](first-pass.md)

The candidate `6c121f00da287298e20109633dec776d62eea3b6` passed Rustfmt, all three producer feature
profiles at 131 tests each, and strict all-features Clippy on stable Rust
1.100.0. Eight same-runner x86_64 pairs compared it with
`51a4d86ec8c0e51e1c6dd231487bfe27e0725a70`, using separate release
test builds and the same eight shapes as the first variant.
[Qualification](https://github.com/mingley/partitionline/actions/runs/44626846788)

| Shape | Baseline median ns/group | Candidate median ns/group | Median paired elapsed change | Paired range | Slower pairs |
| --- | ---: | ---: | ---: | ---: | ---: |
| `single-1` | 44.37 | 44.82 | +1.24% | −23.74% to +4.56% | 7/8 |
| `single-64` | 89.65 | 98.10 | +5.01% | −52.10% to +67.92% | 5/8 |
| `single-8193` | 20208.02 | 19606.24 | −2.88% | −64.73% to +28.43% | 1/8 |
| `partitioned-64` | 3032.07 | 3087.42 | +0.79% | −18.56% to +50.37% | 4/8 |
| `mixed-512` | 39316.40 | 42033.38 | +6.75% | +1.97% to +26.42% | 8/8 |
| `mixed-scatter-8193` | 611392.83 | 646400.41 | +5.67% | +4.25% to +10.15% | 8/8 |
| `mixed-assembly-2051` | 337358.09 | 321390.96 | −4.06% | −5.79% to −3.84% | 0/8 |
| `mixed-wide-16387` | 3552346.88 | 3443826.91 | −3.41% | −7.98% to −1.25% | 0/8 |

Wide and mixed assembly improved in all eight pairs. Mixed-512 and mixed
scatter slowed in all eight pairs. The timing measures grouping work on a
shared runner.
[128 raw samples](chunked-pairs.jsonl) ·
[Timing job](https://github.com/mingley/partitionline/actions/runs/44626846788/job/135922824302)

The committed instruction gate measured 920,136 LZ4 decode instructions
against the frozen 903,610 baseline, the same as the pre-change source.
The other nine counters remained within the frozen +1% limit.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44626846756/job/135922823728)

```text
ci-perf-gate: x86_64-1.100.0 vs baseline @ 713c8a3d861a843bc1e6e229eecf53ce5632b3bf (tol +1%):
  iai_compress_gzip/text: Ir=11243402 vs 11243365 (+0.00%)
  iai_compress_lz4/text: Ir=2291524 vs 2291524 (+0.00%)
  iai_compress_snappy/text: Ir=1578383 vs 1578383 (+0.00%)
  iai_crc/f03: Ir=96702 vs 96702 (+0.00%)
  iai_decode/f03: Ir=368394 vs 368394 (+0.00%)
  iai_decompress_gzip/text: Ir=1174945 vs 1174945 (+0.00%)
  iai_decompress_lz4/text: Ir=920136 vs 903610 (+1.83%)
  iai_decompress_snappy/text: Ir=789514 vs 789514 (+0.00%)
  iai_encode/f03: Ir=779897 vs 779897 (+0.00%)
  iai_request/v9: Ir=195053 vs 195053 (+0.00%)
ci-perf-gate: FAIL
  iai_decompress_lz4/text: +1.83% exceeds +1% (Ir 920136 vs 903610)
```

## Timing build provenance

```text
GROUPING_SOURCE arm=baseline blob=3bd9548b1114464030a01348c6885b6a68bf6de7 instrumented_sha256=e066adb5116cb6f394da474a570e684c9a88c854ea23b1bf995d089543525fc9 production_sha256=eab1df6256fa9f50bfba9c0e84e05ea50c0e0928138d0585c35e4cc359c89f88
GROUPING_SOURCE arm=candidate blob=487b77db6726164a59fbbdf59bb4d6abbaace68e instrumented_sha256=9d20d411cfdbda42bba8f75ecd09e4122c4befbed53a7ed2cbd9cb0362d3871b production_sha256=93e04df40289d181acc76bf07479a3ac292d391045d6a0febec6a1b408a9c59f
GROUPING_BUILD_FILE Cargo.toml sha256=cd14d7fa421e960e14e8dd86dde33b7f4dae5510fbcfca5ab2798ec062d2b677
GROUPING_BUILD_FILE Cargo.lock sha256=a3873c6f1009d2742501943132c1dc17c3b7fbf4eee2bd8f2d907a9251146e4d
GROUPING_HOST arch=x86_64 cpu=0 available=4
GROUPING_BINARY baseline=df2ce39c14fa2efdcbb274ef5908bcfc51d88a8f0ecd35baf502e96987274f92 candidate=6d61239d5e21b4c8fe2d01f7b7ea878d283f7c775e4f85991af4040bdb88d70b
```
