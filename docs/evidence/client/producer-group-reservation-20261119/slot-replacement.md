# Mixed-topic reservation using slot replacement

This trial uses the count returned by map insertion to reserve each
partition list. It preserves the contiguous chunks and 8,192-unit poll
budget. A missing count becomes zero and the map still gets the partition's
slot index.
[First variant and original failure](first-pass.md) ·
[Contiguous chunk trial](chunked.md)

The exact candidate `eb05e43f4e84198823e15a2b2613b7b7f7e831fe` passed Rustfmt, 131 producer tests in
each of the default, all-features and no-default modes, and strict
all-features Clippy with stable Rust 1.100.0. Both deterministic chunk tests
and all six cancellation or retry-order controls passed in each mode.
[Qualification](https://github.com/mingley/partitionline/actions/runs/44629467425)

Eight pairs compared it with `51a4d86ec8c0e51e1c6dd231487bfe27e0725a70` on
one hosted x86_64 runner. The builds used the same workload shapes, test
harness and alternating arm order as the previous trials.
[128 raw samples](slot-replacement-pairs.jsonl) ·
[Timing job](https://github.com/mingley/partitionline/actions/runs/44629467425/job/135931939940)

| Shape | Baseline median ns/group | Candidate median ns/group | Median paired elapsed change | Paired range | Slower pairs |
| --- | ---: | ---: | ---: | ---: | ---: |
| `single-1` | 51.56 | 52.50 | +1.95% | +1.18% to +2.88% | 8/8 |
| `single-64` | 119.65 | 120.01 | +0.31% | −37.69% to +2.07% | 4/8 |
| `single-8193` | 28371.59 | 28432.60 | +2.64% | −4.37% to +3.35% | 5/8 |
| `partitioned-64` | 4722.98 | 4636.44 | −1.81% | −6.58% to +13.58% | 2/8 |
| `mixed-512` | 55793.49 | 55583.04 | −0.18% | −1.93% to +0.92% | 2/8 |
| `mixed-scatter-8193` | 876962.62 | 885018.33 | +0.91% | −3.65% to +1.93% | 7/8 |
| `mixed-assembly-2051` | 470393.76 | 449107.32 | −4.71% | −8.09% to −4.08% | 0/8 |
| `mixed-wide-16387` | 6068967.09 | 6400980.13 | −3.51% | −5.99% to +15.48% | 3/8 |

Mixed-512 and mixed scatter came within 1% in median paired time. Mixed
assembly improved in every pair. One-record grouping was slower in every
pair, with separate medians of 51.56 ns and 52.50 ns. Wide grouping had
three slower pairs and its two aggregate medians differ in direction from
the median of paired changes. The samples measure only grouping.

The committed instruction gate reproduced the original LZ4 decode miss
of 920,136 instructions against 903,610. The other nine counters remained
within the frozen +1% limit.
[Instruction gate](https://github.com/mingley/partitionline/actions/runs/44629467981/job/135931942414)

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
GROUPING_SOURCE arm=candidate blob=bc015178dada0a49c466d4d3b815434687e779ac instrumented_sha256=d74c41c886c0650a06b1977276a51ae6633819accaff8169cac44c33d26d0eb8 production_sha256=8c6c5b39815c82c139a9ec473f6543eabd6577847814bfc886775aacc7af7458
GROUPING_BUILD_FILE Cargo.toml sha256=cd14d7fa421e960e14e8dd86dde33b7f4dae5510fbcfca5ab2798ec062d2b677
GROUPING_BUILD_FILE Cargo.lock sha256=a3873c6f1009d2742501943132c1dc17c3b7fbf4eee2bd8f2d907a9251146e4d
GROUPING_HOST arch=x86_64 cpu=0 available=4
GROUPING_BINARY baseline=df2ce39c14fa2efdcbb274ef5908bcfc51d88a8f0ecd35baf502e96987274f92 candidate=626b03578e28148de622ec93247a1c7ca609bdd447614fb6bff59e319e2fc847
```
