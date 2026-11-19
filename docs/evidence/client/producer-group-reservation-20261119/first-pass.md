# Mixed-topic reservation, first pass

This change uses one partition-map entry to reserve the record list and
replace the count with the slot index. The worker yields after every 8,192
partition reservations. At a yield during this stage, it still owns the
unscattered input records.

## Deterministic checks

The earlier macOS default suite on `51a4d86ec8c0e51e1c6dd231487bfe27e0725a70`
reported a duration of 80.56225 ms in the 2,000,000-topic reservation test,
against its 80 ms assertion. The other 2,334 library tests passed.
[macOS result](https://github.com/mingley/partitionline/actions/runs/44618884541/job/135895060417)

The replacement tests use 16,387 total partitions. The narrow fixture uses
one partition per topic. The wide fixture begins with two partitions, then
visits a topic with 16,385 partitions. The checks inspect slots, original
partition order and capacities after polls that reserve 8,192 and 16,384
partitions and after completion. They also verify that a worker canceled
during wide reservation settles its receipts. The old source with the wide
checkpoint returned Ready at its first poll.
[Prior-source control](https://github.com/mingley/partitionline/actions/runs/44623511357/job/135911127204)

Default, all-features and no-default passed 131 producer tests each on stable
Rust 1.100.0 and x86_64. The formatted source
`8aed5fdb14b7276d53847c7e0ab3a1d8af230ecf` passed Rustfmt and strict all-features Clippy. The first
qualification's Rustfmt job requested a test line wrap. That change begins
inside the test module, so the producer implementation used for timings
is unchanged by formatting.
[First run](https://github.com/mingley/partitionline/actions/runs/44623511357) ·
[Formatted source run](https://github.com/mingley/partitionline/actions/runs/44625020350)

## Paired measurements

The baseline was the pre-change `51a4d86ec8c0e51e1c6dd231487bfe27e0725a70`.
The candidate was `338c3afaafeb750b378ebdd8ff9f21bd413ac3fe`. Each arm compiled its own release
test binary with Rust 1.100.0 and LLVM 23.1.5. Eight pairs alternated baseline
and candidate order on one hosted x86_64 runner with one logical CPU
available to the timed process. Every invocation checked its output count.
These numbers cover grouping. Kafka throughput needs end-to-end measurement.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44623511357/job/135911127669)

| Shape | Baseline median ns/group | Candidate median ns/group | Median paired elapsed change | Paired range |
| --- | ---: | ---: | ---: | ---: |
| `single-1` | 43.62 | 44.63 | +2.16% | −6.89% to +2.45% |
| `single-64` | 89.13 | 89.87 | +0.15% | −30.66% to +2.67% |
| `single-8193` | 16484.18 | 16422.71 | −0.45% | −57.65% to +2.39% |
| `partitioned-64` | 4403.78 | 4680.48 | +6.28% | −6.35% to +15.89% |
| `mixed-512` | 45802.26 | 47944.61 | +5.14% | +2.61% to +12.93% |
| `mixed-scatter-8193` | 725435.14 | 758220.98 | +4.64% | +3.39% to +14.51% |
| `mixed-assembly-2051` | 376523.56 | 381693.74 | +1.38% | −28.85% to +6.87% |
| `mixed-wide-16387` | 3756644.02 | 3970358.02 | +5.67% | −7.02% to +15.91% |

Every mixed-512 and mixed-scatter-8193 pair was slower. This result motivated
the second experiment, which checks the work budget once per partition slice.
[128 raw samples](first-pass-pairs.jsonl) ·
[Qualification workflow](https://github.com/mingley/partitionline/blob/2c5f1c7894a80d053a8b25385666921e942aa378/.github/workflows/producer-group-reservation-qualification.yml)

## Committed instruction gate

The candidate reproduced the earlier LZ4 decode result of 920,136 instructions
against a frozen baseline of 903,610. The other nine counters were within the
frozen +1% limit.
[Candidate gate](https://github.com/mingley/partitionline/actions/runs/44623724828/job/135911887473) ·
[Pre-change gate](https://github.com/mingley/partitionline/actions/runs/44621276914/job/135903280638)

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
GROUPING_SOURCE arm=candidate blob=31899a21774d54e4a125394e5226482ee6ab16d0 instrumented_sha256=b4dae3ed2e5a82a0cb52850e95f9248bf3664379cb841af2d79685c85663564f production_sha256=cf07efa2df8e62ae559d43e0c38412c596d182a4fe7598a2a7c33a46cef2b3f0
GROUPING_BUILD_FILE Cargo.toml sha256=cd14d7fa421e960e14e8dd86dde33b7f4dae5510fbcfca5ab2798ec062d2b677
GROUPING_BUILD_FILE Cargo.lock sha256=a3873c6f1009d2742501943132c1dc17c3b7fbf4eee2bd8f2d907a9251146e4d
GROUPING_HOST arch=x86_64 cpu=0 available=4
GROUPING_BINARY baseline=df2ce39c14fa2efdcbb274ef5908bcfc51d88a8f0ecd35baf502e96987274f92 candidate=29e1f72152530cf482a726678fe83eb144450a47d3304d23336fb517150edb1b
```
