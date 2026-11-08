# Broker frame read fairness

This report records a framing change and measurements. The change was tested
as an isolated candidate and is applied on main. The results do not establish
broker production readiness or a speedup.

## Source and scope

- Baseline: main commit [60caa746c3654c89d01557e35e1100cdc2a84b74](https://github.com/mingley/partitionline/commit/60caa746c3654c89d01557e35e1100cdc2a84b74), transport blob `e80acf8d5fe3614b35f079d3d6364d086b0abb84`.
- Candidate: source-only commit [0a4c241a44f85bc47121b62fb67a6048d8b3f1fa](https://github.com/mingley/partitionline/commit/0a4c241a44f85bc47121b62fb67a6048d8b3f1fa), transport blob `a36cf086c3d683a5f24b818705b15d8ce54245f4`, based on main `c7a91ce50cd231bb7d85e90297d98344e5a98dec`. The main changes from 60c to c7, and from c7 to `1188156ec955a6141a1f06d2c25cddfa2856fa8b`, touched documentation only. No claim is made for any later head.
- Broker Cargo.toml `4e23c42f8d9ac722005098a4b58e1046b711e1a3`, Cargo.lock `19652acd96b929402b1e4a9394a33d12a7e69449`, Raft runtime `d8fec08072b9128bbcb9623dfdf267212689d63d`, and inventory checker `f186411c284d4e2281b5b0246413599c710c4275` are unchanged by the candidate.
- Exact-source qualification workflow commit `02506ff3adcd64684d7494124a3565445ab86482` has the candidate as its parent and adds only its temporary workflow. The job checks both the committed and working-tree transport blob before running.
- Stable rustc 1.99.1 (`58cf8f9c3`), LLVM 23.1.1, x86_64 Linux was used.

The candidate counts successful reads and bytes while assembling a buffered
frame, including any remaining length-prefix bytes, and successful body reads
in the direct path. It requests a Tokio cooperative yield at 128 calls or
64 KiB when a buffered frame remains incomplete. In the direct path it retains
a byte-limit yield and adds a call-limit yield for an incomplete frame. A completed buffered 64 KiB frame or
a completed 128-one-byte-read frame receives no new final yield; the
previously present direct 64 KiB yield remains for the tested 8 KiB chunks.

This is a bound on repeated ready-read progress before the code requests a
yield, not a wall-time or executor fairness guarantee. Counters also accumulate
across ordinary pending socket reads until an explicit yield, so the change can
add a yield after I/O already suspended. Tokio may immediately repoll a yielding
task. A single `poll_read`, allocation, or other synchronous operation can
take arbitrary time. In particular, `fill_complete_frame` can synchronously
shift or copy an already buffered complete frame of any permitted size; that
work is not bounded by the new read-loop budget. Tokio's TCP readiness code
already has cooperative budgeting; the controlled uncooperative reader alone
does not establish a TCP starvation incident.

## Controls and qualification

- Test-only baseline [42467673256](https://github.com/mingley/partitionline/actions/runs/42467673256), commit `b12267362ef598687620d7df62efda9c8a07d624`: one poll of a 256 KiB buffered body consumed all 262,144 bytes in 32 ready reads; one poll of the direct one-byte reader consumed 4,100 reads including the 4-byte prefix. Both profile controls passed.
- Real Tokio-Rustls over preloaded in-memory duplex, before the candidate: [42467971856](https://github.com/mingley/partitionline/actions/runs/42467971856), commit `a462c6d6202f601d3a6a5ced25d07841127d06c1`, completed the 256 KiB body in one poll with 32 reads. Candidate [42468439626](https://github.com/mingley/partitionline/actions/runs/42468439626), test-only commit `e181198f057311057238cc0954a410fda529bc23`, returned Pending after 8 reads and 65,536 bytes, then completed the same exact body. TLS-only and all-feature tests passed; removing the buffered yield failed at the intended “TLS body finished without yielding” assertion.
- Exact candidate [42469327534](https://github.com/mingley/partitionline/actions/runs/42469327534): all 16 focused checks passed across default and all features, including first-poll limits, completion of the same partially read future with exact bytes, an already expired buffered deadline with zero reader polls, actual loopback TCP with a partially prefetched prefix, and existing deadline and direct-yield tests. Four independently restored mutants failed their intended assertions: no buffered yield; no 128-call limit; an extra yield on the final buffered 64 KiB read; an extra yield on the final direct 128th one-byte read.
- Exact boundary observations: the direct 64 KiB body with 8 KiB reads returned Pending after 9 total reads including the prefix, as before. Buffered 64 KiB completed in 8 reads. Direct and buffered 128-byte one-byte-read bodies completed in 132 and 128 reads respectively; at 129 body bytes each returned Pending after the 128th body read.
- Real Tokio-Rustls **over loopback TCP** with a fragmented prefetched prefix and exact 256 KiB body passed TLS-only and all-feature controls in [42468842748](https://github.com/mingley/partitionline/actions/runs/42468842748), test-only commit `bc904071d7dc9a7695b055913200c802f87076c8`. This is a correctness test; it does not establish an OS scheduling bound.
- The same exact-source gate passed locked broker `--all-targets` default (**779 passed, 4 ignored**) and `--all-features` (**927 passed, 4 ignored**). Broker format, strict all-target/all-feature Clippy, strict all-feature Rustdoc, and default/all-feature doctest commands passed; each doctest run had zero tests. The existing pinned Apache API inventory and all 55 Python inventory negative-control tests passed. The post-push [main broker run 42470021226](https://github.com/mingley/partitionline/actions/runs/42470021226) passed the same profile counts and strict checks, inventory, and fuzz supervision. These checks are not a production or compatibility qualification.
- The first baseline setup [42467544477](https://github.com/mingley/partitionline/actions/runs/42467544477) failed compilation because the fixture tried to debug-format an `Exit` without `Debug`; it produced no behavior result. The first exact-source gate [42468983849](https://github.com/mingley/partitionline/actions/runs/42468983849) used a depth-1 checkout and all five jobs failed before their checks with `fatal: ambiguous argument 'HEAD^'`. The corrected gate used depth 2. An earlier staged control run [42468218665](https://github.com/mingley/partitionline/actions/runs/42468218665) failed to restore the no-call-limit mutant before the buffered-final-yield mutant, so that one mutant was combined; the exact-source gate restored each mutation independently.
- These results do not explain the original [42456887194](https://github.com/mingley/partitionline/actions/runs/42456887194) ready-frame deadline observation of 82.681715 ms against its 80 ms wall threshold and 30 ms deadline, or the separate earlier main Raft failures. They are not attributed to this candidate.

## Ready-reader timing

Run [42468376768](https://github.com/mingley/partitionline/actions/runs/42468376768) used a shared AMD EPYC 7763 runner pinned to one logical CPU, release builds, six alternating rounds per profile and variant, warmup and reused buffers. The test-only script was blob `54172b6741ee2201894539ea58f230ed848942fa`. Revisions were baseline 60c, unconditional-yield draft `190ea83f3fe66ed6c4bbdd6a08a1f58606d7478c`, and refined staged source `5b5e3f4f56f58a857b3e7cb718a26a42746a3737`; the refined source was formatted into the exact candidate blob. The results below are medians of per-round elapsed time per frame, and percentages are ratios of medians. The [360 raw round records](ready-reader-rounds.csv) include all three variants. This is an in-process always-ready reader, not Kafka throughput.

| Case | Default baseline ns/frame | Default candidate ns/frame | Change | All baseline ns/frame | All candidate ns/frame | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| direct 32 bytes | 245.2 | 242.1 | -1.26% | 254.9 | 252.4 | -0.99% |
| buffered 32 bytes | 278.1 | 285.5 | +2.65% | 300.4 | 288.9 | -3.84% |
| direct 128 one-byte reads | 4,676.1 | 4,681.8 | +0.12% | 4,695.3 | 4,700.8 | +0.12% |
| buffered 128 one-byte reads | 4,979.4 | 5,029.2 | +1.00% | 4,988.3 | 5,025.9 | +0.76% |
| direct 64 KiB | 2,745.0 | 2,771.4 | +0.96% | 2,750.0 | 2,800.3 | +1.83% |
| buffered 64 KiB | 3,231.5 | 3,211.1 | -0.63% | 3,223.0 | 3,270.4 | +1.47% |
| direct 256 KiB | 11,843.1 | 12,005.5 | +1.37% | 11,746.3 | 11,998.7 | +2.15% |
| buffered 256 KiB | 55,913.1 | 59,067.8 | +5.64% | 55,985.4 | 59,030.7 | +5.44% |
| direct 4,096 one-byte reads | 139,394.8 | 158,598.9 | +13.78% | 139,331.5 | 159,029.4 | +14.14% |
| buffered 4,096 one-byte reads | 150,857.0 | 172,183.0 | +14.14% | 151,034.3 | 172,179.4 | +14.00% |

At the complete-buffered-64-KiB and 128-read boundaries the unconditional
draft returned Pending while baseline and refined returned Ready. Its default
buffered-64-KiB median was 3,937.5 ns/frame versus baseline 3,231.5 and refined
3,211.1; its direct-128-one-byte median was 5,366.7 ns/frame versus baseline
4,676.1 and refined 4,681.8. These are small synthetic samples subject to
shared-host noise and code generation effects. The first two benchmark
submissions [42468297515](https://github.com/mingley/partitionline/actions/runs/42468297515)
and [42468322388](https://github.com/mingley/partitionline/actions/runs/42468322388)
failed workflow validation without a job or measurements.

## TCP and TLS loopback timing

Run [42469158690](https://github.com/mingley/partitionline/actions/runs/42469158690),
benchmark-only commit `03f4e4b7886eb95ad7410fa4d032aeb7881e16e5`, used six
alternating rounds on one pinned logical CPU of a shared AMD EPYC 9V74 runner.
It built distinct release binaries from the pinned baseline and exact
candidate, both with TLS enabled; binary SHA-256 was
`48af49651761807b0c29ac260b1029243068ba8c34aca7d27319e2790edb3649`
for baseline and
`21b2419ed685e5c36dbf17c9c23084a16f6f75e2b7b08bcdb99b888fe18fb65e`
for candidate. The test-only measurement source was blob
`50138382ddfe58ff6d867a7558137852b2870aa1`.

A single connection sent a framed body and waited for a one-byte acknowledgement
before the next frame. TCP_NODELAY was enabled. The server read the wire prefix
into ReadAhead to model a prefetched prefix, then used the actual TCP or
Tokio-Rustls/TCP stream to read the body. The one-byte case intentionally
limited the server-side AsyncRead adapter to one byte per successful read; it
does not mean packets were one byte. Each round used 8 warmups, then 128
256-KiB or 256 4-KiB samples. The [48 raw round records](network-rounds.csv)
retain elapsed time and each round's p50/p95/p99; individual RTTs were not
emitted by this run. The table shows throughput computed from median elapsed
time and the median of the six per-round latency estimates, in microseconds.

| Case | Throughput baseline → candidate MiB/s | p50 baseline → candidate µs | p95 baseline → candidate µs | p99 baseline → candidate µs |
| --- | ---: | ---: | ---: | ---: |
| TCP, 256 KiB | 804.1 → 787.4 (-2.08%) | 305.5 → 311.4 | 329.7 → 336.2 | 337.8 → 348.7 |
| TLS/TCP, 256 KiB | 572.5 → 563.2 (-1.63%) | 433.9 → 441.9 | 449.9 → 461.9 | 465.1 → 469.9 |
| TCP, 4 KiB one-byte adapter | 0.809 → 0.814 (+0.65%) | 4,825.9 → 4,794.3 | 4,864.2 → 4,832.1 | 4,905.9 → 4,865.0 |
| TLS/TCP, 4 KiB one-byte adapter | 13.12 → 11.78 (-10.25%) | 294.5 → 328.3 | 307.4 → 341.5 | 323.0 → 354.7 |

These are sequential framing measurements on one shared machine, not full
broker throughput, concurrent-client fairness, an independent reproduction,
or a population-level tail-latency estimate. The results show measurable
tradeoffs and do not support a speedup claim. A [follow-up](network-rtt-followup.md)
retains 9,216 individual RTTs; its TCP 256 KiB result changed direction on a
different runner, while the TLS cases still showed a cost.
