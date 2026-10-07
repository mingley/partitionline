# Local baseline

This captures the client at `c4b915757b47fd2d6e2ab11c85a9db93c2e8ed25` on
an AMD EPYC KVM guest using stable Rust 1.99.0. Guest frequency was uncontrolled.
All results are local and unsigned. Suite HOLD remains active.

Five independent repetitions cover 122 codec/zstd timing cases, 34 simulated
instruction cases, 18 null-broker cells, request encoding, and latency at three
loads. Ten native broker runs each acknowledge eight million measured records;
Rust checks every measured record and Java checks all records including warmup.
Both throughput reruns meet the declared 20% median-difference limit: 0.6% for
the null broker and 1.8% for the real broker. These are repeatability checks.

The five 80% latency profiles all reject some offers at the unchanged 1,024-task
limit. Their raw timestamps, failed exit statuses, outcomes and Java readbacks
are retained. The longer 20,000-offer windows exceed the unchanged 10,000-sample
latency floor. The earlier shorter run remains a failed attempt. Arrival rates
are 1,000, 5,000 and 8,000 per second, fixed from the original five sequential
capacity measurements. They were not recalibrated after the failure.

`capture/aggregate-01/statistics.json` contains medians and bootstrap 95% confidence
intervals for 1,385 actual metric rows. `summary.json` lists missing cells and
measurement limits. Missing metrics are not zero-filled. The producer resource
wrapper measures its whole process; fetch timing includes full-byte verification.
Latency uses a fixed payload without unique IDs. Callgrind instructions are
simulated counts, not native cycles. Optional miniz's allocation failure remains
unqualified; the default budget is unchanged.

The canonical benchmark reporter validated all 95 complete null-broker result
artifacts. Codec, instruction, request, native readback and latency outputs are
harness data rather than complete benchmark-contract results; their replay
validator checked raw sample hashes, counts, fences, outcomes and percentiles.
It rejected 26 changed latency histories. Three genuine SDKs each rejected bad
CRC and valid-CRC/wrong-payload request bodies. These checks do not establish a
production profile or a fastest-client/server claim.

`capture/` retains original data, actual executed binaries, source-bound command
receipts, tool controllers, setup failures and the interrupted codec run. The
qualified codec mapping excludes that unwaited partial run. `source/` retains
111 bound source files; `retained-paths.json` maps original paths to exact copies.
Native topic segments were deleted only after full readback and fence checks.
Every completed owned broker was waited, its supervisor joined and its ports
rebound. The interrupted codec process is explicitly excluded from that claim.

Reproduce using `scripts/record-local-baseline.py --help` and the captured command
receipts. The external tools are evidence sources with paths to the original
checkout; adjust their path dependencies when building in a new checkout.
