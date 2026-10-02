# KL09-71: rejected bootstrap race

Disposition: **rejected-regression**. Baseline `7f7737ba328487f3e97551b65e4ee8b0dac5766d`; candidate `a3a3592b17bf8d529198aa44216253a6af63471c`; production/test revert `5b43d24978f88815bdd863aa5686b1ded6457c9d`. The additive loopback stalled-TCP cell remains in the harness.

The candidate improved the stalled-first median paired first-ack latency by 66.6196% (95% CI: 66.2511%–67.5603%). It failed the plan's 2% guardrail rule: refused-first 1-partition latency +2.1168%, refused-first 64-partition latency +2.0282%, and whole-phase allocation count +3.3922%. The allocation interval is wide and does not establish a causal allocator regression. These are local null-broker observations, not public Kafka performance claims.

`ab/` preserves all ten alternating AB/BA pairs: 20 valid results, 180 acknowledged records and 200 verified broker/latency sidecars. `first-five-analysis.json` preserves the first five pairs; the sole extension to ten pairs addressed unresolved allocation noise. No samples were removed. `ab-commands.json` records every executed measurement command. `sanitization.json` records raw/sanitized result hashes; measured values and sidecar bytes are unchanged. `baseline-stalled-initial/` is the harness-first TCP-stall reproduction before the implementation. `bootstrap-probe.*` is an earlier unchanged-baseline live/refused-only probe, not the candidate comparison.

Run from the repository root:

```sh
python3 docs/evidence/perf/KL09-71/analyze-ab.py --output /tmp/kl09-71-analysis.json
cmp /tmp/kl09-71-analysis.json docs/evidence/perf/KL09-71/ab-analysis.json
python3 scripts/benchmark-report.py docs/evidence/perf/KL09-71/ab/pair0-A/nb-connect-rep0.result.json --quiet --json
sha256sum -c docs/evidence/perf/KL09-71/checksums.sha256
```

`candidate.patch` applies to the pinned baseline and includes the removed production code and six regression tests. `candidate-bootstrap-tests.rs` is an archival copy, not a standalone test target. `verification/failing-first.txt` records the expected baseline failure; candidate required-suite, TLS-cleanup, strict-clippy and MSRV logs are retained alongside it. `integration/` verifies the rejected candidate (1,791 passed, four existing ignored; four package cells; six packaged MSRV bootstrap tests). It also preserves the initial disk-full build interruption and clean reruns. `integration-final/` independently verifies the reverted shipped state (1,785 passed, four existing ignored; four package cells). Neither ignored tests nor interrupted builds count as passes.

The complete evidence record is `docs/plan/evidence/KL09-71.json`. Positive cost/latency deltas mean regression. Paired confidence intervals resample matched process-pair indices, 20,000 times with seed 71002; per-case and arm-median summaries are also retained. Socket observations are post-ack FD counts, not peak concurrency samples. The rejected implementation capped four dial futures/sockets/handshakes and documented that blocking DNS jobs may outlive cancellation.
