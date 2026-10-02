# KL09-53: request-header cost visibility

Disposition: **rejected-no-gain**, using the early below-1% branch of performance-leadership section 6. Baseline: `aeb205ddee136d915349ef9dd165d58e9d3aa109`. No production candidate, encoder change or cache was created.

The existing `micro-request/v9` and `iai_request/v9` encode the Produce body without its request header. `header-cost-probe.rs` preserves that 100-record body shape separately and adds a read-only cost study of actual classic/flexible headers and complete frames. It uses the default 13-byte `partitionline` client id and borrowed header encoder, with a warmed retained 16 KiB buffer. These supplemental probes are not newly shipped benchmark cells.

Five rotated/interleaved rounds produced 25 CPU probes and 25 Callgrind probes, all on CPU3. Header CPU share was 0.316230% classic (95% CI: 0.299856%–0.351203%) and 0.349153% flexible (0.331037%–0.369757%). Header instruction share was 0.363627% classic and 0.342252% flexible, identical in all five rounds. The header alone retired 350/396 instructions and allocated zero bytes. Whole framed requests retired about 96,253/115,704 instructions. The fresh-buffer original body census remained 17 allocations and 70,766 bytes. These are baseline cost fractions, not candidate speedups or public Kafka performance claims.

`cpu/` retains all raw CPU/rusage results; `instructions/` retains all raw JSON, gzip-compressed Callgrind outputs, stderr, commands and summaries. Instruction collection includes only `header_cost_probe::encode_case` and its callees: 20 warmups, one census, and N loop calls, normalized by N+21. CPU values cover the whole child process, amortized across long loops. Both include some dispatch/clear/black-box overhead.

```sh
python3 docs/evidence/perf/KL09-53/validate-study.py
sha256sum -c docs/evidence/perf/KL09-53/checksums.sha256
```

`run-header-cost.py --help` describes rebuilding and running the baseline probe from a clean checkout. `run-header-instructions.py --help` describes repeating the Callgrind collection with an installed or locally extracted Valgrind. Five matched-round percentile bootstrap intervals use 20,000 resamples and separate fixed seeds (53002 CPU, 53003 instructions). The static validator reproduces all four intervals and verifies source, sample bytes, command exits and raw/compressed output hashes. These are evidence-local schemas, not the runtime result contract.

`tooling/` records initial configured-snapshot HTTP403 failures and subsequent successful official Debian-mirror metadata/package verification. Valgrind 3.24.0 was extracted into scratch only; no system installation or network-policy change occurred. The Debian package SHA-256 is retained. A tool-version metadata label was corrected after capture; the original summary and correction record remain, and every count/sample/statistic is unchanged.

`verification/` contains unchanged-baseline checks: 72 header unit goldens, 30 protocol-oracle tests, eight network-deadline tests, and the fixture oracle script all passed. One existing live-broker oracle test remains ignored; 1,028 unrelated unit tests were filtered by the header selection. Core formatting and strict library/all-feature clippy passed. The fixture script ran its default offline path; no fresh Java regeneration or live broker run was claimed.

The study does not cover tiny metadata/auth bodies, long client ids, TLS or DNS cost, or cache behavior. It leaves every wire encoder, frame/correlation handling, tag handling, negotiation, SASL and deadline path unchanged. The full evidence record is `docs/plan/evidence/KL09-53.json`.
