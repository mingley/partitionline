# KL10-09 parent vs candidate on lb-gateway-fetch-gzip

Local unsigned outcome-branch measurement. Suite HOLD remains.
**No performance claim.** Production code was reverted
(`rejected-regression`).

## What ran

Parent SHA `0df79d8d` (KL10-14 tree, inline decode on the caller) versus
candidate SHA `b3a5c3bf` (bounded `spawn_blocking` decode workers, default
auto = 4 cores on this host). Same consume driver, same knobs as
`lb-gateway-fetch-gzip`: `fetch.min.bytes=1`, `fetch.wait.max.ms=500`,
`fetch.max.bytes=52428800`, `max.partition.fetch.bytes=1048576`,
PLAINTEXT, manual assign of 8 partitions from offset 0, `COUNT=25000`
gzip json1k backlog already on `pl-gateway-fetch` (KL10-14 seed).

Five A-B-B-A interleaved pairs after one discarded warmup pair. Throughput
is consumed/elapsed from a dedicated release driver (no per-record hash
verify; that work is outside the decode hotspot). CPU is `RUSAGE_SELF`
including blocking-pool threads. RSS is `ru_maxrss`.

## Result

Wall-clock records/s rose (median paired +84.6%, 95% CI
[+58.3%, +106.2%]). Fetch-profile guardrails did not: CPU per record
+17.9% (CI [+10.0%, +20.6%]) and peak RSS +8.7% (CI [+8.1%, +9.2%]).
Both exceed the 2% guardrail. Disposition is `rejected-regression`.

## Files

- `results/` — warmup plus 10 paired consumes
- `summary.json` — medians and bootstrap 95% CIs
- `binaries.sha256` — parent and candidate consume drivers
