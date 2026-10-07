# Zstd benchmark driver evidence

The driver covers24 shape/entropy/level workloads, with three operations each.
Every prepared batch checks all record and metadata fields. The optimized
diagnostic run emits72 complete rows with ratio, payload bytes/s and allocation
counts. A frozen executable and before/after source hashes bind the final run.
All72 Criterion smoke cases succeed. Default benchmark tests pass3 cases;
default plus zstd passes4. Strict all-target/all-feature Clippy and format pass.
The result validator passes20 tests, including missing-backend, wrong-level and
missing-corpus negative checks.

Bulk and fetch level3 zstd cells are required and remain not_run. Each peer's
backend target and unresolved campaign requirements are recorded. This closes
the benchmark infrastructure card, without a performance comparison or claim.

An extra fallback build first exhausted disk; its actual failure and generated
cache removal are retained. A low-debug retry exposed an existing gzip budget
mismatch:21 allocations versus the default-backend16. The same mismatch occurs
without zstd. The default-profile budgets are unchanged; fallback qualification
remains open under the baseline card. Nothing skips or masks this failure.

`summary.json` records scope and limitations. `validation/` retains raw output,
tests, failures, original/final source snapshots, compiler receipts and the
frozen diagnostic executable. Earlier diagnostic output remains alongside the
final descriptor/backend-disclosure revision. All measurements are local and
diagnostic; no Suite HOLD, published numbers or global ranking changed.
