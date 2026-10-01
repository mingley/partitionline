#!/usr/bin/env bash
# Retain the exact allocation-gate failure without altering its census budgets.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
report="${PL_ALLOC_REPORT_DIR:-$ROOT/artifacts/alloc-budget}"
mkdir -p "$report"
{
  git rev-parse HEAD
  rustc -Vv
  cargo -V
  uname -sm
} >"$report/identity.log"
set +e
cargo test --locked --manifest-path benchmarks/codec/Cargo.toml \
  --test alloc_budget --test json1k 2>&1 | tee "$report/tests.log"
statuses=("${PIPESTATUS[@]}")
result=${statuses[0]}
if [[ "$result" -eq 0 && "${statuses[1]}" -ne 0 ]]; then
  result=${statuses[1]}
fi
set -e
python3 - "$report/tests.log" "$result" <<'PY'
import pathlib
import re
import sys

log = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
result = int(sys.argv[2])
if not result:
    summaries = re.findall(r"^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;", log, re.M)
    cases = ("alloc_budgets", "seeded_json1k_payloads_and_allocation_baselines")
    if len(summaries) != 2 or any(not re.search(r"^test " + name + r" \.\.\. ok$", log, re.M) for name in cases):
        log = "Incomplete allocation-gate execution: require both serial census tests.\n" + log
        result = 1
if result:
    # Annotation access remains useful when a hosted artifact's download host
    # is unavailable. Full bytes are always retained in tests.log.
    diagnostic = log if len(log) <= 14000 else log[:5000] + "\n[full log retained; middle omitted]\n" + log[-9000:]
    diagnostic = diagnostic.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print("::error title=Allocation gate::" + diagnostic)
sys.exit(result)
PY
