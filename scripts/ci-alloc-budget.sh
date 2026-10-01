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
reason = "Allocation census"
if not result:
    summaries = re.findall(r"^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;", log, re.M)
    cases = ("alloc_budgets", "seeded_json1k_payloads_and_allocation_baselines")
    if len(summaries) != 2 or any(not re.search(r"^test " + name + r" \.\.\. ok$", log, re.M) for name in cases):
        reason = "Incomplete allocation-gate execution: require both serial census tests"
        log = reason + ".\n" + log
        result = 1
if result:
    # Annotation access remains useful when a hosted artifact's download host
    # is unavailable. Full bytes are always retained in tests.log.
    # Hosted annotations truncate long workflow-command lines before their
    # tail. Keep the last 2000 UTF-8 bytes; escaping fits below 6500 bytes even
    # for percent/newline-heavy input. The artifact retains every original byte.
    diagnostic = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", log)
    if len(diagnostic.encode("utf-8")) > 2000:
        diagnostic = "[full log retained; showing tail]\n" + diagnostic.encode("utf-8")[-2000:].decode("utf-8", errors="replace")
    diagnostic = f"{reason}; exited {result}\n" + diagnostic
    diagnostic = diagnostic.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print("::error title=Allocation gate::" + diagnostic)
sys.exit(result)
PY
