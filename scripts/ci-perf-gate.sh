#!/usr/bin/env bash
# KL09-05 deterministic instruction-count regression gate.
#
# Tool choice (recorded review): iai-callgrind 0.14.2, a callgrind-class
# harness declared in benchmarks/codec/Cargo.toml (dev-dependency of the
# workspace-excluded bench crate only). It drives valgrind/callgrind over
# the tracked library benches in benchmarks/codec/benches/iai.rs and
# reports Ir (instructions retired) per bench. Ir is a pure function of
# (binary, input): deterministic across runs, machines and ASLR states
# for a fixed (arch, rustc); --allow-aslr keeps the gate working in
# containers where setarch is blocked. Callgrind has no macOS/Windows
# port, so this gate is Linux-only and SKIPs loudly elsewhere. Wall-clock
# microbench numbers from shared runners are informational only and are
# never compared here.
#
# The native iai baseline files (raw callgrind .out, ~430K/run with
# absolute paths) are not portable, so the stored baseline is the compact
# benchmarks/codec/perf-baseline.json: Ir per tracked bench keyed by
# arch+rustc, with toolchain, valgrind and git-sha provenance. An increase
# above 1% on any tracked bench fails. Unknown (arch, rustc) keys are not
# a measured regression: the gate passes with a BASELINE_MISSING notice
# and prints the values to record. The baseline may be raised only by a
# separate maintainer-approved baseline-change card (via --record).
#
# Usage:
#   bash scripts/ci-perf-gate.sh             # compare current tree vs baseline
#   bash scripts/ci-perf-gate.sh --record    # (over)write this arch+rustc entry
#   bash scripts/ci-perf-gate.sh --self-test # prove an injected slowdown fails
#
# Measured builds use a private CARGO_TARGET_DIR (one per invocation) unless
# the operator already set one: hermetic build trees make every measured
# compile immune to stale fingerprints and bind-mount mtime games, at the
# cost of one full dependency build per run (CI pays that anyway).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CODEC="$ROOT/benchmarks/codec"
BASELINE="$CODEC/perf-baseline.json"
MANIFEST="$CODEC/Cargo.toml"

MODE="run"
case "${1:-}" in
  "") ;;
  --record) MODE="record" ;;
  --self-test) MODE="selftest" ;;
  *) echo "usage: bash scripts/ci-perf-gate.sh [--record|--self-test]" >&2; exit 2 ;;
esac

# The Ir parser carries a regression suite (Left vs Both vs Right); run it
# on every invocation, on every platform: a broken parser must fail here,
# never silently compare stale numbers.
if command -v python3 >/dev/null 2>&1; then
  python3 "$ROOT/scripts/ci-perf-gate-extract.py" --self-test \
    || { echo "ci-perf-gate: FAIL — extractor self-test failed" >&2; exit 1; }
elif [[ "$(uname -s)" == "Linux" ]]; then
  echo "ci-perf-gate: FAIL — missing 'python3' (install python3 (stdlib only))" >&2
  exit 1
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "ci-perf-gate: SKIP (Linux-only; valgrind unavailable on $(uname -s))"
  exit 0
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ci-perf-gate: FAIL — missing '$1' ($2)" >&2
    exit 1
  fi
}
need cargo "install the Rust toolchain"
need python3 "install python3 (stdlib only)"
need valgrind "sudo apt-get install -y valgrind"
need iai-callgrind-runner "cargo install iai-callgrind-runner --version 0.14.2"

ARCH="$(uname -m)"
RUSTC="$(rustc -vV | sed -n 's/^release: //p')"
if [[ -z "$RUSTC" ]]; then echo "ci-perf-gate: FAIL — cannot parse rustc version" >&2; exit 1; fi
KEY="$ARCH-$RUSTC"
VALGRIND="$(valgrind --version | sed 's/^valgrind-//')"
GIT_SHA="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"

GATE_TMP="$(mktemp -d)"
trap 'rm -rf "$GATE_TMP"' EXIT INT TERM
if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  export CARGO_TARGET_DIR="$GATE_TMP/target"
fi

measure() {
  local out="$1" log="$2"
  (cd "$ROOT" && cargo bench --locked --manifest-path "$MANIFEST" --bench iai \
    -- --allow-aslr --output-format=json >"$out" 2>"$log")
}

compare() {
  local out="$1"
  BASELINE_FILE="$BASELINE" BASELINE_KEY="$KEY" GATE_ROOT="$ROOT" python3 - "$out" <<'PY'
import json, os, subprocess, sys

path = sys.argv[1]
extractor = os.path.join(os.environ["GATE_ROOT"], "scripts", "ci-perf-gate-extract.py")
proc = subprocess.run(
    [sys.executable, extractor, path], capture_output=True, text=True
)
if proc.returncode != 0:
    print(proc.stderr.strip() or "ci-perf-gate: extractor failed")
    sys.exit(1)
results = {}
for line in proc.stdout.splitlines():
    bid, ir = line.split("\t")
    results[bid] = int(ir)

base_path = os.environ["BASELINE_FILE"]
key = os.environ["BASELINE_KEY"]
try:
    with open(base_path) as f:
        base = json.load(f)
except FileNotFoundError:
    print(f"ci-perf-gate: FAIL — baseline file missing: {base_path}")
    print("  record one with: bash scripts/ci-perf-gate.sh --record")
    sys.exit(1)

tol = base.get("tolerance_pct", 1.0)
entry = base.get("baselines", {}).get(key)
if entry is None:
    print(f"ci-perf-gate: BASELINE_MISSING for key {key} — not a regression.")
    print("  record values via a baseline-change card:")
    for bid in sorted(results):
        print(f"  {bid}: Ir={results[bid]}")
    sys.exit(0)

want = entry["benches"]
fails, rows = [], []
for bid in sorted(set(want) | set(results)):
    if bid not in results:
        fails.append(f"{bid}: missing from run (tracked bench disappeared)")
        continue
    if bid not in want:
        fails.append(f"{bid}: no baseline entry (record via baseline-change card)")
        continue
    b, a = want[bid], results[bid]
    pct = 100.0 * (a - b) / b if b else 0.0
    rows.append(f"  {bid}: Ir={a} vs {b} ({pct:+.2f}%)")
    if a * 100 > b * (100 + tol):
        fails.append(f"{bid}: +{pct:.2f}% exceeds +{tol}% (Ir {a} vs {b})")

print(f"ci-perf-gate: {key} vs baseline @ {entry.get('git_sha', '?')} (tol +{tol}%):")
print("\n".join(rows))
if fails:
    print("ci-perf-gate: FAIL")
    print("\n".join(f"  {f}" for f in fails))
    sys.exit(1)
print("ci-perf-gate: PASS")
PY
}

record() {
  local out="$1"
  BASELINE_FILE="$BASELINE" BASELINE_KEY="$KEY" ARCH="$ARCH" RUSTC="$RUSTC" \
    VALGRIND="$VALGRIND" GIT_SHA="$GIT_SHA" GATE_ROOT="$ROOT" python3 - "$out" <<'PY'
import json, os, subprocess, sys

path = sys.argv[1]
extractor = os.path.join(os.environ["GATE_ROOT"], "scripts", "ci-perf-gate-extract.py")
proc = subprocess.run(
    [sys.executable, extractor, path], capture_output=True, text=True
)
if proc.returncode != 0:
    print(proc.stderr.strip() or "ci-perf-gate: extractor failed", file=sys.stderr)
    sys.exit(1)
results = {}
for line in proc.stdout.splitlines():
    bid, ir = line.split("\t")
    results[bid] = int(ir)

base_path = os.environ["BASELINE_FILE"]
try:
    with open(base_path) as f:
        base = json.load(f)
except FileNotFoundError:
    base = {"tool": "iai-callgrind 0.14.2 (callgrind Ir, --allow-aslr)",
            "tolerance_pct": 1.0, "baselines": {}}
key = os.environ["BASELINE_KEY"]
old = base.get("baselines", {}).get(key, {}).get("benches", {})
base.setdefault("baselines", {})[key] = {
    "arch": os.environ["ARCH"], "rustc": os.environ["RUSTC"],
    "valgrind": os.environ["VALGRIND"], "git_sha": os.environ["GIT_SHA"],
    "benches": dict(sorted(results.items())),
}
with open(base_path, "w") as f:
    json.dump(base, f, indent=1, sort_keys=True)
    f.write("\n")
print(f"ci-perf-gate: recorded {len(results)} benches for {key} -> {base_path}")
for bid in sorted(set(old) | set(results)):
    o, n = old.get(bid), results.get(bid)
    if o is None:
        print(f"  {bid}: NEW Ir={n}")
    elif n is None:
        print(f"  {bid}: DROPPED (was {o})")
    elif o != n:
        print(f"  {bid}: {o} -> {n} ({100.0*(n-o)/o:+.2f}%)")
PY
}

INJECT_ANCHOR='pub fn encode_record_batch(buf: &mut BytesMut, batch: &RecordBatch) -> Result<()> {'
INJECT_BODY='    // ci-perf-gate self-test injection (reverted by trap).
    for r in &batch.records {
        let _ = std::hint::black_box(vec![r.timestamp.to_le_bytes()]);
    }'

selftest() {
  local tmp out log
  tmp="$(mktemp -d)"; out="$tmp/out.jsonl"; log="$tmp/run.log"
  # The self-test mutates sources mid-run; drop the local units'
  # artifacts before re-measuring so a stale bench binary can never
  # masquerade as the mutated (or reverted) tree. Done by direct removal:
  # `cargo clean -p` is observably unreliable for this manifest
  # arrangement (sometimes reports "Removed 0 files" with the bench
  # binary still present). Deps keep their artifacts, so this costs one
  # partitionline+codec rebuild, not a full dep build.
  force_rebuild() {
    local t="${CARGO_TARGET_DIR:-$CODEC/target}"
    rm -rf "$t"/release/.fingerprint/partitionline-* \
      "$t"/release/.fingerprint/codec-* \
      "$t"/release/.fingerprint/iai-* \
      "$t"/release/deps/partitionline-* \
      "$t"/release/deps/codec-* \
      "$t"/release/deps/iai-* \
      "$t"/release/build/partitionline-* \
      "$t"/release/build/codec-*
    if ls "$t"/release/deps/iai-* >/dev/null 2>&1; then
      echo "ci-perf-gate self-test: ABORT — stale bench binary survived" >&2
      exit 1
    fi
  }
  echo "ci-perf-gate self-test: 1/3 clean tree must PASS"
  measure "$out" "$log" || { echo "ci-perf-gate self-test: ABORT — clean run failed:"; tail -5 "$log"; rm -rf "$tmp"; exit 1; }
  compare "$out" || { echo "ci-perf-gate self-test: ABORT — clean tree fails (stale baseline?)"; rm -rf "$tmp"; exit 1; }

  echo "ci-perf-gate self-test: 2/3 injecting per-record slowdown"
  if ! grep -qF "$INJECT_ANCHOR" "$ROOT/src/protocol/records.rs"; then
    echo "ci-perf-gate self-test: ABORT — injection anchor moved" >&2; rm -rf "$tmp"; exit 1
  fi
  cp "$ROOT/src/protocol/records.rs" "$tmp/records.rs.bak"
  # shellcheck disable=SC2064
  trap "cp '$tmp/records.rs.bak' '$ROOT/src/protocol/records.rs.ciperfgate' && mv '$ROOT/src/protocol/records.rs.ciperfgate' '$ROOT/src/protocol/records.rs'; rm -rf '$tmp' '$GATE_TMP'" EXIT INT TERM
  python3 - "$ROOT/src/protocol/records.rs" <<PY
import os, sys
anchor = """$INJECT_ANCHOR"""
body = """$INJECT_BODY"""
path = sys.argv[1]
text = open(path).read()
assert text.count(anchor) == 1, "anchor not unique"
tmp = path + ".ciperfgate"
with open(tmp, "w") as f:
    f.write(text.replace(anchor, anchor + "\n" + body))
os.replace(tmp, path)  # atomic: no reader ever sees a truncated file
PY
  grep -q "self-test injection" "$ROOT/src/protocol/records.rs" \
    || { echo "ci-perf-gate self-test: ABORT — injection did not apply" >&2; exit 1; }
  force_rebuild
  if measure "$out" "$log" && compare "$out"; then
    echo "ci-perf-gate self-test: FAIL — injected slowdown NOT detected" >&2
    exit 1
  fi
  echo "ci-perf-gate self-test: injected slowdown detected"

  echo "ci-perf-gate self-test: 3/3 revert must PASS again"
  cp "$tmp/records.rs.bak" "$ROOT/src/protocol/records.rs.ciperfgate" \
    && mv "$ROOT/src/protocol/records.rs.ciperfgate" "$ROOT/src/protocol/records.rs"
  trap 'rm -rf "$GATE_TMP"' EXIT INT TERM
  if ! cmp -s "$tmp/records.rs.bak" "$ROOT/src/protocol/records.rs"; then
    echo "ci-perf-gate self-test: ABORT — revert mismatch" >&2; rm -rf "$tmp"; exit 1
  fi
  force_rebuild
  measure "$out" "$log" || { echo "ci-perf-gate self-test: FAIL — reverted run errored"; tail -5 "$log"; rm -rf "$tmp"; exit 1; }
  compare "$out" || { echo "ci-perf-gate self-test: FAIL — reverted tree fails"; rm -rf "$tmp"; exit 1; }
  rm -rf "$tmp"
  echo "ci-perf-gate self-test: OK (slowdown detected, revert green)"
}

case "$MODE" in
  run)
    measure "$GATE_TMP/out.jsonl" "$GATE_TMP/run.log" || { echo "ci-perf-gate: FAIL — bench run errored:"; tail -8 "$GATE_TMP/run.log"; exit 1; }
    compare "$GATE_TMP/out.jsonl"
    ;;
  record) record_mode() {
    measure "$GATE_TMP/out.jsonl" "$GATE_TMP/run.log" || { echo "ci-perf-gate: FAIL — bench run errored:"; tail -8 "$GATE_TMP/run.log"; exit 1; }
    record "$GATE_TMP/out.jsonl"
  }; record_mode ;;
  selftest) selftest ;;
esac
