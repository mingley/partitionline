#!/usr/bin/env bash
# Reproducible profile capture for a named cell or command (KL09-11).
#
# Captures CPU samples, a syscall summary, rusage (context switches, peak
# RSS, faults) and a heap profile, each by running the command again under a
# different locally installed tool detected at runtime. Every run lands in
# one directory with provenance, per-capture artifacts, checksums and a
# machine-readable summary. Any requested capture without a usable tool
# fails closed with "tool missing" instead of reporting partial success.
#
#   bash scripts/perf-profile.sh --cell NAME [--out DIR] [--only LIST]
#       [--sample-secs N] [--heap-every-ms N] [--heap-snaps N] -- CMD [ARGS...]
#   bash scripts/perf-profile.sh --self-test
#
# Tool matrix (first present wins):
#   cpu:      samply | perf record | sample (macOS)
#   syscalls: strace -c | dtruss -c (needs dtrace privileges on macOS)
#   rusage:   python3 wait4 wrapper (voluntary/involuntary ctx switches, max RSS)
#   heap:     valgrind --tool=massif | heap snapshots + RSS series (macOS)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CELL=""
OUT=""
ONLY="cpu,syscalls,rusage,heap"
SAMPLE_SECS=120
HEAP_EVERY_MS=500
HEAP_SNAPS=5

usage() {
    echo "usage: bash scripts/perf-profile.sh --cell NAME [--out DIR] [--only c1,c2]" >&2
    echo "           [--sample-secs N] [--heap-every-ms N] [--heap-snaps N] -- CMD [ARGS...]" >&2
    echo "       bash scripts/perf-profile.sh --self-test" >&2
    exit 2
}

die() {
    echo "perf-profile: $*" >&2
    exit 2
}

tool_missing() {
    echo "perf-profile: tool missing: $*" >&2
    exit 3
}

# --- tool detection (sets <NAME>_TOOL / <NAME>_VERSION or <NAME>_NEED) --------

detect_cpu() {
    if command -v samply >/dev/null 2>&1; then
        CPU_TOOL="samply"
        CPU_VERSION="$(samply --version 2>/dev/null | head -1)"
    elif command -v perf >/dev/null 2>&1 && perf script --help >/dev/null 2>&1; then
        CPU_TOOL="perf"
        CPU_VERSION="$(perf --version 2>/dev/null | head -1)"
    elif command -v sample >/dev/null 2>&1; then
        CPU_TOOL="sample"
        CPU_VERSION="macOS $(sw_vers -productVersion 2>/dev/null || echo unknown)"
    else
        CPU_TOOL=""
        CPU_NEED="cpu requires one of: samply, perf, sample"
    fi
}

detect_syscalls() {
    if command -v strace >/dev/null 2>&1; then
        SYSCALLS_TOOL="strace"
        SYSCALLS_VERSION="$(strace -V 2>&1 | head -1)"
    elif command -v dtruss >/dev/null 2>&1; then
        # dtruss needs dtrace privileges; probe cheaply before claiming it.
        if dtruss -c "$(command -v true)" >/dev/null 2>&1; then
            SYSCALLS_TOOL="dtruss"
            SYSCALLS_VERSION="macOS $(sw_vers -productVersion 2>/dev/null || echo unknown)"
        else
            SYSCALLS_TOOL=""
            SYSCALLS_NEED="syscalls requires one of: strace, privileged dtruss (dtrace denied)"
        fi
    else
        SYSCALLS_TOOL=""
        SYSCALLS_NEED="syscalls requires one of: strace, dtruss"
    fi
}

detect_rusage() {
    if command -v python3 >/dev/null 2>&1; then
        RUSAGE_TOOL="python3-wait4"
        RUSAGE_VERSION="$(python3 --version 2>&1 | head -1)"
    else
        RUSAGE_TOOL=""
        RUSAGE_NEED="rusage requires one of: python3"
    fi
}

detect_heap() {
    if command -v valgrind >/dev/null 2>&1; then
        HEAP_TOOL="massif"
        HEAP_VERSION="$(valgrind --version 2>/dev/null | head -1)"
    elif command -v heap >/dev/null 2>&1 && [ "$(uname -s)" = "Darwin" ]; then
        HEAP_TOOL="heapshots"
        HEAP_VERSION="macOS $(sw_vers -productVersion 2>/dev/null || echo unknown)"
    else
        HEAP_TOOL=""
        HEAP_NEED="heap requires one of: valgrind, heap (macOS)"
    fi
}

detect_sha() {
    if command -v sha256sum >/dev/null 2>&1; then
        SHA="sha256sum"
    elif command -v shasum >/dev/null 2>&1; then
        SHA="shasum -a 256"
    else
        SHA=""
    fi
}

# --- capture runners (each runs CMD once; echo artifact paths) ----------------

run_cpu_samply() {
    samply record --save-only -o "$RUN_DIR/cpu-samply.json.gz" -- "$@" \
        >"$RUN_DIR/cpu-stdout.log" 2>"$RUN_DIR/cpu-stderr.log"
}

run_cpu_perf() {
    perf record -F 99 -g -o "$RUN_DIR/cpu-perf.data" -- "$@" \
        >"$RUN_DIR/cpu-stdout.log" 2>"$RUN_DIR/cpu-stderr.log"
    local rc=$?
    [ $rc -ne 0 ] && return $rc
    perf script -i "$RUN_DIR/cpu-perf.data" >"$RUN_DIR/cpu-perf.script" 2>/dev/null
    if command -v stackcollapse-perf.pl >/dev/null 2>&1; then
        stackcollapse-perf.pl "$RUN_DIR/cpu-perf.script" >"$RUN_DIR/cpu.folded"
        if command -v flamegraph.pl >/dev/null 2>&1; then
            flamegraph.pl "$RUN_DIR/cpu.folded" >"$RUN_DIR/cpu-flamegraph.svg"
        fi
    fi
    return 0
}

run_cpu_sample() {
    "$@" >"$RUN_DIR/cpu-stdout.log" 2>"$RUN_DIR/cpu-stderr.log" &
    local pid=$!
    # -mayDie ends sampling when the target exits, so SAMPLE_SECS is a cap.
    sample "$pid" "$SAMPLE_SECS" -file "$RUN_DIR/cpu-sample.txt" -mayDie >/dev/null 2>&1
    wait "$pid"
}

run_syscalls_strace() {
    strace -c -o "$RUN_DIR/syscalls-strace.txt" "$@" \
        >"$RUN_DIR/syscalls-stdout.log" 2>"$RUN_DIR/syscalls-stderr.log"
}

run_syscalls_dtruss() {
    # dtrace -c summary goes to stderr; keep the workload's stdout apart.
    dtruss -c "$@" >"$RUN_DIR/syscalls-stdout.log" 2>"$RUN_DIR/syscalls-dtruss.txt"
}

run_rusage_py() {
    RUSAGE_OUT="$RUN_DIR/rusage.json" python3 - "$@" <<'EOF' \
        >"$RUN_DIR/rusage-stdout.log" 2>"$RUN_DIR/rusage-stderr.log"
import json, os, resource, sys, time
cmd = sys.argv[1:]
out_path = os.environ["RUSAGE_OUT"]
t0 = time.time()
pid = os.fork()
if pid == 0:
    try:
        os.execvp(cmd[0], cmd)
    except OSError as e:
        sys.stderr.write("exec failed: %s\n" % e)
        os._exit(127)
_, status, ru = os.wait4(pid, 0)
wall = time.time() - t0
if sys.platform.startswith("linux"):
    maxrss_kb = ru.ru_maxrss
else:  # macOS reports bytes
    maxrss_kb = ru.ru_maxrss // 1024
doc = {
    "exit_code": os.waitstatus_to_exitcode(status),
    "wall_s": wall,
    "user_s": ru.ru_utime,
    "sys_s": ru.ru_stime,
    "max_rss_kb": maxrss_kb,
    "voluntary_ctx_switches": ru.ru_nvcsw,
    "involuntary_ctx_switches": ru.ru_nivcsw,
    "minor_faults": ru.ru_minflt,
    "major_faults": ru.ru_majflt,
}
with open(out_path, "w") as f:
    json.dump(doc, f, indent=1)
    f.write("\n")
sys.exit(doc["exit_code"])
EOF
}

run_heap_massif() {
    valgrind --tool=massif --massif-out-file="$RUN_DIR/heap-massif.out" --stacks=yes "$@" \
        >"$RUN_DIR/heap-stdout.log" 2>"$RUN_DIR/heap-stderr.log"
    local rc=$?
    [ $rc -ne 0 ] && return $rc
    if command -v ms_print >/dev/null 2>&1; then
        ms_print "$RUN_DIR/heap-massif.out" >"$RUN_DIR/heap-msprint.txt" 2>/dev/null || true
    fi
    return 0
}

run_heap_heapshots() {
    "$@" >"$RUN_DIR/heap-stdout.log" 2>"$RUN_DIR/heap-stderr.log" &
    local pid=$!
    local snaps=0
    : >"$RUN_DIR/heap-rss-kb.txt"
    while kill -0 "$pid" 2>/dev/null; do
        ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' >>"$RUN_DIR/heap-rss-kb.txt" || true
        if [ "$snaps" -lt "$HEAP_SNAPS" ]; then
            heap -s "$pid" >"$RUN_DIR/heap-snap-$snaps.txt" 2>/dev/null || true
            snaps=$((snaps + 1))
        fi
        sleep "$(awk "BEGIN {print $HEAP_EVERY_MS/1000}")" 2>/dev/null || sleep 1
    done
    wait "$pid"
}

# --- main capture -------------------------------------------------------------

main_capture() {
    case "$CELL" in
        ""|*[!A-Za-z0-9_.-]*)
            die "cell must match [A-Za-z0-9_.-]+ (got '$CELL')" ;;
    esac
    [ "$#" -gt 0 ] || die "no command after --"
    detect_sha
    [ -z "$SHA" ] && tool_missing "checksums requires one of: sha256sum, shasum"

    local stamp sha
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    if [ -z "$OUT" ]; then
        RUN_DIR="profiles/$CELL-$stamp-$sha"
    else
        RUN_DIR="$OUT"
    fi
    if [ -e "$RUN_DIR" ] && [ -n "$(ls -A "$RUN_DIR" 2>/dev/null)" ]; then
        die "refusing to write into non-empty $RUN_DIR"
    fi
    mkdir -p "$RUN_DIR"

    # Debug symbols without changing benchmarked codegen (KL09-11).
    export CARGO_PROFILE_RELEASE_DEBUG=true

    local IFS=,
    # shellcheck disable=SC2206
    local wanted=($ONLY)
    unset IFS
    local overall_rc=0
    local status="ok"
    local ran_any=0
    for cap in "${wanted[@]}"; do
        case "$cap" in
            cpu)
                detect_cpu
                [ -z "$CPU_TOOL" ] && tool_missing "$CPU_NEED"
                set +e
                "run_cpu_${CPU_TOOL}" "$@"
                overall_rc=$?
                set -e
                ran_any=1
                ;;
            syscalls)
                detect_syscalls
                [ -z "$SYSCALLS_TOOL" ] && tool_missing "$SYSCALLS_NEED"
                set +e
                "run_syscalls_${SYSCALLS_TOOL}" "$@"
                overall_rc=$?
                set -e
                ran_any=1
                ;;
            rusage)
                detect_rusage
                [ -z "$RUSAGE_TOOL" ] && tool_missing "$RUSAGE_NEED"
                set +e
                run_rusage_py "$@"
                overall_rc=$?
                set -e
                ran_any=1
                ;;
            heap)
                detect_heap
                [ -z "$HEAP_TOOL" ] && tool_missing "$HEAP_NEED"
                set +e
                "run_heap_${HEAP_TOOL}" "$@"
                overall_rc=$?
                set -e
                ran_any=1
                ;;
            *)
                die "unknown capture '$cap' (want cpu,syscalls,rusage,heap)" ;;
        esac
        # A failing workload invalidates later repetitions: stop scheduling.
        if [ "$overall_rc" -ne 0 ]; then
            status="workload_failed"
            break
        fi
    done
    [ "$ran_any" -eq 1 ] || die "--only selected no captures"

    write_provenance_and_summary "$status" "$overall_rc" "$@"

    # Deterministic checksums over every artifact in the run directory.
    (cd "$RUN_DIR" && rm -f checksums.sha256 && for f in *; do
        [ "$f" = "checksums.sha256" ] && continue
        [ -f "$f" ] || continue
        $SHA "$f" >>checksums.sha256
    done)

    if [ "$overall_rc" -ne 0 ]; then
        echo "perf-profile: workload exited $overall_rc (artifacts kept, no success claimed)" >&2
        exit "$overall_rc"
    fi
    echo "perf-profile: $RUN_DIR"
}

write_provenance_and_summary() {
    local status="$1" rc="$2"
    shift 2
    # argv travels null-delimited so arguments with spaces survive exactly.
    local argv_json
    argv_json="$(printf '%s\0' "$@" | python3 -c 'import json, sys; print(json.dumps(sys.stdin.buffer.read().decode().split("\0")[:-1]))')"
    PROVENANCE_ARGV="$argv_json" RUN_DIR="$RUN_DIR" CELL="$CELL" STATUS="$status" RC="$rc" \
    CPU_TOOL="${CPU_TOOL:-}" CPU_VERSION="${CPU_VERSION:-}" \
    SYSCALLS_TOOL="${SYSCALLS_TOOL:-}" SYSCALLS_VERSION="${SYSCALLS_VERSION:-}" \
    RUSAGE_TOOL="${RUSAGE_TOOL:-}" RUSAGE_VERSION="${RUSAGE_VERSION:-}" \
    HEAP_TOOL="${HEAP_TOOL:-}" HEAP_VERSION="${HEAP_VERSION:-}" \
    ONLY="$ONLY" SAMPLE_SECS="$SAMPLE_SECS" \
    python3 - <<'EOF'
import json, os, platform, subprocess, sys

run_dir = os.environ["RUN_DIR"]

def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=15).stdout.strip()
    except Exception:
        return ""

cpu_model = ""
if sys.platform == "darwin":
    cpu_model = sh(["sysctl", "-n", "machdep.cpu.brand_string"])
else:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu_model = line.split(":", 1)[1].strip()
                    break
    except OSError:
        pass

knobs = {}
for k, v in os.environ.items():
    if k.startswith("KAFKA_") or k in (
        "PAYLOAD_BYTES", "WARMUP_SECS", "MEASURE_SECS", "LINGER_MS", "ACKS",
        "COUNT", "CONNECTIONS", "MAX_IN_FLIGHT", "COMPRESSION", "IDEMPOTENT",
        "MODE", "MAX_WAIT_MS", "MAX_BYTES", "MIN_BYTES",
    ) or k.startswith("CARGO_PROFILE_") or k in ("RUSTFLAGS", "RUSTC_BOOTSTRAP"):
        knobs[k] = v

provenance = {
    "cell": os.environ["CELL"],
    "run_dir": run_dir,
    "captures_requested": os.environ["ONLY"].split(","),
    "command": json.loads(os.environ.get("PROVENANCE_ARGV", "[]")),
    "env_knobs": knobs,
    "cargo_profile_release_debug": os.environ.get("CARGO_PROFILE_RELEASE_DEBUG"),
    "git_sha": sh(["git", "rev-parse", "HEAD"]) or "unknown",
    "git_dirty": bool(sh(["git", "status", "--porcelain"])),
    "rustc": sh(["rustc", "--version"]),
    "cargo": sh(["cargo", "--version"]),
    "python": sh(["python3", "--version"]) or sh(["python3", "-V"]),
    "host": platform.uname()._asdict(),
    "cpu_model": cpu_model,
    "os": "%s %s %s" % (platform.system(), platform.release(), platform.machine()),
    "tools": {
        "cpu": {"tool": os.environ.get("CPU_TOOL") or None,
                "version": os.environ.get("CPU_VERSION") or None},
        "syscalls": {"tool": os.environ.get("SYSCALLS_TOOL") or None,
                     "version": os.environ.get("SYSCALLS_VERSION") or None},
        "rusage": {"tool": os.environ.get("RUSAGE_TOOL") or None,
                   "version": os.environ.get("RUSAGE_VERSION") or None},
        "heap": {"tool": os.environ.get("HEAP_TOOL") or None,
                 "version": os.environ.get("HEAP_VERSION") or None},
    },
    "sample_secs_cap": int(os.environ.get("SAMPLE_SECS", "0")),
}
with open(os.path.join(run_dir, "provenance.json"), "w") as f:
    json.dump(provenance, f, indent=1)
    f.write("\n")

def load(name):
    try:
        with open(os.path.join(run_dir, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None

def present(name):
    return os.path.exists(os.path.join(run_dir, name))

def thread_weights():
    # Top-level `N Thread_` weights from a `sample` call graph.
    weights = []
    try:
        with open(os.path.join(run_dir, "cpu-sample.txt")) as f:
            for line in f:
                s = line.strip()
                if s and s[0].isdigit() and " Thread_" in s:
                    weights.append(int(s.split()[0]))
    except OSError:
        pass
    return weights

def heap_peak():
    peak = 0
    try:
        with open(os.path.join(run_dir, "heap-rss-kb.txt")) as f:
            for line in f:
                try:
                    peak = max(peak, int(line.strip()))
                except ValueError:
                    pass
    except OSError:
        pass
    return peak or None

def heap_snaps():
    return sorted(n for n in os.listdir(run_dir)
                  if n.startswith("heap-snap-") and n.endswith(".txt"))

results = {}
notes = []
req = os.environ["ONLY"].split(",")
if "cpu" in req:
    tool = os.environ.get("CPU_TOOL") or None
    entry = {"tool": tool, "artifacts": []}
    for a in ("cpu-samply.json.gz", "cpu-perf.data", "cpu-perf.script",
              "cpu-sample.txt", "cpu.folded", "cpu-flamegraph.svg"):
        if present(a):
            entry["artifacts"].append(a)
    if tool == "sample":
        entry["thread_sample_weights"] = thread_weights()
    if not present("cpu.folded") and not present("cpu-samply.json.gz"):
        notes.append("no folded stacks: flamegraph.svg needs samply or "
                     "stackcollapse+flamegraph.pl (Linux perf path)")
    results["cpu"] = entry
if "syscalls" in req:
    entry = {"tool": os.environ.get("SYSCALLS_TOOL") or None, "artifacts": []}
    for a in ("syscalls-strace.txt", "syscalls-dtruss.txt"):
        if present(a):
            entry["artifacts"].append(a)
    results["syscalls"] = entry
if "rusage" in req:
    entry = {"tool": os.environ.get("RUSAGE_TOOL") or None,
             "artifacts": ["rusage.json"] if present("rusage.json") else [],
             "rusage": load("rusage.json")}
    results["rusage"] = entry
if "heap" in req:
    tool = os.environ.get("HEAP_TOOL") or None
    entry = {"tool": tool, "artifacts": []}
    for a in ("heap-massif.out", "heap-msprint.txt", "heap-rss-kb.txt"):
        if present(a):
            entry["artifacts"].append(a)
    if tool == "heapshots":
        entry["snapshots"] = heap_snaps()
        entry["peak_rss_kb"] = heap_peak()
        if not entry["snapshots"]:
            notes.append("no heap snapshots: workload exited before the "
                         "first poll; peak RSS still in rusage.json")
    results["heap"] = entry

summary = dict(provenance)
summary["status"] = os.environ["STATUS"]
summary["workload_exit"] = int(os.environ["RC"])
summary["results"] = results
summary["notes"] = notes
with open(os.path.join(run_dir, "summary.json"), "w") as f:
    json.dump(summary, f, indent=1)
    f.write("\n")
EOF
}

# --- self-test ---------------------------------------------------------------

self_test() {
    local failures=0
    note() { echo "self-test: $*"; }
    fail() { echo "self-test FAIL: $*"; failures=$((failures + 1)); }

    note "tool detection (informational; missing tools fail captures, not this test)"
    detect_cpu; detect_syscalls; detect_rusage; detect_heap; detect_sha
    note "cpu=${CPU_TOOL:-MISSING} syscalls=${SYSCALLS_TOOL:-MISSING} rusage=${RUSAGE_TOOL:-MISSING} heap=${HEAP_TOOL:-MISSING} sha=${SHA:-MISSING}"
    [ -z "$SHA" ] && fail "no sha256 tool"

    # Fail-closed: a requested capture without a tool exits 3. Where the
    # tool exists, the capture must instead succeed with artifacts.
    for cap in cpu syscalls rusage heap; do
        local tmp
        tmp="$(mktemp -d)"
        # cpu sampling must attach before the target exits, so give it a
        # live process; the instant commands exercise the short-lived path.
        local probe=(/bin/echo hello)
        [ "$cap" = "cpu" ] && probe=(/bin/sleep 2)
        if bash "$ROOT/scripts/perf-profile.sh" --cell "selftest-$cap" --out "$tmp/run" \
                --only "$cap" --sample-secs 3 -- "${probe[@]}" >/dev/null 2>"$tmp/err"; then
            case "$cap" in
                cpu) [ -f "$tmp/run/cpu-sample.txt" ] || [ -f "$tmp/run/cpu-perf.data" ] || [ -f "$tmp/run/cpu-samply.json.gz" ] || fail "$cap run left no cpu artifact" ;;
                syscalls) [ -f "$tmp/run/syscalls-strace.txt" ] || [ -f "$tmp/run/syscalls-dtruss.txt" ] || fail "$cap run left no syscalls artifact" ;;
                rusage) [ -f "$tmp/run/rusage.json" ] || fail "$cap run left no rusage.json" ;;
                heap) [ -f "$tmp/run/heap-massif.out" ] || [ -f "$tmp/run/heap-rss-kb.txt" ] || fail "$cap run left no heap artifact" ;;
            esac
            [ -f "$tmp/run/summary.json" ] || fail "$cap run left no summary.json"
            note "$cap: tool present, capture ok"
        else
            local rc=$?
            if [ "$rc" -eq 3 ] && grep -q "tool missing" "$tmp/err"; then
                note "$cap: fails closed as designed"
            else
                fail "$cap: exit $rc, expected 0 with artifacts or 3 'tool missing'"
            fi
        fi
        rm -rf "$tmp"
    done

    # End-to-end rusage capture: provenance, summary, checksums validate.
    local tmp
    tmp="$(mktemp -d)"
    if [ -n "$RUSAGE_TOOL" ]; then
        bash "$ROOT/scripts/perf-profile.sh" --cell selftest-e2e --out "$tmp/run" \
            --only rusage -- /bin/echo hello >/dev/null 2>&1 \
            || fail "rusage end-to-end run failed"
        python3 - "$tmp/run" <<'EOF' || fail "summary/provenance shape"
import json, os, sys
run = sys.argv[1]
prov = json.load(open(os.path.join(run, "provenance.json")))
summ = json.load(open(os.path.join(run, "summary.json")))
assert prov["cell"] == "selftest-e2e", prov["cell"]
assert prov["command"] == ["/bin/echo", "hello"], prov["command"]
assert prov["cargo_profile_release_debug"] == "true"
assert prov["git_sha"], "missing git sha"
assert prov["cpu_model"], "missing cpu model"
assert summ["status"] == "ok", summ["status"]
assert summ["results"]["rusage"]["rusage"]["exit_code"] == 0
assert summ["results"]["rusage"]["rusage"]["voluntary_ctx_switches"] >= 0
EOF
        (cd "$tmp/run" && $SHA -c checksums.sha256 >/dev/null 2>&1) \
            || fail "checksums.sha256 does not validate"
    else
        note "rusage tool missing; skipping end-to-end shape check"
    fi

    # Bad inputs fail fast with usage.
    bash "$ROOT/scripts/perf-profile.sh" --cell 'a/b' --only rusage -- /bin/echo hi \
        >/dev/null 2>&1 && fail "bad cell name accepted"
    mkdir -p "$tmp/nonempty" && touch "$tmp/nonempty/x"
    bash "$ROOT/scripts/perf-profile.sh" --cell x --out "$tmp/nonempty" --only rusage \
        -- /bin/echo hi >/dev/null 2>&1 && fail "non-empty out dir accepted"
    bash "$ROOT/scripts/perf-profile.sh" --cell x --only bogus -- /bin/echo hi \
        >/dev/null 2>&1 && fail "bogus capture accepted"
    rm -rf "$tmp"

    if [ "$failures" -eq 0 ]; then
        note "PASS"
    else
        echo "self-test: $failures failure(s)" >&2
        exit 1
    fi
}

# --- args --------------------------------------------------------------------

if [ "${1:-}" = "--self-test" ]; then
    self_test
    exit 0
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        --cell) CELL="${2:?--cell needs a value}"; shift 2 ;;
        --out) OUT="${2:?--out needs a value}"; shift 2 ;;
        --only) ONLY="${2:?--only needs a value}"; shift 2 ;;
        --sample-secs) SAMPLE_SECS="${2:?--sample-secs needs a value}"; shift 2 ;;
        --heap-every-ms) HEAP_EVERY_MS="${2:?--heap-every-ms needs a value}"; shift 2 ;;
        --heap-snaps) HEAP_SNAPS="${2:?--heap-snaps needs a value}"; shift 2 ;;
        --) shift; break ;;
        -h|--help) usage ;;
        *) usage ;;
    esac
done

[ -n "$CELL" ] || usage
main_capture "$@"
