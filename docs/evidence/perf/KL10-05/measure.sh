#!/usr/bin/env bash
# KL10-05 gzip backend A/B on one host.
# Arms: P = parent tree (miniz_oxide, flate2 without runtime_detection)
#       M = candidate tree --no-default-features (miniz_oxide + runtime_detection)
#       Z = candidate tree default features (zlib-rs + runtime_detection)
# usage: measure.sh PARENT_TREE CANDIDATE_TREE OUT_DIR [iai] [criterion] [census]
set -euo pipefail
PARENT=$1 CAND=$2 OUT=$3; shift 3
STEPS=${*:-"census iai criterion"}
REPS=${REPS:-5}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
TROOT=${TROOT:-$OUT/targets}

tree() { case $1 in P) echo "$PARENT" ;; *) echo "$CAND" ;; esac; }
flags() { case $1 in M) echo "--no-default-features" ;; *) echo "" ;; esac; }
manifest() { echo "$(tree "$1")/benchmarks/codec/Cargo.toml"; }

{
  echo "date_utc=$(date -u +%FT%TZ)"
  uname -sm
  rustc -Vv
  (lscpu 2>/dev/null | grep -E '^(Model name|CPU\(s\)|Flags)' | cut -c1-400) || sysctl -n machdep.cpu.brand_string hw.ncpu 2>/dev/null || true
  (valgrind --version 2>/dev/null; iai-callgrind-runner --version 2>/dev/null) || true
  echo "parent=$(git -C "$PARENT" rev-parse HEAD) candidate=$(git -C "$CAND" rev-parse HEAD)"
} > "$OUT/host.txt"

for a in P M Z; do
  export CARGO_TARGET_DIR=$TROOT/$a
  # shellcheck disable=SC2046
  if [[ " $STEPS " == *" census "* ]]; then
    cargo run -q --release --locked --manifest-path "$(manifest $a)" $(flags $a) --bin json1k-census > "$OUT/census-$a.json"
  fi
  if [[ " $STEPS " == *" iai "* ]]; then
    for b in iai json1k_iai; do
      (cd "$(tree $a)" && cargo bench -q --locked --manifest-path "$(manifest $a)" $(flags $a) --bench $b \
        -- --allow-aslr --output-format=json > "$OUT/$b-$a.json" 2> "$OUT/$b-$a.log") || echo "iai $b $a failed" >> "$OUT/runlog.txt"
      python3 "$PARENT/scripts/ci-perf-gate-extract.py" "$OUT/$b-$a.json" > "$OUT/$b-$a.tsv" || echo "extract $b $a failed" >> "$OUT/runlog.txt"
    done
  fi
  if [[ " $STEPS " == *" criterion "* ]]; then
    cargo bench -q --locked --manifest-path "$(manifest $a)" $(flags $a) --bench codec --no-run 2> "$OUT/build-$a.log"
  fi
done
unset CARGO_TARGET_DIR

if [[ " $STEPS " == *" criterion "* ]]; then
  declare -A BIN
  for a in P M Z; do
    BIN[$a]=$(ls -t "$TROOT/$a"/release/deps/codec-* | grep -v '\.d$' | head -1)
  done
  mkdir -p "$OUT/crit"
  for r in $(seq 1 "$REPS"); do
    order="P M Z"; (( r % 2 == 0 )) && order="Z M P"
    for a in $order; do
      echo "rep $r arm $a $(date -u +%T) load $(uptime | sed 's/.*average[s]*: //')" >> "$OUT/runlog.txt"
      (cd "$(tree $a)/benchmarks/codec" && CRITERION_HOME=$OUT/crit/$a-r$r "${BIN[$a]}" --bench gzip \
        --measurement-time 1 --warm-up-time 0.5 --sample-size 20 --nresamples 10000 --noplot \
        > "$OUT/crit/$a-r$r.out" 2>&1) || echo "criterion failed $a r$r" >> "$OUT/runlog.txt"
    done
  done
fi
echo "done $(date -u +%T)" >> "$OUT/runlog.txt"
