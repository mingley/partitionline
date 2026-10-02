#!/usr/bin/env bash
# Record-history integrity gate. Raw logs/journals and failed verdicts are retained.
# COUNT=8000000 PARTITIONS=6 RUNS=3 bash scripts/lab-a-integrity.sh
# This unsigned correctness harness never lifts Suite HOLD.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lab-a-common.sh
source "$ROOT/scripts/lab-a-common.sh"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench-integrity}}"
COUNT="${COUNT:-5000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
WARMUP_SECS="${WARMUP_SECS:-0}"
MEASURE_SECS="${MEASURE_SECS:-0}"
RUNS="${RUNS:-1}"
PARTITIONS="${PARTITIONS:-1}"
SEED="${SEED:-1592590337}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"
LAB_A_LABEL="lab-a-integrity"
export COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS MEASURE_SECS RUNS PARTITIONS SEED
export KAFKA_BOOTSTRAP="$BOOTSTRAP" KAFKA_TOPIC="$TOPIC"
python3 scripts/bench-record-history.py --validate-env
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/work/lab-a-integrity-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
mkdir -p "$(dirname "$ARTIFACT_DIR")"
mkdir "$ARTIFACT_DIR" # Never replace a prior attempt, including a failed one.
finish() {
  local status=$?
  python3 - "$ARTIFACT_DIR" "$status" <<'PY'
import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1]); status=int(sys.argv[2])
with (root/'run-status.json').open('x') as out:
 json.dump({'exit_status':status,'integrity_verified':status==0,'performance_claims_invalidated':status!=0,'qualification':False,'suite_hold':'active'},out,indent=2); out.write('\n')
with (root/'SHA256SUMS').open('x') as out:
 for path in sorted(root.rglob('*')):
  if path.is_file() and path.name!='SHA256SUMS': out.write(hashlib.sha256(path.read_bytes()).hexdigest()+'  '+str(path.relative_to(root))+'\n')
PY
  echo "lab-a-integrity: raw artifacts retained at $ARTIFACT_DIR (exit=$status)"
}
trap finish EXIT
python3 - "$ARTIFACT_DIR/settings.json" <<'PY'
import json,os,sys
keys=['KAFKA_BOOTSTRAP','KAFKA_TOPIC','COUNT','PAYLOAD_BYTES','ACKS','LINGER_MS','WARMUP_SECS','RUNS','PARTITIONS','SEED','IDEMPOTENT']
with open(sys.argv[1],'x') as out: json.dump({key:os.environ.get(key) for key in keys},out,indent=2); out.write('\n')
PY
BROKER_BACKEND="${BROKER_BACKEND:-auto}"
if [[ "$BROKER_BACKEND" == auto && ( -n "${KAFKA_HOME:-}" || "$BOOTSTRAP" != 127.0.0.1:9092 ) ]]; then BROKER_BACKEND=native; fi
if [[ "$BROKER_BACKEND" == native ]]; then
  topics_bin="$(lab_a_find_kafka_bin kafka-topics.sh)" || { echo 'native Kafka tools required' >&2; exit 1; }
  "$topics_bin" --bootstrap-server "$BOOTSTRAP" --list > "$ARTIFACT_DIR/broker-topics.log" 2>&1
  lab_a_using_docker_broker() { return 1; }
elif [[ "$BROKER_BACKEND" == auto || "$BROKER_BACKEND" == docker ]]; then
  [[ "$BOOTSTRAP" == 127.0.0.1:9092 || "$BOOTSTRAP" == localhost:9092 ]] || { echo 'Docker backend requires its 9092 bootstrap' >&2; exit 2; }
  lab_a_prepare_broker || exit 1
else
  echo 'BROKER_BACKEND must be auto|native|docker' >&2; exit 2
fi
if [[ -z "${BENCH_PRODUCE_BINARY:-}" || -z "${BENCH_FETCH_BINARY:-}" ]]; then
  cargo build --release --example bench_produce --example bench_fetch > "$ARTIFACT_DIR/build.log" 2>&1
fi
produce_binary="${BENCH_PRODUCE_BINARY:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/bench_produce}"
fetch_binary="${BENCH_FETCH_BINARY:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/bench_fetch}"
[[ -x "$produce_binary" && -x "$fetch_binary" ]] || { echo 'benchmark binaries must be executable' >&2; exit 2; }
echo "Lab A integrity: COUNT=$COUNT PARTITIONS=$PARTITIONS RUNS=$RUNS topic=$TOPIC"
echo 'Gate: complete unique IDs, SHA-256 payloads, per-key order, independent receipt. Unsigned; Suite HOLD stays active.'
for ((i=1; i<=RUNS; i++)); do
  run="$ARTIFACT_DIR/run-$i"
  mkdir "$run"
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then lab_a_reset_topic > "$run/topic-reset.log" 2>&1; fi
  before_hw="$(lab_a_hw_sum)"
  [[ "$before_hw" == 0 ]] || { echo 'record-history harness requires an empty fresh topic' >&2; exit 1; }
  printf '%s\n' "$before_hw" > "$run/hw-before.txt"
  produce_status=0
  RECORD_HISTORY="$run/producer.jsonl" "$produce_binary" > "$run/produce.stdout.log" 2> "$run/produce.stderr.log" || produce_status=$?
  printf '%s\n' "$produce_status" > "$run/produce.exit-status.txt"
  hw="$(lab_a_hw_sum)"
  printf '%s\n' "$hw" > "$run/hw-after.txt"
  acked="$(python3 - "$run/produce.stdout.log" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
if not rows: raise SystemExit('produce JSON missing')
row=rows[-1]
if row.get('acks') not in (1,-1) or row.get('run_disposition')!='executed': raise SystemExit('producer not acknowledged or failed')
print(row['acked'])
PY
)"
  [[ "$produce_status" == 0 && "$acked" == "$COUNT" ]] || { echo 'producer command/count failed; raw attempt retained' >&2; exit 1; }
  # This producer is explicitly non-transactional; HW is an additional audit.
  # Application counts and histories never derive from offsets. Transactional
  # control offsets are validated as gaps by the history adapter, not records.
  [[ "$((hw-before_hw))" == "$acked" ]] || { echo 'non-transactional HW audit failed; raw attempt retained' >&2; exit 1; }
  fetch_status=0
  RECORD_HISTORY="$run/consumer.jsonl" VERIFY=0 "$fetch_binary" > "$run/fetch.stdout.log" 2> "$run/fetch.stderr.log" || fetch_status=$?
  printf '%s\n' "$fetch_status" > "$run/fetch.exit-status.txt"
  checker_status=0
  python3 scripts/bench-record-history.py --producer "$run/producer.jsonl" --consumer "$run/consumer.jsonl" \
    --history "$run/history.json" --output "$run/verdict.json" > "$run/checker.stdout.log" 2> "$run/checker.stderr.log" || checker_status=$?
  cat "$run/checker.stdout.log"
  [[ "$fetch_status" == 0 && "$checker_status" == 0 ]] || { echo 'record-history integrity failed; performance claims invalidated and raw attempt retained' >&2; exit 1; }
  echo "lab-a-integrity: run $i verified every ID/hash/order; application records=$COUNT, non-transactional HW delta=$((hw-before_hw))"
done
echo "lab-a-integrity: ok — $RUNS complete record-history run(s). Unsigned; Suite HOLD stays active."
