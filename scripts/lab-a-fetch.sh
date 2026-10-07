#!/usr/bin/env bash
# Seed a fresh topic, audit offsets, and fetch with optional record verification.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source "$ROOT/scripts/lab-a-common.sh"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench-fetch}}"
COUNT="${COUNT:-50000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
PARTITIONS="${PARTITIONS:-1}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"
LAB_A_LABEL=lab-a-fetch
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/work/lab-a-fetch-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
VERIFY_HISTORY="${VERIFY_HISTORY:-1}"
[[ "$VERIFY_HISTORY" == 0 || "$VERIFY_HISTORY" == 1 ]] || { echo 'VERIFY_HISTORY must be 0 or 1' >&2; exit 2; }
[[ "$ACKS" == 1 || "$ACKS" == -1 ]] || { echo 'HW audit requires ACKS=1 or -1' >&2; exit 2; }
[[ "$PARTITIONS" =~ ^[1-9][0-9]*$ ]] || { echo 'PARTITIONS must be positive' >&2; exit 2; }
mkdir "$ARTIFACT_DIR" || { echo 'artifact directory must be new' >&2; exit 2; }
export KAFKA_BOOTSTRAP="$BOOTSTRAP" KAFKA_TOPIC="$TOPIC" COUNT PAYLOAD_BYTES ACKS LINGER_MS PARTITIONS
export SEED="${SEED:-1592590337}" WARMUP="${WARMUP:-0}"
export FETCH_MODE="${FETCH_MODE:-manual}" GROUP_ID="${GROUP_ID:-plbench-fetch-$(date -u +%s)-$$}"
# The producer seeds COUNT total records. WARMUP applies only to fetch timing.
export WARMUP_SECS=0 MEASURE_SECS=0
cargo +stable build --locked --release --example bench_produce --example bench_fetch
produce_binary="${BENCH_PRODUCE_BINARY:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/bench_produce}"
fetch_binary="${BENCH_FETCH_BINARY:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/bench_fetch}"
produce_history=()
fetch_history=()
if [[ "$VERIFY_HISTORY" == 1 ]]; then
  produce_history=("RECORD_HISTORY=$ARTIFACT_DIR/producer.jsonl")
  fetch_history=("RECORD_HISTORY=$ARTIFACT_DIR/consumer.jsonl")
fi
env -u KEY_MODE -u PAYLOAD_MODE WARMUP=0 "${produce_history[@]}" "$produce_binary" --print-config > "$ARTIFACT_DIR/produce.settings.json"
env VERIFY=0 "${fetch_history[@]}" "$fetch_binary" --print-config > "$ARTIFACT_DIR/fetch.settings.json"
TIME_BINARY="${TIME_BINARY:-/usr/bin/time}"
[[ -x "$TIME_BINARY" ]] || { echo 'GNU time required (TIME_BINARY)' >&2; exit 2; }
BROKER_BACKEND="${BROKER_BACKEND:-auto}"
if [[ "$BROKER_BACKEND" == auto && ( -n "${KAFKA_HOME:-}" || "$BOOTSTRAP" != 127.0.0.1:9092 ) ]]; then BROKER_BACKEND=native; fi
if [[ "$BROKER_BACKEND" == native ]]; then
  topics_bin="$(lab_a_find_kafka_bin kafka-topics.sh)" || { echo 'native Kafka tools required' >&2; exit 1; }
  "$topics_bin" --bootstrap-server "$BOOTSTRAP" --list >/dev/null
  lab_a_using_docker_broker() { return 1; }
elif [[ "$BROKER_BACKEND" == auto || "$BROKER_BACKEND" == docker ]]; then
  [[ "$BOOTSTRAP" == 127.0.0.1:9092 || "$BOOTSTRAP" == localhost:9092 ]] || { echo 'Docker backend requires its 9092 bootstrap' >&2; exit 2; }
  lab_a_prepare_broker || exit 1
else
  echo 'BROKER_BACKEND must be auto|native|docker' >&2; exit 2
fi
if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then lab_a_reset_topic || exit 1; fi
before_hw="$(lab_a_hw_sum)"
[[ "$before_hw" == 0 ]] || { echo 'expected a fresh empty topic' >&2; exit 1; }
produce_status=0
"$TIME_BINARY" -v -o "$ARTIFACT_DIR/produce.resources.log" env -u KEY_MODE -u PAYLOAD_MODE WARMUP=0 "${produce_history[@]}" "$produce_binary" > "$ARTIFACT_DIR/produce.stdout.log" 2> "$ARTIFACT_DIR/produce.stderr.log" || produce_status=$?
printf '%s\n' "$produce_status" > "$ARTIFACT_DIR/produce.exit-status.txt"
after_hw="$(lab_a_hw_sum)"
acked="$(python3 - "$ARTIFACT_DIR/produce.stdout.log" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
print(rows[-1]['acked'] if rows else -1)
PY
)"
python3 - "$ARTIFACT_DIR/hw.json" "$before_hw" "$after_hw" "$acked" "$COUNT" "$produce_status" <<'PY'
import json,sys
before,after,acked,count,status=map(int,sys.argv[2:])
with open(sys.argv[1],'x') as out:
 json.dump(dict(before_hw=before,after_hw=after,acknowledged=acked,requested=count,client_exit_code=status,
 integrity_ok=status==0 and acked==count and after-before==acked),out,indent=2)
PY
[[ "$produce_status" == 0 && "$acked" == "$COUNT" && "$((after_hw-before_hw))" == "$acked" ]] || { echo 'producer/HW audit failed; artifacts retained' >&2; exit 1; }
fetch_status=0
"$TIME_BINARY" -v -o "$ARTIFACT_DIR/fetch.resources.log" env VERIFY=0 "${fetch_history[@]}" "$fetch_binary" > "$ARTIFACT_DIR/fetch.stdout.log" 2> "$ARTIFACT_DIR/fetch.stderr.log" || fetch_status=$?
printf '%s\n' "$fetch_status" > "$ARTIFACT_DIR/fetch.exit-status.txt"
consumed="$(python3 - "$ARTIFACT_DIR/fetch.stdout.log" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
print(rows[-1]['consumed'] if rows else -1)
PY
)"
[[ "$fetch_status" == 0 && "$consumed" == "$acked" ]] || { echo 'fetch command/count failed; artifacts retained' >&2; exit 1; }
if [[ "$VERIFY_HISTORY" == 1 ]]; then
  checker_status=0
  python3 scripts/bench-record-history.py --producer "$ARTIFACT_DIR/producer.jsonl" --consumer "$ARTIFACT_DIR/consumer.jsonl" --history "$ARTIFACT_DIR/history.json" --output "$ARTIFACT_DIR/verdict.json" > "$ARTIFACT_DIR/checker.stdout.log" 2> "$ARTIFACT_DIR/checker.stderr.log" || checker_status=$?
  [[ "$checker_status" == 0 ]] || { echo 'record verification failed; artifacts retained' >&2; exit 1; }
fi
echo "lab-a-fetch: pass; consumed=$consumed mode=$FETCH_MODE warmup=$WARMUP verified=$VERIFY_HISTORY artifacts=$ARTIFACT_DIR"
