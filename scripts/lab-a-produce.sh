#!/usr/bin/env bash
# Produce integrity harness: fresh topic and independent HW audit per client.
# Native C peer is automated; unsigned runs never confer a comparison win.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lab-a-common.sh
source "$ROOT/scripts/lab-a-common.sh"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-plbench}}"
COUNT="${COUNT:-8000000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-100}"
ACKS="${ACKS:-1}"
LINGER_MS="${LINGER_MS:-5}"
WARMUP_SECS="${WARMUP_SECS:-0}"
RUNS="${RUNS:-5}"
PARTITIONS="${PARTITIONS:-6}"
BROKER_NAME="${BROKER_NAME:-pl-lab-a-kafka}"
LAB_A_LABEL="lab-a-produce"
CLIENTS="${CLIENTS:-partitionline,librdkafka}"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/work/lab-a-produce-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
case "$CLIENTS" in partitionline|librdkafka|partitionline,librdkafka) ;; *) echo 'CLIENTS must be partitionline, librdkafka, or partitionline,librdkafka' >&2; exit 2;; esac
# This HW integrity harness permits exact record-count warmup. Duration-only
# warmup remains a campaign-driver concern.
[[ "$WARMUP_SECS" == 0 ]] || { echo 'lab-a-produce: WARMUP_SECS must be 0 for exact HW accounting; this is an integrity harness' >&2; exit 2; }
[[ "$ACKS" == 1 || "$ACKS" == -1 ]] || { echo 'lab-a-produce: ACKS=0 has no broker acknowledgments and cannot pass HW==acked' >&2; exit 2; }
[[ "$RUNS" =~ ^[1-9][0-9]*$ && "$COUNT" =~ ^[1-9][0-9]*$ && "$PARTITIONS" =~ ^[1-9][0-9]*$ ]] || { echo 'positive RUNS/COUNT/PARTITIONS required' >&2; exit 2; }
if [[ "${IDEMPOTENT:-0}" == 1 && "$ACKS" != -1 ]]; then echo 'IDEMPOTENT=1 requires ACKS=-1' >&2; exit 2; fi
mkdir -p "$ARTIFACT_DIR"
export KAFKA_BOOTSTRAP="$BOOTSTRAP" KAFKA_TOPIC="$TOPIC" COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS
# Validate and retain the actual Rust settings before creating or deleting topics.
export CONNECTIONS="${CONNECTIONS:-1}" MAX_IN_FLIGHT="${MAX_IN_FLIGHT:-5}"
export BATCH_BYTES="${BATCH_BYTES:-${BATCH_SIZE:-1000000}}" BATCH_RECORDS="${BATCH_RECORDS:-32768}"
export QUEUE_KBYTES="${QUEUE_KBYTES:-32768}" QUEUE_MESSAGES="${QUEUE_MESSAGES:-1000000}"
export COMPRESSION="${COMPRESSION:-none}"
export WARMUP="${WARMUP:-0}" RECORD_SEED="${RECORD_SEED:-${SEED:-0x5EED0001}}"
export KEY_MODE="${KEY_MODE:-id}" PAYLOAD_MODE="${PAYLOAD_MODE:-seeded}" PARTITIONS
export IDEMPOTENT="${IDEMPOTENT:-0}"
if [[ "$CLIENTS" == *librdkafka* ]]; then
  if [[ -z "${C_PEER_BINARY:-}" ]]; then C_PEER_BINARY="$(bash benchmarks/peers/librdkafka/build.sh)"; fi
  [[ -x "$C_PEER_BINARY" ]] || { echo 'C_PEER_BINARY is not executable' >&2; exit 2; }
fi
if [[ "$CLIENTS" == *partitionline* ]]; then
  cargo +stable build --locked --release --example bench_produce
  PARTITIONLINE_BINARY="${PARTITIONLINE_BINARY:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/bench_produce}"
  env -u QUEUE_MESSAGES "$PARTITIONLINE_BINARY" --print-config > "$ARTIFACT_DIR/partitionline-settings.json"
fi
TIME_BINARY="${TIME_BINARY:-/usr/bin/time}"
[[ -x "$TIME_BINARY" ]] || { echo 'GNU time required for CPU and peak RSS evidence (TIME_BINARY)' >&2; exit 2; }
# Common helpers historically prefer Docker on port 9092. A custom endpoint
# must never reset a different Docker broker than the one the client uses.
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
# Deterministic randomized paired order, persisted before execution.
python3 - "$CLIENTS" "$RUNS" "${PAIRING_SEED:-1593835524}" "$ARTIFACT_DIR/order.json" <<'PY'
import json,random,sys
clients=sys.argv[1].split(','); rng=random.Random(int(sys.argv[3],0)); rows=[]
for repetition in range(1,int(sys.argv[2])+1):
 order=clients[:]; rng.shuffle(order)
 rows.extend({'repetition':repetition,'client':client} for client in order)
with open(sys.argv[4],'x') as out: json.dump(rows,out,indent=2)
PY
while read -r repetition client; do
  stem="$ARTIFACT_DIR/$repetition-$client"
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then lab_a_reset_topic || exit 1; fi
  before_hw="$(lab_a_hw_sum)" || exit 1
  if [[ "$before_hw" != 0 && -z "${SKIP_TOPIC_RESET:-}" ]]; then echo 'topic reset did not produce zero HW' >&2; exit 1; fi
  status=0
  if [[ "$client" == partitionline ]]; then
    "$TIME_BINARY" -v -o "$stem.resources.log" env -u QUEUE_MESSAGES "$PARTITIONLINE_BINARY" > "$stem.stdout.log" 2> "$stem.stderr.log" || status=$?
    acked="$(python3 - "$stem.stdout.log" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
print(rows[-1]['acked'] if rows else -1)
PY
)"
    warmup_acked="$(python3 - "$stem.stdout.log" <<'PY_WARM'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
print(rows[-1]['warmup_records'] if rows else -1)
PY_WARM
)"
  else
    LATENCY_SAMPLES="${LATENCY_SAMPLES:-0}" \
      REPETITION_INDEX="$repetition" TOTAL_REPETITIONS="$RUNS" PAIRING_ORDER="see order.json" \
      "$TIME_BINARY" -v -o "$stem.resources.log" python3 benchmarks/peers/librdkafka/run.py produce --binary "$C_PEER_BINARY" --result "$stem.json" > "$stem.stdout.log" 2> "$stem.stderr.log" || status=$?
    acked="$(python3 - "$stem.json" <<'PY'
import json,sys
try: print(json.load(open(sys.argv[1]))['outcomes']['acknowledged'])
except (FileNotFoundError,KeyError): print(-1)
PY
)"
    warmup_acked="$(python3 - "$stem.json" <<'PY_WARM'
import json,sys
try: print(json.load(open(sys.argv[1]))['execution']['warmup_records'])
except (FileNotFoundError,KeyError): print(-1)
PY_WARM
)"
  fi
  hw="$(lab_a_hw_sum)" || exit 1
  delta=$((hw-before_hw))
  python3 - "$stem.hw.json" "$before_hw" "$hw" "$acked" "$status" "$COUNT" "$warmup_acked" "$WARMUP" <<'PY'
import json,sys
before,after,acked,status,count,warmup,requested_warmup=map(int,sys.argv[2:]); delta=after-before
with open(sys.argv[1],'x') as out: json.dump(dict(before_hw=before,after_hw=after,hw_delta=delta,acknowledged=acked,client_exit_code=status,requested=count,
 warmup_acknowledged=warmup,requested_warmup=requested_warmup,measured_hw_delta=delta-warmup,
 integrity_ok=status==0 and acked==count and warmup==requested_warmup and delta==acked+warmup),out,indent=2)
PY
  echo "lab-a-produce: repetition=$repetition client=$client exit=$status acked=$acked hw_delta=$delta"
  if [[ "$status" != 0 || "$acked" != "$COUNT" || "$warmup_acked" != "$WARMUP" || "$delta" != "$((acked+warmup_acked))" ]]; then
    echo "lab-a-produce: FAIL; retained artifacts at $ARTIFACT_DIR" >&2; exit 1
  fi
done < <(python3 - "$ARTIFACT_DIR/order.json" <<'PY'
import json,sys
for row in json.load(open(sys.argv[1])): print(row['repetition'],row['client'])
PY
)
echo "lab-a-produce: ok; unsigned HW integrity evidence at $ARTIFACT_DIR. Suite HOLD active. No comparison claim."
