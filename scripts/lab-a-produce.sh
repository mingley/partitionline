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
# bench_produce cannot expose its warmup count separately. HW delta would include
# those records. Warmup for a qualification campaign belongs to a campaign driver.
[[ "$WARMUP_SECS" == 0 ]] || { echo 'lab-a-produce: WARMUP_SECS must be 0 for exact HW accounting; this is an integrity harness' >&2; exit 2; }
[[ "$ACKS" == 1 || "$ACKS" == -1 ]] || { echo 'lab-a-produce: ACKS=0 has no broker acknowledgments and cannot pass HW==acked' >&2; exit 2; }
[[ "$RUNS" =~ ^[1-9][0-9]*$ && "$COUNT" =~ ^[1-9][0-9]*$ && "$PARTITIONS" =~ ^[1-9][0-9]*$ ]] || { echo 'positive RUNS/COUNT/PARTITIONS required' >&2; exit 2; }
if [[ "${IDEMPOTENT:-0}" == 1 && "$ACKS" != -1 ]]; then echo 'IDEMPOTENT=1 requires ACKS=-1' >&2; exit 2; fi
mkdir -p "$ARTIFACT_DIR"
export KAFKA_BOOTSTRAP="$BOOTSTRAP" KAFKA_TOPIC="$TOPIC" COUNT PAYLOAD_BYTES ACKS LINGER_MS WARMUP_SECS
# These knobs are effective in bench_produce. Its batching and buffer caps are
# fixed in source; refuse pretending that requested overrides took effect.
export CONNECTIONS="${CONNECTIONS:-1}" MAX_IN_FLIGHT="${MAX_IN_FLIGHT:-5}"
export BATCH_BYTES="${BATCH_BYTES:-1000000}" BATCH_RECORDS="${BATCH_RECORDS:-32768}"
if [[ "$CLIENTS" == *partitionline* && ( "$BATCH_BYTES" != 1000000 || "$BATCH_RECORDS" != 32768 ) ]]; then
  echo 'bench_produce fixes BATCH_BYTES=1000000 and BATCH_RECORDS=32768; requested override cannot be matched' >&2; exit 2
fi
export QUEUE_KBYTES="${QUEUE_KBYTES:-32768}" QUEUE_MESSAGES="${QUEUE_MESSAGES:-1000000}"
export COMPRESSION="${COMPRESSION:-none}"
if [[ "$CLIENTS" == *librdkafka* ]]; then
  if [[ -z "${C_PEER_BINARY:-}" ]]; then C_PEER_BINARY="$(bash benchmarks/peers/librdkafka/build.sh)"; fi
  [[ -x "$C_PEER_BINARY" ]] || { echo 'C_PEER_BINARY is not executable' >&2; exit 2; }
fi
if [[ "$CLIENTS" == *partitionline* ]]; then cargo build --release --example bench_produce; fi
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
python3 - "$ARTIFACT_DIR/partitionline-settings.json" <<'PY'
import json,os,sys
settings={k:os.environ.get(k) for k in ['KAFKA_BOOTSTRAP','KAFKA_TOPIC','COUNT','PAYLOAD_BYTES','ACKS','LINGER_MS','MAX_IN_FLIGHT','CONNECTIONS','COMPRESSION','IDEMPOTENT']}
settings.update(batch_bytes=1000000,batch_records=32768,buffer_memory_bytes=33554432,partitioner='default null-key round robin',payload='constant x, null key',warmup_records=0,
 comparison_limit='C also has an independent record queue cap; connection routing differs across implementations. Equal-semantics campaign and receipt IDs are not established by this script.')
with open(sys.argv[1],'x') as out: json.dump(settings,out,indent=2)
PY
while read -r repetition client; do
  stem="$ARTIFACT_DIR/$repetition-$client"
  if [[ -z "${SKIP_TOPIC_RESET:-}" ]]; then lab_a_reset_topic || exit 1; fi
  before_hw="$(lab_a_hw_sum)" || exit 1
  if [[ "$before_hw" != 0 && -z "${SKIP_TOPIC_RESET:-}" ]]; then echo 'topic reset did not produce zero HW' >&2; exit 1; fi
  status=0
  if [[ "$client" == partitionline ]]; then
    "${PARTITIONLINE_BINARY:-${CARGO_TARGET_DIR:-$ROOT/target}/release/examples/bench_produce}" > "$stem.stdout.log" 2> "$stem.stderr.log" || status=$?
    acked="$(python3 - "$stem.stdout.log" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.startswith('{')]
print(rows[-1]['acked'] if rows else -1)
PY
)"
  else
    WARMUP=0 KEY_MODE=none PAYLOAD_MODE=constant-x LATENCY_SAMPLES="${LATENCY_SAMPLES:-0}" \
      REPETITION_INDEX="$repetition" TOTAL_REPETITIONS="$RUNS" PAIRING_ORDER="see order.json" \
      python3 benchmarks/peers/librdkafka/run.py produce --binary "$C_PEER_BINARY" --result "$stem.json" > "$stem.stdout.log" 2> "$stem.stderr.log" || status=$?
    acked="$(python3 - "$stem.json" <<'PY'
import json,sys
try: print(json.load(open(sys.argv[1]))['outcomes']['acknowledged'])
except (FileNotFoundError,KeyError): print(-1)
PY
)"
  fi
  hw="$(lab_a_hw_sum)" || exit 1
  delta=$((hw-before_hw))
  python3 - "$stem.hw.json" "$before_hw" "$hw" "$acked" "$status" "$COUNT" <<'PY'
import json,sys
before,after,acked,status,count=map(int,sys.argv[2:]); delta=after-before
with open(sys.argv[1],'x') as out: json.dump(dict(before_hw=before,after_hw=after,hw_delta=delta,acknowledged=acked,client_exit_code=status,requested=count,
 integrity_ok=status==0 and acked==count and delta==acked),out,indent=2)
PY
  echo "lab-a-produce: repetition=$repetition client=$client exit=$status acked=$acked hw_delta=$delta"
  if [[ "$status" != 0 || "$acked" != "$COUNT" || "$delta" != "$acked" ]]; then
    echo "lab-a-produce: FAIL; retained artifacts at $ARTIFACT_DIR" >&2; exit 1
  fi
done < <(python3 - "$ARTIFACT_DIR/order.json" <<'PY'
import json,sys
for row in json.load(open(sys.argv[1])): print(row['repetition'],row['client'])
PY
)
echo "lab-a-produce: ok; unsigned HW integrity evidence at $ARTIFACT_DIR. Suite HOLD active. No comparison claim."
