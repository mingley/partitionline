#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
profile="${1:?select default or all}"
features=()
case "$profile" in
  default) ;;
  all) features+=(--all-features) ;;
  *) echo 'select default or all' >&2; exit 2 ;;
esac

test "$(uname -m)" = x86_64
report="$root/target/producer-live-receipts/$profile"
mkdir -p "$report"
name="pl-receipts-$$"
port=19094
owned=0
cleanup() {
  local result=$?
  if [[ "$owned" == 1 ]]; then
    docker logs "$name" >"$report/broker.log" 2>&1 || true
    if ! docker rm -f -v "$name" >"$report/cleanup.log" 2>&1; then
      result=1
    elif docker container inspect "$name" >/dev/null 2>&1; then
      result=1
    else
      echo "removed $name" >>"$report/cleanup.log"
    fi
  fi
  exit "$result"
}
trap cleanup EXIT

version=4.3.1
image="$(python3 - "$version" <<'PY'
import json
import sys
from pathlib import Path

cells = json.loads(Path("tests/conformance/current-broker-cells.json").read_text())["cells"]
print(next(cell["reference"] for cell in cells if cell["version"] == sys.argv[1]))
PY
)"
git rev-parse HEAD >"$report/source.log"
rustc +stable -Vv >"$report/rustc.log"
cargo +stable -V >"$report/cargo.log"
docker pull --platform linux/amd64 "$image" >"$report/pull.log"
docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}' \
  "$image" >"$report/image.log"

docker create --platform linux/amd64 --pull never --name "$name" -p "127.0.0.1:$port:9092" \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:9094,CONTROLLER://:9093 \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port,INTERNAL://localhost:9094" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  "$image" >"$report/container-id.log"
owned=1
test "$(docker inspect --format '{{.Image}}' "$name")" = \
  "$(docker image inspect --format '{{.Id}}' "$image")"
docker start "$name" >"$report/start.log"

kafka() {
  local deadline="$1" command="$2"
  shift 2
  timeout "$deadline" docker exec "$name" "/opt/kafka/bin/$command" \
    --bootstrap-server localhost:9094 "$@"
}
ready=0
for attempt in $(seq 1 30); do
  if kafka 5s kafka-broker-api-versions.sh >"$report/readiness.log" 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" = 1
timeout 15s docker exec "$name" /opt/kafka/bin/kafka-topics.sh --version >"$report/kafka-version.log"
python3 - "$report/kafka-version.log" "$version" <<'PY'
from pathlib import Path
import sys

version = Path(sys.argv[1]).read_text().split()
if not version or version[0] != sys.argv[2]:
    raise SystemExit(f"Unexpected Kafka version {version}")
PY

topics=(pl-receipts-a pl-receipts-b pl-receipts-c)
for topic in "${topics[@]}"; do
  kafka 30s kafka-topics.sh --create --topic "$topic" --partitions 2 \
    --replication-factor 1 --config min.insync.replicas=1 >>"$report/topics.log"
done

export PL_RECEIPT_BOOTSTRAP="127.0.0.1:$port"
export PL_RECEIPT_ACKS_PER_PARTITION=512
export PL_RECEIPT_QUEUE_PER_PHASE=64
total=$((PL_RECEIPT_ACKS_PER_PARTITION + 2 * PL_RECEIPT_QUEUE_PER_PHASE))
timeout 120s cargo +stable test --locked "${features[@]}" --test producer_live_receipts \
  acknowledged_receipts_and_queued_delivery -- --exact --ignored --nocapture \
  2>&1 | tee "$report/rust.log"

for topic in "${topics[@]}"; do
  kafka 25s kafka-get-offsets.sh --topic "$topic" --time -1 >"$report/$topic.end.log"
  for partition in 0 1; do
    kafka 30s kafka-console-consumer.sh --topic "$topic" --partition "$partition" \
      --offset earliest --max-messages "$total" --timeout-ms 15000 \
      --formatter-property print.partition=true --formatter-property print.offset=true \
      --formatter-property print.key=true >"$report/$topic-$partition.records.tsv" \
      2>"$report/$topic-$partition.consumer.log"
  done
done

python3 - "$report" "$total" "$PL_RECEIPT_ACKS_PER_PARTITION" "$PL_RECEIPT_QUEUE_PER_PHASE" "${topics[@]}" <<'PY' | tee "$report/verification.json"
import hashlib
import json
import sys
from pathlib import Path

report = Path(sys.argv[1])
total = int(sys.argv[2])
acknowledged, queued = map(int, sys.argv[3:5])
topics = sys.argv[5:]
partition_count = 2 * len(topics)
receipt_marker = (
    f"receipt_delivery acknowledged={partition_count * acknowledged}"
    f" after_flush={partition_count * (acknowledged + queued)}"
    f" after_close={partition_count * total}"
)
if (report / "rust.log").read_text().splitlines().count(receipt_marker) != 1:
    raise SystemExit("Rust receipt test did not verify the expected acknowledgments")
verified = []
for topic in topics:
    ends = (report / f"{topic}.end.log").read_text().splitlines()
    expected_ends = {f"{topic}:{partition}:{total}" for partition in range(2)}
    if len(ends) != len(expected_ends) or set(ends) != expected_ends:
        raise SystemExit(f"{topic} unexpected end offsets {ends}")
    for partition in range(2):
        path = report / f"{topic}-{partition}.records.tsv"
        data = path.read_bytes()
        expected = "".join(
            f"Partition:{partition}\tOffset:{offset}\t{offset:08}\t{topic}:{partition}:{offset:08}\n"
            for offset in range(total)
        ).encode()
        if data != expected:
            observed = data.decode(errors="replace").splitlines()
            wanted = expected.decode().splitlines()
            first = next((i for i, pair in enumerate(zip(observed, wanted)) if pair[0] != pair[1]), None)
            detail = "" if first is None else f", first mismatch at {first}: {observed[first]!r}"
            raise SystemExit(f"{topic}/{partition} expected {total} exact records, got {len(observed)}{detail}")
        verified.append({
            "topic": topic,
            "partition": partition,
            "records": total,
            "sha256": hashlib.sha256(data).hexdigest(),
        })
print(json.dumps({
    "source": (report / "source.log").read_text().strip(),
    "image": (report / "image.log").read_text().strip(),
    "verified_records": sum(cell["records"] for cell in verified),
    "partitions": verified,
}, indent=2))
PY
