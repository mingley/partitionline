#!/usr/bin/env bash
# One attempt only: on ambiguous delivery, retain partial evidence and never retry.
set -euo pipefail
pair="$1"
arm="$2"
out="$ACK_EVIDENCE/pair-$pair-$arm"
mkdir "$out"
topic="ack-match-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$pair-$arm"
export KAFKA_TOPIC="$topic" ACK_TRACE="$out/ack.csv" C_CONFIG="$out/c-config.json"
kafka="ack-kafka-$GITHUB_RUN_ID"
topic_tool=/opt/kafka/bin/kafka-topics.sh
offset_tool=/opt/kafka/bin/kafka-get-offsets.sh
timeout 30s docker exec "$kafka" "$topic_tool" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --topic "$topic" --partitions 1 --replication-factor 1 --config min.insync.replicas=1 \
    > "$out/topic-create.log" 2>&1
timeout 30s docker exec "$kafka" "$topic_tool" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --describe --topic "$topic" > "$out/topic-describe.log" 2>&1
grep -Eq 'PartitionCount:[[:space:]]*1' "$out/topic-describe.log"
grep -Eq 'ReplicationFactor:[[:space:]]*1' "$out/topic-describe.log"
grep -q 'min.insync.replicas=1' "$out/topic-describe.log"
timeout 30s docker exec "$kafka" "$offset_tool" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$topic" --time -1 > "$out/pre-offsets.txt"
python3 benchmarks/peers/kafka-ack/fences.py --offsets "$out/pre-offsets.txt" --topic "$topic" \
    --partitions 1 --warmup 0 --count 0 --output "$out/pre-fences.tsv" > "$out/pre-fence-check.json"
if [ "$arm" = rust ]; then
    driver=target/release/examples/bench_kafka_paired
elif [ "$arm" = c ]; then
    driver="$RUNNER_TEMP/ack-c-peer/paired-ack"
else
    echo "invalid arm: $arm" >&2
    exit 2
fi
taskset -c "$CLIENT_CPU" "$driver" > "$out/driver.log" 2>&1 &
driver_pid=$!
(
    sleep 180 & sleeper=$!
    trap 'kill "$sleeper" 2>/dev/null || true' EXIT TERM
    wait "$sleeper" || exit 0
    echo "driver watchdog timeout: outcome unknown" > "$out/timeout.txt"
    kill -TERM "$driver_pid" 2>/dev/null || true
    sleep 5 & sleeper=$!
    wait "$sleeper" || exit 0
    kill -KILL "$driver_pid" 2>/dev/null || true
) &
watchdog=$!
taskset -c "$SAMPLER_CPU" python3 benchmarks/peers/kafka-ack/socket_sample.py \
    "$driver_pid" 19092 "$out/sockets.json" > "$out/socket-sampler.log" 2>&1 &
sampler=$!
set +e
wait "$driver_pid"
driver_status=$?
kill "$watchdog" 2>/dev/null
wait "$watchdog" 2>/dev/null
wait "$sampler"
sampler_status=$?
set -e
printf '%s\n' "$driver_status" > "$out/driver-exit.txt"
printf '%s\n' "$sampler_status" > "$out/socket-sampler-exit.txt"
if [ "$driver_status" -ne 0 ]; then
    timeout 30s docker exec "$kafka" "$offset_tool" --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --topic "$topic" --time -1 > "$out/failure-offsets.txt" 2>&1 || true
    cat "$out/driver.log" "$out/socket-sampler.log"
    exit 1
fi
timeout 30s docker exec "$kafka" "$offset_tool" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$topic" --time -1 > "$out/post-offsets.txt"
python3 benchmarks/peers/kafka-ack/fences.py --offsets "$out/post-offsets.txt" --topic "$topic" \
    --partitions 1 --warmup "$WARMUP" --count "$COUNT" --output "$out/fences.tsv" > "$out/fence-check.json"
timeout 90s taskset -c "$CLIENT_CPU" java -cp "$RUNNER_TEMP/ack-java:$RUNNER_TEMP/kafka-libs/*" \
    ReceiptAudit "$KAFKA_BOOTSTRAP" "$topic" read_uncommitted "$out/fences.tsv" "$out/java-receipts.jsonl" 60000 \
    > "$out/java-audit.log" 2>&1
python3 benchmarks/peers/kafka-ack/verify.py --kind "$arm" --trace "$ACK_TRACE" --fences "$out/fences.tsv" \
    --receipts "$out/java-receipts.jsonl" --warmup "$WARMUP" --count "$COUNT" --payload "$PAYLOAD_BYTES" \
    --c-config "$C_CONFIG" | tee "$out/verdict.json"

# Preserve a successful driver’s independent readback even if sampling failed.
# The arm still fails unless the retained socket evidence is complete and matched.
python3 benchmarks/peers/kafka-ack/check_sample.py "$out"
