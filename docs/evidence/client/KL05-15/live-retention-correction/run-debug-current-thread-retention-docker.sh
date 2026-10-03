#!/usr/bin/env bash
set -euo pipefail
source_root="${1:?immutable source directory required}"
source_sha="${2:?pushed source SHA required}"
report_root="${3:?scratch report directory required}"
[[ "$source_sha" =~ ^[0-9a-f]{40}$ ]]
reference='apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837'
name="pl-client-share-v2-${source_sha:0:12}-$$"
port=29182
mkdir -p "$report_root"
owned=0
phase=prerequisites
docker_local() {
  env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH docker --host=unix:///var/run/docker.sock "$@"
}
cleanup() {
  result=$?
  set +e
  if [[ "$owned" == 1 ]]; then
    docker_local logs "$name" >"$report_root/broker.log" 2>&1
    docker_local rm --force "$name" >"$report_root/cleanup.log" 2>&1
  fi
  python3 - "$report_root" "$phase" "$result" <<'PY'
from pathlib import Path
import json,sys
root=Path(sys.argv[1]);(root/'exit.json').write_text(json.dumps({'phase':sys.argv[2],'exit_code':int(sys.argv[3])},indent=2)+'\n')
PY
  exit "$result"
}
trap cleanup EXIT
docker_local info --format '{{.ServerVersion}}' >"$report_root/docker-version.log"
docker_local image inspect "$reference" --format '{{json .RepoDigests}}' >"$report_root/image-digests.json"
phase=broker-create
docker_local create --name "$name" --cpuset-cpus '0-2,4' -p "127.0.0.1:$port:9092" \
  -e KAFKA_HEAP_OPTS='-Xms128m -Xmx512m' \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:9094,CONTROLLER://:9093 \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port,INTERNAL://localhost:9094" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1 -e KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS=1 \
  -e KAFKA_GROUP_SHARE_ENABLE=true -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  "$reference" >"$report_root/container-id.log"
owned=1
docker_local inspect --format '{{json .HostConfig.CpusetCpus}}' "$name" >"$report_root/cpuset.json"
docker_local inspect --format '{{.Image}}' "$name" >"$report_root/container-image-id.log"
docker_local image inspect --format '{{.Id}}' "$reference" >"$report_root/expected-image-id.log"
cmp "$report_root/container-image-id.log" "$report_root/expected-image-id.log"
docker_local start "$name" >"$report_root/start.log"
phase=broker-readiness
ready=0
for attempt in $(seq 1 45); do
  if timeout 10s env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH docker --host=unix:///var/run/docker.sock exec "$name" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9094 >"$report_root/readiness-$attempt.log" 2>&1; then
    ready=1
    cp "$report_root/readiness-$attempt.log" "$report_root/readiness.log"
    break
  fi
  sleep 1
done
[[ "$ready" == 1 ]]
phase=features
for which in version features; do
  if [[ "$which" == version ]]; then
    docker_local exec "$name" /opt/kafka/bin/kafka-topics.sh --version >"$report_root/broker-version.log" 2>&1
  else
    docker_local exec "$name" /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9094 describe >"$report_root/features-before.log" 2>&1
  fi
done
share_level="$(python3 - "$report_root/features-before.log" <<'PY'
from pathlib import Path
import re,sys
m=re.search(r'Feature: share\.version\s+.*?FinalizedVersionLevel: (\d+)',Path(sys.argv[1]).read_text())
if not m: raise SystemExit('share.version absent')
print(m[1])
PY
)"
if [[ "$share_level" == 0 ]]; then
  docker_local exec "$name" /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9094 upgrade --feature share.version=1 >"$report_root/features-upgrade.log" 2>&1
fi
docker_local exec "$name" /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9094 describe >"$report_root/features.log" 2>&1
phase=rust-current-v2-proof
prefix="pl-share-v2-${source_sha:0:12}"
cd "$source_root"
export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup
export PATH=/workspace/work/cargo/bin:$PATH CARGO_TARGET_DIR=/workspace/work/client-share-target
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export KAFKA_BOOTSTRAP="127.0.0.1:$port" PL_COMPAT_REFERENCE="$reference" PL_COMPAT_SOURCE_SHA="$source_sha" PL_SHARE_PREFIX="$prefix"
taskset -c 0-2,4 /workspace/work/client-share-assessment/debug-share-runtime-current-thread-retention >"$report_root/runtime.log" 2>&1
phase=supplemental-trace-complete
exit 0
phase=independent-java-read
# The four ordinary persisted records must also decode with this pinned broker's Java CLI.
docker_local exec "$name" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9094 --topic "$prefix-records" --from-beginning --max-messages 4 --timeout-ms 20000 \
  --formatter-property print.key=true --formatter-property print.timestamp=true --formatter-property print.partition=true --formatter-property print.offset=true >"$report_root/java-records.log" 2>"$report_root/java-records.stderr.log"
phase=independent-real-batch
docker_local exec "$name" /opt/kafka/bin/kafka-dump-log.sh --deep-iteration --print-data-log \
  --files "/tmp/kafka-logs/$prefix-records-0/00000000000000000000.log" >"$report_root/java-batch.log" 2>"$report_root/java-batch.stderr.log"
phase=validate-report
python3 - "$report_root" "$source_sha" "$reference" "$name" <<'PY'
from pathlib import Path
import hashlib,json,platform,re,sys
root=Path(sys.argv[1]);sha,ref,name=sys.argv[2:]
raw=(root/'runtime.log').read_text(); rows=[line.split('SHARE_V2_LIVE_REPORT ',1)[1] for line in raw.splitlines() if 'SHARE_V2_LIVE_REPORT ' in line]
assert len(rows)==1, 'exactly one complete runtime report required'
runtime=json.loads(rows[0]);assert runtime['source_sha']==sha and runtime['reference']==ref
assert runtime['runtime_version']==2 and runtime['disposition']=='supported'
assert runtime['accepted_offsets']==list(range(4)) and runtime['release_delivery_count']==3 and runtime['expiry_delivery_count']==2 and runtime['renew']=='successful'
assert runtime['finalized_features']['share.version'][0]>=1
assert (root/'broker-version.log').read_text().startswith('4.3.1')
assert (root/'container-image-id.log').read_text()==(root/'expected-image-id.log').read_text()
assert json.loads((root/'cpuset.json').read_text())=='0-2,4'
assert any(ref.split('@',1)[1] in digest for digest in json.loads((root/'image-digests.json').read_text()))
api=(root/'readiness.log').read_text();assert re.search(r'ShareFetch\(78\): 1 to 2',api) and re.search(r'ShareAcknowledge\(79\): 1 to 2',api)
lines=(root/'java-records.log').read_text().splitlines(); assert len(lines)==4
for index,line in enumerate(lines):
    expected=f'CreateTime:{1000+index}\tPartition:0\tOffset:{index}\tkey-{index}\tvalue-{index}'
    assert line==expected, (line,expected)
batches=re.findall(r'baseOffset: (\d+) lastOffset: (\d+) count: (\d+)',(root/'java-batch.log').read_text())
assert batches==[('0','3','4')], ('one actual four-record persisted batch required',batches)
report={'schema_version':1,'source_sha':sha,'reference':ref,'container':name,'host_os':platform.system(),'host_arch':platform.machine(),'runtime':runtime,'independent_java_records':4,'independent_persisted_batches':1,'disposition':'passed','sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.is_file() and p.name not in ('report.json','exit.json')}}
(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print('PASS: fresh pinned Docker v2 acquisition/Renew/expiry/release/count/accept + independent Java records')
PY
phase=complete
