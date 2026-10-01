#!/usr/bin/env bash
# KL01-10: fresh digest-pinned Linux broker; every required history must finish.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
version="${1:?explicit current broker version required}"
reference="$(python3 - "$version" <<'PY'
import json,sys
from pathlib import Path
cells=json.loads(Path('tests/conformance/current-broker-cells.json').read_text())['cells']
print(next(c['reference'] for c in cells if c['version']==sys.argv[1]))
PY
)"
[[ -z "${KAFKA_IMAGE:-}" || "$KAFKA_IMAGE" == "$reference" ]] || { echo 'requested image differs from frozen cell' >&2; exit 1; }
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || { echo 'native Linux x86_64 cell required' >&2; exit 1; }
[[ -z "$(git status --porcelain --untracked-files=normal)" ]] || { echo 'clean committed source required' >&2; exit 1; }
source_sha="$(git rev-parse HEAD)"
report="${PL_COMPAT_REPORT_DIR:-$ROOT/target/broker-compatibility/$version}"
mkdir -p "$report"
name="pl-compat-${version//./-}-$$"
port="${PL_COMPAT_PORT:-19092}"
owned=0
phase=prerequisites
cleanup() {
  local result=$?
  set +e
  if [[ "$owned" == 1 ]]; then
    docker logs "$name" >"$report/broker.log" 2>&1
    docker rm -f "$name" >"$report/cleanup.log" 2>&1
  fi
  if [[ "$result" -ne 0 ]]; then
    python3 - "$report" "$phase" "$result" <<'PY'
from pathlib import Path
import sys
root=Path(sys.argv[1]); details=[]
for name in ('runtime.log','build.log','readiness.log','features.log','report-validation.log','broker.log'):
 path=root/name
 if path.is_file(): details.append(name+':\n'+path.read_text(encoding='utf-8',errors='replace')[-4000:])
message=f'{sys.argv[2]} exited {sys.argv[3]}\n'+'\n'.join(details)
message=message[-14000:].replace('%','%25').replace('\r','%0D').replace('\n','%0A')
print('::error title=Required broker compatibility::'+message)
PY
  fi
  exit "$result"
}
trap cleanup EXIT
rustc -Vv >"$report/rustc.log"
cargo -V >"$report/cargo.log"
if ! docker image inspect "$reference" >/dev/null 2>&1; then
  docker pull "$reference" >"$report/pull.log" 2>&1
fi
phase=broker-start
# Only the successfully created container is owned by this invocation.
docker create --name "$name" -p "127.0.0.1:$port:9092" \
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
  "$reference" >"$report/container-id.log"
owned=1
docker start "$name" >"$report/start.log"
phase=broker-readiness
ready=0
for attempt in $(seq 1 45); do
  if timeout 10s docker exec "$name" /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:9094 >"$report/readiness-$attempt.log" 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" == 1 ]] || { echo 'broker API readiness deadline exceeded' >&2; exit 1; }
cp "$report/readiness-$attempt.log" "$report/readiness.log"
phase=required-feature-levels
timeout 30s docker exec "$name" /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server localhost:9094 describe >"$report/features-before.log" 2>&1
share_level="$(python3 - "$report/features-before.log" <<'PY'
from pathlib import Path
import re,sys
text=Path(sys.argv[1]).read_text()
match=re.search(r'Feature: share\.version\s+.*?FinalizedVersionLevel: (\d+)',text)
if not match: raise SystemExit('share.version not reported')
print(int(match[1]))
PY
)"
if [[ "$share_level" == 0 ]]; then
  timeout 30s docker exec "$name" /opt/kafka/bin/kafka-features.sh \
    --bootstrap-server localhost:9094 upgrade --feature share.version=1 >"$report/features-upgrade.log" 2>&1
fi
timeout 30s docker exec "$name" /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server localhost:9094 describe >"$report/features.log" 2>&1
timeout 20s docker exec "$name" /opt/kafka/bin/kafka-topics.sh --version >"$report/broker-version.log" 2>&1
python3 - "$report" "$name" "$reference" <<'PY'
import json,platform,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); name=sys.argv[2]; reference=sys.argv[3]
command=lambda *args: subprocess.check_output(args,text=True).strip()
fields=dict(line.split(': ',1) for line in (root/'rustc.log').read_text().splitlines() if ': ' in line)
data={'requested':reference,'actual_reference':command('docker','inspect','--format','{{.Config.Image}}',name),
'container_image_id':command('docker','inspect','--format','{{.Image}}',name),
'inspected_image_id':command('docker','image','inspect','--format','{{.Id}}',reference),
'repo_digests':json.loads(command('docker','image','inspect','--format','{{json .RepoDigests}}',reference)),
'kafka_cli_version':(root/'broker-version.log').read_text(),'host_os':platform.system(),'host_arch':platform.machine(),
'rustc_host':fields['host'],'rustc_release':fields['release']}
(root/'identity.json').write_text(json.dumps(data,indent=2)+'\n')
PY
phase=build-required-profile
cargo test --locked --test broker_compatibility --no-run >"$report/build.log" 2>&1
phase=runtime-required-profile
export KAFKA_BOOTSTRAP="127.0.0.1:$port"
export PL_COMPAT_REFERENCE="$reference" PL_COMPAT_SOURCE_SHA="$source_sha" PL_COMPAT_PREFIX="plcompat-${version//./-}"
cargo test --locked --test broker_compatibility -- --ignored --exact live_compatibility_required --nocapture >"$report/runtime.log" 2>&1
phase=independent-java-history-and-offsets
prefix="plcompat-${version//./-}"
# Kafka 4.2 deprecated --property and writes its warning to record stdout.
# Keep strict record parsing and use each pinned CLI's supported option.
formatter_option=--formatter-property
if [[ "$version" == 4.1.2 ]]; then formatter_option=--property; fi
for topic in input output; do
  timeout 40s docker exec "$name" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9094 --topic "$prefix-$topic" --from-beginning \
    --max-messages 16 --timeout-ms 20000 --isolation-level read_committed \
    "$formatter_option" print.key=true "$formatter_option" print.timestamp=true \
    "$formatter_option" print.partition=true "$formatter_option" print.offset=true \
    >"$report/java-$topic.log" 2>"$report/java-$topic.stderr.log"
done
for group in classic cooperative kip848 transaction; do
  timeout 30s docker exec "$name" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9094 --describe --group "$prefix-$group" \
    >"$report/java-offsets-$group.log" 2>&1
done
phase=validate-complete-history
python3 -B scripts/report-broker-compatibility.py "$report" "$version" "$source_sha" >"$report/report-validation.log" 2>&1
cat "$report/report-validation.log"
if [[ "${PL_COMPAT_RUN_VERIFIABLE:-0}" == 1 && "$version" == 4.1.2 ]]; then
  phase=required-live-verifiable-scenario
  REQUIRE_BROKER=1 PL_VERIFIABLE_CONTAINER="$name" \
    PL_VERIFIABLE_REPORT_DIR="$report/verifiable" \
    bash scripts/ci-verifiable-scenario.sh >"$report/verifiable-validation.log" 2>&1
  cat "$report/verifiable-validation.log"
fi
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  python3 - "$report/report.json" >>"$GITHUB_OUTPUT" <<'PY'
import json,sys
from pathlib import Path
report=json.loads(Path(sys.argv[1]).read_text());runtime=report['runtime']
print('scenarios='+str(len(runtime['scenarios'])))
print('records='+str(runtime['share_accepted']))
print('digest='+report['cell']['digest'].removeprefix('sha256:'))
print('rust='+report['identity']['rustc_release'])
print('features='+'-'.join(alias+str(runtime['finalized_features'][name][1]) for alias,name in
 [('share','share.version'),('group','group.version'),('txn','transaction.version'),('meta','metadata.version')]))
PY
fi
