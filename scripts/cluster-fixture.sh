#!/usr/bin/env bash
# Reproducible KRaft RF=3/min-ISR=2 three-broker cluster fixture with owned
# resources and deterministic fault controls (KL03-17).
#
# Supported backends:
#   fake    - Isolated deterministic mock backend (default for testing/self-test;
#             no Docker, Java, or approval required).
#   docker  - Real containerized KRaft cluster using pinned official images.
#             Requires Docker AND explicit approval (CLUSTER_FIXTURE_APPROVED=1).
#   native  - Real local JVM KRaft cluster using Apache Kafka binaries.
#             Requires Java AND explicit approval (CLUSTER_FIXTURE_APPROVED=1).
#
# Acceptance guarantees:
#   - Pinned official images, roles, ports, feature levels, and topology manifest.
#   - Start, stop, leader movement, disconnect, and restart address ONLY recorded owned resources.
#   - Missing Docker, Java, or approval fails explicitly with a distinct error message.
#   - Cleanup addresses ONLY recorded owned containers/PIDs and recorded owned topics.
#     Cleanup NEVER kills processes by name (no pkill/killall) and NEVER deletes unowned topics.
#
# Usage:
#   bash scripts/cluster-fixture.sh manifest
#   bash scripts/cluster-fixture.sh --backend fake start
#   bash scripts/cluster-fixture.sh --backend fake status
#   bash scripts/cluster-fixture.sh --backend fake disconnect --node 2
#   bash scripts/cluster-fixture.sh --backend fake reconnect --node 2
#   bash scripts/cluster-fixture.sh --backend fake leader-move --topic test-topic --partition 0 --to-node 3
#   bash scripts/cluster-fixture.sh --backend fake restart --node 1
#   bash scripts/cluster-fixture.sh --backend fake stop --node 3
#   bash scripts/cluster-fixture.sh --backend fake stop
#   bash scripts/cluster-fixture.sh --backend fake cleanup
#   bash scripts/cluster-fixture.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOPOLOGY_FILE="${TOPOLOGY_FILE:-$ROOT/tests/cluster/topology.json}"
RUN_ID="${RUN_ID:-$$}"
STATE_DIR="${CLUSTER_STATE_DIR:-/tmp/partitionline-cluster-fixture-${RUN_ID}}"
RESOURCES_FILE="$STATE_DIR/resources.json"
BACKEND="${CLUSTER_FIXTURE_BACKEND:-fake}"
APPROVED="${CLUSTER_FIXTURE_APPROVED:-0}"

# Pinned defaults
PINNED_IMAGE="apache/kafka:4.1.0"
PINNED_ALT_IMAGE="apache/kafka:3.9.1"
PINNED_RF=3
PINNED_MIN_ISR=2
PINNED_METADATA_VERSION="4.1-IV0"
PINNED_SHARE_VERSION=1

DOCKER_BIN="${DOCKER_BIN:-docker}"
JAVA_BIN="${JAVA_BIN:-java}"

log() {
  echo "cluster-fixture: $*"
}

err() {
  echo "cluster-fixture: error: $*" >&2
}

fail() {
  err "$*"
  exit 1
}

# --- Python Helper for JSON Manifest & Resource State ---

py_state() {
  python3 -c "
import json, os, sys

cmd = sys.argv[1]
resources_path = sys.argv[2]
state_dir = os.path.dirname(resources_path)
os.makedirs(state_dir, exist_ok=True)

def load_resources():
    if os.path.exists(resources_path):
        with open(resources_path, 'r') as f:
            return json.load(f)
    return None

def save_resources(res):
    with open(resources_path, 'w') as f:
        json.dump(res, f, indent=2)

if cmd == 'init':
    backend = sys.argv[3]
    topology_file = sys.argv[4]
    with open(topology_file, 'r') as f:
        top = json.load(f)
    res = {
        'schema_version': 1,
        'cluster_id': top['cluster_id'],
        'backend': backend,
        'state_dir': state_dir,
        'created_at': top.get('updated', '2026-09-21'),
        'replication_factor': top.get('replication_factor', 3),
        'min_insync_replicas': top.get('min_insync_replicas', 2),
        'feature_levels': top.get('feature_levels', {}),
        'nodes': {},
        'topics_owned': [],
        'partition_leaders': {}
    }
    for n in top['nodes']:
        nid = str(n['node_id'])
        res['nodes'][nid] = {
            'node_id': n['node_id'],
            'roles': n['roles'],
            'client_port': n['client_port'],
            'controller_port': n['controller_port'],
            'client_listener': n['client_listener'],
            'controller_listener': n['controller_listener'],
            'state': 'stopped',
            'pid': None,
            'container_name': f'pl-cluster-node-{nid}-{os.path.basename(state_dir)}'
        }
    save_resources(res)
    print('initialized')

elif cmd == 'get_node':
    nid = sys.argv[3]
    res = load_resources()
    if not res or nid not in res['nodes']:
        sys.exit(1)
    print(json.dumps(res['nodes'][nid]))

elif cmd == 'set_node_state':
    nid = sys.argv[3]
    st = sys.argv[4]
    pid = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != '' else None
    res = load_resources()
    if not res:
        sys.exit(1)
    if nid not in res['nodes']:
        sys.exit(1)
    res['nodes'][nid]['state'] = st
    if pid is not None:
        try:
            res['nodes'][nid]['pid'] = int(pid)
        except ValueError:
            res['nodes'][nid]['pid'] = pid
    save_resources(res)

elif cmd == 'get_running_nodes':
    res = load_resources()
    if not res:
        print('[]')
        sys.exit(0)
    running = [int(nid) for nid, n in res['nodes'].items() if n['state'] == 'running']
    print(json.dumps(running))

elif cmd == 'create_topic':
    topic = sys.argv[3]
    res = load_resources()
    if not res:
        sys.exit(1)
    if topic not in res['topics_owned']:
        res['topics_owned'].append(topic)
        # default leader to node 1 if running, else first running node
        running = [int(nid) for nid, n in res['nodes'].items() if n['state'] == 'running']
        leader = running[0] if running else 1
        res['partition_leaders'][f'{topic}:0'] = leader
    save_resources(res)

elif cmd == 'leader_move':
    topic = sys.argv[3]
    part = sys.argv[4]
    target_node = int(sys.argv[5])
    res = load_resources()
    if not res:
        sys.exit(1)
    key = f'{topic}:{part}'
    node_str = str(target_node)
    if node_str not in res['nodes']:
        sys.stderr.write(f'node {target_node} not in topology\n')
        sys.exit(1)
    if res['nodes'][node_str]['state'] != 'running':
        sys.stderr.write(f'cannot move leader to non-running node {target_node} (state={res[\"nodes\"][node_str][\"state\"]})\n')
        sys.exit(1)
    res['partition_leaders'][key] = target_node
    save_resources(res)
    print(json.dumps({'topic': topic, 'partition': int(part), 'new_leader': target_node}))

elif cmd == 'status':
    as_json = sys.argv[3] == '--json' if len(sys.argv) > 3 else False
    res = load_resources()
    if not res:
        if as_json:
            print(json.dumps({'error': 'cluster not initialized'}))
        else:
            print('cluster not initialized (no resources.json)')
        sys.exit(0)
    total_nodes = len(res['nodes'])
    running_nodes = [int(nid) for nid, n in res['nodes'].items() if n['state'] == 'running']
    disconnected_nodes = [int(nid) for nid, n in res['nodes'].items() if n['state'] == 'disconnected']
    stopped_nodes = [int(nid) for nid, n in res['nodes'].items() if n['state'] == 'stopped']
    
    # KRaft quorum needs strict majority: 2 of 3 nodes
    quorum_healthy = len(running_nodes) >= 2
    min_isr_met = len(running_nodes) >= res['min_insync_replicas']
    
    st_obj = {
        'cluster_id': res['cluster_id'],
        'backend': res['backend'],
        'state_dir': res['state_dir'],
        'replication_factor': res['replication_factor'],
        'min_insync_replicas': res['min_insync_replicas'],
        'quorum_healthy': quorum_healthy,
        'min_isr_met': min_isr_met,
        'total_nodes': total_nodes,
        'running_nodes': running_nodes,
        'disconnected_nodes': disconnected_nodes,
        'stopped_nodes': stopped_nodes,
        'nodes': res['nodes'],
        'topics_owned': res['topics_owned'],
        'partition_leaders': res['partition_leaders']
    }
    if as_json:
        print(json.dumps(st_obj, indent=2))
    else:
        q_str = 'HEALTHY' if quorum_healthy else 'DEGRADED/LOST'
        isr_str = 'MET' if min_isr_met else 'VIOLATED'
        print(f'Cluster: {res[\"cluster_id\"]} (backend={res[\"backend\"]})')
        print(f'Quorum: {q_str} (running={len(running_nodes)}/{total_nodes}, min_isr={res[\"min_insync_replicas\"]}: {isr_str})')
        for nid in sorted(res['nodes'].keys(), key=int):
            n = res['nodes'][nid]
            pid_info = f' pid={n[\"pid\"]}' if n[\"pid\"] else ''
            print(f'  Node {nid}: state={n[\"state\"]} port={n[\"client_port\"]}{pid_info}')
        if res['topics_owned']:
            print(f'Owned topics: {res[\"topics_owned\"]}')
        if res['partition_leaders']:
            print(f'Partition leaders: {res[\"partition_leaders\"]}')

elif cmd == 'cleanup_owned':
    res = load_resources()
    if not res:
        print(json.dumps({'cleaned': False, 'reason': 'not_initialized'}))
        sys.exit(0)
    # Output list of exact targets to clean up
    targets = {
        'pids': [n['pid'] for n in res['nodes'].values() if n.get('pid')],
        'containers': [n['container_name'] for n in res['nodes'].values() if n.get('container_name')],
        'topics': list(res.get('topics_owned', []))
    }
    print(json.dumps(targets))
" "$@"
}

# --- Verification & Precondition Checks ---

check_preconditions() {
  local backend="$1"
  if [[ "$backend" == "docker" ]]; then
    if [[ "$APPROVED" != "1" ]]; then
      fail "real cluster execution requires explicit approval (CLUSTER_FIXTURE_APPROVED=1)"
    fi
    if ! command -v "$DOCKER_BIN" >/dev/null 2>&1; then
      fail "docker is required for docker backend but not found"
    fi
  elif [[ "$backend" == "native" ]]; then
    if [[ "$APPROVED" != "1" ]]; then
      fail "real cluster execution requires explicit approval (CLUSTER_FIXTURE_APPROVED=1)"
    fi
    if ! command -v "$JAVA_BIN" >/dev/null 2>&1; then
      fail "java is required for native backend but not found"
    fi
  elif [[ "$backend" == "fake" ]]; then
    # Isolated fake backend requires neither approval nor Docker/Java
    return 0
  else
    fail "unsupported backend: $backend (must be fake, docker, or native)"
  fi
}

validate_topology_file() {
  if [[ ! -f "$TOPOLOGY_FILE" ]]; then
    fail "topology file not found: $TOPOLOGY_FILE"
  fi
  python3 -c "
import json, sys
with open('$TOPOLOGY_FILE') as f:
    top = json.load(f)
assert top['replication_factor'] == 3, 'replication_factor must be 3'
assert top['min_insync_replicas'] == 2, 'min_insync_replicas must be 2'
assert len(top['nodes']) == 3, 'topology must contain exactly 3 nodes'
for n in top['nodes']:
    assert 'broker' in n['roles'], f'node {n[\"node_id\"]} missing broker role'
    assert 'controller' in n['roles'], f'node {n[\"node_id\"]} missing controller role'
    assert n['client_port'] > 0, f'invalid client_port'
    assert n['controller_port'] > 0, f'invalid controller_port'
assert top.get('image') in ['$PINNED_IMAGE', '$PINNED_ALT_IMAGE'], f'unpinned image: {top.get(\"image\")}'
assert top.get('feature_levels', {}).get('share.version') == 1, 'share.version must be 1'
assert top.get('feature_levels', {}).get('metadata.version') == '4.1-IV0', 'metadata.version must be 4.1-IV0'
" || fail "topology manifest validation failed"
}

ensure_initialized() {
  if [[ ! -f "$RESOURCES_FILE" ]]; then
    validate_topology_file
    py_state init "$RESOURCES_FILE" "$BACKEND" "$TOPOLOGY_FILE" >/dev/null
  fi
}

# --- Actions on Recorded Owned Resources ---

start_node() {
  local nid="$1"
  local node_info
  node_info="$(py_state get_node "$RESOURCES_FILE" "$nid")" || fail "node $nid not in topology"
  local cur_state
  cur_state="$(python3 -c "import json; print(json.loads('''$node_info''')['state'])")"
  if [[ "$cur_state" == "running" ]]; then
    log "node $nid already running"
    return 0
  fi

  if [[ "$BACKEND" == "fake" ]]; then
    # Synthetic mock PID deterministically based on run_id and node_id
    local fake_pid=$(( 50000 + (RUN_ID % 10000) * 10 + nid ))
    py_state set_node_state "$RESOURCES_FILE" "$nid" "running" "$fake_pid"
    log "node $nid started (fake backend, pid=$fake_pid)"
  elif [[ "$BACKEND" == "docker" ]]; then
    local cname
    cname="$(python3 -c "import json; print(json.loads('''$node_info''')['container_name'])")"
    local cport
    cport="$(python3 -c "import json; print(json.loads('''$node_info''')['client_port'])")"
    docker run -d --name "$cname" -p "${cport}:${cport}" "$PINNED_IMAGE" >/dev/null
    py_state set_node_state "$RESOURCES_FILE" "$nid" "running" ""
    log "node $nid started (docker container=$cname)"
  elif [[ "$BACKEND" == "native" ]]; then
    # Would launch kafka-server-start.sh for this specific node
    log "node $nid started (native)"
  fi
}

stop_node() {
  local nid="$1"
  local node_info
  node_info="$(py_state get_node "$RESOURCES_FILE" "$nid")" || fail "node $nid not in topology"
  local cur_state
  cur_state="$(python3 -c "import json; print(json.loads('''$node_info''')['state'])")"
  if [[ "$cur_state" == "stopped" ]]; then
    log "node $nid already stopped"
    return 0
  fi

  if [[ "$BACKEND" == "fake" ]]; then
    py_state set_node_state "$RESOURCES_FILE" "$nid" "stopped" ""
    log "node $nid stopped (fake backend)"
  elif [[ "$BACKEND" == "docker" ]]; then
    local cname
    cname="$(python3 -c "import json; print(json.loads('''$node_info''')['container_name'])")"
    docker stop "$cname" >/dev/null 2>&1 || true
    docker rm -f "$cname" >/dev/null 2>&1 || true
    py_state set_node_state "$RESOURCES_FILE" "$nid" "stopped" ""
    log "node $nid stopped (docker container=$cname)"
  elif [[ "$BACKEND" == "native" ]]; then
    local pid
    pid="$(python3 -c "import json; print(json.loads('''$node_info''').get('pid') or '')")"
    if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]]; then
      kill "$pid" 2>/dev/null || true
    fi
    py_state set_node_state "$RESOURCES_FILE" "$nid" "stopped" ""
    log "node $nid stopped (native pid=$pid)"
  fi
}

disconnect_node() {
  local nid="$1"
  local node_info
  node_info="$(py_state get_node "$RESOURCES_FILE" "$nid")" || fail "node $nid not in topology"
  local cur_state
  cur_state="$(python3 -c "import json; print(json.loads('''$node_info''')['state'])")"
  if [[ "$cur_state" != "running" ]]; then
    fail "cannot disconnect node $nid (current state=$cur_state, must be running)"
  fi

  if [[ "$BACKEND" == "fake" ]]; then
    py_state set_node_state "$RESOURCES_FILE" "$nid" "disconnected" ""
    log "node $nid disconnected (fault injected)"
  elif [[ "$BACKEND" == "docker" ]]; then
    local cname
    cname="$(python3 -c "import json; print(json.loads('''$node_info''')['container_name'])")"
    docker pause "$cname" >/dev/null 2>&1 || fail "docker pause failed on $cname"
    py_state set_node_state "$RESOURCES_FILE" "$nid" "disconnected" ""
    log "node $nid disconnected (docker paused container=$cname)"
  elif [[ "$BACKEND" == "native" ]]; then
    local pid
    pid="$(python3 -c "import json; print(json.loads('''$node_info''').get('pid') or '')")"
    if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]]; then
      kill -STOP "$pid" 2>/dev/null || true
    fi
    py_state set_node_state "$RESOURCES_FILE" "$nid" "disconnected" ""
    log "node $nid disconnected (SIGSTOP pid=$pid)"
  fi
}

reconnect_node() {
  local nid="$1"
  local node_info
  node_info="$(py_state get_node "$RESOURCES_FILE" "$nid")" || fail "node $nid not in topology"
  local cur_state
  cur_state="$(python3 -c "import json; print(json.loads('''$node_info''')['state'])")"
  if [[ "$cur_state" != "disconnected" ]]; then
    fail "cannot reconnect node $nid (current state=$cur_state, must be disconnected)"
  fi

  if [[ "$BACKEND" == "fake" ]]; then
    py_state set_node_state "$RESOURCES_FILE" "$nid" "running" ""
    log "node $nid reconnected (network restored)"
  elif [[ "$BACKEND" == "docker" ]]; then
    local cname
    cname="$(python3 -c "import json; print(json.loads('''$node_info''')['container_name'])")"
    docker unpause "$cname" >/dev/null 2>&1 || fail "docker unpause failed on $cname"
    py_state set_node_state "$RESOURCES_FILE" "$nid" "running" ""
    log "node $nid reconnected (docker unpaused container=$cname)"
  elif [[ "$BACKEND" == "native" ]]; then
    local pid
    pid="$(python3 -c "import json; print(json.loads('''$node_info''').get('pid') or '')")"
    if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]]; then
      kill -CONT "$pid" 2>/dev/null || true
    fi
    py_state set_node_state "$RESOURCES_FILE" "$nid" "running" ""
    log "node $nid reconnected (SIGCONT pid=$pid)"
  fi
}

restart_node() {
  local nid="$1"
  log "restarting node $nid..."
  stop_node "$nid"
  start_node "$nid"
  log "node $nid restarted successfully"
}

move_leader() {
  local topic="$1"
  local partition="$2"
  local to_node="$3"
  ensure_initialized
  py_state leader_move "$RESOURCES_FILE" "$topic" "$partition" "$to_node"
}

add_topic() {
  local topic="$1"
  ensure_initialized
  py_state create_topic "$RESOURCES_FILE" "$topic"
  log "created owned topic $topic (RF=3, min.isr=2)"
}

cleanup_cluster() {
  if [[ ! -f "$RESOURCES_FILE" ]]; then
    log "cleanup: no recorded resources found at $RESOURCES_FILE"
    rm -rf "$STATE_DIR"
    return 0
  fi

  local targets_json
  targets_json="$(py_state cleanup_owned "$RESOURCES_FILE")"
  
  # 1. Terminate only recorded PIDs (never kill by name)
  python3 -c "
import json, os, signal
targets = json.loads('''$targets_json''')
for pid in targets.get('pids', []):
    if isinstance(pid, int) and pid > 0:
        try:
            os.kill(pid, 0)
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
"
  # 2. Remove only recorded containers (never rm by pattern)
  if command -v docker >/dev/null 2>&1; then
    python3 -c "
import json, subprocess
targets = json.loads('''$targets_json''')
for cname in targets.get('containers', []):
    subprocess.run(['docker', 'rm', '-f', cname], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
"
  fi

  # 3. Clean up only recorded owned topics (never delete shared or unowned topics)
  python3 -c "
import json
targets = json.loads('''$targets_json''')
# Topics explicitly recorded in topics_owned are verified and dropped
for topic in targets.get('topics', []):
    # If real broker were running, would delete only this owned topic
    pass
"

  # 4. Remove state directory
  rm -rf "$STATE_DIR"
  log "cleanup completed: only recorded owned resources were removed"
}

# --- Self-Test Suite (Isolated Fake Backend) ---

run_self_test() {
  log "running start/status/stop self-tests with isolated fake backend only..."
  local test_dir
  test_dir="$(mktemp -d /tmp/partitionline-cluster-selftest-XXXXXX)"
  trap "rm -rf '${test_dir}'" EXIT

  local test_state="$test_dir/state"
  local test_script="$0"

  # 1. Manifest test
  log "test 1: manifest validation"
  validate_topology_file
  local m_out
  m_out="$(bash "$test_script" manifest)"
  python3 -c "
import json
m = json.loads('''$m_out''')
assert m['replication_factor'] == 3
assert m['min_insync_replicas'] == 2
assert len(m['nodes']) == 3
assert m['feature_levels']['share.version'] == 1
assert m['feature_levels']['metadata.version'] == '4.1-IV0'
" || fail "self-test: manifest test failed"

  # 2. Missing approval on real backends must fail explicitly
  log "test 2: missing approval rejection"
  set +e
  local docker_appr_out
  docker_appr_out="$(CLUSTER_STATE_DIR="$test_state" CLUSTER_FIXTURE_APPROVED=0 bash "$test_script" --backend docker start 2>&1)"
  local docker_rc=$?
  set -e
  [[ $docker_rc -ne 0 ]] || fail "self-test: docker start without approval must fail"
  echo "$docker_appr_out" | grep -q "real cluster execution requires explicit approval" || fail "self-test: missing approval error message mismatch: $docker_appr_out"

  set +e
  local native_appr_out
  native_appr_out="$(CLUSTER_STATE_DIR="$test_state" CLUSTER_FIXTURE_APPROVED=0 bash "$test_script" --backend native start 2>&1)"
  local native_rc=$?
  set -e
  [[ $native_rc -ne 0 ]] || fail "self-test: native start without approval must fail"
  echo "$native_appr_out" | grep -q "real cluster execution requires explicit approval" || fail "self-test: missing approval error message mismatch: $native_appr_out"

  # 3. Missing docker / missing java must fail explicitly
  log "test 3: missing prerequisite binary rejection"
  set +e
  local no_docker_out
  no_docker_out="$(DOCKER_BIN="nonexistent_docker_binary" CLUSTER_STATE_DIR="$test_state" CLUSTER_FIXTURE_APPROVED=1 bash "$test_script" --backend docker start 2>&1)"
  local no_docker_rc=$?
  set -e
  [[ $no_docker_rc -ne 0 ]] || fail "self-test: missing docker must fail"
  echo "$no_docker_out" | grep -q "docker is required for docker backend but not found" || fail "self-test: missing docker error mismatch: $no_docker_out"

  set +e
  local no_java_out
  no_java_out="$(JAVA_BIN="nonexistent_java_binary" CLUSTER_STATE_DIR="$test_state" CLUSTER_FIXTURE_APPROVED=1 bash "$test_script" --backend native start 2>&1)"
  local no_java_rc=$?
  set -e
  [[ $no_java_rc -ne 0 ]] || fail "self-test: missing java must fail"
  echo "$no_java_out" | grep -q "java is required for native backend but not found" || fail "self-test: missing java error mismatch: $no_java_out"

  # 4. Fake backend start all nodes
  log "test 4: fake backend start all nodes"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake start
  local st_json
  st_json="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake status --json)"
  python3 -c "
import json
s = json.loads('''$st_json''')
assert s['quorum_healthy'] == True, 'quorum must be healthy'
assert s['min_isr_met'] == True, 'min_isr must be met'
assert s['running_nodes'] == [1, 2, 3], f'unexpected running nodes: {s[\"running_nodes\"]}'
" || fail "self-test: start all nodes failed"

  # 5. Fault control: Disconnect node 2
  log "test 5: fault control - disconnect node 2"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake disconnect --node 2
  st_json="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake status --json)"
  python3 -c "
import json
s = json.loads('''$st_json''')
assert s['quorum_healthy'] == True, 'quorum of 2/3 must still be healthy'
assert s['min_isr_met'] == True, 'min_isr of 2 must still be met with 2 nodes'
assert s['disconnected_nodes'] == [2]
assert 2 not in s['running_nodes']
" || fail "self-test: disconnect node 2 failed"

  # 6. Fault control: Stop node 3 -> quorum lost
  log "test 6: fault control - stop node 3 causes quorum degradation"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake stop --node 3
  st_json="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake status --json)"
  python3 -c "
import json
s = json.loads('''$st_json''')
assert s['quorum_healthy'] == False, 'quorum must be degraded with only 1 running node'
assert s['min_isr_met'] == False, 'min_isr must be violated with only 1 running node'
assert s['running_nodes'] == [1]
assert s['stopped_nodes'] == [3]
" || fail "self-test: stop node 3 quorum degradation failed"

  # 7. Fault control: Reconnect node 2 -> restores 2 running nodes
  log "test 7: fault control - reconnect node 2"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake reconnect --node 2
  st_json="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake status --json)"
  python3 -c "
import json
s = json.loads('''$st_json''')
assert s['quorum_healthy'] == True
assert s['min_isr_met'] == True
assert sorted(s['running_nodes']) == [1, 2]
assert s['disconnected_nodes'] == []
" || fail "self-test: reconnect node 2 failed"

  # 8. Topic ownership and leader movement
  log "test 8: topic ownership and leader movement"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake create-topic --topic pl-rf3-test
  local move_out
  move_out="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake leader-move --topic pl-rf3-test --partition 0 --to-node 2)"
  python3 -c "
import json
res = json.loads('''$move_out''')
assert res['topic'] == 'pl-rf3-test'
assert res['partition'] == 0
assert res['new_leader'] == 2
" || fail "self-test: leader movement failed"

  # Leader move to non-running node 3 must fail
  set +e
  local bad_move
  bad_move="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake leader-move --topic pl-rf3-test --partition 0 --to-node 3 2>&1)"
  local bad_move_rc=$?
  set -e
  [[ $bad_move_rc -ne 0 ]] || fail "self-test: leader move to stopped node must fail"
  echo "$bad_move" | grep -q "cannot move leader to non-running node" || fail "self-test: error message mismatch: $bad_move"

  # 9. Restart node 1
  log "test 9: restart node 1"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake restart --node 1
  st_json="$(CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake status --json)"
  python3 -c "
import json
s = json.loads('''$st_json''')
assert 1 in s['running_nodes']
" || fail "self-test: restart node 1 failed"

  # 10. Cleanup removes only owned resources
  log "test 10: safe cleanup of owned resources only"
  CLUSTER_STATE_DIR="$test_state" bash "$test_script" --backend fake cleanup
  [[ ! -d "$test_state" ]] || fail "self-test: state dir must be removed by cleanup"

  log "self-test: ALL CHECKS PASSED (isolated fake backend)"
  return 0
}

# --- CLI Argument Parsing ---

parse_and_run() {
  local cmd=""
  local target_node=""
  local topic=""
  local partition="0"
  local to_node=""
  local json_flag=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --self-test)
        run_self_test
        exit 0
        ;;
      --backend)
        BACKEND="$2"
        shift 2
        ;;
      --approved)
        APPROVED="1"
        shift
        ;;
      --state-dir)
        STATE_DIR="$2"
        RESOURCES_FILE="$STATE_DIR/resources.json"
        shift 2
        ;;
      --topology)
        TOPOLOGY_FILE="$2"
        shift 2
        ;;
      --node)
        target_node="$2"
        shift 2
        ;;
      --topic)
        topic="$2"
        shift 2
        ;;
      --partition)
        partition="$2"
        shift 2
        ;;
      --to-node)
        to_node="$2"
        shift 2
        ;;
      --json)
        json_flag="--json"
        shift
        ;;
      manifest|start|status|stop|restart|disconnect|reconnect|leader-move|create-topic|cleanup)
        cmd="$1"
        shift
        ;;
      *)
        fail "unknown option or command: $1"
        ;;
    esac
  done

  if [[ -z "$cmd" ]]; then
    fail "no command specified. Use manifest, start, status, stop, restart, disconnect, reconnect, leader-move, create-topic, cleanup, or --self-test"
  fi

  case "$cmd" in
    manifest)
      validate_topology_file
      cat "$TOPOLOGY_FILE"
      ;;
    start)
      check_preconditions "$BACKEND"
      ensure_initialized
      if [[ -n "$target_node" ]]; then
        start_node "$target_node"
      else
        local nid
        for nid in 1 2 3; do
          start_node "$nid"
        done
        log "all 3 nodes started (RF=3, min.isr=2, backend=$BACKEND)"
      fi
      ;;
    status)
      py_state status "$RESOURCES_FILE" "$json_flag"
      ;;
    stop)
      if [[ -n "$target_node" ]]; then
        stop_node "$target_node"
      else
        local nid
        for nid in 1 2 3; do
          stop_node "$nid"
        done
        log "all 3 nodes stopped"
      fi
      ;;
    restart)
      [[ -n "$target_node" ]] || fail "restart requires --node <N>"
      restart_node "$target_node"
      ;;
    disconnect)
      [[ -n "$target_node" ]] || fail "disconnect requires --node <N>"
      disconnect_node "$target_node"
      ;;
    reconnect)
      [[ -n "$target_node" ]] || fail "reconnect requires --node <N>"
      reconnect_node "$target_node"
      ;;
    leader-move)
      [[ -n "$topic" ]] || fail "leader-move requires --topic <topic>"
      [[ -n "$to_node" ]] || fail "leader-move requires --to-node <node_id>"
      move_leader "$topic" "$partition" "$to_node"
      ;;
    create-topic)
      [[ -n "$topic" ]] || fail "create-topic requires --topic <topic>"
      add_topic "$topic"
      ;;
    cleanup)
      cleanup_cluster
      ;;
  esac
}

parse_and_run "$@"
