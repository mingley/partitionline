#!/usr/bin/env python3
"""Run the KL03-21 group-protocol churn campaign.

Requires CLUSTER_FIXTURE_APPROVED=1 and native Kafka 4.1.0 for --run.
--self-test is offline: frozen plan + history checker only.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from group_churn_cases import (  # noqa: E402
    DEFAULT_FREEZE,
    PROFILE,
    cell_by_id,
    cells,
    evaluate_cell,
    load_freeze,
    record_id,
    synthetic_history,
    validate_freeze,
)

CHECKER = REPO_ROOT / "scripts" / "check-record-history.py"
FIXTURE = REPO_ROOT / "scripts" / "cluster-fixture.sh"
SASL_NODE = REPO_ROOT / "scripts" / "sasl-plain-node.sh"
EXAMPLE_NAME = "group_churn"
CRASH_EXIT = 77


def run_cmd(
    args: List[str],
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        cwd=str(REPO_ROOT),
        env=env or os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )


def check_history(path: Path) -> Dict[str, Any]:
    proc = run_cmd([sys.executable, str(CHECKER), "--json", str(path)])
    try:
        payload = json.loads(proc.stdout) if proc.stdout.strip() else {}
    except json.JSONDecodeError:
        payload = {"raw_stdout": proc.stdout, "raw_stderr": proc.stderr}
    payload["exit"] = proc.returncode
    payload["valid"] = proc.returncode == 0
    return payload


def self_test(freeze_path: Path) -> int:
    freeze = load_freeze(freeze_path)
    report = validate_freeze(freeze)
    planned = cells(freeze)
    if len(planned) != 20:
        print(f"self-test: expected 20 cells, got {len(planned)}", file=sys.stderr)
        return 1
    tmp = Path(os.environ.get("TMPDIR", "/tmp")) / "kl03-21-self-test"
    tmp.mkdir(parents=True, exist_ok=True)
    for cell in planned:
        good = synthetic_history(cell)
        path = tmp / f"{cell['id'].replace('/', '-')}.json"
        path.write_text(json.dumps(good))
        checked = check_history(path)
        if not checked.get("valid"):
            print(f"self-test: synthetic {cell['id']} failed: {checked}", file=sys.stderr)
            return 1
        bad = synthetic_history(cell, duplicate=True)
        bad_path = tmp / f"{cell['id'].replace('/', '-')}-dup.json"
        bad_path.write_text(json.dumps(bad))
        if check_history(bad_path).get("valid"):
            print(f"self-test: duplicate history for {cell['id']} must not pass", file=sys.stderr)
            return 1
    fixture = run_cmd(["bash", str(FIXTURE), "--self-test"])
    if fixture.returncode != 0:
        print(fixture.stderr, file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "profile": PROFILE,
                "cell_count": report["cell_count"],
                "profiles": report["profiles"],
                "churns": report["churns"],
                "replay_cell": report["replay_cell"],
                "distinct_budgets": report["distinct_budgets"],
                "checker_good": True,
                "checker_duplicate_rejected": True,
                "fixture_self_test": True,
            }
        )
    )
    return 0


def fixture_env(state_dir: Path, freeze: Dict[str, Any]) -> Dict[str, str]:
    env = os.environ.copy()
    env["CLUSTER_FIXTURE_APPROVED"] = "1"
    env["CLUSTER_FIXTURE_BACKEND"] = "native"
    env["CLUSTER_STATE_DIR"] = str(state_dir)
    env["KAFKA_HOME"] = freeze["peer"]["kafka_home"]
    env["TOPOLOGY_FILE"] = str(REPO_ROOT / freeze["peer"]["topology"])
    return env


def fixture(env: Dict[str, str], *args: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return run_cmd(["bash", str(FIXTURE), "--backend", "native", *args], env=env, timeout=timeout)


def sasl_env(state_dir: Path, freeze: Dict[str, Any]) -> Dict[str, str]:
    env = os.environ.copy()
    env["CLUSTER_FIXTURE_APPROVED"] = "1"
    env["CLUSTER_STATE_DIR"] = str(state_dir)
    env["KAFKA_HOME"] = freeze["peer"]["kafka_home"]
    secure = freeze["peer"]["secure_peer"]
    env["SASL_PLAIN_USER"] = secure["username"]
    env["SASL_PLAIN_PASS"] = secure["password"]
    return env


def sasl_node(env: Dict[str, str], *args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return run_cmd(["bash", str(SASL_NODE), *args], env=env, timeout=timeout)


def build_example() -> Path:
    proc = run_cmd(
        ["cargo", "+stable", "build", "--locked", "--example", EXAMPLE_NAME],
        timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"example build failed:\n{proc.stdout}\n{proc.stderr}")
    path = REPO_ROOT / "target" / "debug" / "examples" / EXAMPLE_NAME
    if not path.is_file():
        raise RuntimeError(f"example binary missing at {path}")
    return path


def wait_for(path: Path, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.04)
    return False


def cell_names(cell: Dict[str, Any]) -> Dict[str, str]:
    proto, churn = cell["protocol"], cell["churn"]
    return {
        "topic": f"pl-grp-{proto}-{churn.replace('_', '-')}",
        "group": f"pl-grp-{proto}-{churn.replace('_', '-')}-g",
    }


def base_env(
    env: Dict[str, str],
    freeze: Dict[str, Any],
    cell: Dict[str, Any],
    names: Dict[str, str],
    bootstrap: str,
    *,
    secure: bool,
) -> Dict[str, str]:
    trial = env.copy()
    budgets = freeze["budgets"]
    trial.update(
        {
            "KAFKA_BOOTSTRAP": bootstrap,
            "KAFKA_TOPIC": names["topic"],
            "KAFKA_GROUP": names["group"],
            "GROUP_CHURN_PROFILE": cell["protocol"],
            "GROUP_CHURN_CHURN": cell["churn"],
            "GROUP_CHURN_RECORDS": str(freeze.get("records", 6)),
            "GROUP_CHURN_RF": "1" if secure else "3",
            "GROUP_CHURN_SESSION_MS": str(budgets["session_timeout_ms"]),
            "GROUP_CHURN_HEARTBEAT_MS": str(budgets["heartbeat_interval_ms"]),
            "GROUP_CHURN_MAX_POLL_MS": str(budgets["max_poll_interval_ms"]),
            "GROUP_CHURN_SHARE_LOCK_MS": str(budgets["share_lock_hold_ms"]),
            "GROUP_CHURN_VERIFY_MS": str(budgets["verify_wait_ms"]),
        }
    )
    if secure:
        peer = freeze["peer"]["secure_peer"]
        trial["KAFKA_USERNAME"] = peer["username"]
        trial["KAFKA_PASSWORD"] = peer["password"]
    return trial


def run_phase(binary: Path, env: Dict[str, str], phase: str, timeout: float) -> subprocess.CompletedProcess:
    trial = env.copy()
    trial["GROUP_CHURN_PHASE"] = phase
    return run_cmd([str(binary)], env=trial, timeout=int(timeout))


def start_member(binary: Path, env: Dict[str, str], member: str, paths: Dict[str, Path]) -> subprocess.Popen:
    trial = env.copy()
    trial["GROUP_CHURN_PHASE"] = "member"
    trial["GROUP_CHURN_MEMBER"] = member
    trial["GROUP_CHURN_READY"] = str(paths[f"ready_{member}"])
    trial["GROUP_CHURN_GO"] = str(paths[f"go_{member}"])
    trial["GROUP_CHURN_HISTORY"] = str(paths[f"hist_{member}"])
    trial["GROUP_CHURN_PAUSED"] = str(paths["paused"])
    trial["GROUP_CHURN_RESUME"] = str(paths["resume"])
    trial["GROUP_CHURN_LOST"] = str(paths["lost"])
    trial["GROUP_CHURN_RECOVERED"] = str(paths["recovered"])
    return subprocess.Popen(
        [str(binary)],
        cwd=str(REPO_ROOT),
        env=trial,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def share_recover_and_produce(
    binary: Path,
    trial: Dict[str, str],
    paths: Dict[str, Path],
    procs: Dict[str, subprocess.Popen],
    result: Dict[str, Any],
    trial_s: float,
) -> Optional[Dict[str, Any]]:
    paths["lost"].write_text("lost\n")
    if not wait_for(paths["recovered"], 25):
        for proc in procs.values():
            if proc.poll() is None:
                proc.kill()
        result["ok"] = False
        result["error"] = "recover_timeout_a"
        return result
    produced = run_phase(binary, trial, "produce", trial_s)
    result["produce_exit"] = produced.returncode
    if produced.returncode != 0:
        for proc in procs.values():
            if proc.poll() is None:
                proc.kill()
        result["ok"] = False
        result["error"] = "produce_failed"
        result["stderr"] = (produced.stderr or "")[-2000:]
        return result
    return None


def find_coordinator(freeze: Dict[str, Any], bootstrap: str, group: str, share: bool) -> Optional[int]:
    home = Path(freeze["peer"]["kafka_home"])
    script = home / "bin" / ("kafka-share-groups.sh" if share else "kafka-consumer-groups.sh")
    args = [str(script), "--bootstrap-server", bootstrap, "--group", group, "--describe"]
    if not share:
        args.append("--state")
    proc = run_cmd(args, timeout=30)
    text = (proc.stdout or "") + "\n" + (proc.stderr or "")
    for token in text.replace("(", " ").replace(")", " ").split():
        if token.isdigit() and token in {"1", "2", "3"}:
            return int(token)
    return None


def assemble(
    cell: Dict[str, Any],
    topic: str,
    member_payloads: List[Dict[str, Any]],
    records: int,
) -> Dict[str, Any]:
    attempted = []
    for i in range(records):
        attempted.append(
            {
                "id": record_id(i),
                "topic": topic,
                "partition": i % 3,
                "key": record_id(i),
                "payload": record_id(i),
                "status": "acked",
            }
        )
    consumed = []
    seen = set()
    for payload in member_payloads:
        if payload.get("uncommitted"):
            continue
        for rec in payload.get("consumed") or []:
            rec_id = str(rec.get("id"))
            if rec_id in seen:
                continue
            seen.add(rec_id)
            item = dict(rec)
            if cell["protocol"] == "share":
                item.setdefault("ack_type", "accept")
            consumed.append(item)
    return {
        "id": cell["id"],
        "config": {
            "acks": -1,
            "idempotent": True,
            "transactional": False,
            "isolation_level": "read_uncommitted",
            "delivery": "share" if cell["protocol"] == "share" else "partition",
        },
        "attempted": attempted,
        "consumed": consumed,
    }


def run_one_cell(
    env: Dict[str, str],
    freeze: Dict[str, Any],
    cell: Dict[str, Any],
    binary: Path,
    work: Path,
    bootstrap: str,
    *,
    secure: bool,
    sasl_state: Optional[Path] = None,
) -> Dict[str, Any]:
    names = cell_names(cell)
    control = work / "control" / cell["protocol"]
    histories = work / "histories"
    control.mkdir(parents=True, exist_ok=True)
    histories.mkdir(parents=True, exist_ok=True)
    slug = cell["id"].replace("/", "-")
    paths = {
        "ready_a": control / f"{slug}-a.ready",
        "ready_b": control / f"{slug}-b.ready",
        "go_a": control / f"{slug}-a.go",
        "go_b": control / f"{slug}-b.go",
        "hist_a": histories / f"{slug}-a.member.json",
        "hist_b": histories / f"{slug}-b.member.json",
        "paused": control / f"{slug}.paused",
        "resume": control / f"{slug}.resume",
        "lost": control / f"{slug}.lost",
        "recovered": control / f"{slug}.recovered",
        "history": histories / f"{slug}.json",
    }
    for path in paths.values():
        if path.exists():
            path.unlink()
    trial = base_env(env, freeze, cell, names, bootstrap, secure=secure)
    budgets = freeze["budgets"]
    trial_s = budgets["trial_deadline_ms"] / 1000.0
    started = time.monotonic()
    result: Dict[str, Any] = {"id": cell["id"], "protocol": cell["protocol"], "churn": cell["churn"]}

    seed_trial = trial.copy()
    if cell["protocol"] == "share":
        # Default share.auto.offset.reset is latest. Create the topic, join so
        # the first fetch initializes at log-start, then produce.
        seed_trial["GROUP_CHURN_SKIP_PRODUCE"] = "1"
    seed = run_phase(binary, seed_trial, "seed", trial_s)
    result["seed_exit"] = seed.returncode
    if seed.returncode != 0:
        result["ok"] = False
        result["error"] = "seed_failed"
        result["stderr"] = (seed.stderr or "")[-2000:]
        return result

    members = ["a"]
    if cell["two_members"]:
        members.append("b")
    # Start A first so the survivor has an assignment before B joins.
    procs = {"a": start_member(binary, trial, "a", paths)}
    if not wait_for(paths["ready_a"], 25):
        for proc in procs.values():
            proc.kill()
        result["ok"] = False
        result["error"] = "ready_timeout_a"
        return result
    if "b" in members:
        procs["b"] = start_member(binary, trial, "b", paths)
        if not wait_for(paths["ready_b"], 25):
            for proc in procs.values():
                proc.kill()
            result["ok"] = False
            result["error"] = "ready_timeout_b"
            return result

    if cell["protocol"] == "share" and cell["churn"] in ("pause", "max_poll_expiry"):
        produced = run_phase(binary, trial, "produce", trial_s)
        result["produce_exit"] = produced.returncode
        if produced.returncode != 0:
            for proc in procs.values():
                proc.kill()
            result["ok"] = False
            result["error"] = "produce_failed"
            result["stderr"] = (produced.stderr or "")[-2000:]
            return result

    finished: Dict[str, Any] = {}
    if cell["churn"] == "member_loss":
        paths["go_b"].write_text('{"action":"die"}\n')
        proc_b = procs["b"]
        try:
            stdout_b, stderr_b = proc_b.communicate(timeout=30)
            rc_b = proc_b.returncode
        except subprocess.TimeoutExpired:
            proc_b.kill()
            stdout_b, stderr_b = proc_b.communicate(timeout=10)
            rc_b = -1
        result["b_exit"] = rc_b
        result["b_stderr"] = (stderr_b or "")[-1500:]
        if rc_b != CRASH_EXIT:
            for proc in procs.values():
                if proc.poll() is None:
                    proc.kill()
            result["ok"] = False
            result["error"] = "expected_member_b_crash"
            result["stdout"] = (stdout_b or "")[-1500:]
            return result
        finished["b"] = True
        if cell["protocol"] == "share":
            recovered = share_recover_and_produce(binary, trial, paths, procs, result, trial_s)
            if recovered is not None:
                return recovered
        paths["go_a"].write_text('{"action":"continue"}\n')
    elif cell["churn"] == "max_poll_expiry":
        paths["go_a"].write_text('{"action":"hold"}\n')
        if "b" in procs:
            paths["go_b"].write_text('{"action":"continue"}\n')
    elif cell["churn"] == "pause":
        paths["go_a"].write_text('{"action":"continue"}\n')
        if not wait_for(paths["paused"], 20):
            for proc in procs.values():
                proc.kill()
            result["ok"] = False
            result["error"] = "pause_timeout"
            return result
        time.sleep(budgets["pause_ms"] / 1000.0)
        paths["resume"].write_text("resume\n")
    elif cell["churn"] == "coordinator_loss":
        node = find_coordinator(freeze, bootstrap, names["group"], cell["protocol"] == "share") or 1
        result["coordinator_node"] = node
        restart = fixture(env, "restart", "--node", str(node), timeout=180)
        result["inject_exit"] = restart.returncode
        time.sleep(2.0)
        if cell["protocol"] == "share":
            recovered = share_recover_and_produce(binary, trial, paths, procs, result, trial_s)
            if recovered is not None:
                return recovered
        paths["go_a"].write_text('{"action":"continue"}\n')
    elif cell["churn"] == "secure_reconnect":
        senv = sasl_env(sasl_state or Path("/tmp/partitionline-kl03-21-sasl"), freeze)
        restart = sasl_node(senv, "restart", timeout=120)
        result["inject_exit"] = restart.returncode
        time.sleep(2.0)
        if cell["protocol"] == "share":
            recovered = share_recover_and_produce(binary, trial, paths, procs, result, trial_s)
            if recovered is not None:
                return recovered
        paths["go_a"].write_text('{"action":"continue"}\n')
    else:
        paths["go_a"].write_text('{"action":"continue"}\n')

    member_payloads = []
    events: Dict[str, List[str]] = {}
    for member, proc in procs.items():
        if member in finished:
            continue
        try:
            stdout, stderr = proc.communicate(timeout=trial_s)
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate(timeout=10)
            rc = -1
        result[f"{member}_exit"] = rc
        result[f"{member}_stderr"] = (stderr or "")[-1500:]
        if member == "b" and cell["churn"] == "member_loss":
            if rc != CRASH_EXIT:
                result["ok"] = False
                result["error"] = "expected_member_b_crash"
                return result
            continue
        if rc != 0:
            result["ok"] = False
            result["error"] = f"member_{member}_failed"
            result["stdout"] = (stdout or "")[-1500:]
            return result
        if paths[f"hist_{member}"].exists():
            payload = json.loads(paths[f"hist_{member}"].read_text())
            member_payloads.append(payload)
            events[member] = list(payload.get("events") or [])

    assembled = assemble(cell, names["topic"], member_payloads, int(freeze.get("records", 6)))
    committed_ids = [str(r["id"]) for r in assembled["consumed"]]
    verdict = evaluate_cell(cell, committed_ids, events, records=int(freeze.get("records", 6)))
    result["verdict"] = verdict
    result["events"] = events
    if not verdict.get("ok"):
        result["ok"] = False
        result["error"] = "expectation_failed"
        paths["history"].write_text(json.dumps(assembled, indent=2) + "\n")
        return result
    paths["history"].write_text(json.dumps(assembled, indent=2) + "\n")
    checked = check_history(paths["history"])
    result["history_path"] = str(paths["history"])
    result["checker"] = {"valid": checked.get("valid"), "exit": checked.get("exit")}
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    result["ok"] = bool(checked.get("valid"))
    if not result["ok"]:
        result["error"] = "history_invalid"
        result["checker_detail"] = checked
    return result


def write_report(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def selected_cells(args: argparse.Namespace, freeze: Dict[str, Any]) -> List[Dict[str, Any]]:
    planned = cells(freeze)
    if args.cells:
        wanted = [item.strip() for item in args.cells.split(",") if item.strip()]
        return [cell_by_id(item, freeze) for item in wanted]
    return planned


def run_campaign(args: argparse.Namespace) -> int:
    if os.environ.get("CLUSTER_FIXTURE_APPROVED") != "1":
        print("run requires CLUSTER_FIXTURE_APPROVED=1", file=sys.stderr)
        return 2
    freeze = load_freeze(args.freeze)
    validate_freeze(freeze)
    selected = selected_cells(args, freeze)
    out = Path(args.out)
    state_dir = Path(args.state_dir)
    sasl_state = Path(str(args.state_dir) + "-sasl")
    env = fixture_env(state_dir, freeze)
    senv = sasl_env(sasl_state, freeze)
    bootstrap = json.loads((REPO_ROOT / freeze["peer"]["topology"]).read_text())["bootstrap_servers"]
    secure_bs = freeze["peer"]["secure_peer"]["bootstrap"]
    failed_dir = out / "failed-attempts"
    failed_dir.mkdir(parents=True, exist_ok=True)

    need_plain = any(not c["secure"] for c in selected)
    need_secure = any(c["secure"] for c in selected)
    if need_plain:
        start = fixture(env, "start", timeout=int(freeze["budgets"]["cluster_ready_timeout_ms"] / 1000))
        if start.returncode != 0:
            print(start.stderr, file=sys.stderr)
            return 1
        warmup = fixture(env, "create-topic", "--topic", "pl-grp-warmup", timeout=60)
        if warmup.returncode != 0:
            print(warmup.stderr, file=sys.stderr)
            fixture(env, "cleanup", timeout=60)
            return 1
    if need_secure:
        started = sasl_node(senv, "start", timeout=120)
        if started.returncode != 0:
            print(started.stderr, file=sys.stderr)
            if need_plain:
                fixture(env, "cleanup", timeout=60)
            return 1

    binary = build_example()
    outcomes: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    try:
        for cell in selected:
            bs = secure_bs if cell["secure"] else bootstrap
            used = senv if cell["secure"] else env
            result = run_one_cell(
                used,
                freeze,
                cell,
                binary,
                out,
                bs,
                secure=cell["secure"],
                sasl_state=sasl_state,
            )
            outcomes.append(result)
            if not result.get("ok"):
                failed.append(result)
                (failed_dir / f"{cell['id'].replace('/', '-')}.json").write_text(
                    json.dumps(result, indent=2) + "\n"
                )
                print(f"{cell['id']} FAIL {result.get('error')}", file=sys.stderr)
            else:
                print(f"{cell['id']} ok elapsed_ms={result.get('elapsed_ms')}", file=sys.stderr)
    finally:
        if need_plain:
            fixture(env, "cleanup", timeout=60)
        if need_secure:
            sasl_node(senv, "cleanup", timeout=60)

    report = {
        "card": "KL03-21",
        "profile": PROFILE,
        "peer": freeze["peer"],
        "cell_count": len(selected),
        "ok_count": sum(1 for o in outcomes if o.get("ok")),
        "failed_count": len(failed),
        "budgets": freeze["budgets"],
        "older_brokers": freeze["older_brokers"],
        "outcomes": outcomes,
        "failed": [{"id": f["id"], "error": f.get("error")} for f in failed],
    }
    write_report(out / "campaign-report.json", report)
    print(json.dumps({k: report[k] for k in ("card", "profile", "ok_count", "failed_count", "cell_count")}, indent=2))
    return 0 if not failed and len(outcomes) == len(selected) else 1


def replay_cell(args: argparse.Namespace) -> int:
    if os.environ.get("CLUSTER_FIXTURE_APPROVED") != "1":
        print("replay requires CLUSTER_FIXTURE_APPROVED=1", file=sys.stderr)
        return 2
    freeze = load_freeze(args.freeze)
    cell_id = args.replay if args.replay else freeze["replay_cell"]
    cell = cell_by_id(cell_id, freeze)
    out = Path(args.out) / "replay"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    state_dir = Path(str(args.state_dir) + "-replay")
    if state_dir.exists():
        shutil.rmtree(state_dir)
    env = fixture_env(state_dir, freeze)
    bootstrap = json.loads((REPO_ROOT / freeze["peer"]["topology"]).read_text())["bootstrap_servers"]
    start = fixture(env, "start", timeout=int(freeze["budgets"]["cluster_ready_timeout_ms"] / 1000))
    if start.returncode != 0:
        print(start.stderr, file=sys.stderr)
        return 1
    try:
        binary = build_example()
        result = run_one_cell(env, freeze, cell, binary, out, bootstrap, secure=False)
    finally:
        fixture(env, "cleanup", timeout=60)
    write_report(out / "replay-report.json", result)
    print(json.dumps({"replay_cell": cell["id"], "ok": result.get("ok"), "error": result.get("error")}, indent=2))
    return 0 if result.get("ok") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, default=DEFAULT_FREEZE)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--replay", nargs="?", const="", default=None)
    parser.add_argument("--cells", type=str, default=None, help="comma-separated proto/churn ids")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "docs" / "evidence" / "groups")
    parser.add_argument("--state-dir", type=Path, default=Path("/tmp/partitionline-kl03-21"))
    args = parser.parse_args()
    if args.self_test:
        return self_test(args.freeze)
    if args.replay is not None:
        return replay_cell(args)
    if args.run:
        return run_campaign(args)
    parser.error("specify --self-test, --run, or --replay [CELL]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
