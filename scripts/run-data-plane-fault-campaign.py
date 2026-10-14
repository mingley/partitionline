#!/usr/bin/env python3
"""Run the KL03-19 nontransactional data-plane fault campaign.

Requires CLUSTER_FIXTURE_APPROVED=1 and a native Kafka 4.1.0 tree for --run.
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

from data_plane_faults import (  # noqa: E402
    DEFAULT_FREEZE,
    FAULTS,
    PROFILE,
    fault_counts,
    load_freeze,
    plan_all,
    plan_seed,
    synthetic_history,
)

CHECKER = REPO_ROOT / "scripts" / "check-record-history.py"
FIXTURE = REPO_ROOT / "scripts" / "cluster-fixture.sh"
EXAMPLE_NAME = "data_plane_fault"


def run_cmd(args: List[str], env: Optional[Dict[str, str]] = None, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
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
    if proc.returncode != 0:
        payload.setdefault("stderr", proc.stderr)
    return payload


def self_test(freeze_path: Path) -> int:
    freeze = load_freeze(freeze_path)
    plans = plan_all(freeze)
    if len(plans) != 1000:
        print(f"self-test: expected 1000 plans, got {len(plans)}", file=sys.stderr)
        return 1
    counts = fault_counts(plans)
    if set(counts) != set(FAULTS) or min(counts.values()) < 200:
        print(f"self-test: expected >=200 of each fault, got {counts}", file=sys.stderr)
        return 1
    replay = plan_seed(int(freeze["replay_seed"]), freeze)
    good = synthetic_history(replay)
    tmp = Path(os.environ.get("TMPDIR", "/tmp")) / "kl03-19-self-test-good.json"
    tmp.write_text(json.dumps(good))
    good_check = check_history(tmp)
    if not good_check.get("valid"):
        print(f"self-test: valid synthetic history failed: {good_check}", file=sys.stderr)
        return 1
    bad = synthetic_history(replay, swap=True)
    tmp_bad = Path(os.environ.get("TMPDIR", "/tmp")) / "kl03-19-self-test-swap.json"
    tmp_bad.write_text(json.dumps(bad))
    bad_check = check_history(tmp_bad)
    if bad_check.get("valid"):
        print("self-test: swapped history must not pass", file=sys.stderr)
        return 1
    fixture = run_cmd(["bash", str(FIXTURE), "--self-test"])
    if fixture.returncode != 0:
        print(fixture.stdout)
        print(fixture.stderr, file=sys.stderr)
        print("self-test: cluster-fixture --self-test failed", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "profile": PROFILE,
                "plans": len(plans),
                "fault_counts": counts,
                "checker_good": True,
                "checker_swap_rejected": True,
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


def fixture_status(env: Dict[str, str]) -> Dict[str, Any]:
    proc = fixture(env, "status", "--json", timeout=30)
    if not proc.stdout.strip():
        return {"error": proc.stderr}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {"error": proc.stdout, "stderr": proc.stderr}


def restore_cluster(env: Dict[str, str]) -> None:
    status = fixture_status(env)
    for nid in status.get("disconnected_nodes") or []:
        fixture(env, "reconnect", "--node", str(nid), timeout=30)
    for nid in status.get("stopped_nodes") or []:
        fixture(env, "start", "--node", str(nid), timeout=180)


def inject_fault(env: Dict[str, str], plan: Dict[str, Any]) -> Dict[str, Any]:
    started = time.monotonic()
    fault = plan["fault"]
    node = str(plan["node"])
    if fault in ("leader_move", "stale_metadata"):
        proc = fixture(
            env,
            "leader-move",
            "--topic",
            plan["topic"],
            "--partition",
            str(plan["partition"]),
            "--to-node",
            node,
            timeout=60,
        )
    elif fault in ("disconnect", "lost_ack"):
        proc = fixture(env, "disconnect", "--node", node, timeout=30)
    elif fault == "restart":
        proc = fixture(env, "restart", "--node", node, timeout=180)
    else:
        raise ValueError(f"unknown fault {fault}")
    elapsed_ms = int((time.monotonic() - started) * 1000)
    return {
        "fault": fault,
        "node": plan["node"],
        "exit": proc.returncode,
        "elapsed_ms": elapsed_ms,
        "stdout": proc.stdout[-2000:],
        "stderr": proc.stderr[-2000:],
    }


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
        time.sleep(0.02)
    return False


def run_trial(
    env: Dict[str, str],
    binary: Path,
    plan: Dict[str, Any],
    work: Path,
    bootstrap: str,
) -> Dict[str, Any]:
    seed = plan["seed"]
    control = work / "control"
    histories = work / "histories"
    control.mkdir(parents=True, exist_ok=True)
    histories.mkdir(parents=True, exist_ok=True)
    ready = control / f"{seed:04d}.ready"
    go = control / f"{seed:04d}.go"
    history = histories / f"{seed:04d}.json"
    for p in (ready, go):
        if p.exists():
            p.unlink()
    trial_env = env.copy()
    trial_env.update(
        {
            "KAFKA_BOOTSTRAP": bootstrap,
            "KAFKA_TOPIC": plan["topic"],
            "DATA_PLANE_SEED": str(seed),
            "DATA_PLANE_RECORDS": str(plan["records"]),
            "DATA_PLANE_FAULT_AFTER": str(plan["fault_after"]),
            "DATA_PLANE_PARTITION": str(plan["partition"]),
            "DATA_PLANE_READY": str(ready),
            "DATA_PLANE_GO": str(go),
            "DATA_PLANE_HISTORY": str(history),
            "DATA_PLANE_TRIAL_MS": str(plan["budgets"]["trial_deadline_ms"]),
            "DATA_PLANE_REQUEST_MS": str(plan["budgets"]["produce_request_timeout_ms"]),
            "DATA_PLANE_DELIVERY_MS": str(plan["budgets"]["delivery_timeout_ms"]),
        }
    )
    started = time.monotonic()
    proc = subprocess.Popen(
        [str(binary)],
        cwd=str(REPO_ROOT),
        env=trial_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    ready_s = plan["budgets"]["ready_wait_ms"] / 1000.0
    if not wait_for(ready, ready_s):
        proc.kill()
        stdout, stderr = proc.communicate(timeout=10)
        return {
            "seed": seed,
            "ok": False,
            "error": "ready_timeout",
            "stdout": stdout[-2000:],
            "stderr": stderr[-2000:],
        }
    inject = inject_fault(env, plan)
    go.write_text(json.dumps(inject))
    isolation_s = plan["budgets"].get("isolation_ms", 2000) / 1000.0
    restore_started = time.monotonic()
    if plan["fault"] in ("disconnect", "lost_ack"):
        time.sleep(isolation_s)
        restore_cluster(env)
    trial_s = plan["budgets"]["trial_deadline_ms"] / 1000.0
    remaining = max(5.0, trial_s - (time.monotonic() - started))
    try:
        stdout, stderr = proc.communicate(timeout=remaining)
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate(timeout=10)
        rc = -1
    restore_cluster(env)
    restore_ms = int((time.monotonic() - restore_started) * 1000)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    result: Dict[str, Any] = {
        "seed": seed,
        "id": plan["id"],
        "fault": plan["fault"],
        "node": plan["node"],
        "partition": plan["partition"],
        "example_exit": rc,
        "elapsed_ms": elapsed_ms,
        "restore_ms": restore_ms,
        "restoration_target_ms": plan["budgets"]["restoration_target_ms"],
        "inject": inject,
        "history_path": str(history) if history.exists() else None,
        "stdout": (stdout or "")[-2000:],
        "stderr": (stderr or "")[-2000:],
    }
    if rc != 0 or not history.exists():
        result["ok"] = False
        result["error"] = "example_failed" if rc != 0 else "missing_history"
        return result
    checked = check_history(history)
    result["checker"] = {
        "valid": checked.get("valid"),
        "exit": checked.get("exit"),
        "summary": checked.get("summary") or checked.get("history_id"),
        "violations": checked.get("violations"),
        "minimal_counterexample": checked.get("minimal_counterexample"),
    }
    result["ok"] = bool(checked.get("valid"))
    if not result["ok"]:
        result["error"] = "history_invalid"
    return result


def write_report(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def run_campaign(args: argparse.Namespace) -> int:
    if os.environ.get("CLUSTER_FIXTURE_APPROVED") != "1":
        print("run requires CLUSTER_FIXTURE_APPROVED=1", file=sys.stderr)
        return 2
    freeze = load_freeze(args.freeze)
    if args.seeds:
        start_s, end_s = args.seeds.split(":", 1)
        seeds = range(int(start_s), int(end_s) + 1)
    else:
        seeds = range(int(freeze["seeds"]["start"]), int(freeze["seeds"]["end"]) + 1)
    out = Path(args.out)
    state_dir = Path(args.state_dir)
    env = fixture_env(state_dir, freeze)
    bootstrap = json.loads((REPO_ROOT / freeze["peer"]["topology"]).read_text())["bootstrap_servers"]
    failed_dir = out / "failed-attempts"
    failed_dir.mkdir(parents=True, exist_ok=True)
    (out / "histories").mkdir(parents=True, exist_ok=True)

    start = fixture(env, "start", timeout=int(freeze["budgets"]["cluster_ready_timeout_ms"] / 1000))
    if start.returncode != 0:
        print(start.stderr, file=sys.stderr)
        print("native cluster start failed", file=sys.stderr)
        return 1
    topic = fixture(env, "create-topic", "--topic", freeze["topic"], timeout=60)
    if topic.returncode != 0:
        print(topic.stderr, file=sys.stderr)
        fixture(env, "cleanup", timeout=60)
        return 1

    binary = build_example()
    outcomes: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    try:
        for seed in seeds:
            plan = plan_seed(seed, freeze)
            result = run_trial(env, binary, plan, out, bootstrap)
            outcomes.append(result)
            if not result.get("ok"):
                failed.append(result)
                (failed_dir / f"{seed:04d}.json").write_text(json.dumps(result, indent=2) + "\n")
                print(f"seed {seed} FAIL {result.get('error')} fault={plan['fault']}", file=sys.stderr)
            elif seed % 50 == 0:
                print(f"seed {seed} ok fault={plan['fault']}", file=sys.stderr)
    finally:
        fixture(env, "cleanup", timeout=60)

    report = {
        "card": "KL03-19",
        "profile": PROFILE,
        "peer": freeze["peer"],
        "freeze": str(args.freeze),
        "seed_count": len(list(seeds)),
        "ok_count": sum(1 for o in outcomes if o.get("ok")),
        "failed_count": len(failed),
        "fault_counts": fault_counts([plan_seed(s, freeze) for s in seeds]),
        "outcomes": outcomes,
        "failed": [{"seed": f["seed"], "error": f.get("error"), "fault": f.get("fault")} for f in failed],
    }
    write_report(out / "campaign-report.json", report)
    print(json.dumps({k: report[k] for k in ("card", "profile", "ok_count", "failed_count", "seed_count")}, indent=2))
    return 0 if not failed and len(outcomes) >= len(list(seeds)) else 1


def replay_seed(args: argparse.Namespace) -> int:
    if os.environ.get("CLUSTER_FIXTURE_APPROVED") != "1":
        print("replay requires CLUSTER_FIXTURE_APPROVED=1", file=sys.stderr)
        return 2
    freeze = load_freeze(args.freeze)
    seed = int(args.replay)
    plan = plan_seed(seed, freeze)
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
    topic = fixture(env, "create-topic", "--topic", freeze["topic"], timeout=60)
    if topic.returncode != 0:
        fixture(env, "cleanup", timeout=60)
        return 1
    try:
        binary = build_example()
        result = run_trial(env, binary, plan, out, bootstrap)
    finally:
        fixture(env, "cleanup", timeout=60)
    write_report(out / "replay-report.json", result)
    print(json.dumps({"replay_seed": seed, "ok": result.get("ok"), "error": result.get("error")}, indent=2))
    return 0 if result.get("ok") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, default=DEFAULT_FREEZE)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--replay", type=int, default=None)
    parser.add_argument("--seeds", type=str, default=None, help="inclusive start:end, default freeze range")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "docs" / "evidence" / "data-plane")
    parser.add_argument("--state-dir", type=Path, default=Path("/tmp/partitionline-kl03-19"))
    args = parser.parse_args()
    if args.self_test:
        return self_test(args.freeze)
    if args.replay is not None:
        return replay_seed(args)
    if args.run:
        return run_campaign(args)
    parser.error("specify --self-test, --run, or --replay SEED")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
