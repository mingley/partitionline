#!/usr/bin/env python3
"""Run the KL03-20 transactional crash and fencing campaign.

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

from txn_crash_cases import (  # noqa: E402
    DEFAULT_FREEZE,
    PROFILE,
    assemble_history,
    case_by_id,
    cases,
    evaluate_observation,
    load_freeze,
    synthetic_history,
    validate_freeze,
)

CHECKER = REPO_ROOT / "scripts" / "check-record-history.py"
FIXTURE = REPO_ROOT / "scripts" / "cluster-fixture.sh"
EXAMPLE_NAME = "txn_crash"
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
    if proc.returncode != 0:
        payload.setdefault("stderr", proc.stderr)
    return payload


def self_test(freeze_path: Path) -> int:
    freeze = load_freeze(freeze_path)
    report = validate_freeze(freeze)
    tmp = Path(os.environ.get("TMPDIR", "/tmp")) / "kl03-20-self-test"
    tmp.mkdir(parents=True, exist_ok=True)
    for case in cases(freeze):
        variants = [False]
        if case.get("expect_output") == "atomic":
            variants = [False, True]
        for atomic_committed in variants:
            good = synthetic_history(case, atomic_committed=atomic_committed)
            path = tmp / f"{case['id']}-{'commit' if atomic_committed else 'abort'}.json"
            path.write_text(json.dumps(good))
            checked = check_history(path)
            if not checked.get("valid"):
                print(
                    f"self-test: synthetic {case['id']} atomic_committed={atomic_committed} failed: {checked}",
                    file=sys.stderr,
                )
                return 1
        if case.get("expect_output") in ("anchor_only", "atomic") and not case.get("fence"):
            leaked = synthetic_history(case, leak_aborted=True)
            bad = tmp / f"{case['id']}-leak.json"
            bad.write_text(json.dumps(leaked))
            leaked_check = check_history(bad)
            if leaked_check.get("valid"):
                print(f"self-test: leaked aborted output for {case['id']} must not pass", file=sys.stderr)
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
                "case_count": report["case_count"],
                "crashes": report["crashes"],
                "replay_case": report["replay_case"],
                "transaction_timeout_ms": report["transaction_timeout_ms"],
                "leader_recovery_ms": report["leader_recovery_ms"],
                "checker_good": True,
                "checker_abort_leak_rejected": True,
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


def case_names(case_id: str) -> Dict[str, str]:
    slug = case_id.replace("_", "-")
    return {
        "input": f"pl-txn-{slug}-in",
        "output": f"pl-txn-{slug}-out",
        "group": f"pl-txn-{slug}-g",
        "txn_id": f"pl-txn-{slug}-t",
    }


def phase_env(
    env: Dict[str, str],
    freeze: Dict[str, Any],
    case: Dict[str, Any],
    names: Dict[str, str],
    bootstrap: str,
    history: Path,
    ready: Path,
) -> Dict[str, str]:
    trial = env.copy()
    budgets = freeze["budgets"]
    trial.update(
        {
            "KAFKA_BOOTSTRAP": bootstrap,
            "KAFKA_TOPIC": names["input"],
            "KAFKA_OUTPUT_TOPIC": names["output"],
            "KAFKA_GROUP": names["group"],
            "KAFKA_TRANSACTIONAL_ID": names["txn_id"],
            "TXN_CRASH_CASE": case["id"],
            "TXN_CRASH_AT": str(case.get("crash") or ""),
            "TXN_CRASH_HISTORY": str(history),
            "TXN_CRASH_READY": str(ready),
            "TXN_TIMEOUT_MS": str(budgets["transaction_timeout_ms"]),
            "TXN_REQUEST_MS": str(budgets["request_timeout_ms"]),
            "TXN_VERIFY_MS": str(budgets["verify_wait_ms"]),
            "TXN_INPUT_RECORDS": str(freeze.get("input_records", 2)),
        }
    )
    return trial


def run_phase(
    binary: Path,
    env: Dict[str, str],
    phase: str,
    timeout: float,
) -> subprocess.CompletedProcess:
    trial = env.copy()
    trial["TXN_CRASH_PHASE"] = phase
    return run_cmd([str(binary)], env=trial, timeout=int(timeout))


def java_console_consume(freeze: Dict[str, Any], bootstrap: str, topic: str, timeout_ms: int) -> Dict[str, Any]:
    home = Path(freeze["peer"]["kafka_home"])
    script = home / "bin" / "kafka-console-consumer.sh"
    proc = run_cmd(
        [
            str(script),
            "--bootstrap-server",
            bootstrap,
            "--topic",
            topic,
            "--from-beginning",
            "--isolation-level",
            "read_committed",
            "--timeout-ms",
            str(timeout_ms),
            "--max-messages",
            "16",
        ],
        timeout=max(10, timeout_ms // 1000 + 10),
    )
    payloads = [line for line in (proc.stdout or "").splitlines() if line != ""]
    return {
        "exit": proc.returncode,
        "payloads": payloads,
        "stderr": (proc.stderr or "")[-2000:],
    }


def java_group_offset(
    freeze: Dict[str, Any],
    bootstrap: str,
    group: str,
    topic: str,
) -> Dict[str, Any]:
    home = Path(freeze["peer"]["kafka_home"])
    script = home / "bin" / "kafka-consumer-groups.sh"
    proc = run_cmd(
        [
            str(script),
            "--bootstrap-server",
            bootstrap,
            "--group",
            group,
            "--describe",
        ],
        timeout=30,
    )
    offset = -1
    for line in (proc.stdout or "").splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[1] == topic and parts[2] == "0":
            current = parts[3]
            offset = -1 if current == "-" else int(current)
            break
    return {
        "exit": proc.returncode,
        "committed_offset": offset,
        "stdout": (proc.stdout or "")[-2000:],
        "stderr": (proc.stderr or "")[-2000:],
    }


def run_one_case(
    env: Dict[str, str],
    freeze: Dict[str, Any],
    case: Dict[str, Any],
    binary: Path,
    work: Path,
    bootstrap: str,
) -> Dict[str, Any]:
    names = case_names(case["id"])
    control = work / "control"
    histories = work / "histories"
    control.mkdir(parents=True, exist_ok=True)
    histories.mkdir(parents=True, exist_ok=True)
    ready = control / f"{case['id']}.ready"
    observe = histories / f"{case['id']}.observe.json"
    history = histories / f"{case['id']}.json"
    if ready.exists():
        ready.unlink()
    trial = phase_env(env, freeze, case, names, bootstrap, observe, ready)
    budgets = freeze["budgets"]
    trial_s = budgets["trial_deadline_ms"] / 1000.0
    started = time.monotonic()
    result: Dict[str, Any] = {
        "id": case["id"],
        "crash": case.get("crash"),
        "fence": bool(case.get("fence")),
        "names": names,
    }

    seed = run_phase(binary, trial, "seed", trial_s)
    result["seed_exit"] = seed.returncode
    if seed.returncode != 0:
        result["ok"] = False
        result["error"] = "seed_failed"
        result["stderr"] = (seed.stderr or "")[-2000:]
        return result

    try:
        worker = run_phase(binary, trial, "run", trial_s)
    except subprocess.TimeoutExpired as exc:
        result["ok"] = False
        result["error"] = "run_timeout"
        result["stdout"] = ((exc.stdout or "") if isinstance(exc.stdout, str) else "")[-2000:]
        result["stderr"] = ((exc.stderr or "") if isinstance(exc.stderr, str) else "")[-2000:]
        return result
    result["run_exit"] = worker.returncode
    result["run_stderr"] = (worker.stderr or "")[-2000:]
    expect_crash = case.get("crash") is not None
    if expect_crash:
        if worker.returncode != CRASH_EXIT:
            result["ok"] = False
            result["error"] = "expected_crash_exit"
            return result
    elif worker.returncode != 0:
        result["ok"] = False
        result["error"] = "run_failed"
        return result

    if case.get("crash") == "during_endtxn":
        time.sleep(3.0)

    verify = run_phase(binary, trial, "verify", trial_s)
    result["verify_exit"] = verify.returncode
    if verify.returncode != 0 or not observe.exists():
        result["ok"] = False
        result["error"] = "verify_failed"
        result["stderr"] = (verify.stderr or "")[-2000:]
        return result

    observation = json.loads(observe.read_text())
    consumed = observation.get("consumed") or []
    payloads = [str(r.get("payload")) for r in consumed]
    pl_offset = int(observation.get("committed_offset", -1))
    java_out = java_console_consume(
        freeze, bootstrap, names["output"], int(budgets["verify_wait_ms"])
    )
    java_off = java_group_offset(freeze, bootstrap, names["group"], names["input"])
    result["partitionline_payloads"] = payloads
    result["java_payloads"] = java_out["payloads"]
    result["partitionline_offset"] = pl_offset
    result["java_offset"] = java_off["committed_offset"]
    if payloads != java_out["payloads"]:
        result["ok"] = False
        result["error"] = "java_consumer_disagree"
        result["java"] = java_out
        return result
    if pl_offset != java_off["committed_offset"]:
        result["ok"] = False
        result["error"] = "java_offset_disagree"
        result["java_offsets"] = java_off
        return result

    verdict = evaluate_observation(
        case,
        payloads,
        pl_offset,
        input_records=int(freeze.get("input_records", 2)),
    )
    result["verdict"] = verdict
    if not verdict.get("ok"):
        result["ok"] = False
        result["error"] = "expectation_failed"
        return result

    assembled = assemble_history(
        case,
        output_topic=names["output"],
        input_topic=names["input"],
        txn_id=names["txn_id"],
        consumed=consumed,
        committed_offset=pl_offset,
        input_records=int(freeze.get("input_records", 2)),
    )
    history.write_text(json.dumps(assembled, indent=2) + "\n")
    checked = check_history(history)
    result["history_path"] = str(history)
    result["checker"] = {
        "valid": checked.get("valid"),
        "exit": checked.get("exit"),
        "summary": checked.get("summary") or checked.get("history_id"),
        "violations": checked.get("violations"),
        "minimal_counterexample": checked.get("minimal_counterexample"),
    }
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
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
    validate_freeze(freeze)
    selected = cases(freeze)
    if args.cases:
        wanted = [item.strip() for item in args.cases.split(",") if item.strip()]
        selected = [case_by_id(item, freeze) for item in wanted]
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

    binary = build_example()
    outcomes: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    try:
        for case in selected:
            result = run_one_case(env, freeze, case, binary, out, bootstrap)
            outcomes.append(result)
            if not result.get("ok"):
                failed.append(result)
                (failed_dir / f"{case['id']}.json").write_text(json.dumps(result, indent=2) + "\n")
                print(f"{case['id']} FAIL {result.get('error')}", file=sys.stderr)
            else:
                print(f"{case['id']} ok elapsed_ms={result.get('elapsed_ms')}", file=sys.stderr)
    finally:
        fixture(env, "cleanup", timeout=60)

    report = {
        "card": "KL03-20",
        "profile": PROFILE,
        "peer": freeze["peer"],
        "freeze": str(args.freeze),
        "case_count": len(selected),
        "ok_count": sum(1 for o in outcomes if o.get("ok")),
        "failed_count": len(failed),
        "transaction_timeout_ms": freeze["budgets"]["transaction_timeout_ms"],
        "leader_recovery_ms": freeze["budgets"]["leader_recovery_ms"],
        "outcomes": outcomes,
        "failed": [{"id": f["id"], "error": f.get("error")} for f in failed],
    }
    write_report(out / "campaign-report.json", report)
    print(
        json.dumps(
            {k: report[k] for k in ("card", "profile", "ok_count", "failed_count", "case_count")},
            indent=2,
        )
    )
    return 0 if not failed and len(outcomes) == len(selected) else 1


def replay_case(args: argparse.Namespace) -> int:
    if os.environ.get("CLUSTER_FIXTURE_APPROVED") != "1":
        print("replay requires CLUSTER_FIXTURE_APPROVED=1", file=sys.stderr)
        return 2
    freeze = load_freeze(args.freeze)
    case_id = args.replay if isinstance(args.replay, str) and args.replay else freeze["replay_case"]
    case = case_by_id(case_id, freeze)
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
        result = run_one_case(env, freeze, case, binary, out, bootstrap)
    finally:
        fixture(env, "cleanup", timeout=60)
    write_report(out / "replay-report.json", result)
    print(json.dumps({"replay_case": case["id"], "ok": result.get("ok"), "error": result.get("error")}, indent=2))
    return 0 if result.get("ok") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, default=DEFAULT_FREEZE)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--replay", nargs="?", const="", default=None, help="replay frozen case (default freeze.replay_case)")
    parser.add_argument("--cases", type=str, default=None, help="comma-separated case ids")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "docs" / "evidence" / "transactions")
    parser.add_argument("--state-dir", type=Path, default=Path("/tmp/partitionline-kl03-20"))
    args = parser.parse_args()
    if args.self_test:
        return self_test(args.freeze)
    if args.replay is not None:
        return replay_case(args)
    if args.run:
        return run_campaign(args)
    parser.error("specify --self-test, --run, or --replay [CASE]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
