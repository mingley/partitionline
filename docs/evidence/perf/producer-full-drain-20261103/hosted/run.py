#!/usr/bin/env python3
"""Hosted-only producer drain A/B and diagnostic census; never a qualification."""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import platform
import random
import selectors
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import types
from pathlib import Path

BASE = "45cbff24de76490594970fc962cd04e7be9fafb1"
PATCH_SHA = "aab17e656ec58d263f084bf60055da99e35e4c506317ad8b5042eded1a80491a"
WORKER_PATCH_SHA = "cc21e5d9dde0a076cfbb1f092a4f556ea79eecdd91764affa61f9370d8b8884f"
MIN_FREE_BYTES = 3 * 1024**3 // 2
STOP_FREE_BYTES = 2 * 1024**3
HELPER_SHA = "34ddbf04a4bbead974fad4a451d30c7a56818248bd5b58c88c87bdfc41896da9"
OWNER = None
WATCH_PATHS: list[Path] = []
JOB_DEADLINE = float("inf")
RUN_OUT: Path | None = None
BASE_PRODUCER_SHA = "4f461c00864e3da5e20925796c1f489580a4046202cffd86fefeb64cbd145a32"
CANDIDATE_PRODUCER_SHA = "298d0c6a7a1ea0513d7008a9a492354aa42becc89d89dcb1fbcf495707652c04"
REQUIRED_TESTS = (
    "full_batch_drain_preserves_order_and_delivery_ownership",
    "other_batch_drains_preserve_queue_storage_and_order",
    "batch_drain_rejects_a_count_beyond_the_pending_length",
    "one_topic_produce_groups_share_its_allocation",
    "worker_drain_preserves_accounting_and_delivery_ownership",
)
ASSETS = Path(__file__).resolve().parent
ARCHIVE_PATHS = [
    "Cargo.toml", "Cargo.lock", "clippy.toml", "README.md", "CHANGELOG.md",
    "LICENSE-APACHE", "LICENSE-MIT", "NOTICE", "src", "examples", "tests",
    "benchmarks/nullbroker", "benchmarks/runtime/src/bin/nb-serve.rs",
]
CASES = {
    # Exploration only: a producer ceiling, with counts below qualification floors.
    "one_record": {"PARTITIONS": "1", "BATCH_RECORDS": "1", "LINGER_MS": "0", "MAX_IN_FLIGHT": "1", "COUNT": "20000"},
    "bulk": {"PARTITIONS": "6", "BATCH_RECORDS": "32768", "LINGER_MS": "5", "MAX_IN_FLIGHT": "5", "COUNT": "1000000"},
}
COMMON_ENV = {
    "KAFKA_TOPIC": "drain-census", "PAYLOAD_BYTES": "100", "WARMUP": "10000",
    "WARMUP_SECS": "0", "MEASURE_SECS": "0", "ACKS": "1", "IDEMPOTENT": "0",
    "CONNECTIONS": "1", "COMPRESSION": "none", "BATCH_BYTES": "1048576",
    "MAX_REQUEST_SIZE": "1048576", "BUFFER_MEMORY": "33554432", "PAYLOAD_MODE": "seeded",
    "KEY_MODE": "id", "RECORD_SEED": "1592590337", "RUN_TIMEOUT_MS": "600000",
    "DELIVERY_TIMEOUT_MS": "30000",
}
CENSUS_NAMES = [
    "calls", "zero", "nonzero", "nonempty_full", "eligible", "eligible_records",
    "full_unequal_capacity", "nonzero_partial", "drained_records", "equal_capacity_all_calls",
    "eligible_1", "eligible_2_8", "eligible_9_64", "eligible_65_512",
    "eligible_513_4096", "eligible_4097_plus",
]
BASE_BODY = '''    batch.extend(pending.drain(..n));\n'''
CANDIDATE_BODY = '''    if n != 0 && n == pending.len() && batch.capacity() == pending.capacity() {
        // Transfer the whole allocation when both sides retain their capacity.
        std::mem::swap(&mut batch, pending);
    } else {
        batch.extend(pending.drain(..n));
    }
'''
HELPER = "fn fill_spare(spare: &mut Vec<Pending>, pending: &mut Vec<Pending>, n: usize) -> Vec<Pending> {\n"


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def replace_once(content: str, old: str, new: str, label: str) -> str:
    count = content.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one source anchor, found {count}")
    return content.replace(old, new, 1)


def cargo_env(target: Path) -> dict[str, str]:
    env = os.environ.copy()
    for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET"):
        env.pop(key, None)
    env.update({"CARGO_TARGET_DIR": str(target), "CARGO_BUILD_JOBS": "2", "CARGO_INCREMENTAL": "0"})
    return env


def existing_parent(path: Path) -> Path:
    while not path.exists():
        parent = path.parent
        if parent == path:
            raise RuntimeError(f"no existing ancestor for {path}")
        path = parent
    return path


def check_free(paths: list[Path], reserve: int = 0, minima: dict[str, int] | None = None) -> None:
    for path in paths:
        existing = existing_parent(path)
        available = shutil.disk_usage(existing).free
        if minima is not None:
            key = str(existing)
            minima[key] = min(minima.get(key, available), available)
        required = STOP_FREE_BYTES + reserve
        if available < required:
            raise RuntimeError(f"free-space stop or reserve breached at {existing}: {available} < {required}")


def load_owner():
    path = ASSETS / "candidate-command-ownership.py"
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != HELPER_SHA:
        raise RuntimeError("command-ownership helper hash mismatch")
    module = types.ModuleType("pinned_command_ownership")
    exec(compile(data, str(path), "exec"), module.__dict__)
    return module


def wait_monitored(task, timeout: int, paths: list[Path], minima: dict[str, int]) -> str:
    deadline = time.monotonic() + timeout
    while not task.finished():
        try:
            check_free(paths, minima=minima)
        except RuntimeError:
            return "disk_stop"
        now = time.monotonic()
        if now >= JOB_DEADLINE:
            return "job_deadline"
        if now >= deadline:
            return "timeout"
        time.sleep(0.1)
    check_free(paths, minima=minima)
    return "finished"


def run_logged(command: list[str], cwd: Path, log: Path, *, env: dict[str, str] | None = None, timeout: int = 3600, watch: list[Path] | None = None) -> bool:
    if OWNER is None:
        raise RuntimeError("ownership helper not loaded")
    if time.monotonic() >= JOB_DEADLINE:
        raise TimeoutError("overall job deadline reached")
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    paths = [*WATCH_PATHS, cwd, log.parent, *(watch or [])]
    if env is not None and "CARGO_TARGET_DIR" in env:
        paths.append(Path(env["CARGO_TARGET_DIR"]))
    minima: dict[str, int] = {}
    check_free(paths, minima=minima)
    task = None
    reason = "not_started"
    code: int | str = "not_started"
    with log.open("w") as out:
        out.write(json.dumps({"command": command, "cwd": str(cwd)}) + "\n")
        out.flush()
        try:
            with OWNER.owned_command(command, cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT) as task:
                reason = wait_monitored(task, timeout, paths, minima)
            code = task.cleanup_receipt.get("leader_exit", "missing_exit") if reason == "finished" else reason
        finally:
            receipt = None if task is None else task.cleanup_receipt
            write_json(log.with_suffix(log.suffix + ".receipt.json"), {
                "command": command, "cwd": str(cwd), "stop_reason": reason,
                "minimum_free_bytes": minima, "cleanup_receipt": receipt,
                "elapsed_seconds": (time.monotonic_ns() - started) / 1e9,
            })
        out.write(json.dumps({"exit": code, "wall_seconds": (time.monotonic_ns() - started) / 1e9}) + "\n")
    return code == 0


def verify_attempt(report: dict, broker: dict, case: dict[str, str], diagnostic: bool, census: dict | None) -> list[str]:
    """Reject incomplete or inconsistent runs; null broker never proves delivery durability."""
    errors: list[str] = []
    target = int(case["COUNT"])
    warmup = int(COMMON_ENV["WARMUP"])
    if report.get("run_disposition") != "executed" or report.get("acks") != 1:
        errors.append("producer did not execute with acks=1")
    for field, expected in (("accepted", target), ("acked", target), ("warmup_records", warmup),
                            ("accepted_total", target + warmup), ("acknowledged_total", target + warmup)):
        if report.get(field) != expected:
            errors.append(f"{field} does not match {expected}")
    if report.get("integrity_verified") is not False or report.get("performance_claims_valid") is not False:
        errors.append("diagnostic must remain explicitly unqualified")
    if not isinstance(report.get("elapsed_s"), (int, float)) or report.get("elapsed_s", 0) <= 0:
        errors.append("invalid producer elapsed time")
    settings = report.get("effective_settings")
    expected_settings = {"acks": 1, "linger_ms": int(case["LINGER_MS"]),
                         "batch_size_bytes": 1048576, "batch_records": int(case["BATCH_RECORDS"]),
                         "max_in_flight": int(case["MAX_IN_FLIGHT"]), "idempotence": False,
                         "connections": 1, "payload_bytes": 100, "compression": "none",
                         "payload_mode": "seeded", "key_mode": "id", "record_seed": 1592590337,
                         "buffer_memory_bytes": 33554432, "max_request_size": 1048576,
                         "delivery_timeout_ms": 30000, "run_timeout_ms": 600000,
                         "warmup_requested_records": warmup, "warmup_min_duration_ms": 0,
                         "measure_duration_ms": 0,
                         "explicit_partitions": int(case["PARTITIONS"]), "requested_records": target}
    if not isinstance(settings, dict) or any(settings.get(k) != v for k, v in expected_settings.items()):
        errors.append("effective producer settings do not match scenario")
    if broker.get("label") != "client-ceiling":
        errors.append("missing client-ceiling broker label")
    if broker.get("accepted_records") != target + warmup:
        errors.append("broker accepted count does not match producer")
    failures = broker.get("validation_failures")
    names = {"framing", "crc", "count", "sequence", "transactional"}
    if not isinstance(failures, dict) or set(failures) != names or any(v != 0 for v in failures.values()):
        errors.append("broker validation failed or was incomplete")
    if broker.get("injected_errors") != 0 or broker.get("leader_mismatches") != 0:
        errors.append("broker recorded injected errors or leader mismatches")
    offsets = broker.get("end_offsets")
    partitions = int(case["PARTITIONS"])
    expected_offsets = {f"drain-census/{p}": (warmup // partitions + int(p < warmup % partitions)
                        + target // partitions + int(p < target % partitions)) for p in range(partitions)}
    if offsets != expected_offsets:
        errors.append("broker offsets do not match the per-partition offered counts")
    if diagnostic:
        values = census.get("counts") if isinstance(census, dict) else None
        if not isinstance(values, list) or len(values) != len(CENSUS_NAMES) or any(not isinstance(x, int) or x < 0 for x in values):
            errors.append("missing diagnostic census")
        elif values[0] == 0 or values[3] != values[4] + values[6] or sum(values[10:]) != values[4] or values[0] != values[1] + values[2]:
            errors.append("inconsistent or empty diagnostic census")
    elif census is not None:
        errors.append("instrumented binary used as timing binary")
    return errors


def phase_errors(payloads: list[dict], case: dict[str, str]) -> list[str]:
    warmup = int(COMMON_ENV["WARMUP"])
    expected = [
        {"kind": "phase_end", "phase": "warmup", "accepted_total": warmup,
         "acknowledged_total": warmup, "produce_errors": 0},
        {"kind": "phase_end", "phase": "measure", "accepted_total": warmup + int(case["COUNT"]),
         "acknowledged_total": warmup + int(case["COUNT"]), "produce_errors": 0},
    ]
    observed = [value for value in payloads if value.get("kind") == "phase_end"]
    return [] if observed == expected else ["phase-end acceptance and acknowledgement snapshots do not reconcile"]


def missing_test_successes(log: str) -> list[str]:
    lines = log.splitlines()
    return [name for name in REQUIRED_TESTS
            if not any(line.startswith(f"test producer::tests::{name} ") and line.endswith(" ... ok") for line in lines)]


def trial_order(seed: int = 0x45CB, pairs: int = 5) -> list[tuple[str, int, str]]:
    rng = random.Random(seed)
    plan = []
    for name in CASES:
        for pair in range(pairs):
            arms = ["baseline", "candidate"]
            rng.shuffle(arms)
            plan.extend((name, pair, arm) for arm in arms)
    return plan


def archive_sources(repo: Path, dest: Path, archive: Path, out: Path) -> None:
    listing_log = out / "archive-listing.log"
    if not run_logged(["git", "ls-tree", "-r", "-l", BASE, "--", *ARCHIVE_PATHS], repo, listing_log, timeout=600):
        raise RuntimeError("could not list pinned source tree")
    lines = [line for line in listing_log.read_text().splitlines() if line.startswith(("100", "120"))]
    if not lines:
        raise RuntimeError("pinned source listing is empty")
    size = sum(int(fields[3]) for line in lines if (fields := line.split(maxsplit=4))[3].isdigit())
    # Reserve the tar, both source trees and conservative per-file overhead.
    check_free([archive.parent], reserve=3 * size + 64 * 1024**2)
    command = ["git", "archive", "--format=tar", "--output", str(archive), BASE, *ARCHIVE_PATHS]
    if not run_logged(command, repo, out / "archive.log", timeout=600, watch=[archive.parent]):
        raise RuntimeError("pinned source archive failed or exhausted the free-space floor")
    dest.mkdir(parents=True)
    check_free([dest], reserve=2 * archive.stat().st_size + 32 * 1024**2)
    with tarfile.open(archive) as tf:
        tf.extractall(dest, filter="data")


def make_trees(repo: Path, work: Path, out: Path) -> dict[str, Path]:
    if digest(ASSETS / "candidate.patch") != PATCH_SHA:
        raise RuntimeError("frozen candidate patch hash mismatch")
    if digest(ASSETS / "worker-control.patch") != WORKER_PATCH_SHA:
        raise RuntimeError("frozen worker control patch hash mismatch")
    archive = work / "source.tar"
    trees = {arm: work / arm for arm in ("baseline", "candidate")}
    check_free([repo, work, out])
    archive_sources(repo, trees["baseline"], archive, out)
    check_free([repo, work, out])
    shutil.copytree(trees["baseline"], trees["candidate"])
    check_free([repo, work, out])
    archive.unlink()
    preimage = (trees["baseline"] / "src/producer.rs").read_text()
    if hashlib.sha256(preimage.encode()).hexdigest() != BASE_PRODUCER_SHA:
        raise RuntimeError("pinned baseline producer source does not match")
    for arm, tree in trees.items():
        if not run_logged(["git", "apply", "--check", str(ASSETS / "candidate.patch")], tree, out / "patches" / f"{arm}-candidate-check.log", timeout=60):
            raise RuntimeError("candidate patch check failed")
        if not run_logged(["git", "apply", str(ASSETS / "candidate.patch")], tree, out / "patches" / f"{arm}-candidate-apply.log", timeout=60):
            raise RuntimeError("candidate patch application failed")
        source_path = tree / "src/producer.rs"
        source = source_path.read_text()
        if hashlib.sha256(source.encode()).hexdigest() != CANDIDATE_PRODUCER_SHA:
            raise RuntimeError("candidate source does not match frozen source")
        if not run_logged(["git", "apply", "--check", str(ASSETS / "worker-control.patch")], tree, out / "patches" / f"{arm}-worker-check.log", timeout=60):
            raise RuntimeError("worker patch check failed")
        if not run_logged(["git", "apply", str(ASSETS / "worker-control.patch")], tree, out / "patches" / f"{arm}-worker-apply.log", timeout=60):
            raise RuntimeError("worker patch application failed")
        source = source_path.read_text()
        if arm == "baseline":
            source = replace_once(source, CANDIDATE_BODY, BASE_BODY, "baseline restore")
            # Identical fixtures, with only the allocation identity expectation changed.
            source = replace_once(source, "        assert_eq!(batch.as_ptr(), pending_ptr);\n        assert_eq!(pending.as_ptr(), spare_ptr);\n",
                                  "        assert_eq!(batch.as_ptr(), spare_ptr);\n        assert_eq!(pending.as_ptr(), pending_ptr);\n", "baseline pointer expectation")
            source = replace_once(source, "                assert_eq!(batch.as_ptr(), pending_ptr);\n                assert_eq!(worker.pending.as_ptr(), spare_ptr);\n",
                                  "                assert_eq!(batch.as_ptr(), spare_ptr);\n                assert_eq!(worker.pending.as_ptr(), pending_ptr);\n", "baseline worker pointer expectation")
            source_path.write_text(source)
        diff = "".join(difflib.unified_diff(preimage.splitlines(keepends=True), source.splitlines(keepends=True),
                                            fromfile="pinned/src/producer.rs", tofile=f"{arm}/src/producer.rs"))
        (out / f"{arm}-source.diff").write_text(diff)
    return trees


def strengthen_phase_boundaries(tree: Path, out: Path, arm: str) -> None:
    path = tree / "examples/bench_produce.rs"
    before = path.read_text()
    old = '''        if phase == "warmup" {
            warmup_count = sent;
            warmup_elapsed = start.elapsed().as_secs_f64();
        } else {
            measured_count = sent;
            elapsed = start.elapsed().as_secs_f64();
        }
        if failure.is_some() {
'''
    new = '''        let phase_metrics = producer.metrics();
        if failure.is_none()
            && (phase_metrics.records_acked != next_id || phase_metrics.produce_errors != 0)
        {
            failure = Some(partitionline::Error::protocol(
                "phase ended before accepted records were acknowledged without errors",
            ));
        }
        if phase == "warmup" {
            warmup_count = sent;
            warmup_elapsed = start.elapsed().as_secs_f64();
        } else {
            measured_count = sent;
            elapsed = start.elapsed().as_secs_f64();
        }
        println!("{{\\"kind\\":\\"phase_end\\",\\"phase\\":\\"{}\\",\\"accepted_total\\":{},\\"acknowledged_total\\":{},\\"produce_errors\\":{}}}",
            phase, next_id, phase_metrics.records_acked, phase_metrics.produce_errors);
        if failure.is_some() {
'''
    after = replace_once(before, old, new, "phase-end acknowledgements")
    path.write_text(after)
    (out / f"{arm}-phase-boundary.diff").write_text("".join(difflib.unified_diff(
        before.splitlines(keepends=True), after.splitlines(keepends=True),
        fromfile="pinned/examples/bench_produce.rs", tofile="qualified/examples/bench_produce.rs")))


def instrument(tree: Path) -> None:
    source_path = tree / "src/producer.rs"
    source = source_path.read_text()
    if "static PL_FULL_DRAIN_CENSUS" in source:
        raise RuntimeError("diagnostic instrumentation is already present")
    source = replace_once(source, HELPER, (ASSETS / "census.rs").read_text() + "\n" + HELPER + "    pl_full_drain_observe(spare, pending, n);\n", "census helper")
    source_path.write_text(source)
    example_path = tree / "examples/bench_produce.rs"
    example = example_path.read_text()
    example = replace_once(example, '        let mut pending_record = None;\n',
                           '        if phase == "measure" { partitionline::producer::__pl_full_drain_census_reset(); }\n        let mut pending_record = None;\n', "warmup boundary")
    example = replace_once(example, '        if failure.is_some() {\n            break;\n',
                           '        if phase == "measure" { println!("{{\\"kind\\":\\"full_drain_census\\",\\"counts\\":{:?}}}", partitionline::producer::__pl_full_drain_census_snapshot()); }\n        if failure.is_some() {\n            break;\n', "census output")
    example_path.write_text(example)


def read_ready(proc: subprocess.Popen, timeout: float) -> str:
    if proc.stdout is None:
        raise RuntimeError("server stdout not captured")
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    try:
        if not selector.select(timeout):
            raise TimeoutError("null broker did not become ready")
        line = proc.stdout.readline()
        if not line.startswith("READY ports="):
            raise RuntimeError(f"unexpected null broker readiness: {line!r}")
        ports = line.strip().removeprefix("READY ports=").split(",")
        if len(ports) != 1 or not ports[0].isdigit() or not 0 < int(ports[0]) < 65536:
            raise RuntimeError(f"invalid broker port line: {line!r}")
        return ports[0]
    finally:
        selector.close()


def run_attempt(server_bin: Path, client_bin: Path, case_name: str, arm: str, index: str, diagnostic: bool, directory: Path) -> bool:
    directory.mkdir(parents=True, exist_ok=False)
    case = CASES[case_name]
    artifact = directory / "broker.json"
    cmd = [str(server_bin), "--bind", "127.0.0.1", "--partitions", case["PARTITIONS"],
           "--serve-for-secs", "900", "--artifact", str(artifact), "--synth-records-per-partition", "1"]
    client_env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR", "LANG", "LD_LIBRARY_PATH") if k in os.environ}
    client_env.update(COMMON_ENV)
    client_env.update(case)
    details = {"case": case_name, "arm": arm, "pair": index, "diagnostic_only": diagnostic,
               "server_command": cmd, "client_binary_sha256": digest(client_bin), "server_binary_sha256": digest(server_bin)}
    server = None
    client_code: int | str = "not_started"
    server_code: int | str = "not_started"
    errors: list[str] = []
    try:
        with (directory / "server.stderr").open("w") as stderr:
            server = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1)
            details["server_pid"] = server.pid
            port = read_ready(server, 30)
            (directory / "server.ready").write_text(f"READY ports={port}\n")
            client_env["KAFKA_BOOTSTRAP"] = f"127.0.0.1:{port}"
            details["client_settings"] = {k: v for k, v in client_env.items() if k in COMMON_ENV or k in case or k == "KAFKA_BOOTSTRAP"}
            client_cmd = ["/usr/bin/time", "-v", "-o", str(directory / "client.time"), str(client_bin)]
            details["client_command"] = client_cmd
            with (directory / "client.stdout").open("w") as stdout, (directory / "client.stderr").open("w") as client_err:
                client = subprocess.Popen(client_cmd, env=client_env, stdout=stdout, stderr=client_err)
                details["client_pid"] = client.pid
                try:
                    client_code = client.wait(timeout=660)
                except subprocess.TimeoutExpired:
                    client_code = "timeout"
                    client.terminate()
                    try:
                        client.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        client.kill()
                        client.wait(timeout=3)
            if client_code != 0:
                errors.append(f"client exit {client_code}")
            if server.stdin is not None and server.poll() is None:
                server.stdin.write("quit\n")
                server.stdin.flush()
            try:
                server_code = server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                server_code = "timeout"
                server.kill()
                server.wait(timeout=20)
            if server_code != 0:
                errors.append(f"server exit {server_code}")
            if server.stdout is not None:
                (directory / "server.stdout").write_text(server.stdout.read())
        lines = (directory / "client.stdout").read_text().splitlines()
        payloads = [json.loads(line) for line in lines if line.startswith("{")]
        reports = [x for x in payloads if "run_disposition" in x]
        censuses = [x for x in payloads if x.get("kind") == "full_drain_census"]
        errors.extend(phase_errors(payloads, case))
        if len(reports) != 1 or len(censuses) != int(diagnostic):
            errors.append("unexpected client report or census count")
        if len(reports) == 1 and artifact.is_file():
            broker = json.loads(artifact.read_text())
            census = censuses[0] if len(censuses) == 1 else None
            errors.extend(verify_attempt(reports[0], broker, case, diagnostic, census))
            if census is not None:
                write_json(directory / "census-named.json", dict(zip(CENSUS_NAMES, census["counts"])))
        else:
            errors.append("missing producer or broker artifact")
    except (OSError, ValueError, RuntimeError, TimeoutError, BrokenPipeError, json.JSONDecodeError) as exc:
        errors.append(f"attempt exception: {exc}")
    finally:
        if server is not None and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=3)
        details.update({"client_exit": client_code, "server_exit": server_code, "errors": errors,
                        "successful": not errors, "integrity_verified": False, "performance_claims_valid": False})
        write_json(directory / "attempt.json", details)
    return not errors


def owned_paths(repo_arg: Path, work_arg: Path, out_arg: Path, env: dict[str, str]) -> tuple[Path, Path, Path, Path]:
    workspace = Path(env["GITHUB_WORKSPACE"]).resolve(strict=True)
    runner_temp = Path(env["RUNNER_TEMP"]).resolve(strict=True)
    repo, work, out = repo_arg.resolve(), work_arg.resolve(), out_arg.resolve()
    if (workspace == Path("/") or runner_temp == Path("/") or repo != workspace
            or work != runner_temp / "full-drain-work" or out != runner_temp / "full-drain-results"
            or work.exists() or work_arg.is_symlink() or out_arg.is_symlink()):
        raise RuntimeError("refusing unowned workspace, work or output paths")
    if not out.is_dir():
        raise RuntimeError("workflow must create its exact output directory")
    return repo, work, out, runner_temp


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or platform.machine() != "x86_64":
        raise RuntimeError("hosted GitHub Actions x86_64 runner required; do not run locally")
    global OWNER, WATCH_PATHS, JOB_DEADLINE, RUN_OUT
    repo, work, out, runner_temp = owned_paths(args.repo, args.work, args.out, os.environ)
    RUN_OUT = out
    started_at = int((out / "started-unix.txt").read_text().strip())
    elapsed = time.time() - started_at
    if elapsed < -5 or elapsed >= 150 * 60:
        raise RuntimeError("workflow start time invalid or overall job deadline reached")
    JOB_DEADLINE = time.monotonic() + 150 * 60 - max(0, elapsed)
    home = Path(os.environ["HOME"]).resolve(strict=True)
    cargo_home = Path(os.environ.get("CARGO_HOME", home / ".cargo")).expanduser().resolve()
    rustup_home = Path(os.environ.get("RUSTUP_HOME", home / ".rustup")).expanduser().resolve()
    system_tmp = Path(os.environ.get("TMPDIR", "/tmp")).expanduser().resolve()
    WATCH_PATHS = [repo, work, out, runner_temp, home, cargo_home, rustup_home, system_tmp]
    check_free(WATCH_PATHS)
    work.mkdir(parents=True, exist_ok=False)
    OWNER = load_owner()
    metadata = {"base": BASE, "candidate_sha256": digest(ASSETS / "candidate.patch"),
                "census_sha256": digest(ASSETS / "census.rs"), "runner": platform.uname()._asdict(),
                "affinity": sorted(os.sched_getaffinity(0)), "loadavg": os.getloadavg(),
                "free_space_floor_bytes": MIN_FREE_BYTES, "free_space_stop_bytes": STOP_FREE_BYTES,
                "free_space_initial_bytes": shutil.disk_usage(work).free,
                "watched_paths": [str(p) for p in WATCH_PATHS], "deadline_seconds": 150 * 60,
                "github": {k: os.environ.get(k) for k in ("GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_EVENT_NAME")},
                "label": "exploratory client-ceiling; not Kafka, production, release or performance qualification"}
    for path in ("/proc/cpuinfo", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max", "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"):
        p = Path(path)
        if p.is_file():
            metadata[path] = p.read_text()
    write_json(out / "metadata.json", metadata)
    capsule = out / "capsule"
    capsule.mkdir()
    for name in ("run.py", "trial.py", "test_harness.py", "workflow.yml", "candidate.patch", "worker-control.patch", "candidate-command-ownership.py", "census.rs", "README.md", "SHA256SUMS"):
        shutil.copy2(ASSETS / name, capsule / name)
    write_json(out / "capsule-hashes.json", {item.name: digest(item) for item in sorted(capsule.iterdir())})
    ok = run_logged(["rustc", "+stable", "-Vv"], repo, out / "toolchain.log", timeout=60)
    if not ok or "host: x86_64-unknown-linux-gnu" not in (out / "toolchain.log").read_text():
        raise RuntimeError("could not verify native stable x86_64 toolchain")
    if not run_logged(["git", "fetch", "--no-tags", "--depth=1", "origin", BASE], repo, out / "fetch.log", timeout=600):
        raise RuntimeError("could not fetch pinned source")
    if not run_logged(["git", "cat-file", "-e", BASE + "^{commit}"], repo, out / "base-check.log", timeout=60):
        raise RuntimeError("pinned source commit unavailable")
    trees = make_trees(repo, work, out)
    gates: list[dict] = []
    for arm, tree in trees.items():
        env = cargo_env(work / f"target-{arm}")
        commands = [
            ["cargo", "+stable", "fmt", "--all", "--", "--check"],
            ["cargo", "+stable", "clippy", "--locked", "--all-targets", "--all-features", "--", "-D", "warnings"],
            ["cargo", "+stable", "test", "--locked", "--lib", "producer::tests::", "--", "--test-threads=1"],
            ["cargo", "+stable", "test", "--locked", "--all-features", "--lib", "producer::tests::", "--", "--test-threads=1"],
        ]
        for index, cmd in enumerate(commands):
            log = out / "gates" / f"{arm}-{index}.log"
            passed = run_logged(cmd, tree, log, env=env)
            missing = []
            if index >= 2:
                missing = missing_test_successes(log.read_text())
                passed = passed and not missing
            gates.append({"arm": arm, "command": cmd, "passed": passed, "missing_test_successes": missing})
    write_json(out / "gates.json", gates)
    if not all(x["passed"] for x in gates):
        status = "incomplete" if time.monotonic() >= JOB_DEADLINE else "correctness_gate_failed"
        write_json(out / "summary.json", {"status": status, "performance_claims_valid": False})
        return 1
    binaries: dict[tuple[str, str], Path] = {}
    for arm, tree in trees.items():
        strengthen_phase_boundaries(tree, out, arm)
    if (out / "baseline-phase-boundary.diff").read_bytes() != (out / "candidate-phase-boundary.diff").read_bytes():
        raise RuntimeError("benchmark phase-boundary transforms differ between arms")
    for arm, tree in trees.items():
        env = cargo_env(work / f"target-{arm}")
        command = ["cargo", "+stable", "build", "--locked", "--release", "--example", "bench_produce"]
        if not run_logged(command, tree, out / "builds" / f"{arm}-timing.log", env=env):
            raise RuntimeError(f"{arm} timing build failed")
        binary = work / f"{arm}-timing"
        built = Path(env["CARGO_TARGET_DIR"]) / "release/examples/bench_produce"
        check_free([work], reserve=built.stat().st_size + 1024**2)
        shutil.copy2(built, binary)
        binaries[(arm, "timing")] = binary
        before_source = (tree / "src/producer.rs").read_text()
        before_example = (tree / "examples/bench_produce.rs").read_text()
        instrument(tree)
        instrumented = (tree / "src/producer.rs").read_text()
        instrumented_example = (tree / "examples/bench_produce.rs").read_text()
        (out / f"{arm}-diagnostic-source.diff").write_text("".join(difflib.unified_diff(
            before_source.splitlines(keepends=True), instrumented.splitlines(keepends=True),
            fromfile="timed/src/producer.rs", tofile="diagnostic/src/producer.rs")))
        (out / f"{arm}-diagnostic-example.diff").write_text("".join(difflib.unified_diff(
            before_example.splitlines(keepends=True), instrumented_example.splitlines(keepends=True),
            fromfile="timed/examples/bench_produce.rs", tofile="diagnostic/examples/bench_produce.rs")))
        (out / f"{arm}-diagnostic-source.sha256").write_text(hashlib.sha256(instrumented.encode()).hexdigest() + "\n")
        if not run_logged(command, tree, out / "builds" / f"{arm}-diagnostic.log", env=env):
            raise RuntimeError(f"{arm} diagnostic build failed")
        binary = work / f"{arm}-diagnostic"
        built = Path(env["CARGO_TARGET_DIR"]) / "release/examples/bench_produce"
        check_free([work], reserve=built.stat().st_size + 1024**2)
        shutil.copy2(built, binary)
        binaries[(arm, "diagnostic")] = binary
    null = trees["baseline"] / "benchmarks/nullbroker"
    (null / "examples").mkdir(exist_ok=True)
    shutil.copy2(trees["baseline"] / "benchmarks/runtime/src/bin/nb-serve.rs", null / "examples/nb-serve.rs")
    env = cargo_env(work / "target-null")
    command = ["cargo", "+stable", "build", "--locked", "--release", "--manifest-path", str(null / "Cargo.toml"), "--example", "nb-serve"]
    if not run_logged(command, null, out / "builds/nullbroker.log", env=env):
        raise RuntimeError("null broker build failed")
    server_bin = Path(env["CARGO_TARGET_DIR"]) / "release/examples/nb-serve"
    write_json(out / "binary-hashes.json", {f"{a}-{b}": digest(path) for (a, b), path in binaries.items()} | {"nullbroker": digest(server_bin)})
    order = trial_order()
    write_json(out / "order.json", order)
    results = []
    for case, pair, arm in order:
        directory = out / "trials" / case / f"pair-{pair}-{arm}"
        command = [sys.executable, "-B", str(ASSETS / "trial.py"), str(server_bin), str(binaries[(arm, "timing")]), case, arm, str(pair), "timing", str(directory)]
        passed = run_logged(command, work, out / "trial-ownership" / f"{case}-{pair}-{arm}.log", timeout=720)
        results.append({"case": case, "pair": pair, "arm": arm, "diagnostic": False, "passed": passed})
        write_json(out / "progress.json", results)
    for case in CASES:
        for arm in ("baseline", "candidate"):
            directory = out / "census" / case / arm
            command = [sys.executable, "-B", str(ASSETS / "trial.py"), str(server_bin), str(binaries[(arm, "diagnostic")]), case, arm, "census", "diagnostic", str(directory)]
            passed = run_logged(command, work, out / "trial-ownership" / f"{case}-census-{arm}.log", timeout=720)
            results.append({"case": case, "arm": arm, "diagnostic": True, "passed": passed})
            write_json(out / "progress.json", results)
    success = all(x["passed"] for x in results)
    write_json(out / "summary.json", {"status": "executed" if success else "failed", "attempts": results,
                                      "integrity_verified": False, "performance_claims_valid": False})
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        if RUN_OUT is not None:
            write_json(RUN_OUT / "incomplete.json", {"status": "incomplete", "reason": str(exc), "performance_claims_valid": False})
        print(f"hosted full-drain harness failed: {exc}", file=sys.stderr)
        raise
