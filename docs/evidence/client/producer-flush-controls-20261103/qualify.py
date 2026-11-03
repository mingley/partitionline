#!/usr/bin/env python3
"""Reproduce four pinned producer flush regression controls on hosted x86_64.

A successful run reproduces the expected assertions on the pinned base. It is
not a production qualification or evidence that a later source revision passes.
"""
import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import types
from pathlib import Path

BASE_COMMIT = "45cbff24de76490594970fc962cd04e7be9fafb1"
SOURCE_SHA256 = "4f461c00864e3da5e20925796c1f489580a4046202cffd86fefeb64cbd145a32"
PATCH_SHA256 = "dcb7fa244056a3ccad7c81ba38611e5cb2157d4a7bdfbe8245cedad2cdfd0c03"
OWNER_SHA256 = "34ddbf04a4bbead974fad4a451d30c7a56818248bd5b58c88c87bdfc41896da9"
PATCHED_SOURCE_SHA256 = "226ca81821663a3d67488dc6086607bde0722ae5bf72a163e8524457fb8ffa50"
DISK_FLOOR_BYTES = 3 * 1024**3 // 2
DISK_STOP_BYTES = 2 * 1024**3
DISK_POLL_SECONDS = 2
TESTS = (
    ("flush_reports_an_accepted_record_expiring_in_pending", "flush lost the delivery timeout: Ok(())"),
    ("cancelled_flush_cannot_consume_an_unobserved_record_failure", "cancelled flush discarded the only failure report"),
    ("flush_cannot_succeed_while_a_retired_worker_still_owns_an_accepted_record", "flush reported success before the retired worker processed the record"),
    ("flush_reports_failure_when_retry_guard_drops_an_accepted_record", "flush lost the retry failure: Ok(())"),
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def classify_test(returncode, timed_out, output, expected_message):
    if timed_out:
        return "timeout"
    if returncode == 0:
        if "running 1 test" in output and "test result: ok. 1 passed; 0 failed;" in output:
            return "unexpected-pass"
        return "invalid-test-selection"
    if (
        returncode == 101
        and "running 1 test" in output
        and "test result: FAILED. 0 passed; 1 failed;" in output
        and expected_message in output
    ):
        return "expected-assertion"
    return "unexpected-failure"


def disk_sample(paths, sample_log, phase):
    reading = {"time_unix": time.time(), "phase": phase, "available_bytes": {}}
    for label, path in paths.items():
        reading["available_bytes"][label] = shutil.disk_usage(path).free
    with sample_log.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(reading, sort_keys=True) + "\n")
    return min(reading["available_bytes"].values())


def load_owner(patch):
    path = patch.parent / "candidate-command-ownership.py"
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != OWNER_SHA256:
        raise RuntimeError("command ownership helper hash mismatch")
    owner = types.ModuleType("pinned_command_ownership")
    owner.__file__ = str(path)
    exec(compile(data, str(path), "exec"), owner.__dict__)
    return owner


def run(owner, command, cwd, logfile, timeout, disk_paths, disk_log):
    with logfile.open("w", encoding="utf-8") as stream:
        stream.write("command: " + json.dumps(command) + "\n")
        minimum_free = disk_sample(disk_paths, disk_log, logfile.name + ":before")
        if minimum_free < DISK_STOP_BYTES:
            stream.write("command not started: available space below 2 GiB stop threshold\n")
            return {"command": command, "exit_code": None, "timed_out": False, "disk_stop": True,
                    "minimum_free_bytes": minimum_free, "log": logfile.name}
        stream.flush()
        deadline = time.monotonic() + timeout
        timed_out = False
        disk_stop = False
        # The pinned helper keeps the leader unreaped until all members of its
        # process session are stopped and reaped, even after ordinary success.
        # It is intended for normal Cargo descendants, not escaped sessions.
        with owner.owned_command(command, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT) as task:
            next_sample = time.monotonic() + DISK_POLL_SECONDS
            while not task.finished():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    stream.write(f"command timed out after {timeout}s; stopping owned session\n")
                    stream.flush()
                    break
                time.sleep(min(0.25, remaining))
                if time.monotonic() < next_sample:
                    continue
                next_sample = time.monotonic() + DISK_POLL_SECONDS
                free = disk_sample(disk_paths, disk_log, logfile.name + ":running")
                minimum_free = min(minimum_free, free)
                if free < DISK_STOP_BYTES:
                    disk_stop = True
                    stream.write("available space below 2 GiB stop threshold; stopping owned session\n")
                    stream.flush()
                    break
        receipt = task.cleanup_receipt
        code = receipt["leader_exit"]
        free = disk_sample(disk_paths, disk_log, logfile.name + ":after")
        minimum_free = min(minimum_free, free)
        if free < DISK_STOP_BYTES:
            disk_stop = True
        stream.write(f"\nexit_code: {code}\ntimed_out: {str(timed_out).lower()}\ndisk_stop: {str(disk_stop).lower()}\n")
    return {"command": command, "exit_code": code, "timed_out": timed_out, "disk_stop": disk_stop,
            "minimum_free_bytes": minimum_free, "cleanup_receipt": receipt, "log": logfile.name}


def get_output(owner, command, cwd, logfile, disk_paths, disk_log):
    outcome = run(owner, command, cwd, logfile, 30, disk_paths, disk_log)
    if outcome["exit_code"] != 0 or outcome["timed_out"] or outcome["disk_stop"]:
        raise RuntimeError(f"metadata command failed; inspect {logfile.name}")
    body = logfile.read_text(encoding="utf-8", errors="replace").partition("\n")[2]
    return body.rsplit("\nexit_code:", 1)[0].strip()


def self_test():
    failure = "running 1 test\ntest result: FAILED. 0 passed; 1 failed;\nneedle"
    passed = "running 1 test\ntest result: ok. 1 passed; 0 failed;"
    checks = (
        (101, False, failure, "expected-assertion"),
        (101, False, failure.replace("needle", "setup panic"), "unexpected-failure"),
        (101, False, failure.replace("running 1 test", "running 0 tests"), "unexpected-failure"),
        (1, False, failure, "unexpected-failure"),
        (101, True, failure, "timeout"),
        (0, False, passed, "unexpected-pass"),
        (0, False, "running 0 tests\ntest result: ok. 0 passed; 0 failed;", "invalid-test-selection"),
    )
    for code, timed_out, output, expected in checks:
        actual = classify_test(code, timed_out, output, "needle")
        if actual != expected:
            raise RuntimeError(f"classifier returned {actual!r}, expected {expected!r}")
    print(f"classifier self-test passed ({len(checks)} cases)")


def qualify(source, patch, output):
    results = {
        "base_commit": BASE_COMMIT,
        "expected_source_sha256": SOURCE_SHA256,
        "expected_patch_sha256": PATCH_SHA256,
        "expected_owner_sha256": OWNER_SHA256,
        "expected_patched_source_sha256": PATCHED_SOURCE_SHA256,
        "machine": platform.machine(),
        "disk_floor_bytes": DISK_FLOOR_BYTES,
        "disk_stop_bytes": DISK_STOP_BYTES,
        "disk_poll_seconds": DISK_POLL_SECONDS,
        "platform": platform.platform(),
        "profiles": {},
        "verdict": "incomplete",
    }
    try:
        if platform.machine() != "x86_64":
            raise RuntimeError("expected an x86_64 host")
        owner = load_owner(patch)
        cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
        if not cargo_home.exists():
            cargo_home = Path.home()
        rustup_home = Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup")))
        if not rustup_home.exists():
            rustup_home = Path.home()
        disk_paths = {"source": source, "evidence": output, "cargo_home": cargo_home,
                      "rustup_home": rustup_home, "temporary": Path(tempfile.gettempdir())}
        disk_log = output / "disk.jsonl"
        target_dir = os.environ.get("CARGO_TARGET_DIR")
        if not target_dir or Path(target_dir).resolve() != source / "target":
            raise RuntimeError("CARGO_TARGET_DIR must point at the monitored source/target")
        actual = get_output(owner, ["git", "rev-parse", "HEAD"], source, output / "git-head.log", disk_paths, disk_log)
        results["checkout_commit"] = actual
        if actual != BASE_COMMIT:
            raise RuntimeError(f"checkout mismatch: {actual}")
        if get_output(owner, ["git", "status", "--porcelain=v1", "--untracked-files=all"], source, output / "git-status.log", disk_paths, disk_log):
            raise RuntimeError("source checkout is not clean")
        source_file = source / "src/producer.rs"
        if digest(source_file) != SOURCE_SHA256:
            raise RuntimeError("base producer source hash mismatch")
        if digest(patch) != PATCH_SHA256:
            raise RuntimeError("regression patch hash mismatch")
        shutil.copyfile(patch, output / "regression-controls.patch")
        results["rustc"] = get_output(owner, ["rustc", "-Vv"], source, output / "rustc-version.log", disk_paths, disk_log)
        results["cargo"] = get_output(owner, ["cargo", "-Vv"], source, output / "cargo-version.log", disk_paths, disk_log)
        results["toolchain"] = get_output(owner, ["rustup", "show", "active-toolchain"], source, output / "rustup-toolchain.log", disk_paths, disk_log)
        apply = run(owner, ["git", "apply", "--check", str(patch)], source, output / "apply-check.log", 30, disk_paths, disk_log)
        if apply["exit_code"] != 0 or apply["timed_out"] or apply["disk_stop"]:
            raise RuntimeError("patch check failed; inspect apply-check.log")
        apply = run(owner, ["git", "apply", str(patch)], source, output / "apply.log", 30, disk_paths, disk_log)
        if apply["exit_code"] != 0 or apply["timed_out"] or apply["disk_stop"]:
            raise RuntimeError("patch apply failed; inspect apply.log")
        if digest(source_file) != PATCHED_SOURCE_SHA256:
            raise RuntimeError("patched producer source hash mismatch")
        results["patched_source_sha256"] = digest(source_file)
        for profile, feature_args in (("default", []), ("all-features", ["--all-features"])):
            section = {"tests": {}}
            results["profiles"][profile] = section
            build = ["cargo", "test", "--locked", "--lib", *feature_args, "--no-run"]
            compilation = run(owner, build, source, output / f"compile-{profile}.log", 1500, disk_paths, disk_log)
            section["compile"] = compilation
            if compilation["exit_code"] != 0 or compilation["timed_out"] or compilation["disk_stop"]:
                for name, _ in TESTS:
                    section["tests"][name] = {"classification": "not-run-compile-failed"}
                continue
            for name, message in TESTS:
                full_name = f"producer::tests::{name}"
                command = ["cargo", "test", "--locked", "--lib", *feature_args, full_name, "--", "--exact", "--nocapture"]
                logfile = output / f"{profile}-{name}.log"
                outcome = run(owner, command, source, logfile, 120, disk_paths, disk_log)
                outcome["classification"] = (
                    "disk-stop" if outcome["disk_stop"] else classify_test(
                        outcome["exit_code"], outcome["timed_out"], logfile.read_text(encoding="utf-8", errors="replace"), message
                    )
                )
                outcome["expected_assertion"] = message
                section["tests"][name] = outcome
        good = all(
            item["classification"] == "expected-assertion"
            for section in results["profiles"].values()
            for item in section["tests"].values()
        ) and len(results["profiles"]) == 2 and all(len(s["tests"]) == len(TESTS) for s in results["profiles"].values())
        results["verdict"] = "expected-assertions-reproduced" if good else "inconclusive-or-unexpected"
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        results["setup_error"] = str(exc)
        results["verdict"] = "setup-failed"
    finally:
        (output / "result.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if results["verdict"] == "expected-assertions-reproduced" else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout", type=Path)
    parser.add_argument("--patch", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not (args.checkout and args.patch and args.output):
        parser.error("--checkout, --patch and --output are required")
    source = args.checkout.resolve(strict=True)
    patch = args.patch.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    return qualify(source, patch, output)


if __name__ == "__main__":
    sys.exit(main())
