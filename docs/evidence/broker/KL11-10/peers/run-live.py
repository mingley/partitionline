#!/usr/bin/env python3
"""Retain fresh Java/native/public-Rust deletion and restart histories on one lane."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


RELEASES = ("4.1.2", "4.2.1", "4.3.1")
JAR_PINS = {
    "4.1.2": "33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed",
    "4.2.1": "9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159",
    "4.3.1": "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e",
}
SLF4J_PIN = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
NATIVE_PIN = "8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(source, output):
    output.mkdir()
    files = []
    total = 0
    for path in sorted(source.rglob("*")):
        if path.name in ("ready", "stop"):
            continue
        if path.is_symlink():
            raise RuntimeError("No symlinks admitted in retained server state")
        if not path.is_file():
            continue
        size = path.stat().st_size
        total += size
        if len(files) >= 128 or size > 1024 * 1024 or total > 16 * 1024 * 1024:
            raise RuntimeError("Retained state exceeds explicit file/byte bounds")
        relative = path.relative_to(source)
        copied = output / relative
        copied.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, copied)
        files.append({"path": str(relative), "bytes": size, "sha256": sha(copied)})
    return {"files": files, "bytes": total}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-binary", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-receipt", type=Path, required=True)
    parser.add_argument("--toolchain", choices=("stable", "1.85.0"), required=True)
    parser.add_argument("--features", choices=("default", "all-features"), required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--sdk-work", type=Path, required=True)
    parser.add_argument("--java-build-receipt", type=Path, required=True)
    parser.add_argument("--native-binary", type=Path, required=True)
    parser.add_argument("--native-build-receipt", type=Path, required=True)
    parser.add_argument("--rust-binary", type=Path, required=True)
    parser.add_argument("--rust-build-receipt", type=Path, required=True)
    parser.add_argument("--jars", type=Path, default=Path("/workspace/work/broker-wire/jars"))
    parser.add_argument("--native-library", type=Path, default=Path("/workspace/work/c-peer/lib/librdkafka.so.1"))
    parser.add_argument("--port", type=int, default=19145)
    parser.add_argument("--development-overlay", action="store_true")
    args = parser.parse_args()
    if len(args.source_sha) != 40 or any(c not in "0123456789abcdef" for c in args.source_sha):
        parser.error("Full hexadecimal Git source pin required")
    if not 1024 <= args.port <= 65535:
        parser.error("Bounded nonprivileged port required")
    args.evidence.mkdir(parents=True, exist_ok=False)
    args.scratch.mkdir(parents=True, exist_ok=False)
    server_dir = args.scratch / "server"
    server_dir.mkdir()
    results = []
    servers = []
    states = []
    slf4j = args.jars / "slf4j-api-1.7.36.jar"
    if sha(slf4j) != SLF4J_PIN or sha(args.native_library) != NATIVE_PIN:
        raise RuntimeError("Pinned runtime dependency mismatch")
    for release in RELEASES:
        if sha(args.jars / f"kafka-clients-{release}.jar") != JAR_PINS[release]:
            raise RuntimeError("Pinned Apache SDK mismatch")
    source = Path(__file__).parent
    java_build = json.loads(args.java_build_receipt.read_text())
    native_build = json.loads(args.native_build_receipt.read_text())
    rust_build = json.loads(args.rust_build_receipt.read_text())
    if not all(receipt.get("passed") is True for receipt in (java_build, native_build, rust_build)):
        raise RuntimeError("Only successful source build receipts can supply live peers")
    java_rows = {row["release"]: row for row in java_build["results"]}
    if set(java_rows) != set(RELEASES):
        raise RuntimeError("Exactly three pinned Java build lanes required")
    for release, row in java_rows.items():
        if row["common_source_sha256"] != sha(source / "RetentionPeer.java"):
            raise RuntimeError("Java peer source changed after strict compilation")
        for item in row["classes"]:
            actual = args.sdk_work / release / "classes" / Path(item["path"]).name
            if sha(actual) != item["sha256"]:
                raise RuntimeError("Compiled Java class changed after its build receipt")
    native_pins = native_build["pins"]
    if native_pins[str(args.native_binary)] != sha(args.native_binary):
        raise RuntimeError("Native peer binary changed after strict compilation")
    native_source_pins = [value for name, value in native_pins.items() if name.endswith("retention-native.c")]
    if native_source_pins != [sha(source / "retention-native.c")]:
        raise RuntimeError("Native peer source changed after strict compilation")
    if rust_build["binary"]["sha256"] != sha(args.rust_binary):
        raise RuntimeError("Public Rust peer binary changed after strict compilation")
    for name in ("Cargo.toml", "Cargo.lock", "src/main.rs"):
        if rust_build["after"][name] != sha(source / "rust-adopter" / name):
            raise RuntimeError("Public Rust peer source/lock changed after strict compilation")
    passed = True
    failure = None
    env = dict(os.environ)
    env.update(PARTITIONLINE_RETENTION_LIVE_PORT=str(args.port),
               PARTITIONLINE_RETENTION_LIVE_DIR=str(server_dir))
    server_command = ["taskset", "-c", "0-2,4", str(args.server_binary),
                      "serve_live_probe", "--exact", "--nocapture"]

    def peer(kind, release, phase, command, expected_report):
        label = f"{phase}-{release}-{kind}"
        log = args.evidence / (label + ".log")
        with log.open("wb") as output:
            try:
                complete = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT,
                                          check=False, timeout=45)
                code = complete.returncode
            except subprocess.TimeoutExpired:
                code = -9
        report = None
        if expected_report.exists():
            try:
                report = json.loads(expected_report.read_text())
            except (ValueError, OSError):
                pass
        good = code == 0 and report is not None and report.get("passed") is True
        item = {"kind": kind, "release": release, "phase": phase, "command": command,
                "exit_code": code, "passed": good, "log": log.name,
                "log_sha256": sha(log), "report": str(expected_report.relative_to(args.evidence)),
                "report_sha256": sha(expected_report) if expected_report.exists() else None}
        results.append(item)
        print(json.dumps({"peer": label, "passed": good, "exit_code": code}), flush=True)
        return good

    try:
        for phase in ("seed", "restart"):
            for flag in ("ready", "stop"):
                (server_dir / flag).unlink(missing_ok=True)
            log = args.evidence / (phase + "-server.log")
            with log.open("wb") as stream:
                process = subprocess.Popen(server_command, env=env, stdout=stream, stderr=subprocess.STDOUT)
                entry = {"phase": phase, "command": server_command, "ready": False}
                servers.append(entry)
                try:
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline and process.poll() is None:
                        if (server_dir / "ready").exists():
                            entry["ready"] = True
                            break
                        time.sleep(0.04)
                    if not entry["ready"]:
                        raise RuntimeError(f"{phase} server readiness failed")
                    for release in RELEASES:
                        topic = "retention-java-" + release.replace(".", "-")
                        classes = args.sdk_work / release / "classes"
                        classpath = ":".join(map(str, (classes, args.jars / f"kafka-clients-{release}.jar", slf4j)))
                        java = ["taskset", "-c", "0-2,4", "java", "-Xmx128m", "-cp", classpath,
                                "RetentionPeer", release, str(args.port), phase, str(args.evidence), topic]
                        if not peer("java", release, phase, java, args.evidence / f"{release}-{phase}.json"):
                            raise RuntimeError(f"{phase} Java {release} failed")
                        end = "7" if phase == "seed" else "8"
                        native_report = args.evidence / f"{phase}-{release}-native.json"
                        native = ["taskset", "-c", "0-2,4", str(args.native_binary),
                                  f"127.0.0.1:{args.port}", topic, phase, end, str(native_report)]
                        if not peer("native", release, phase, native, native_report):
                            raise RuntimeError(f"{phase} native reader of Java {release} failed")
                        rust_report = args.evidence / f"{phase}-{release}-rust.json"
                        rust = ["taskset", "-c", "0-2,4", str(args.rust_binary),
                                f"127.0.0.1:{args.port}", topic, phase, end, str(rust_report)]
                        if not peer("rust", release, phase, rust, rust_report):
                            raise RuntimeError(f"{phase} public Rust reader of Java {release} failed")
                finally:
                    (server_dir / "stop").write_text("stop\n")
                    try:
                        entry["exit_code"] = process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        entry["exit_code"] = process.wait()
                    entry["log"] = log.name
                    stream.flush()
                    entry["log_sha256"] = sha(log)
            entry["log_sha256"] = sha(log)
            states.append({"phase": phase,
                           **snapshot(server_dir, args.evidence / (phase + "-state"))})
            if entry["exit_code"] != 0:
                raise RuntimeError(f"{phase} server did not stop cleanly")
    except Exception as error:
        passed = False
        failure = f"{type(error).__name__}: {error}"
        if not (args.evidence / "failed-state").exists():
            states.append({"phase": "failure", **snapshot(server_dir, args.evidence / "failed-state")})
    receipts = {name: {"path": str(path), "sha256": sha(path)} for name, path in (
        ("source", args.source_receipt), ("java_build", args.java_build_receipt),
        ("native_build", args.native_build_receipt), ("rust_build", args.rust_build_receipt))}
    report = {"schema_version": 1, "source_sha": args.source_sha, "passed": passed,
              "scope": "development overlay" if args.development_overlay else "immutable committed source",
              "toolchain": args.toolchain, "features": args.features, "port": args.port,
              "failure": failure, "actual_peer_jobs": len(results), "results": results,
              "servers": servers, "states": states, "receipts": receipts,
              "runner_sha256": sha(Path(__file__)),
              "binaries": {name: {"path": str(path), "sha256": sha(path)} for name, path in (
                  ("server", args.server_binary), ("native", args.native_binary), ("rust", args.rust_binary))},
              "limits": "Ordinary RF1 local durable end only; manual consumers; no replicated ISR, transactions or consumer-group coordinator."}
    (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": passed, "actual_peer_jobs": len(results), "failure": failure}), flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
