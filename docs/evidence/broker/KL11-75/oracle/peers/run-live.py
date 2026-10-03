#!/usr/bin/env python3
"""Genuine five-producer/five-reader ordinary compaction and restart histories."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import time

RELEASES = ("4.1.2", "4.2.1", "4.3.1")
PRODUCERS = ("j412", "j421", "j431", "native", "rust")
SCENARIOS = {"mixed": 9, "removed": 4, "nulls": 3}
JAR_PINS = {
    "4.1.2": "33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed",
    "4.2.1": "9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159",
    "4.3.1": "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e",
}
SLF4J_PIN = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
NATIVE_PIN = "8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_json(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise RuntimeError("Bounded regular JSON receipt required")
    return json.loads(path.read_text())


def snapshot(source, output):
    output.mkdir()
    files, total = [], 0
    for path in sorted(source.rglob("*")):
        if path.name in ("ready", "stop") or path.name.startswith("compact-"):
            continue
        if path.is_symlink():
            raise RuntimeError("No symlinks admitted in retained server state")
        if not path.is_file():
            continue
        size = path.stat().st_size
        total += size
        if len(files) >= 2048 or size > 1024 * 1024 or total > 32 * 1024 * 1024:
            raise RuntimeError("Retained state exceeds explicit file/byte bounds")
        relative = path.relative_to(source)
        copied = output / relative
        copied.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, copied)
        files.append({"path": str(relative), "bytes": size, "sha256": sha(copied)})
    return {"files": files, "bytes": total}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-receipt", required=True, type=Path)
    parser.add_argument("--server-binary", required=True, type=Path)
    parser.add_argument("--toolchain", choices=("stable", "1.85.0"), required=True)
    parser.add_argument("--features", choices=("default", "all-features"), required=True)
    parser.add_argument("--sdk-work", required=True, type=Path)
    parser.add_argument("--java-build-receipt", required=True, type=Path)
    parser.add_argument("--native-binary", required=True, type=Path)
    parser.add_argument("--native-build-receipt", required=True, type=Path)
    parser.add_argument("--rust-binary", required=True, type=Path)
    parser.add_argument("--rust-build-receipt", required=True, type=Path)
    parser.add_argument("--scratch", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--port", type=int, default=19165)
    parser.add_argument("--jars", type=Path, default=Path("/workspace/work/broker-wire/jars"))
    parser.add_argument("--native-library", type=Path, default=Path("/workspace/work/c-peer/lib/librdkafka.so.1"))
    args = parser.parse_args()
    if len(args.source_sha) != 40 or any(c not in "0123456789abcdef" for c in args.source_sha):
        parser.error("Full hexadecimal source pin required")
    if not 1024 <= args.port <= 65535:
        parser.error("Nonprivileged bounded TCP port required")
    args.scratch.mkdir(parents=True, exist_ok=False)
    args.evidence.mkdir(parents=True, exist_ok=False)
    server_dir = args.scratch / "server"
    server_dir.mkdir()
    source = Path(__file__).parent
    results, servers, states, operations = [], [], [], []
    identities = {}
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = str(args.native_library.parent)
    bootstrap = f"127.0.0.1:{args.port}"
    passed, failure, process, stream = False, None, None, None

    def prepare():
        server_proof = checked_json(args.source_receipt)
        if server_proof.get("source_commit", server_proof.get("source_sha")) != args.source_sha:
            raise RuntimeError("Broker build receipt requires the exact committed source")
        lane = args.toolchain + "-" + args.features + "-all-targets"
        server_hash = sha(args.server_binary)
        bindings = [row for row in server_proof.get("retained_binaries", [])
                    if row.get("lane") == lane and row.get("sha256") == server_hash
                    and row.get("source_commit") == args.source_sha
                    and row.get("build_command")]
        if len(bindings) != 1:
            raise RuntimeError("Exact source/toolchain/feature/build-command server ELF binding required")
        if server_proof.get("commands") and any(row.get("exit_code", row.get("returncode")) != 0 for row in server_proof["commands"]):
            raise RuntimeError("Accepted broker build receipt includes a failed command")
        if sha(args.native_library) != NATIVE_PIN or sha(args.jars / "slf4j-api-1.7.36.jar") != SLF4J_PIN:
            raise RuntimeError("Pinned runtime dependency changed")
        for release in RELEASES:
            if sha(args.jars / f"kafka-clients-{release}.jar") != JAR_PINS[release]:
                raise RuntimeError("Official Apache public client changed")
        java = checked_json(args.java_build_receipt)
        native = checked_json(args.native_build_receipt)
        rust = checked_json(args.rust_build_receipt)
        if not all(row.get("passed") is True for row in (java, native, rust)):
            raise RuntimeError("Successful actual peer build receipts required")
        if rust.get("source_sha") != args.source_sha:
            raise RuntimeError("Public Rust dependency requires the same exact source archive")
        if rust.get("toolchain") != args.toolchain or rust.get("all_features") != (args.features == "all-features"):
            raise RuntimeError("Public Rust peer must match the selected compiler/feature lane")
        for row in java["results"]:
            if row["common_source_sha256"] != sha(source / "CompactionPeer.java"):
                raise RuntimeError("Java source changed after compilation")
            for item in row["classes"]:
                path = args.sdk_work / row["release"] / "classes" / Path(item["path"]).name
                if sha(path) != item["sha256"]:
                    raise RuntimeError("Java executable class changed")
        if {row["release"] for row in java["results"]} != set(RELEASES):
            raise RuntimeError("Exactly three official Java releases required")
        if native["pins"].get(str(args.native_binary)) != sha(args.native_binary):
            raise RuntimeError("Native executable changed")
        if [value for name, value in native["pins"].items() if name.endswith("compaction-native.c")] != [sha(source / "compaction-native.c")]:
            raise RuntimeError("Native own source changed")
        if rust["binary"]["sha256"] != sha(args.rust_binary):
            raise RuntimeError("Public Rust executable changed")
        for name in ("Cargo.toml", "Cargo.lock", "src/main.rs"):
            if rust["after"][name] != sha(source / "rust-adopter" / name):
                raise RuntimeError("Public Rust own source/lock changed")

    def start(stage):
        nonlocal process, stream
        for name in ("ready", "stop", "compact-request", "compact-result"):
            (server_dir / name).unlink(missing_ok=True)
        log = args.evidence / f"server-{stage}.log"
        stream = log.open("wb")
        command = ["taskset", "-c", "2,4", str(args.server_binary), "serve_live_probe", "--exact", "--nocapture"]
        server_env = dict(env, PARTITIONLINE_FETCH_LIVE_PORT=str(args.port), PARTITIONLINE_FETCH_LIVE_DIR=str(server_dir),
                          PARTITIONLINE_FETCH_LIVE_SEGMENTS="1", PARTITIONLINE_FETCH_LIVE_COMPACTION="1")
        process = subprocess.Popen(command, env=server_env, stdout=stream, stderr=subprocess.STDOUT)
        servers.append({"stage": stage, "command": command, "log": str(log), "pid": process.pid,
                        "config": {key: server_env[key] for key in server_env if key.startswith("PARTITIONLINE_FETCH_LIVE_")}})
        deadline = time.monotonic() + 15
        while not (server_dir / "ready").exists():
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("Actual server did not become ready")
            time.sleep(0.04)

    def stop(stage):
        nonlocal process, stream
        (server_dir / "stop").write_bytes(b"stop")
        try:
            code = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError("Graceful actual server shutdown exceeded bound")
        stream.close()
        servers[-1].update(exit_code=code, log_sha256=sha(Path(servers[-1]["log"])))
        process = stream = None
        if code != 0:
            raise RuntimeError("Actual server failed during peer phase")
        states.append({"stage": stage, **snapshot(server_dir, args.evidence / f"state-{stage}")})

    def peer(reader, producer, operation, stage):
        prefix = "cp-" + producer
        name = f"{stage}-{operation}-{reader}-{producer}"
        report = args.evidence / (name + ".json")
        log = args.evidence / (name + ".log")
        common = [bootstrap, prefix, operation, stage]
        if reader.startswith("j"):
            release = RELEASES[("j412", "j421", "j431").index(reader)]
            cp = ":".join(map(str, (args.sdk_work / release / "classes", args.jars / f"kafka-clients-{release}.jar", args.jars / "slf4j-api-1.7.36.jar")))
            command = ["taskset", "-c", "2,4", "java", "-Xmx128m", "-cp", cp, "CompactionPeer", *common, release, str(report)]
        else:
            binary = args.native_binary if reader == "native" else args.rust_binary
            command = ["taskset", "-c", "2,4", str(binary), *common, str(report)]
        with log.open("wb") as out:
            completed = subprocess.run(command, env=env, stdout=out, stderr=subprocess.STDOUT, check=False, timeout=100)
        item = {"reader": reader, "producer": producer, "operation": operation, "stage": stage,
                "command": command, "exit_code": completed.returncode, "log": str(log), "log_sha256": sha(log)}
        results.append(item)
        if report.exists():
            item.update(report=str(report), report_sha256=sha(report))
        if completed.returncode != 0:
            raise RuntimeError(f"Actual {reader} {operation}/{stage} failed against {producer}")
        observed = checked_json(report)
        if observed.get("passed") is not True:
            raise RuntimeError("Peer receipt did not pass")
        item.update(assertions=observed["assertions"], records=observed["records"])
        for topic, identity in observed["identities"].items():
            if len(identity) != 32 or any(c not in "0123456789abcdef" for c in identity) or identity == "0" * 32:
                raise RuntimeError("Invalid public topic identity")
            if topic in identities and identities[topic] != identity:
                raise RuntimeError("Topic UUID changed across compaction/process restart")
            identities[topic] = identity
        # Compare actual cross-language consumer receipts, independent of their own assertions.
        for event in observed["history"]:
            if event["label"] in ("public-consumer-record", "public-producer-delivery"):
                record = event["record"]
                encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                event["independent_record_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
        return observed

    def reads(stage):
        references = {}
        for producer in PRODUCERS:
            for reader in PRODUCERS:
                observed = peer(reader, producer, "read", stage)
                records = [event for event in observed["history"] if event["label"] == "public-consumer-record"]
                semantic = [(event["seek"], event["record"]) for event in records]
                if producer in references and references[producer] != semantic:
                    raise RuntimeError("Actual public cross-language retained record histories differ")
                references[producer] = semantic
        # All producers use the same literal scenario inputs, so compare their
        # independently observed records after removing only the topic identity.
        standardized = []
        for producer, semantic in references.items():
            rows = []
            for seek, record in semantic:
                normalized = dict(record)
                normalized["topic"] = normalized["topic"].removeprefix("cp-" + producer + "-")
                rows.append((seek, normalized))
            standardized.append(rows)
        if any(rows != standardized[0] for rows in standardized[1:]):
            raise RuntimeError("Actual retained records differ between public producer implementations")

    def compact(stage, clock):
        if len(identities) != 15:
            raise RuntimeError("All fifteen actual public topic UUIDs required before operator work")
        for topic, identity in sorted(identities.items()):
            (server_dir / "compact-result").unlink(missing_ok=True)
            payload = bytes.fromhex(identity) + struct.pack(">iq", 0, clock)
            if len(payload) != 28:
                raise RuntimeError("Exact local operator frame required")
            temporary = server_dir / "compact-request.tmp"
            temporary.write_bytes(payload)
            temporary.rename(server_dir / "compact-request")
            deadline = time.monotonic() + 10
            while not (server_dir / "compact-result").exists():
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("Durable local compaction operator failed")
                time.sleep(0.02)
            result = checked_json(server_dir / "compact-result")
            required = {"start_offset", "end_offset", "scanned_records", "retained_records", "rewritten_segments", "bytes_before", "bytes_after"}
            if set(result) != required or any(not isinstance(value, int) or value < 0 for value in result.values()):
                raise RuntimeError("Unexpected bounded local compaction receipt")
            if result["retained_records"] > result["scanned_records"] or result["bytes_after"] > result["bytes_before"]:
                raise RuntimeError("Compaction operator receipt violates monotonic cleaning bounds")
            operations.append({"stage": stage, "topic": topic, "uuid_hex": identity, "clock_ms": clock,
                               "operator_request_hex": payload.hex(), "result": result})
            (args.evidence / f"operator-{stage}-{topic}.json").write_text(json.dumps(operations[-1], indent=2) + "\n")

    try:
        prepare()
        start("initial")
        for producer in PRODUCERS:
            peer(producer, producer, "seed", "initial")
        reads("initial")
        stop("initial")
        for stage, clock in (("first", 2000), ("before-expiry", 2999), ("expired", 3000)):
            start(stage)
            compact(stage, clock)
            reads(stage)
            stop(stage)
        start("restart")
        reads("restart")
        for producer in PRODUCERS:
            peer(producer, producer, "append", "appended")
        reads("appended")
        stop("appended")
        passed = len(results) == 160 and len(operations) == 45 and len(identities) == 15
        if not passed:
            raise RuntimeError("Complete declared actual peer job matrix required")
    except Exception as error:
        failure = f"{type(error).__name__}: {error}"
    finally:
        if process is not None:
            (server_dir / "stop").write_bytes(b"stop")
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if stream is not None:
                stream.close()
            servers[-1].update(exit_code=process.returncode, log_sha256=sha(Path(servers[-1]["log"])))
        if not passed:
            states.append({"stage": "failure", **snapshot(server_dir, args.evidence / "state-failure")})
        receipts = {name: {"path": str(path), "sha256": sha(path)} for name, path in (
            ("source", args.source_receipt), ("java_build", args.java_build_receipt), ("native_build", args.native_build_receipt), ("rust_build", args.rust_build_receipt))}
        report = {"schema_version": 1, "source_sha": args.source_sha, "passed": passed, "failure": failure,
                  "toolchain": args.toolchain, "features": args.features, "port": args.port,
                  "actual_peer_jobs": len(results), "results": results, "servers": servers, "states": states,
                  "operator_calls": operations, "public_topic_identities": identities, "receipts": receipts,
                  "runner_sha256": sha(Path(__file__)), "binaries": {name: {"path": str(path), "sha256": sha(path)} for name, path in (
                      ("server", args.server_binary), ("native", args.native_binary), ("rust", args.rust_binary))},
                  "limits": "Ordinary RF1, manual assignment; no transactions/idempotent producer/control state or group coordinator. Public consumers expose retained records/positions, not empty-batch header preservation. Local explicit test operator is not a Kafka wire API."}
        (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": passed, "actual_peer_jobs": len(results), "operator_calls": len(operations), "failure": failure}), flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
