#!/usr/bin/env python3
"""Freeze, run, resume and check resource samples without deleting topics."""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
PROFILE = {"payload_bytes": 256, "partitions": 1, "producer_budget_bytes": 4096,
           "consumer_budget_bytes": 4096, "max_poll_records": 1, "max_in_flight": 1,
           "acks": "all", "compression": "none", "max_runtime_tasks": 128,
           "max_socket_fds": 64, "decode_ceiling_bytes": 64 * 1024 * 1024}


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def digest(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def atomic(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as stream:
        json.dump(data, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sources():
    paths = [*ROOT.glob("src/**/*.rs"), ROOT / "Cargo.toml", ROOT / "Cargo.lock",
             ROOT / "rust-toolchain.toml", ROOT / "examples/resource_soak.rs",
             ROOT / "examples/common/resource_peer.rs", Path(__file__),
             ROOT / "scripts/run-resource-soak.sh"]
    return {str(p.relative_to(ROOT)): digest(p) for p in sorted(paths)}


def rows(path):
    with path.open() as stream:
        for number, line in enumerate(stream, 1):
            if len(line) > 1024 * 1024:
                raise ValueError(f"{path}:{number}: oversized row")
            try:
                yield json.loads(line)
            except (ValueError, TypeError) as error:
                raise ValueError(f"{path}:{number}: malformed row") from error


def check_samples(raw, host, mode):
    previous = None
    final = closure = peer = None
    count = 0
    for row in rows(raw):
        kind = row.get("kind")
        if kind == "sample":
            if final is not None:
                raise ValueError("sample after final")
            names = ("offered", "accepted", "completed", "failed", "ambiguous", "pending",
                     "producer_queue_bytes", "consumer_buffered_bytes", "application_decoded_bytes",
                     "delivered", "delivered_bytes", "fetch_errors", "runtime_tasks", "peer_connections",
                     "utc_ms", "elapsed_ms")
            if any(type(row.get(name)) is not int or row[name] < 0 for name in names):
                raise ValueError("missing, negative or noninteger sample field")
            if row["offered"] != row["accepted"] + row["failed"]:
                raise ValueError("offered != accepted + admission failures")
            if row["accepted"] != row["completed"] + row["ambiguous"] + row["pending"]:
                raise ValueError("accepted outcome mismatch")
            if row["producer_queue_bytes"] > PROFILE["producer_budget_bytes"]:
                raise ValueError("producer budget exceeded")
            ceiling = PROFILE["consumer_budget_bytes"] if mode == "short" else PROFILE["decode_ceiling_bytes"]
            if row["consumer_buffered_bytes"] > ceiling or row["application_decoded_bytes"] > ceiling:
                raise ValueError("consumer payload ceiling exceeded")
            if row["runtime_tasks"] > PROFILE["max_runtime_tasks"] or row["peer_connections"] > 16:
                raise ValueError("task/connection cap exceeded")
            if previous:
                for name in ("offered", "accepted", "completed", "failed", "ambiguous",
                             "delivered", "delivered_bytes", "elapsed_ms"):
                    if row[name] < previous[name]:
                        raise ValueError(f"counter regressed: {name}")
                if (previous["consumer_buffered_bytes"] > PROFILE["consumer_budget_bytes"]
                        and row["consumer_buffered_bytes"] > previous["consumer_buffered_bytes"]):
                    raise ValueError("buffer grew while over its soft budget")
            previous = row
            count += 1
            if row.get("final") is True:
                final = row
        elif kind == "closure" and closure is None:
            closure = row
        elif kind == "peer_closed" and peer is None:
            peer = row
        else:
            raise ValueError("unknown or duplicate record kind")
    if not final or not closure or not peer or count < 2:
        raise ValueError("incomplete sample/closure receipt")
    if any(final[name] != 0 for name in ("pending", "producer_queue_bytes",
                                       "consumer_buffered_bytes", "application_decoded_bytes")):
        raise ValueError("work or payload retained after close")
    if closure.get("consumer_joined") is not True or peer.get("joined") is not True:
        raise ValueError("missing joins")
    if peer.get("runtime_tasks") != 0:
        raise ValueError("runtime tasks retained after all owned resources closed")
    if (type(closure.get("load_elapsed_ms")) is not int or closure["load_elapsed_ms"] < 0
            or closure["load_elapsed_ms"] > final["elapsed_ms"]
            or type(closure.get("interrupted")) is not bool):
        raise ValueError("invalid load duration or interruption receipt")
    if mode == "short" and (peer.get("owned") is not True or peer.get("port_reusable") is not True
                             or peer.get("connections") != 0):
        raise ValueError("peer lifecycle incomplete")
    host_count = 0
    for row in rows(host):
        if any(type(row.get(n)) is not int or row[n] < 0 for n in ("rss_bytes", "threads", "socket_fds")):
            raise ValueError("missing host telemetry")
        if row["socket_fds"] > PROFILE["max_socket_fds"]:
            raise ValueError("socket descriptor cap exceeded")
        host_count += 1
    if host_count < 2:
        raise ValueError("missing host samples")
    return {"samples": count, "host_samples": host_count, "final": final,
            "load_elapsed_ms": closure["load_elapsed_ms"], "interrupted": closure["interrupted"],
            "rss_bound_claimed": False, "production_qualification": False}


def proc_sample(pid):
    proc = Path(f"/proc/{pid}")
    status = {}
    for line in (proc / "status").read_text().splitlines():
        key, _, value = line.partition(":")
        status[key] = value.strip()
    sockets = 0
    for fd in (proc / "fd").iterdir():
        try:
            sockets += os.readlink(fd).startswith("socket:[")
        except FileNotFoundError:
            pass
    return {"utc": utc(), "monotonic_ns": time.monotonic_ns(), "pid": pid,
            "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
            "threads": int(status["Threads"]), "socket_fds": sockets}


def checked_inputs(args):
    if not sys.platform.startswith("linux"):
        raise ValueError("driver requires Linux /proc telemetry")
    binary = args.binary.resolve(strict=True)
    if not os.access(binary, os.X_OK):
        raise ValueError("binary is not executable")
    if not 100 <= args.duration_ms <= 86_400_000 or not 1 <= args.rate <= 1_000_000:
        raise ValueError("duration/rate out of bounds")
    if not 0 <= args.slow_ms <= 60_000 or not 0 <= args.peer_delay_ms <= 60_000:
        raise ValueError("delay out of bounds")
    baseline = None
    if args.mode == "controlled":
        if not args.bootstrap or not args.topic or not args.baseline:
            raise ValueError("controlled mode requires bootstrap, owned topic and measured baseline")
        baseline = json.loads(args.baseline.read_text())
        if baseline.get("profile") != {**PROFILE, "idempotent": True}:
            raise ValueError("baseline profile differs")
        rate = baseline.get("sustainable_records_per_second")
        if type(rate) not in (int, float) or not 0 < rate <= 500_000 or args.rate < 2 * rate:
            raise ValueError("offered rate must be at least 2x the measured sustainable rate")
        if baseline.get("binary_sha256") != digest(binary) or baseline.get("sources") != sources():
            raise ValueError("baseline binary/source differs")
        artifact = args.baseline.parent / baseline["raw_artifact"]
        if digest(artifact) != baseline.get("raw_sha256") or not baseline.get("measured_utc"):
            raise ValueError("baseline raw receipt missing or changed")
    topic = args.topic or ("pl-soak-" + hashlib.sha256(str(args.output.resolve()).encode()).hexdigest()[:16])
    if not topic.startswith("pl-soak-") or len(topic) > 200 or not all(c.isascii() and (c.isalnum() or c in "-_") for c in topic):
        raise ValueError("topic must be an explicitly owned pl-soak-* topic")
    return {"schema_version": 1, "created_utc": utc(), "mode": args.mode,
            "duration_ms": args.duration_ms, "rate": args.rate, "slow_ms": args.slow_ms,
            "peer_delay_ms": args.peer_delay_ms, "topic": topic, "bootstrap": args.bootstrap,
            "binary": str(binary), "binary_sha256": digest(binary), "sources": sources(),
            "profile": {**PROFILE, "idempotent": args.mode == "controlled"}, "baseline": baseline,
            "baseline_sha256": digest(args.baseline) if args.baseline else None,
            "host": {"node": platform.node(), "platform": platform.platform(),
                     "allocator": "Rust system allocator; host libc reported by platform",
                     "libc": platform.libc_ver(), "cpu_count": os.cpu_count()},
            "stop": "SIGINT/SIGTERM to this driver, or create the attempt stop file; resume the same output",
            "limits": ["short peer is authored; synthetic fetch data is separate from produced records",
                       "RSS includes peer allocations in short mode", "no topic create/delete or offset commit",
                       "counter samples do not prove record integrity, replica durability or speed ranking"]}


def audit(directory):
    frozen = json.loads((directory / "frozen.json").read_text())
    manifest = json.loads((directory / "manifest.json").read_text())
    if digest(directory / "frozen.json") != manifest["frozen_sha256"]:
        raise ValueError("frozen manifest changed")
    if not manifest["attempts"]:
        raise ValueError("no executed attempts")
    checked = []
    for number, attempt in enumerate(manifest["attempts"], 1):
        if attempt.get("number") != number or attempt.get("status") not in ("passed", "interrupted", "failed"):
            raise ValueError("incomplete/nonsequential attempt; retain and investigate before resuming")
        for path, expected in attempt["artifacts"].items():
            if digest(directory / path) != expected:
                raise ValueError(f"artifact changed: {path}")
        if attempt["status"] == "failed":
            raise ValueError(f"retained failed attempt {number}: {attempt.get('error')}")
        result = check_samples(directory / attempt["raw"], directory / attempt["host"], frozen["mode"])
        if result != attempt["check"] or attempt["exit_code"] != 0 or not attempt["waited"]:
            raise ValueError("result receipt differs or child not waited")
        if (attempt["status"] == "interrupted") != result["interrupted"]:
            raise ValueError("attempt interruption status differs")
        checked.append(result)
    elapsed = sum(r["load_elapsed_ms"] for r in checked)
    completed = not checked[-1]["interrupted"] and elapsed >= frozen["duration_ms"]
    if manifest.get("completed") is not completed:
        raise ValueError("completion receipt differs from executed load duration")
    return {"attempts": len(checked), "load_elapsed_ms": elapsed,
            "completed": manifest.get("completed") is True,
            "production_qualification": False, "performance_claims_valid": False}


def execute(args):
    directory = args.output.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.resume:
            frozen = json.loads((directory / "frozen.json").read_text())
            manifest = json.loads((directory / "manifest.json").read_text())
            audit(directory)
            if sources() != frozen["sources"] or digest(frozen["binary"]) != frozen["binary_sha256"]:
                raise ValueError("source/binary changed since freeze; start a new run")
            if manifest.get("completed"):
                return audit(directory)
        else:
            if any(p.name != ".lock" for p in directory.iterdir()):
                raise ValueError("output is not empty; use --resume or a new directory")
            frozen = checked_inputs(args)
            atomic(directory / "frozen.json", frozen)
            manifest = {"frozen_sha256": digest(directory / "frozen.json"), "attempts": [], "completed": False}
            atomic(directory / "manifest.json", manifest)
        completed = sum(a.get("check", {}).get("load_elapsed_ms", 0) for a in manifest["attempts"])
        remaining = max(100, frozen["duration_ms"] - completed)
        number = len(manifest["attempts"]) + 1
        attempt_dir = directory / f"attempt-{number:04d}"
        attempt_dir.mkdir()
        stop_file = attempt_dir / "stop"
        raw = attempt_dir / "samples.jsonl"
        host = attempt_dir / "host.jsonl"
        stderr = attempt_dir / "stderr.log"
        env = {**os.environ, "SOAK_MODE": frozen["mode"], "SOAK_BOOTSTRAP": frozen["bootstrap"] or "",
               "SOAK_TOPIC": frozen["topic"], "SOAK_DURATION_MS": str(remaining),
               "SOAK_RATE": str(frozen["rate"]), "SOAK_SLOW_MS": str(frozen["slow_ms"]),
               "SOAK_PEER_DELAY_MS": str(frozen["peer_delay_ms"]), "SOAK_STOP_FILE": str(stop_file),
               "SOAK_ID_BASE": str((number - 1) << 48)}
        attempt = {"number": number, "started_utc": utc(), "status": "running",
                   "raw": str(raw.relative_to(directory)), "host": str(host.relative_to(directory)),
                   "command": [frozen["binary"]], "environment": {k: v for k, v in env.items() if k.startswith("SOAK_")},
                   "binary_sha256_before": digest(frozen["binary"]), "artifacts": {}}
        manifest["attempts"].append(attempt)
        atomic(directory / "manifest.json", manifest)
        interrupted = False
        def stopping(_sig, _frame):
            nonlocal interrupted
            interrupted = True
            stop_file.touch()
        handlers = {sig: signal.signal(sig, stopping) for sig in (signal.SIGINT, signal.SIGTERM)}
        child = None
        try:
            with raw.open("w") as out, stderr.open("w") as err, host.open("w") as samples:
                child = subprocess.Popen(attempt["command"], env=env, stdout=out, stderr=err)
                attempt["pid"] = child.pid
                atomic(directory / "manifest.json", manifest)
                deadline = time.monotonic() + remaining / 1000 + 20
                interrupt_deadline = None
                while child.poll() is None:
                    if interrupted and interrupt_deadline is None:
                        interrupt_deadline = time.monotonic() + 10
                    if time.monotonic() > min(deadline, interrupt_deadline or deadline):
                        raise TimeoutError("child deadline exceeded")
                    try:
                        samples.write(json.dumps(proc_sample(child.pid)) + "\n")
                        samples.flush()
                        os.fsync(samples.fileno())
                    except (FileNotFoundError, KeyError, ProcessLookupError):
                        if child.poll() is None:
                            time.sleep(.01)
                            continue
                    time.sleep(.05)
                attempt["exit_code"] = child.wait()
                attempt["waited"] = True
                for stream in (out, err, samples):
                    stream.flush()
                    os.fsync(stream.fileno())
            if attempt["exit_code"] != 0:
                raise ValueError(f"child exited {attempt['exit_code']}; see {stderr}")
            result = check_samples(raw, host, frozen["mode"])
            attempt["check"] = result
            attempt["status"] = "interrupted" if result["interrupted"] else "passed"
            manifest["completed"] = not result["interrupted"] and completed + result["load_elapsed_ms"] >= frozen["duration_ms"]
        except Exception as error:
            attempt["status"] = "failed"
            attempt["error"] = str(error)
            if child:
                if child.poll() is None:
                    child.kill()
                attempt["exit_code"] = child.wait()
                attempt["waited"] = True
            raise
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
            attempt["ended_utc"] = utc()
            attempt["artifacts"] = {str(p.relative_to(directory)): digest(p) for p in attempt_dir.iterdir() if p.is_file()}
            atomic(directory / "manifest.json", manifest)
        return audit(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--mode", choices=("short", "controlled"), default="short")
    parser.add_argument("--duration-ms", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=800)
    parser.add_argument("--slow-ms", type=int, default=25)
    parser.add_argument("--peer-delay-ms", type=int, default=40)
    parser.add_argument("--bootstrap")
    parser.add_argument("--topic")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        if not args.check and not args.resume and args.binary is None:
            raise ValueError("--binary is required for a new run")
        result = audit(args.output) if args.check else execute(args)
        print(json.dumps(result, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError, TimeoutError) as error:
        print(f"resource soak failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
