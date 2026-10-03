#!/usr/bin/env python3
"""Observe a copied public test state with owned processes and numeric traces."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import threading


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["snapshot", "server", "peer", "preload", "original-state", "state", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--production-source-sha", required=True)
    args = parser.parse_args()
    args.output.mkdir()
    shutil.copytree(args.original_state, args.state)
    fixtures = args.snapshot / "partitionline-broker/tests/fixtures/tls"
    argv = ["taskset", "-c", "0-2,4", str(args.server), str(args.state), str(fixtures), "29372", "29373",
            str(args.snapshot / "partitionline-broker/tests/fixtures/sasl-wire/apache-bootstrap.tsv"), "restart"]
    events, ready = [], queue.Queue()
    with (args.output / "server.stderr.log").open("xb") as stderr:
        server = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, text=True)

        def read():
            with (args.output / "server.stdout.log").open("x") as output:
                for line in server.stdout:
                    assert len(line) <= 4096 and len(events) <= 10000
                    output.write(line)
                    output.flush()
                    events.append(json.loads(line))
                    if events[-1].get("event") == "ready":
                        ready.put(True)
            ready.put(False)

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        try:
            assert ready.get(timeout=10), "owned diagnostic server exited"
            peer_argv = ["taskset", "-c", "0-2,4", str(args.peer), "localhost:29372", "localhost:29373",
                         str(fixtures / "ca1.cert.pem"), "admin"]
            env = os.environ.copy()
            env["LD_PRELOAD"] = str(args.preload)
            with (args.output / "peer.stdout.log").open("xb") as out, (args.output / "peer.stderr.log").open("xb") as err:
                peer = subprocess.run(peer_argv, stdout=out, stderr=err, env=env, timeout=45)
        finally:
            if server.poll() is None:
                server.stdin.write("STOP\n")
                server.stdin.flush()
                server.stdin.close()
            try:
                server_code = server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()  # Only this directly owned evidence process.
                server.wait(timeout=5)
                raise
            reader.join(timeout=3)
    assert server_code == 0 and not reader.is_alive()
    assert events[-1]["worker_failures"] == 0
    assert events[-1]["tls_accepted"] == events[-1]["tls_joined"]
    assert events[-1]["plain_accepted"] == events[-1]["plain_joined"]
    report = {"scope": "Development diagnosis; copied public synthetic state and owned server. Numeric request framing/digest only, no auth tokens/keys logged. Preload forwards unchanged bytes; final live driver has no preload.",
              "production_source_sha": args.production_source_sha,
              "diagnostic_runner_sha256": sha(Path(__file__)), "server_binary_sha256": sha(args.server),
              "peer_binary_sha256": sha(args.peer), "preload_binary_sha256": sha(args.preload),
              "commands": [{"argv": argv, "pid": server.pid, "exit_code": server_code},
                           {"argv": peer_argv, "environment": {"LD_PRELOAD": str(args.preload)}, "exit_code": peer.returncode}],
              "server_events": events,
              "logs_sha256": {p.name: sha(p) for p in sorted(args.output.glob("*.log"))}}
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"diagnostic_peer_exit": peer.returncode, "joined_server_exit": server_code}))


if __name__ == "__main__":
    main()
