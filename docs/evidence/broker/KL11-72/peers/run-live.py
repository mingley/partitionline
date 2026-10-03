#!/usr/bin/env python3
"""Bounded owned Rust listener lifecycle, real Java/C peers and restart audit."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import shutil
import socket
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit("Use normal Python without -O; pin checks are assertions.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--git-repository", type=Path, required=True)
    parser.add_argument("--jars", type=Path, required=True)
    parser.add_argument("--peer-build", type=Path, required=True)
    parser.add_argument("--peer-receipt", type=Path, required=True)
    parser.add_argument("--native-library", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tls-port", type=int, default=29372)
    parser.add_argument("--plain-port", type=int, default=29373)
    args = parser.parse_args()
    args.output.mkdir()
    args.state.mkdir(mode=0o700)
    assert len(args.source_sha) == 40 and all(c in "0123456789abcdef" for c in args.source_sha)
    assert 1024 <= args.tls_port <= 65535 and 1024 <= args.plain_port <= 65535 and args.tls_port != args.plain_port
    native = json.loads((HERE / "native-references/pins.json").read_text())
    assert sha(args.native_library) == native["library_sha256"]
    fixture_dir = ROOT / "partitionline-broker/tests/fixtures/tls"
    bootstrap = ROOT / "partitionline-broker/tests/fixtures/sasl-wire/apache-bootstrap.tsv"
    paths = [HERE / "server/Cargo.toml", HERE / "server/Cargo.lock", HERE / "server/src/main.rs",
             HERE / "SaslSocketPeer.java", HERE / "sasl-native-peer.c", HERE / "compile-peers.py",
             HERE / "audit-journal.py", HERE.parent / "oracle/SaslWireOracle.java", Path(__file__), ROOT / "partitionline-broker/Cargo.toml",
             ROOT / "partitionline-broker/Cargo.lock", bootstrap]
    paths += sorted((ROOT / "partitionline-broker/src").rglob("*.rs"))
    paths += [fixture_dir / name for name in ["server1.cert.der", "server1.key.der", "ca1.cert.pem"]]
    objects = {}
    for path in paths:
        relative = path.relative_to(ROOT).as_posix()
        raw = subprocess.run(["git", "show", args.source_sha + ":" + relative],
                             cwd=args.git_repository, check=True, capture_output=True).stdout
        assert raw == path.read_bytes(), "source object differs: " + relative
        objects[relative] = sha(path)
    peer_receipt = json.loads(args.peer_receipt.read_text())
    assert peer_receipt["verdict"] == "passed" and peer_receipt["source_sha"] == args.source_sha
    for relative, expected in peer_receipt["source_sha256"].items():
        assert sha(ROOT / relative) == expected
    assert sha(args.peer_build / "sasl-native-peer") == peer_receipt["native_binary_sha256"]
    for version, pin in peer_receipt["native_peer_build"].items():
        assert sha(args.jars / ("kafka-clients-" + version + ".jar")) == pin["jar_sha256"]
        assert {p.name: sha(p) for p in sorted((args.peer_build / version).glob("*.class"))} == pin["classes_sha256"]
    report = {"source_sha": args.source_sha, "verified_source_objects_sha256": objects,
              "scope": "Actual Rust TLS/SASL/Profile/Store/MetadataRouter composed in an evidence-only harness; independent Java SASL clients/generated requests and native librdkafka/OpenSSL connections/admin. No ready-to-deploy broker/full KIP368 claim.",
              "production_qualification": False, "commands": [], "server_lifecycles": [],
              "native_library_sha256": sha(args.native_library), "ports": [args.tls_port, args.plain_port],
              "peer_compilation_receipt_sha256": sha(args.peer_receipt),
              "provenance": {"original_peer_oracle_worker": "zstd_decision",
                             "followup_orchestration_worker": "consumer_lookups (production implementer)",
                             "independent_expectations": "Pinned Apache generated parsers/SASL clients and unmodified native SDK; follow-up orchestration is explicitly attributed."},
              "limits": {"connections_per_listener": 64, "handlers_per_listener": 8, "request_bytes": 16384,
                         "response_bytes": 1048576, "preauth_absolute_seconds": 3, "read_handler_write_seconds": 5}}
    result_path = args.output / "results.json"

    def retain():
        result_path.write_text(json.dumps(report, indent=2) + "\n")

    def execute(argv, name, timeout, env=None, expected_exit=0):
        out, err = args.output / (name + ".stdout.log"), args.output / (name + ".stderr.log")
        timed_out = False
        with out.open("xb") as stdout, err.open("xb") as stderr:
            try:
                result = subprocess.run(argv, cwd=ROOT, stdout=stdout, stderr=stderr, env=env, timeout=timeout)
                code = result.returncode
            except subprocess.TimeoutExpired:
                code, timed_out = None, True
        report["commands"].append({"argv": argv, "exit_code": code, "expected_exit_code": expected_exit,
                                  "completion_verdict": "passed" if code == 0 and not timed_out else "failed",
                                  "expected_observation": code == expected_exit and not timed_out,
                                  "timeout_seconds": timeout, "timed_out": timed_out,
                                  "stdout": out.name, "stderr": err.name, "stdout_sha256": sha(out), "stderr_sha256": sha(err)})
        retain()
        assert code == expected_exit and not timed_out, name + " failed; logs retained"

    env = os.environ.copy()
    env.update(CARGO_HOME="/workspace/work/cargo", RUSTUP_HOME="/workspace/work/rustup",
               PATH="/workspace/work/cargo/bin:" + env["PATH"], CARGO_TARGET_DIR=str(args.target),
               CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
    execute(["taskset", "-c", "0-2,4", "cargo", "+stable", "clean", "-p", "partitionline-broker", "--manifest-path",
             str(ROOT / "partitionline-broker/Cargo.toml")], "broker-package-clean", 30, env)
    execute(["taskset", "-c", "0-2,4", "cargo", "+stable", "clean", "-p", "partitionline-sasl-wire-evidence", "--manifest-path",
             str(HERE / "server/Cargo.toml")], "harness-package-clean", 30, env)
    execute(["taskset", "-c", "0-2,4", "cargo", "+stable", "build", "--locked", "--offline", "--manifest-path",
             str(HERE / "server/Cargo.toml")], "actual-server-build", 300, env)
    server = args.target / "debug/partitionline-sasl-wire-evidence"
    report["server_binary_sha256"] = sha(server)
    report["native_peer_binary_sha256"] = sha(args.peer_build / "sasl-native-peer")
    report["toolchain"] = subprocess.run(["/workspace/work/cargo/bin/rustup", "run", "stable", "rustc", "--version", "--verbose"],
                                          env=env, capture_output=True, text=True, check=True).stdout
    spec = importlib.util.spec_from_file_location("journal_audit", HERE / "audit-journal.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Server:
        def __init__(self, phase):
            self.phase, self.events, self.ready = phase, [], queue.Queue()
            self.stdout = args.output / (phase + "-server.stdout.log")
            self.stderr = args.output / (phase + "-server.stderr.log")
            self.err_file = self.stderr.open("xb")
            self.argv = ["taskset", "-c", "0-2,4", str(server), str(args.state), str(fixture_dir),
                         str(args.tls_port), str(args.plain_port), str(bootstrap), phase]
            self.process = subprocess.Popen(self.argv, cwd=ROOT, env=env, stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=self.err_file, text=True)

            def read():
                try:
                    with self.stdout.open("x") as output:
                        for line in self.process.stdout:
                            assert len(line) <= 4096 and len(self.events) <= 10000, "bounded safe server receipts"
                            output.write(line)
                            output.flush()
                            event = json.loads(line)
                            self.events.append(event)
                            if event.get("event") == "ready":
                                self.ready.put(True)
                finally:
                    self.ready.put(False)

            self.reader = threading.Thread(target=read, daemon=True)
            self.reader.start()
            try:
                assert self.ready.get(timeout=15), "owned actual server exited before readiness"
            except BaseException:
                self.stop(validate=False)
                raise

        def stop(self, expect_pending=False, validate=True):
            pending = []
            if expect_pending and self.process.poll() is None:
                for port in [args.tls_port, args.plain_port]:
                    sock = socket.create_connection(("127.0.0.1", port), timeout=3)
                    sock.settimeout(5)
                    pending.append(sock)
                pending[1].sendall(b"\x00")
                time.sleep(0.15)
            if self.process.poll() is None:
                try:
                    self.process.stdin.write("STOP\n")
                    self.process.stdin.flush()
                except BrokenPipeError:
                    pass
                finally:
                    self.process.stdin.close()
            try:
                code = self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()  # Only this runner's directly owned process.
                self.process.wait(timeout=5)
                code = None
            self.reader.join(timeout=3)
            self.err_file.close()
            for sock in pending:
                try:
                    assert sock.recv(1) == b"", "pending socket survived joined shutdown"
                except ConnectionResetError:
                    pass
                finally:
                    sock.close()
            lifecycle = {"phase": self.phase, "argv": self.argv, "exit_code": code,
                         "stdout": self.stdout.name, "stderr": self.stderr.name,
                         "stdout_sha256": sha(self.stdout), "stderr_sha256": sha(self.stderr),
                         "events": self.events, "pending_sockets_at_stop": len(pending)}
            report["server_lifecycles"].append(lifecycle)
            retain()
            if not validate:
                return lifecycle
            assert code == 0 and not self.reader.is_alive(), "owned server shutdown failed"
            shutdown = self.events[-1]
            assert shutdown["event"] == "shutdown" and shutdown["worker_failures"] == 0
            assert shutdown["tls_accepted"] == shutdown["tls_joined"]
            assert shutdown["plain_accepted"] == shutdown["plain_joined"]
            assert shutdown["credential_store_joined"]
            if expect_pending:
                assert shutdown["shutdown_connections"] >= 1, "no pending connection cancellation observed"
            return lifecycle

    def java(version, phase):
        cp = ":".join(map(str, [args.peer_build / version, args.jars / ("kafka-clients-" + version + ".jar"),
                               args.jars / "slf4j-api-1.7.36.jar"]))
        execute(["taskset", "-c", "0-2,4", "java", "-Xms16m", "-Xmx128m", "-cp", cp, "SaslSocketPeer", str(args.tls_port),
                 str(args.plain_port), str(fixture_dir / "ca1.cert.pem"), phase], version + "-" + phase, 90)

    def native(phase):
        execute(["taskset", "-c", "0-2,4", str(args.peer_build / "sasl-native-peer"),
                 "localhost:" + str(args.tls_port), "localhost:" + str(args.plain_port),
                 str(fixture_dir / "ca1.cert.pem"), phase], "native-" + phase, 120)

    def native_describe_cleanup_failure(phase):
        name = "native-" + phase
        execute(["taskset", "-c", "0-2,4", str(args.peer_build / "sasl-native-peer"),
                 "localhost:" + str(args.tls_port), "localhost:" + str(args.plain_port),
                 str(fixture_dir / "ca1.cert.pem"), phase], name, 30, expected_exit=-6)
        events = [json.loads(line) for line in (args.output / (name + ".stdout.log")).read_text().splitlines()]
        assert any(event.get("operation") == "native-describe" and event.get("error") == 31 for event in events)
        assert events[-1] == {"operation": "native-describe-cleanup", "top_level_error": 31, "action": "destroy-event"}
        assert not any(event.get("status") == "pass" for event in events), "failed native completion mislabeled passing"
        report["commands"][-1]["completion_verdict"] = "failed_external_sdk_completion"
        report.setdefault("known_external_sdk_failures", []).append({
            "phase": phase, "verdict": "failed_external_sdk_completion", "exit_code": -6,
            "parsed_describe_error": 31,
            "cause": "Pinned native Describe parser stores static/stack error text as owned admin_result.errstr; event cleanup frees it. Response validity is independently decoded; no production behavior workaround.",
            "clean_native_completion": False, "pre_cleanup_marker_verified": True,
            "stdout": name + ".stdout.log", "stderr": name + ".stderr.log"})
        retain()

    def checkpoint(phase, expected_entries):
        original = args.state / "credentials.log"
        assert original.stat().st_mode & 0o777 == 0o600, "private credential journal permissions"
        target = args.output / (phase + "-public-test-verifier-journal.bin")
        shutil.copyfile(original, target)
        audited = module.audit(target)
        assert audited["entries"] == expected_entries
        (args.output / (phase + "-journal-audit.json")).write_text(json.dumps(audited, indent=2) + "\n")
        report[phase + "_journal_audit"] = audited
        retain()

    active = None
    try:
        active = Server("initial")
        for version in ["4.1.2", "4.2.1", "4.3.1"]:
            java(version, "sessions")
        java("4.3.1", "admin")
        native("sessions")
        native("admin")
        native_describe_cleanup_failure("describe-user-denied")
        first = active.stop(expect_pending=True)
        active = None
        assert not any(event.get("correlation") == 727200 for event in first["events"]), "application dispatched before proof"
        dispatches = [event for event in first["events"] if event.get("event") == "application-dispatch"]
        assert all(event["generation"] > 0 for event in dispatches)
        assert any(event["user"] == "user" and event["tls"] for event in dispatches)
        assert any(event["user"] == "user" and not event["tls"] for event in dispatches)
        assert any(event["user"] == "unicode" for event in dispatches)
        generations = {event["correlation"]: event["generation"] for event in first["events"]
                       if event.get("correlation") in [727201, 727202]}
        assert generations[727202] > generations[727201], "new session did not capture rotated generation"
        checkpoint("initial", 7)
        active = Server("restart")
        java("4.3.1", "restart")
        native("restart")
        active.stop(expect_pending=True)
        active = None
        checkpoint("final", 9)
        assert {r["user"] for r in report["final_journal_audit"]["retained_identity_algorithms"]} == {"user", "admin", "unicode"}
        final_journal_sha = report["final_journal_audit"]["journal_sha256"]
        active = Server("default-admin")
        java("4.3.1", "admin-denied")
        native("admin-denied")
        native_describe_cleanup_failure("describe-admin-denied")
        active.stop(expect_pending=True)
        active = None
        assert sha(args.state / "credentials.log") == final_journal_sha, "denied default-admin requests modified durable state"
        # No sensitive SASL exception messages, callbacks, auth frames or keys
        # are printed by these peers. Inspect all retained stdout/stderr too.
        forbidden = [password for values in module.PASSWORDS.values() for password in values]
        for path in args.output.glob("*.log"):
            text = path.read_text()
            assert not any(password in text for password in forbidden), "password appeared in runtime log"
        schema_events = []
        for name in ["4.3.1-admin", "4.3.1-admin-denied"]:
            events = [json.loads(line) for line in (args.output / (name + ".stdout.log")).read_text().splitlines()]
            matches = [event for event in events if event.get("operation") == "describe-schema" and event.get("error") == 31]
            assert matches, "official Java whole-input Describe31 schema proof missing"
            schema_events.append({"phase": name, "decoder": "Apache4.3.1 generated DescribeUserScramCredentialsResponseData",
                                  "whole_input_consumed": True, "results_empty": True, "error_message_null": True,
                                  "error": 31, "stdout": name + ".stdout.log"})
        assert len(report.get("known_external_sdk_failures", [])) == 2
        verdict = "scoped_server_gates_passed_with_sdk_completion_failures"
        report.update(verdict=verdict, driver_completed=True, server_contract_gates="passed",
                      positive_native_admin_restart_completion="passed", native_describe_error_completion="failed",
                      unqualified_all_peer_pass=False, describe31_official_schema_proofs=schema_events,
                      preauth_application_dispatches=0,
                      captured_rotation_generation=generations[727201], new_rotation_generation=generations[727202],
                      joined_owned_listener_lifecycles=3, passwords_absent_from_logs=True,
                      authenticated_java_releases=["4.1.2", "4.2.1", "4.3.1"], native_runtime_version="2.15.0")
        retain()
        print(json.dumps({"verdict": verdict, "owned_listener_lifecycles": 3, "verifier_entries": 9,
                          "failed_external_sdk_completions": 2}))
    except BaseException as failure:
        report.update(verdict="failed", failure_class=type(failure).__name__, failure_context=str(failure))
        retain()
        raise
    finally:
        if active is not None:
            active.stop(validate=False)


if __name__ == "__main__":
    main()
