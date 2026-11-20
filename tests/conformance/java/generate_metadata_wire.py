#!/usr/bin/env python3
"""Compare finite Metadata v13 bodies with pinned Apache SDKs."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "tests/conformance/java/ConformanceMetadataWire.java"
VERSION_SOURCE = ROOT / "tests/conformance/java/ConformanceMetadataVersions.java"
RUST = ROOT / "tests/metadata_wire_conformance.rs"
PINS = {
    "3.9.1": "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9",
    "4.1.0": "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627",
    "4.1.2": "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
    "4.2.1": "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
    "4.3.1": "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36",
}
COMMITS = {
    "3.9.1": "f745dfdcee2b9851204ddbbcd423626ab87294bc",
    "4.1.0": "13f70256db3c994c590e5d262a7cc50b9e973204",
    "4.1.2": "c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c",
    "4.2.1": "18d5ecd939c8d510fdd72d0abb1f7099659dcd58",
    "4.3.1": "26b251a451ce941d3d7a55e6487bcb7f16b5ad48",
}
CURRENT = ("4.1.2", "4.2.1", "4.3.1")
CASES = ("all", "empty", "name", "id", "tagged")
NESTED_CASES = ("by-name", "by-id", "known-empty")
END = time.monotonic() + 570
CHILD = None


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def interrupt(signum, _frame):
    raise InterruptedError("signal " + str(signum))


def group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def reap_adopted(pgid):
    while True:
        try:
            pid, _status = os.waitpid(-pgid, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def wait_for_group(child, seconds, receipt):
    deadline = time.monotonic() + seconds
    while True:
        if child.poll() is not None:
            reap_adopted(child.pid)
            alive = group_exists(child.pid)
            receipt["parent_exited_group_still_present"] |= alive
            if not alive:
                return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)


def stop_group(child):
    """Stop the owned process group and reap children adopted by the subreaper."""
    old_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    for sig in old_handlers:
        signal.signal(sig, signal.SIG_IGN)
    receipt = {"sigkill_used": False, "parent_exited_group_still_present": False}
    try:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        if not wait_for_group(child, 2, receipt):
            receipt["sigkill_used"] = True
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if not wait_for_group(child, 3, receipt):
                raise RuntimeError("process group remains after bounded SIGKILL; cleanup unproven")
        receipt["group_empty"] = True
        receipt["direct_child_waited"] = child.returncode is not None
        return receipt
    finally:
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)


class StartupTimeout(TimeoutError):
    """A supervised child did not report readiness before its deadline."""


def wait_for_marker(child, path, marker, seconds):
    deadline = min(END, time.monotonic() + seconds)
    while True:
        if path.stat().st_size > 65536:
            raise ValueError("readiness output exceeds budget")
        if marker in path.read_text():
            return
        if child.poll() is not None:
            raise RuntimeError("child exited before readiness")
        if time.monotonic() >= deadline:
            raise StartupTimeout("child readiness deadline")
        time.sleep(0.02)


def run(command, out, env=None, timeout=30, expect_failure=False, marker=None,
        expect_timeout=False, expect_startup_timeout=False, startup_seconds=20,
        preflight_marker=None):
    global CHILD
    remaining = min(timeout, END - time.monotonic())
    if remaining <= 0:
        raise TimeoutError("overall Metadata deadline")
    cleanup = None
    timed_out = False
    startup_timed_out = False
    preflight_completed = preflight_marker is None
    with out.open("x") as stream:
        CHILD = subprocess.Popen(
            command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True,
        )
        child = CHILD
        cleanup_started = False
        try:
            if expect_timeout:
                if marker is None:
                    raise ValueError("timeout control requires a readiness marker")
                if preflight_marker is not None:
                    wait_for_marker(child, out, preflight_marker, 20)
                    preflight_completed = True
                wait_for_marker(child, out, marker, startup_seconds)
                remaining = min(timeout, END - time.monotonic())
                if remaining <= 0:
                    raise TimeoutError("overall Metadata deadline")
            try:
                child.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                timed_out = True
                cleanup_started = True
                cleanup = stop_group(child)
        except BaseException as error:
            if not cleanup_started:
                cleanup_started = True
                cleanup = stop_group(child)
            if expect_startup_timeout and preflight_completed and isinstance(error, StartupTimeout):
                startup_timed_out = True
            else:
                raise
        finally:
            CHILD = None
        if group_exists(child.pid):
            cleanup = stop_group(child)
            raise RuntimeError("unexpected surviving process group: " + str(cleanup))
    if out.stat().st_size > 1_048_576:
        raise ValueError("process output exceeds budget")
    contents = out.read_text()
    if startup_timed_out != expect_startup_timeout:
        raise RuntimeError("unexpected startup timeout state in " + str(out))
    if not startup_timed_out:
        if timed_out != expect_timeout:
            print(contents[-8192:], flush=True)
            raise RuntimeError("unexpected timeout state in " + str(out) + ": " + str(cleanup))
        if not timed_out and (child.returncode != 0) != expect_failure:
            print(contents[-8192:], flush=True)
            raise RuntimeError("unexpected exit " + str(child.returncode) + " in " + str(out))
        if marker and marker not in contents:
            print(contents[-8192:], flush=True)
            raise RuntimeError("missing output marker in " + str(out))
    return {"exit_code": child.returncode, "log": str(out), "group_empty": True,
            "direct_child_waited": child.returncode is not None, "timeout_cleanup": cleanup,
            "startup_timed_out": startup_timed_out}


def verify_timeout_cleanup(output):
    # The child exits on TERM; its same-group descendant ignores TERM and needs KILL.
    descendant = ("import os,signal,time; "
                  "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                  "print(os.getpgrp(), flush=True); time.sleep(60)")
    parent = ("import os,subprocess,sys,time; "
              "p=subprocess.Popen([sys.executable,'-c',sys.argv[1]],"
              "stdout=subprocess.PIPE,text=True); "
              "group=int(p.stdout.readline()); "
              "assert group == os.getpgrp(); "
              "print('descendant-ready',flush=True); time.sleep(60)")
    command = [sys.executable, "-c", parent, descendant]
    runtime = run(command, output / "timeout-control.log", timeout=1,
                  expect_timeout=True, marker="descendant-ready")
    startup = run(command, output / "startup-timeout-control.log", timeout=1,
                  expect_timeout=True, expect_startup_timeout=True, startup_seconds=0.25,
                  preflight_marker="descendant-ready", marker="deliberately-absent")
    for result in (runtime, startup):
        cleanup = result["timeout_cleanup"]
        if not (cleanup["sigkill_used"] and cleanup["parent_exited_group_still_present"]
                and cleanup["group_empty"] and cleanup["direct_child_waited"]):
            raise RuntimeError("descendant cleanup control did not exercise escalation")
    print(json.dumps({"runtime_descendant_control": runtime["timeout_cleanup"],
                      "startup_descendant_control": startup["timeout_cleanup"]}), flush=True)
    return {"runtime": runtime, "startup": startup}


def peer(cache, release):
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / ("kafka-clients-" + release + ".jar")
    if not path.exists():
        url = "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/" + release + "/" + path.name
        with urllib.request.urlopen(url, timeout=25) as response:
            data = response.read(16 * 1024 * 1024 + 1)
        if len(data) > 16 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != PINS[release]:
            raise ValueError("wrong SDK download")
        path.write_bytes(data)
    if path.stat().st_size > 16 * 1024 * 1024 or digest(path) != PINS[release]:
        raise ValueError("wrong SDK cache")
    return path.resolve()


def corrupt_top_error(source, destination):
    lines = source.read_text().splitlines()
    if len(lines) != 5:
        raise ValueError("wrong case count")
    fields = lines[0].split(" ")
    if len(fields) != 3 or not fields[2].endswith("001f00"):
        raise ValueError("unexpected untagged v13 error suffix")
    fields[2] = fields[2][:-6] + "001e00"
    lines[0] = " ".join(fields)
    destination.write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--nested", action="store_true", help="compare the successful and mixed-response corpus")
    args = parser.parse_args()
    cases = NESTED_CASES if args.nested else CASES
    suffix = "-nested" if args.nested else ""
    rust_test = "metadata_v13_apache_nested_wire" if args.nested else "metadata_v13_apache_wire"
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupt)
    results = []
    input_paths = (SOURCE, RUST, Path(__file__).resolve())
    if args.nested:
        input_paths += (VERSION_SOURCE,)
    inputs = {str(path.relative_to(ROOT)): digest(path) for path in input_paths}
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "could not enable child subreaper")
        timeout_control = None if args.nested else verify_timeout_cleanup(output)
        for release in PINS if args.nested else CURRENT:
            directory = output / release
            directory.mkdir()
            jar = peer(output / "jars", release)
            java = ["java", "-Xmx128m", "--class-path", str(jar), str(SOURCE), str(jar)]
            fixture = directory / "apache.txt"
            result = {"release": release, "jar_sha256": digest(jar), "upstream_commit": COMMITS[release]}
            if args.nested:
                maximum = 12 if release == "3.9.1" else 13
                versions = f"metadata_declared=request:0-{maximum},response:0-{maximum}"
                version_log = directory / "versions.log"
                version_command = java[:4] + [str(VERSION_SOURCE), str(jar)]
                result["versions"] = run(version_command, version_log, marker=versions)
                if version_log.read_text().strip() != versions:
                    raise ValueError("Metadata declared version range differs: " + release)
                result["declared_maximum"] = maximum
                print(json.dumps({"peer": release, "versions": versions,
                                  "jar_sha256": result["jar_sha256"],
                                  "source_commit": result["upstream_commit"]}), flush=True)
                if maximum < 13:
                    result["disposition"] = "outside_declared_version_range"
                    results.append(result)
                    continue
            result["generate"] = run(java + ["generate" + suffix, str(fixture)], directory / "generate.log",
                                     marker=f"generated_pairs={len(cases)}")
            if fixture.stat().st_size > 32768:
                raise ValueError("fixture budget exceeded")
            if args.nested:
                rows = [line.split() for line in fixture.read_text().splitlines()]
                if tuple(row[0] for row in rows if len(row) == 3) != cases or len(rows) != len(cases):
                    raise ValueError("nested fixture identities differ")
            print(json.dumps({"peer": release, "apache_fixture_sha256": digest(fixture)}), flush=True)
            for profile in ("default", "all-features"):
                reverse = directory / (profile + "-rust.txt")
                env = os.environ.copy()
                env.update(METADATA_WIRE_INPUT=str(fixture), METADATA_WIRE_OUTPUT=str(reverse))
                command = ["cargo", "test", "--locked"]
                if profile == "all-features":
                    command.append("--all-features")
                command += ["--test", "metadata_wire_conformance", "--",
                            rust_test, "--ignored", "--exact"]
                result[profile] = run(command, directory / (profile + "-rust.log"),
                                      env=env, timeout=270, marker="1 passed")
                result[profile + "-java"] = run(java + ["verify" + suffix, str(reverse)],
                                                directory / (profile + "-java.log"),
                                                marker=f"independently_parsed_pairs={len(cases)}")
                if args.nested and reverse.read_bytes() != fixture.read_bytes():
                    raise ValueError("represented nested fields did not round trip byte for byte")
                print(json.dumps({"peer": release, "profile": profile, "rust_reverse_sha256": digest(reverse),
                                  "rust_decoded_pairs": len(cases), "java_parsed_pairs": len(cases)}), flush=True)
                if not args.nested and release == "4.1.2" and profile == "default":
                    bad_fixture = directory / "corrupt-apache.txt"
                    corrupt_top_error(fixture, bad_fixture)
                    bad_env = env | {"METADATA_WIRE_INPUT": str(bad_fixture),
                                     "METADATA_WIRE_OUTPUT": str(directory / "bad-rust.txt")}
                    result["rust-negative"] = run(command, directory / "rust-negative.log", env=bad_env,
                                                  timeout=30, expect_failure=True, marker="assertion")
                    bad_reverse = directory / "corrupt-rust.txt"
                    corrupt_top_error(reverse, bad_reverse)
                    result["java-negative"] = run(java + ["verify", str(bad_reverse)],
                                                  directory / "java-negative.log", expect_failure=True,
                                                  marker="Rust response fields differ")
                    print(json.dumps({"peer": release, "profile": profile,
                                      "corruption_controls": "both rejected"}), flush=True)
            results.append(result)
        if inputs != {str(path.relative_to(ROOT)): digest(path) for path in input_paths}:
            raise ValueError("source changed during run")
        summary = {"status": "pass", "api": "Metadata", "version": 13, "results": results,
                   "cases": cases, "inputs": inputs, "timeout_control": timeout_control,
                   "limit": "Finite body fixtures. Negotiation and live broker histories remain open."}
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        applicable = sum("generate" in result for result in results)
        print(json.dumps({"status": "pass", "pinned_sdks": len(results), "peers": applicable,
                          "pairs_per_profile_per_peer": len(cases)}))
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps(
            {"error": type(error).__name__, "message": str(error), "completed": results}, indent=2) + "\n")
        raise
    finally:
        if CHILD is not None:
            stop_group(CHILD)


if __name__ == "__main__":
    main()
