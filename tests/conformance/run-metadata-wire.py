#!/usr/bin/env python3
"""Compare a finite Metadata v13 body cohort with pinned Apache SDKs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "tests/conformance/java/ConformanceMetadataWire.java"
RUST = ROOT / "tests/metadata_wire_conformance.rs"
PINS = {
    "4.1.2": "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
    "4.2.1": "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
    "4.3.1": "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36",
}
COMMITS = {
    "4.1.2": "c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c",
    "4.2.1": "18d5ecd939c8d510fdd72d0abb1f7099659dcd58",
    "4.3.1": "26b251a451ce941d3d7a55e6487bcb7f16b5ad48",
}
END = time.monotonic() + 570
CHILD = None


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def interrupt(signum, _frame):
    raise InterruptedError("signal " + str(signum))


def stop_group(child):
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=3)


def run(command, out, env=None, timeout=30, expect_failure=False, marker=None):
    global CHILD
    remaining = min(timeout, END - time.monotonic())
    if remaining <= 0:
        raise TimeoutError("overall Metadata deadline")
    with out.open("x") as stream:
        CHILD = subprocess.Popen(
            command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True,
        )
        try:
            CHILD.wait(timeout=remaining)
        finally:
            if CHILD.poll() is None:
                stop_group(CHILD)
            child = CHILD
            CHILD = None
        # Direct children must exit and the process group must be empty.
        try:
            os.killpg(child.pid, 0)
        except ProcessLookupError:
            pass
        else:
            stop_group(child)
            raise RuntimeError("child process group still alive")
    if out.stat().st_size > 1_048_576:
        raise ValueError("process output exceeds budget")
    contents = out.read_text()
    if (child.returncode != 0) != expect_failure:
        raise RuntimeError("unexpected exit " + str(child.returncode) + " in " + str(out))
    if marker and marker not in contents:
        raise RuntimeError("missing output marker in " + str(out))
    return {"exit_code": child.returncode, "log": str(out), "group_empty": True}


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
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupt)
    results = []
    inputs = {str(p.relative_to(ROOT)): digest(p) for p in (SOURCE, RUST, Path(__file__).resolve())}
    try:
        for release in PINS:
            directory = output / release
            directory.mkdir()
            jar = peer(output / "jars", release)
            java = ["java", "-Xmx128m", "--class-path", str(jar), str(SOURCE), str(jar)]
            fixture = directory / "apache.txt"
            result = {"release": release, "jar_sha256": digest(jar), "upstream_commit": COMMITS[release]}
            result["generate"] = run(java + ["generate", str(fixture)], directory / "generate.log",
                                     marker="generated_pairs=5")
            if fixture.stat().st_size > 32768:
                raise ValueError("fixture budget exceeded")
            for profile in ("default", "all-features"):
                reverse = directory / (profile + "-rust.txt")
                env = os.environ.copy()
                env.update(METADATA_WIRE_INPUT=str(fixture), METADATA_WIRE_OUTPUT=str(reverse))
                command = ["cargo", "test", "--locked"]
                if profile == "all-features":
                    command.append("--all-features")
                command += ["--test", "metadata_wire_conformance", "--",
                            "metadata_v13_apache_wire", "--ignored", "--exact"]
                result[profile] = run(command, directory / (profile + "-rust.log"),
                                      env=env, timeout=270, marker="1 passed")
                result[profile + "-java"] = run(java + ["verify", str(reverse)],
                                                directory / (profile + "-java.log"),
                                                marker="independently_parsed_pairs=5")
                if release == "4.1.2" and profile == "default":
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
            results.append(result)
        if inputs != {str(p.relative_to(ROOT)): digest(p) for p in (SOURCE, RUST, Path(__file__).resolve())}:
            raise ValueError("source changed during run")
        summary = {"status": "pass", "api": "Metadata", "version": 13, "results": results,
                   "cases": ["all", "empty", "name", "id", "tagged"], "inputs": inputs,
                   "limit": "Finite body fixtures, not a live broker or complete Metadata conformance."}
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"status": "pass", "peers": len(results), "pairs_per_profile_per_peer": 5}))
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps(
            {"error": type(error).__name__, "message": str(error), "completed": results}, indent=2) + "\n")
        raise
    finally:
        if CHILD is not None:
            stop_group(CHILD)


if __name__ == "__main__":
    main()
