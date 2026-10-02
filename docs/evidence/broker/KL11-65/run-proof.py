#!/usr/bin/env python3
"""Decode immutable Rust proof bytes with Apache, retaining both output streams."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof-root", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--slf4j", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    classes = args.output / "classes"
    classes.mkdir()
    producers = Path(__file__).resolve().parent
    results = {"commands": [], "inputs": {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in [args.jar, args.slf4j, producers / "AssignedBatchOracle.java",
                     producers / "verify-proof.py", Path(__file__).resolve()]}}

    def run(name, command, output=None):
        stdout = output or args.output / f"{name}.stdout.log"
        stderr = args.output / f"{name}.stderr.log"
        with stdout.open("wb") as out, stderr.open("wb") as err:
            result = subprocess.run(command, stdout=out, stderr=err)
        results["commands"].append({"name": name, "argv": command,
            "exit_code": result.returncode, "outputs": {
                str(p.relative_to(args.output)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in [stdout, stderr]}})
        (args.output / "commands.json").write_text(json.dumps(results, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(result.returncode)

    classpath = f"{args.jar}:{args.slf4j}"
    run("compile", ["java", "--add-modules", "jdk.compiler", "com.sun.tools.javac.Main",
        "-Xlint:all", "-Werror", "-cp", classpath, "-d", str(classes),
        str(producers / "AssignedBatchOracle.java")])
    for toolchain in ["stable", "1.85.0"]:
        oracle = args.output / toolchain
        oracle.mkdir()
        proof = args.proof_root / f"proof-{toolchain}"
        for name in ["multiple", "basic"]:
            fixture = args.fixtures / ("valid-multiple-batches.bin" if name == "multiple"
                                       else "valid-basic.bin")
            for kind, path in [("original", fixture), ("assigned", proof / f"assigned-{name}.bin")]:
                results["inputs"][str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
                run(f"{toolchain}-{kind}-{name}", ["java", "-cp", f"{classes}:{classpath}",
                    "AssignedBatchOracle", str(path)], oracle / f"{kind}-{name}.jsonl")
        run(f"{toolchain}-verify", ["python3", "-B", str(producers / "verify-proof.py"),
            "--proof", str(proof), "--fixtures", str(args.fixtures), "--oracle", str(oracle)])
    names = ["assigned-multiple.bin", "assigned-basic.bin", "partition.journal"]
    results["byte_identical_toolchain_outputs"] = all(
        (args.proof_root / "proof-stable" / name).read_bytes() ==
        (args.proof_root / "proof-1.85.0" / name).read_bytes() for name in names)
    assert results["byte_identical_toolchain_outputs"]
    (args.output / "commands.json").write_text(json.dumps(results, indent=2) + "\n")
    print(f"Passed {len(results['commands'])} independent proof commands; toolchain bytes identical.")


if __name__ == "__main__":
    main()
