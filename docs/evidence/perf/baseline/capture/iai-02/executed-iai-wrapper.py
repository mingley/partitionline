#!/usr/bin/env python3
"""Retain five fresh instruction-count cohorts for the pinned codec shapes."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shlex
import sys


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "source-pins", "output", "recorder", "iai", "json1k-iai",
                 "runner", "valgrind-root", "parent-exec"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--commit", required=True)
    a = p.parse_args()
    spec = importlib.util.spec_from_file_location("iai_baseline_recorder", a.recorder)
    baseline = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baseline)
    args = argparse.Namespace(source=a.source, source_pins=a.source_pins, output=a.output,
            commit=a.commit, family="iai", repetitions=5, seed=912, cpus="2", cells=None,
            binary=[("iai", a.iai), ("json1k_iai", a.json1k_iai)])
    r = baseline.Recorder(args)
    inputs = [Path(__file__).resolve(), a.runner.resolve(), a.parent_exec.resolve()]
    inputs += [p for p in (a.valgrind_root / "usr/libexec/valgrind").rglob("*") if p.is_file()]
    inputs.append(a.valgrind_root / "usr/bin/valgrind")
    shims = r.output / "tool-launchers"
    shims.mkdir()
    for name, executable in (("iai-callgrind-runner", a.runner),
                             ("valgrind", a.valgrind_root / "usr/bin/valgrind")):
        path = shims / name
        # Each subprocess is tied to its immediate parent, including the runner and Valgrind.
        path.write_text('#!/bin/sh\nexec ' + shlex.quote(str(Path(sys.executable).resolve())) + ' -B ' +
                        shlex.quote(str(a.parent_exec.resolve())) + ' "$PPID" ' +
                        shlex.quote(str(executable.resolve())) + ' "$@"\n')
        path.chmod(0o755)
        inputs.append(path)
    r.input_pins.update({str(path): baseline.sha(path) for path in inputs})
    baseline.save(r.output / "instruction-inputs.json", r.input_pins)
    r.env.update(PATH=str(shims) + ":" + r.env["PATH"],
                 VALGRIND_LIB=str((a.valgrind_root / "usr/libexec/valgrind").resolve()),
                 IAI_CALLGRIND_RUNNER=str(shims / "iai-callgrind-runner"),
                 CARGO_PKG_NAME="codec",
                 CARGO_MANIFEST_DIR=str(a.source.resolve() / "benchmarks/codec"))
    r.env.pop("RUST_LOG", None)
    r.env["CARGO_NET_OFFLINE"] = "true"
    os.chdir(a.source.resolve() / "benchmarks/codec")
    for rep in range(1, 6):
        directory = r.output / f"r{rep:02d}"
        directory.mkdir()
        for name, expected in (("iai", 10), ("json1k_iai", 24)):
            home = directory / (name + "-callgrind")
            print(f"start {name} repetition {rep}", flush=True)
            r.command([sys.executable, "-B", a.parent_exec.resolve(), str(os.getpid()),
                       r.binaries[name], "--allow-aslr=true", "--save-summary=json", "--home", home],
                      directory, name, timeout=400)
            cases = {}
            for path in home.rglob("summary.json"):
                row = json.loads(path.read_text())
                if row["kind"] != "LibraryBenchmark": raise ValueError("library instruction cohort required")
                measured = row["callgrind_summary"]["callgrind_run"]["total"]
                if measured["regressions"]: raise ValueError("unexpected instruction comparison baseline")
                metrics = measured["summary"]
                instructions = metrics["Ir"]["metrics"]
                if set(instructions) != {"Left"} or type(instructions["Left"]) is not int or instructions["Left"] <= 0:
                    raise ValueError("fresh positive instructions required")
                key = row["module_path"] + "/" + row["id"]
                if key in cases: raise ValueError("duplicate instruction case")
                cases[key] = dict(instructions=instructions["Left"],
                                  source_summary=str(path), sha256=baseline.sha(path),
                                  event="Ir", scope="Simulated user instructions; no native cycle estimate")
            if len(cases) != expected: raise ValueError(f"expected {expected} instruction cases, found {len(cases)}")
            row = dict(family=name, repetition=rep, cases=cases)
            baseline.save(directory / (name + "-validated.json"), row)
            r.rows.append(row)
            print(f"done {name} repetition {rep}", flush=True)
    r.finish()


if __name__ == "__main__":
    main()
