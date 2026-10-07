#!/usr/bin/env python3
"""Record repeated local measurements from an unchanged source checkout."""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
import sys
import time


NB_CELLS = (
    "nb-produce-bulk", "nb-produce-idem", "nb-produce-mixed-topics",
    "nb-produce-headers", "nb-produce-128p", "nb-produce-flush-heavy",
    "nb-send-seq", "nb-produce-idle-rss", "nb-produce-retry", "nb-connect",
    "nb-fetch-bulk", "nb-fetch-1000p", "nb-fetch-committed-aborts",
    "nb-fetch-seek-in-batch", "nb-fetch-capped-paused", "nb-fetch-appdelay",
    "nb-fetch-multinode", "nb-fetch-gzip-overfetch",
)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    with Path(path).open("x") as file:
        json.dump(value, file, indent=2, allow_nan=False)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())


def bootstrap(values, seed=912):
    if len(values) < 5 or any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("five finite, nonnegative independent repetitions required")
    rng = random.Random(seed)
    samples = sorted(statistics.median(rng.choices(values, k=len(values)))
                     for _ in range(20_000))
    return dict(repetitions=len(values), values=values,
                median=statistics.median(values),
                bootstrap_95_ci=[samples[499], samples[19499]],
                bootstrap_resamples=20_000, bootstrap_seed=seed)


def load_module(path):
    spec = importlib.util.spec_from_file_location("baseline_owned_commands", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def host():
    governors = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        p = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor")
        governors[str(cpu)] = p.read_text().strip() if p.exists() else None
    return dict(kernel=os.uname().release, machine=os.uname().machine,
                cpu_affinity=sorted(os.sched_getaffinity(0)), governors=governors,
                frequency_policy="Guest frequency is not controlled; governors unavailable"
                    if not any(governors.values()) else "Recorded guest governors",
                cpuinfo=Path("/proc/cpuinfo").read_text(),
                os_release=Path("/etc/os-release").read_text(),
                lscpu=subprocess.check_output(["lscpu"], text=True),
                rustc=subprocess.check_output(["rustc", "+stable", "-vV"], text=True),
                cargo=subprocess.check_output(["cargo", "+stable", "-V"], text=True),
                perf_event_paranoid=Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip(),
                captured_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))


class Recorder:
    def __init__(self, args):
        self.args = args
        self.source = args.source.resolve()
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.pins = json.loads(args.source_pins.read_text())
        self.binaries = {}
        (self.output / "bin").mkdir()
        for name, path in args.binary:
            target = self.output / "bin" / name
            shutil.copy2(path, target)
            self.binaries[name] = target
        self.input_pins = {str(p): sha(p) for p in [Path(__file__).resolve(),
                           args.source_pins.resolve(), *self.binaries.values()]}
        self.guard()
        module_path = self.source / "scripts/run-benchmark-matrix.py"
        if str(module_path.relative_to(self.source)) not in self.pins:
            raise ValueError("owned command runner must be source-pinned")
        self.owned = load_module(module_path)
        if ctypes.CDLL(None).prctl(36, 1, 0, 0, 0):
            raise OSError("cannot become child subreaper")
        self.lock = (self.output.parent / ".local-baseline.lock").open("a")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.env = self.owned.base_env()
        self.env["NB_SERVE"] = str(self.binaries.get("nb-serve", ""))
        self.rows = []
        save(self.output / "plan.json", dict(schema_version=1,
             source_commit=args.commit, source_pins=self.pins, inputs=self.input_pins,
             family=args.family, repetitions=args.repetitions, seed=args.seed,
             cpu_affinity=args.cpus, nb_cells=NB_CELLS if args.family == "nb" else [],
             criterion=dict(warmup_seconds=1, measurement_seconds=2, samples=30,
                            confidence_level=.95, resamples=20_000),
             scope="local/unsigned", suite_hold="active"))
        save(self.output / "host-before.json", host())
        shutil.copyfile(__file__, self.output / "executed-runner.py")
        os.chdir(self.source)

    def guard(self):
        for relative, digest in self.pins.items():
            if sha(self.source / relative) != digest:
                raise ValueError("source changed: " + relative)
        for path, digest in self.input_pins.items():
            if sha(path) != digest:
                raise ValueError("executable or runner changed: " + path)
        commit = subprocess.check_output(["git", "-C", str(self.source),
                                           "rev-parse", "HEAD"], text=True).strip()
        dirty = subprocess.check_output(["git", "-C", str(self.source),
                                         "status", "--porcelain"])
        if commit != self.args.commit or dirty:
            raise ValueError("source checkout is dirty or has a different commit")

    def command(self, command, directory, label, timeout=400, extra_env=None):
        self.guard()
        env = dict(self.env, **(extra_env or {}))
        actual = ["taskset", "-c", self.args.cpus, *map(str, command)]
        result = self.owned.execute(actual, env, directory, label, timeout)
        self.guard()
        return result

    def nb(self):
        rng = random.Random(self.args.seed)
        cells = self.args.cells or list(NB_CELLS)
        if not set(cells) <= set(NB_CELLS):
            raise ValueError("unknown null-broker cell")
        for rep in range(self.args.repetitions):
            order = list(cells)
            rng.shuffle(order)
            for cell in order:
                directory = self.output / f"r{rep + 1:02d}-{cell}"
                directory.mkdir()
                print(f"start {cell} repetition {rep + 1}", flush=True)
                self.command([self.binaries["runtime"], "--cell", cell,
                              "--out", directory, "--repetitions", "1"],
                             directory, "runtime")
                results = list(directory.glob("*.result.json"))
                if len(results) != 1:
                    raise ValueError("one runtime result required")
                path = results[0]
                self.command([sys.executable, "-B", self.source /
                              "scripts/benchmark-report.py", path], directory,
                             "validator", timeout=15)
                data = json.loads(path.read_text())
                if data["scenario"]["cell_disposition"] != "executed":
                    raise ValueError("runtime cell did not execute successfully")
                if data["provenance"]["source"]["git_commit"] != self.args.commit:
                    raise ValueError("artifact source differs")
                self.rows.append(dict(cell=cell, repetition=rep + 1,
                     artifact=str(path), sha256=sha(path), measurements=data["measurements"],
                     execution=data["execution"]))
                save(directory / "validated.json", self.rows[-1])
                print(f"done {cell} repetition {rep + 1}", flush=True)

    def codec(self):
        for rep in range(self.args.repetitions):
            directory = self.output / f"r{rep + 1:02d}"
            directory.mkdir()
            for name, argv in [("preflight", ["preflight"]), ("census", ["census"]),
                               ("slowcheck", ["slowcheck"])]:
                self.command([self.binaries["codec-preflight"], *argv],
                             directory, name, timeout=30)
            for name in ["json1k-census", "zstd-census"]:
                self.command([self.binaries[name]], directory, name, timeout=60)
            for name, count in [("codec", 50), ("zstd", 72)]:
                criterion_home = directory / (name + "-criterion")
                print(f"start {name} repetition {rep + 1}", flush=True)
                self.command([self.binaries[name], "--bench", "--noplot",
                              "--warm-up-time", "1", "--measurement-time", "2",
                              "--sample-size", "30", "--nresamples", "20000"],
                             directory, name, timeout=600,
                             extra_env={"CRITERION_HOME": str(criterion_home)})
                cases = {}
                for estimate in criterion_home.glob("**/new/estimates.json"):
                    case = json.loads((estimate.parent / "benchmark.json").read_text())
                    sample = json.loads((estimate.parent / "sample.json").read_text())
                    stats = json.loads(estimate.read_text())
                    if len(sample["iters"]) != 30 or len(sample["times"]) != 30:
                        raise ValueError("thirty Criterion samples required")
                    if any(not math.isfinite(v) or v <= 0
                           for v in [*sample["times"], *sample["iters"]]):
                        raise ValueError("positive finite native samples required")
                    value = stats["median"]["point_estimate"]
                    if not math.isfinite(value) or value <= 0:
                        raise ValueError("invalid native timing")
                    key = case["full_id"]
                    if key in cases:
                        raise ValueError("duplicate Criterion case")
                    cases[key] = dict(median_ns_per_operation=value,
                         criterion=stats, throughput=case["throughput"],
                         estimates_sha256=sha(estimate), samples_sha256=sha(estimate.parent / "sample.json"))
                if len(cases) != count:
                    raise ValueError(f"expected {count} {name} cases, found {len(cases)}")
                row = dict(family=name, repetition=rep + 1, cases=cases)
                save(directory / (name + "-validated.json"), row)
                self.rows.append(row)
                print(f"done {name} repetition {rep + 1}", flush=True)

    def finish(self):
        self.guard()
        save(self.output / "rows.json", self.rows)
        save(self.output / "host-after.json", host())
        save(self.output / "completion.json", dict(status="recorded", scope="local/unsigned",
             suite_hold="active", repetitions=self.args.repetitions,
             source_guards_passed=True, owned_process_groups_empty=True,
             rows=len(self.rows)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("family", choices=["nb", "codec"])
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-pins", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", nargs=2, action="append", required=True, metavar=("NAME", "PATH"))
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--seed", type=int, default=912)
    parser.add_argument("--cpus", default="2,4")
    parser.add_argument("--cells", nargs="*")
    parser.add_argument("--partial", action="store_true",
                        help="Record a partial cohort; five repetitions are still required for aggregate statistics")
    args = parser.parse_args()
    minimum = 1 if args.partial else 5
    if not minimum <= args.repetitions <= 25:
        parser.error(f"repetitions must be {minimum}..25")
    if len({name for name, _ in args.binary}) != len(args.binary):
        parser.error("duplicate binary name")
    recorder = Recorder(args)
    try:
        getattr(recorder, args.family)()
        recorder.finish()
    except BaseException as error:
        save(recorder.output / "failure.json", dict(error=type(error).__name__, message=str(error)))
        raise


if __name__ == "__main__":
    main()
