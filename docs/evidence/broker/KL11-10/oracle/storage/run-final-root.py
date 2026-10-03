#!/usr/bin/env python3
"""Bind frozen independent retention checks to a complete immutable broker replay."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def digest(data):
    return hashlib.sha256(data).hexdigest()


def files(root):
    return {str(p.relative_to(root)): {"sha256": digest(p.read_bytes()),
                                      "mode": p.stat().st_mode & 0o777}
            for p in sorted(root.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch(r"[0-9a-f]{40}", args.source_sha)
    args.output.mkdir(parents=True, exist_ok=False)
    script = args.source / "docs/evidence/broker/KL11-10/oracle/storage"
    freeze_path = script / "source-freeze.json"
    freeze = json.loads(freeze_path.read_bytes())
    pins = dict(freeze["source_files_sha256"])
    pins[freeze["fixture"]["path"]] = freeze["fixture"]["sha256"]
    pins[str(freeze_path.relative_to(args.source))] = digest(freeze_path.read_bytes())
    git_modes = {}
    for name, expected in pins.items():
        blob = subprocess.check_output(["git", "show", args.source_sha + ":" + name], cwd=args.repository)
        assert digest(blob) == expected, name
        tree = subprocess.check_output(["git", "ls-tree", args.source_sha, "--", name], cwd=args.repository).decode()
        mode = tree.split()[0]
        assert mode in ("100644", "100755"), (name, mode)
        git_modes[name] = 0o755 if mode == "100755" else 0o644

    def verify_source():
        for name, expected in pins.items():
            path = args.source / name
            assert digest(path.read_bytes()) == expected, name
            assert path.stat().st_mode & 0o777 == git_modes[name], name

    qa_path = args.runtime / "validation.json"
    qa_bytes = qa_path.read_bytes()
    qa = json.loads(qa_bytes)
    assert qa["source_commit"] == args.source_sha
    assert len(qa["commands"]) == 19
    assert all(command["exit_code"] == 0 and command["source_before"] == command["source_after"]
               and command["source_after"]["all_git_blobs_match"] for command in qa["commands"])
    captures = {command["capture_directory"]: command for command in qa["commands"]
                if "capture_directory" in command}
    expected_cells = {tc + "-" + feature for tc in ("stable", "1.85.0")
                      for feature in ("default", "all-features")}
    assert set(captures) == expected_cells, captures
    commands = []
    counts = {"actual_histories": 0, "actual_states": 0, "raw_input_files": 0,
              "deliberate_negative_controls": 0, "positive_controls": 0}
    fixture = args.source / freeze["fixture"]["path"]

    def run(argv, label, raw):
        verify_source()
        before = files(raw)
        assert len(before) == 566, (label, len(before))
        (args.output / (label + "-inputs-before.json")).write_text(json.dumps(before, indent=2) + "\n")
        log = args.output / (label + ".log")
        command = ["taskset", "-c", "0-2,4", *argv]
        with log.open("wb") as stream:
            result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        after = files(raw)
        (args.output / (label + "-inputs-after.json")).write_text(json.dumps(after, indent=2) + "\n")
        commands.append({"argv": command, "exit_code": result.returncode, "log": log.name,
                         "log_sha256": digest(log.read_bytes()), "raw_inputs_unchanged": before == after,
                         "raw_input_files": len(before), "source_sha": args.source_sha})
        (args.output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
        verify_source()
        assert before == after, "actual retained input changed: " + label
        assert result.returncode == 0, "authentic checker failure retained: " + label

    for cell in sorted(expected_cells):
        raw = args.runtime / cell / "retention-fault-histories"
        report = args.output / (cell + "-positive.json")
        run(["python3", str(script / "check-retention-history.py"), "--histories", str(raw),
             "--fixture", str(fixture), "--output", str(report)], cell + "-positive", raw)
        positive = json.loads(report.read_bytes())
        assert positive["passed"] and positive["inputs_unchanged"]
        assert (positive["histories"], positive["states"], positive["raw_input_files"]) == (46, 92, 566)
        controls = args.output / (cell + "-controls")
        run(["python3", str(script / "check-counterexamples.py"), "--histories", str(raw),
             "--fixture", str(fixture), "--output", str(controls)], cell + "-controls", raw)
        negatives = json.loads((controls / "validation.json").read_bytes())
        assert negatives["passed"] and len(negatives["negative_controls"]) == 8
        counts["actual_histories"] += positive["histories"]
        counts["actual_states"] += positive["states"]
        counts["raw_input_files"] += positive["raw_input_files"]
        counts["deliberate_negative_controls"] += len(negatives["negative_controls"])
        counts["positive_controls"] += 1
    assert qa_path.read_bytes() == qa_bytes, "full replay receipt changed"
    receipt = {"schema_version": 1, "passed": True, "source_sha": args.source_sha,
               "source_files_sha256": pins, "source_modes": git_modes, "counts": counts,
               "full_replay_receipt_sha256": digest(qa_bytes), "commands": commands,
               "runner": {"path": str(Path(__file__)), "sha256": digest(Path(__file__).read_bytes()),
                          "scope": "Root orchestration; independently frozen validators are checked against exact Git blobs"},
               "artifacts": files(args.output), "limitations": freeze["limitations"]}
    (args.output / "validation.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
