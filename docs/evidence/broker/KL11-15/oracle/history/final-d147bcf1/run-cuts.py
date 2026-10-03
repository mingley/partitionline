#!/usr/bin/env python3
"""Run pinned independent Node image-publication and authoritative Install checks."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def sha(data):
    return hashlib.sha256(data).hexdigest()


def files(root):
    return {str(p.relative_to(root)): {"sha256": sha(p.read_bytes()), "mode": p.stat().st_mode & 0o777}
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
    root = args.source / "docs/evidence/broker/KL11-15/oracle/history"
    freeze = json.loads((root / "source-freeze.json").read_bytes())
    pins = freeze["source_files_sha256"]
    assert len(pins) == 9
    modes = {}
    for name, expected in pins.items():
        blob = subprocess.check_output(["git", "show", args.source_sha + ":" + name], cwd=args.repository)
        assert sha(blob) == expected, name
        entry = subprocess.check_output(["git", "ls-tree", args.source_sha, "--", name], cwd=args.repository).decode()
        assert entry.split()[0] in ("100644", "100755"), entry
        modes[name] = 0o755 if entry.split()[0] == "100755" else 0o644

    def verify():
        for name, expected in pins.items():
            path = args.source / name
            assert sha(path.read_bytes()) == expected and path.stat().st_mode & 0o777 == modes[name], name

    qa_path = args.runtime / "validation.json"
    qa_bytes = qa_path.read_bytes()
    qa = json.loads(qa_bytes)
    assert qa["source_commit"] == args.source_sha and len(qa["commands"]) == 19
    assert all(c["exit_code"] == 0 and c["source_before"] == c["source_after"] and
               c["source_before"]["all_git_blobs_match"] for c in qa["commands"])
    cells = {c["capture_directory"] for c in qa["commands"] if "capture_directory" in c}
    assert cells == {tc + "-" + f for tc in ("stable", "1.85.0") for f in ("default", "all-features")}
    commands = []
    counts = {"node_publication_cuts": 0, "node_publication_states": 0,
              "authoritative_install_cuts": 0, "authoritative_install_states": 0}
    for cell in sorted(cells):
        for kind, script, raw_name, extra in (
            ("node", "verify-node-cuts.py", "snapshot-inner", []),
            ("install", "verify-install-cuts.py", "snapshot", ["--source-sha", args.source_sha]),
        ):
            raw = args.runtime / cell / raw_name
            label = cell + "-" + kind
            before = files(raw)
            (args.output / (label + "-inputs-before.json")).write_text(json.dumps(before, indent=2) + "\n")
            result_path = args.output / (label + ".json")
            argv = ["taskset", "-c", "0-2,4", "python3", str(root / script), str(raw),
                    *extra, "--output", str(result_path)]
            verify()
            log = args.output / (label + ".log")
            with log.open("wb") as stream:
                result = subprocess.run(argv, stdout=stream, stderr=subprocess.STDOUT,
                                        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
            after = files(raw)
            (args.output / (label + "-inputs-after.json")).write_text(json.dumps(after, indent=2) + "\n")
            commands.append({"argv": argv, "exit_code": result.returncode, "log_sha256": sha(log.read_bytes()),
                             "raw_inputs_unchanged": before == after, "raw_input_files": len(before)})
            (args.output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
            verify()
            assert before == after, "actual raw inputs changed: " + label
            assert result.returncode == 0, "authentic checker failure retained: " + label
            report = json.loads(result_path.read_bytes())
            assert report["passed"] and report["inputs_unchanged"]
            if kind == "node":
                assert report["cases"] == 6 and len(report["states"]) == 12
                counts["node_publication_cuts"] += 6
                counts["node_publication_states"] += 12
            else:
                assert report["source_sha"] == args.source_sha and report["actual_cuts"] == 2
                assert len(report["raw_states"]) == 4
                counts["authoritative_install_cuts"] += 2
                counts["authoritative_install_states"] += 4
    assert qa_path.read_bytes() == qa_bytes
    receipt = {"schema_version": 1, "passed": True, "source_sha": args.source_sha,
               "source_files_sha256": pins, "source_modes": modes, "counts": counts,
               "commands": commands, "full_replay_receipt_sha256": sha(qa_bytes),
               "runner": {"path": str(Path(__file__)), "sha256": sha(Path(__file__).read_bytes()),
                          "scope": "Root orchestration; nine independent validator files pinned to exact Git blobs"},
               "artifacts": files(args.output),
               "scope": "Finite actual process/IO cuts; independent receiver authority and selected image checks",
               "limitations": ["Typed leader/fixed voter fixture; no autonomous network or voter-transition qualification.",
                               "Process-visible durability only; physical powerloss and hardware failures remain separate."]}
    (args.output / "validation.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
