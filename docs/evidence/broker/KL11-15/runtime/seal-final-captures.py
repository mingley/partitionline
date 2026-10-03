#!/usr/bin/env python3
"""Retain exact d147 snapshot inputs without changing the shared matrix captures."""
from pathlib import Path
import hashlib
import json
import shutil
import stat
import subprocess

REPO = Path("/workspace/partitionline")
SHARED = Path("/workspace/work/retention-final-d147bcf1")
OUT = REPO / "docs/evidence/broker/KL11-15/runtime/final-d147bcf1"
SOURCE_SHA = "d147bcf1c0164778bdbad625842363f3721bc10e"
CELLS = ("stable-default", "stable-all-features", "1.85.0-default", "1.85.0-all-features")
SOURCE_PATHS = ("partitionline-broker/src/raft/replication.rs", "partitionline-broker/src/raft/snapshot.rs", "partitionline-broker/tests/raft_replication.rs", "partitionline-broker/tests/raft_snapshot.rs")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def manifest(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            assert not path.is_symlink(), path
            result[str(path.relative_to(root))] = {"sha256": digest(path), "mode": stat.S_IMODE(path.stat().st_mode), "bytes": path.stat().st_size}
    return result


assert not OUT.exists(), "Final output must be fresh"
OUT.mkdir()
commands = [{"argv": ["python3", str(REPO / "docs/evidence/broker/KL11-15/runtime/seal-final-captures.py")], "scope": "SHA256/mode verified capture copy and exact Git source comparison", "exit_code": 0}]
rows = []
for cell in CELLS:
    for name in ("snapshot", "snapshot-inner"):
        source = SHARED / cell / name
        before = manifest(source)
        target = OUT / cell / name
        shutil.copytree(source, target, copy_function=shutil.copy2)
        after = manifest(source)
        retained = manifest(target)
        assert before == after == retained, (cell, name)
        row = {"cell": cell, "kind": name, "original_path": str(source), "retained_path": str(target.relative_to(REPO)), "file_count": len(before), "bytes": sum(x["bytes"] for x in before.values()), "original_unchanged": True, "copy_bytes_and_modes_identical": True, "files": before}
        rows.append(row)
source_rows = []
for name in SOURCE_PATHS:
    argv = ["git", "show", f"{SOURCE_SHA}:{name}"]
    actual = subprocess.check_output(argv, cwd=REPO)
    commands.append({"argv": argv, "cwd": str(REPO), "exit_code": 0})
    archive_path = SHARED / "source" / name
    assert archive_path.read_bytes() == actual, name
    tree_argv = ["git", "ls-tree", SOURCE_SHA, "--", name]
    tree = subprocess.check_output(tree_argv, cwd=REPO).decode().strip().split()
    commands.append({"argv": tree_argv, "cwd": str(REPO), "exit_code": 0})
    target = OUT / "source" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(archive_path, target)
    assert target.read_bytes() == actual
    assert stat.S_IMODE(archive_path.stat().st_mode) == stat.S_IMODE(target.stat().st_mode)
    source_rows.append({"path": name, "git_blob": tree[2], "git_mode": tree[0], "sha256": digest(target), "bytes": len(actual), "retained_path": str(target.relative_to(REPO))})
receipt = {"schema_version": 1, "source_sha": SOURCE_SHA, "commands": commands, "results": {"copied_groups": len(rows), "file_count": sum(x["file_count"] for x in rows), "raw_bytes": sum(x["bytes"] for x in rows), "all_originals_unchanged": True, "all_retained_bytes_and_modes_identical": True, "four_rust_sources_equal_git_blobs": True}, "captures": rows, "source_files": source_rows, "matrix_receipt_original": {"path": str(SHARED / "validation.json"), "sha256": digest(SHARED / "validation.json")}}
(OUT / "capture-retention.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt["results"]))
print(digest(OUT / "capture-retention.json"))
