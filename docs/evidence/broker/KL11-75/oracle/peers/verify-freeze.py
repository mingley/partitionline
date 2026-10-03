#!/usr/bin/env python3
"""Check the frozen peer input bytes/modes without compiling or starting peers."""
import hashlib
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / "source-freeze.json").read_text())
    total = 0
    for row in manifest["files"]:
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Scoped relative input required")
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError("Regular frozen peer input required")
        data = path.read_bytes()
        if len(data) != row["bytes"] or hashlib.sha256(data).hexdigest() != row["sha256"] or path.stat().st_mode & 0o777 != row["mode"]:
            raise ValueError("Frozen peer source/evidence bytes or modes changed: " + row["path"])
        if path.suffix in (".class", ".jar", ".pyc") or data.startswith(b"\x7fELF"):
            raise ValueError("Executable/runtime dependency cache is outside Git")
        total += len(data)
    if total != manifest["input_bytes"] or len(manifest["files"]) != manifest["input_files"]:
        raise ValueError("Input count/size mismatch")
    print(json.dumps({"passed": True, "input_files": len(manifest["files"]), "input_bytes": total,
                      "source_freeze_sha256": hashlib.sha256((root / "source-freeze.json").read_bytes()).hexdigest(),
                      "actual_broker_peer_jobs": 0}), flush=True)


if __name__ == "__main__":
    main()
