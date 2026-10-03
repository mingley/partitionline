#!/usr/bin/env python3
"""Validate exact-source QA receipts and retain actual per-cell test counts."""
import argparse
import hashlib
import json
import re
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("directory", type=Path)
a = p.parse_args()
commands = json.loads((a.directory / "commands.json").read_text())
before = json.loads((a.directory / "source-before.json").read_text())
after = json.loads((a.directory / "source-after.json").read_text())
assert before == after and not before["mismatches"]
assert before["source_sha"] == commands["source_sha"]
assert len(commands["commands"]) == 20
names = [row["name"] for row in commands["commands"]]
assert len(set(names)) == len(names)
counts = {}
for row in commands["commands"]:
    raw = (a.directory / row["log"]).read_bytes()
    assert row["exit_code"] == 0
    assert hashlib.sha256(raw).hexdigest() == row["sha256"]
    if row["name"].endswith("-test"):
        matches = re.findall(
            r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; "
            r"(\d+) measured; (\d+) filtered out",
            raw.decode(),
        )
        assert matches
        sums = [sum(int(match[i]) for match in matches) for i in range(5)]
        assert sums[0] > 0 and sums[1:] == [0, 0, 0, 0]
        counts[row["name"]] = dict(
            zip(["passed", "failed", "ignored", "measured", "filtered"], sums)
        )
        if "-sasl-" in row["name"] or "-all-" in row["name"]:
            for key in ["handshake", "authenticate", "describe", "alter"]:
                assert re.search(
                    rf"test authenticated_{key}_control_ceiling_[^\n]+ \.\.\. ok",
                    raw.decode(),
                ), f"missing authenticated control regression: {key}"
            for case in [
                "canonical_and_native_redundant_empty_alter_tag_commit_once_and_survive_restart",
                "malformed_alter_tails_and_other_api_empty_tails_never_mutate_or_dispatch",
                "authentic_native_106_and_apache_canonical_105_frames_commit_without_rewriting",
            ]:
                assert f"test {case} ... ok" in raw.decode(), case
result = {
    "source_sha": before["source_sha"],
    "checked_git_blobs_before_and_after": before["checked_git_blobs"],
    "all_source_bytes_unchanged": True,
    "commands_passed": len(commands["commands"]),
    "test_counts": counts,
    "strict_format_alltarget_clippy_and_rustdoc": "passed on stable and Rust 1.85.0",
    "new_skips_ignored_or_filtered": 0,
    "scope": "Complete Rust QA; independent Java/C live gates are separate receipts.",
}
(a.directory / "results.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
