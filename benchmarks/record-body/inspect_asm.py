#!/usr/bin/env python3
"""Show optimized x86 symbols and diffs; timing is handled separately."""
import difflib
import re
import sys
from pathlib import Path

base, candidate, out = map(Path, sys.argv[1:])
wanted = (
    "bench_record_body::run",
    "bench_record_body::encode",
    "partitionline::protocol::records::start_record_batch",
    "partitionline::protocol::records::encode_record",
    "partitionline::protocol::buf::put_unsigned_groups",
    "partitionline::protocol::buf::put_unsigned_varint_width",
    "partitionline::protocol::buf::put_unsigned_varlong_width",
    "partitionline::protocol::records::checked_record_body_length",
    "partitionline::protocol::records::write_record_batch_scratch",
    "partitionline::protocol::records::write_uncompressed_batch_cooperative",
)
header = re.compile(r"^([0-9a-f]+) <(.+)>:$")
address = re.compile(r"^\s*[0-9a-f]+:\s*")
hash_suffix = re.compile(r"::h[0-9a-f]{16}(?=$|[.>])")
branch_target = re.compile(r"^(\s*(?:j[a-z]+|callq?|jmpq?)\s+)[0-9a-f]+(?=\s|$)")
comment_addr = re.compile(r"\s+#\s*[0-9a-f]+(?=\s|$)")
def symbols(path):
    found = {}
    label = None
    body = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = header.match(line)
        if match:
            if label is not None:
                found.setdefault(label, []).append(body)
            name = hash_suffix.sub("", match.group(2))
            label = name if any(term in name for term in wanted) else None
            body = []
        elif label is not None:
            clean = address.sub("", line)
            clean = branch_target.sub(r"\1<addr>", clean)
            clean = comment_addr.sub(" # <addr>", clean)
            if clean.strip():
                body.append(clean)
    if label is not None:
        found.setdefault(label, []).append(body)
    return found
before, after = symbols(base), symbols(candidate)
all_names = sorted(set(before) | set(after))
with out.open("w", encoding="utf-8") as f:
    print("selected-symbol-count", len(all_names), file=f)
    for name in all_names:
        left, right = before.get(name, []), after.get(name, [])
        print("\nSYMBOL", name, "baseline_instances", len(left), "candidate_instances", len(right), file=f)
        if len(left) != 1 or len(right) != 1:
            continue
        print("baseline_instructions", len(left[0]), "candidate_instructions", len(right[0]), file=f)
        diff = list(difflib.unified_diff(left[0], right[0], fromfile="baseline", tofile="candidate", lineterm="", n=5))
        print("diff_lines", len(diff), file=f)
        print("\n".join(diff), file=f)
print(out.read_text(encoding="utf-8"))
