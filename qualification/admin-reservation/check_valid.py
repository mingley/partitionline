import pathlib
import sys

def parse(path):
    rows = [line.split("|") for line in pathlib.Path(path).read_text().splitlines()]
    if len(rows) != 24 or any(len(row) != 9 for row in rows):
        raise AssertionError(f"expected 24 complete valid allocation rows in {path}")
    return rows

baseline = parse(sys.argv[1])
candidate = parse(sys.argv[2])
for old, new in zip(baseline, candidate):
    if old[:3] != new[:3] or old[7:] != new[7:]:
        raise AssertionError(f"case, cursor or count mismatch: {old} vs {new}")
print(f"verified {len(baseline)} valid cases with matching counts and cursor positions")
