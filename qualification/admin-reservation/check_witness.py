import pathlib
import sys

def parse(path):
    lines = pathlib.Path(path).read_text().splitlines()
    rows = [line.split("|", 6) for line in lines]
    if len(rows) != 22 or any(len(row) != 7 for row in rows):
        raise AssertionError(f"expected 22 complete witness rows in {path}")
    return rows

baseline = parse(sys.argv[1])
candidate = parse(sys.argv[2])
for old, new in zip(baseline, candidate):
    if (old[0], old[1], old[5], old[6]) != (new[0], new[1], new[5], new[6]):
        raise AssertionError(f"error or cursor mismatch: {old} vs {new}")
    if int(old[4]) < 1_000_000:
        raise AssertionError(f"baseline did not reproduce large reservation: {old}")
    if int(new[4]) > 16 * 1024:
        raise AssertionError(f"candidate requested more than 16 KiB at once: {new}")
print(f"verified {len(baseline)} malformed allocation controls, including matching errors and remaining bytes")
