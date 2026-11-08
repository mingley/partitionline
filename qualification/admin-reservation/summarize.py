import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
pairs = {}
for pair in range(8):
    for arm in ("baseline", "candidate"):
        rows = (root / f"bench-{pair}-{arm}.txt").read_text().splitlines()
        if len(rows) != 6:
            raise AssertionError(f"missing workloads: {pair} {arm}")
        for row in rows:
            label, count, repetitions, nanos, total = row.split("|")
            key = (label, int(count), int(repetitions), int(total))
            pairs.setdefault(key, {}).setdefault(pair, {})[arm] = int(nanos)
for key, rows in pairs.items():
    ratios = []
    for pair in range(8):
        value = rows[pair]
        ratios.append(value["candidate"] / value["baseline"])
    print(f"{key}: candidate/baseline median={statistics.median(ratios):.5f} range={min(ratios):.5f}..{max(ratios):.5f}, pairs={','.join(f'{x:.5f}' for x in ratios)}")
