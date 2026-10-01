#!/usr/bin/env python3
"""Summarize interleaved criterion runs: CRIT_DIR/<arm>-r<rep>/**/new/estimates.json."""
import json, random, statistics, sys
from pathlib import Path
crit = Path(sys.argv[1])
arms = sys.argv[2].split(",") if len(sys.argv) > 2 else ["P", "M", "Z"]
data = {}  # (arm, rep) -> {id: median_ns}
for d in sorted(crit.iterdir()):
    if not d.is_dir() or "-r" not in d.name:
        continue
    arm, rep = d.name.split("-r")
    m = {}
    for est in d.rglob("new/estimates.json"):
        bj = json.loads((est.parent / "benchmark.json").read_text())
        m[bj["full_id"]] = json.loads(est.read_text())["median"]["point_estimate"]
    data[(arm, int(rep))] = m
reps = sorted({r for _, r in data})
ids = sorted(set().union(*[set(v) for v in data.values()]))
random.seed(7)
def boot(xs, n=10000):
    if len(xs) < 2:
        return (float("nan"), float("nan"))
    s = sorted(statistics.median(random.choices(xs, k=len(xs))) for _ in range(n))
    return (s[int(0.025 * n)], s[int(0.975 * n)])
out = {}
print(f"{'cell':44}" + "".join(f"{a+' med ns':>14}" for a in arms) + "".join(f"{a+' vs P thrpt%':>26}" for a in arms if a != "P"))
for i in ids:
    row = {a: [data[(a, r)][i] for r in reps if (a, r) in data and i in data[(a, r)]] for a in arms}
    if not all(row.values()):
        continue
    meds = {a: statistics.median(v) for a, v in row.items()}
    res = {"median_ns": meds, "repetitions": len(reps)}
    line = f"{i:44}" + "".join(f"{meds[a]:14.0f}" for a in arms)
    for a in arms:
        if a == "P":
            continue
        paired = [100.0 * (p / x - 1.0) for p, x in zip(row["P"], row[a])]
        med = statistics.median(paired); lo, hi = boot(paired)
        res[f"{a}_vs_P_throughput_delta_pct"] = {"median": med, "ci95": [lo, hi], "paired": paired}
        line += f"{med:+10.1f} [{lo:+6.1f},{hi:+6.1f}]"
    out[i] = res
    print(line)
Path(sys.argv[3] if len(sys.argv) > 3 else crit / "summary.json").write_text(json.dumps(out, indent=1, sort_keys=True))
