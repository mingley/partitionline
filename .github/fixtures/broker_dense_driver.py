import csv, glob, hashlib, os, re, subprocess, sys

cpu = min(os.sched_getaffinity(0))
print(f"MEASURE_CPU {cpu}", flush=True)
binaries = {}
for arm in ("baseline", "candidate"):
    for kind, target in (("router", "produce_dense_partitions"), ("durable", "produce_dense_bench")):
        paths = [p for p in glob.glob(f"{os.environ['GITHUB_WORKSPACE']}/target-{arm}/release/deps/{target}-*") if os.path.isfile(p) and os.access(p, os.X_OK)]
        if len(paths) != 1:
            raise RuntimeError(f"{arm} {kind}: expected one executable, got {paths}")
        digest = hashlib.sha256(open(paths[0], "rb").read()).hexdigest()
        binaries[(arm, kind)] = paths[0]
        print(f"BINARY arm={arm} kind={kind} path={paths[0]} sha256={digest}", flush=True)
for kind in ("router", "durable"):
    if hashlib.sha256(open(binaries[("baseline",kind)], "rb").read()).digest() == hashlib.sha256(open(binaries[("candidate",kind)], "rb").read()).digest():
        raise RuntimeError(f"identical {kind} baseline/candidate binaries")

cases = [
    ("router",1,"ascending",2000),("router",16,"reversed",1000),("router",17,"reversed",1000),
    ("router",32,"ascending",1000),("router",32,"reversed",1000),
    ("router",256,"ascending",200),("router",256,"reversed",200),("router",256,"rotate",200),
    ("router",256,"duplicates",200),("router",256,"sparse_last",200),("router",256,"negative_last",200),
    ("router",1024,"reversed",50),("router",1024,"sparse_last",50),
    ("router",4096,"reversed",15),("router",4096,"sparse_last",15),
    ("durable",32,"ascending",10),("durable",32,"reversed",10),("durable",32,"rotate",10),
    ("durable",256,"ascending",10),("durable",256,"reversed",10),("durable",256,"rotate",10),
]
def run(arm, case):
    kind, count, shape, iterations = case
    test = "produce_dense_router_timing" if kind == "router" else "produce_dense_durable_timing"
    prefix = "DENSE_ROUTER " if kind == "router" else "DENSE_DURABLE "
    env = dict(os.environ, PL_DENSE_COUNT=str(count), PL_DENSE_ITERATIONS=str(iterations), PL_DENSE_SHAPE=shape)
    cmd = ["taskset","-c",str(cpu),binaries[(arm,kind)],"--exact",test,"--ignored","--nocapture"]
    try:
        p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        print(f"FAIL_TIMEOUT arm={arm} case={case} stdout={e.stdout!r} stderr={e.stderr!r}",flush=True)
        raise
    if p.returncode:
        print(f"FAIL_PROCESS arm={arm} case={case} exit={p.returncode}\nSTDOUT\n{p.stdout}\nSTDERR\n{p.stderr}",flush=True)
        raise RuntimeError("benchmark test failed")
    if "running 1 test" not in p.stdout or "1 passed; 0 failed" not in p.stdout:
        raise RuntimeError(f"unexpected test count arm={arm} case={case} output={p.stdout!r}")
    lines = [x for x in p.stdout.splitlines() if x.startswith(prefix)]
    if len(lines) != 1:
        raise RuntimeError(f"missing measurement arm={arm} case={case} output={p.stdout!r}")
    line = lines[0]
    match = re.search(r"elapsed_ns=(\d+)", line)
    if not match: raise RuntimeError(line)
    if kind == "durable" and f"verified_records={2*(iterations+1)} " not in line:
        raise RuntimeError(f"wrong readback count: {line}")
    return int(match.group(1)), line

with open("dense-timings.csv", "w", newline="") as f:
    out = csv.writer(f)
    out.writerow(["kind","count","shape","iterations","block","arm","elapsed_ns"])
    for case_id, case in enumerate(cases):
        for arm in ("baseline", "candidate"):
            value,line = run(arm,case)
            print(f"WARMUP arm={arm} {line}",flush=True)
        for block in range(12):
            order = ("baseline","candidate") if (block + case_id) % 2 == 0 else ("candidate","baseline")
            for arm in order:
                value,line = run(arm,case)
                row = [*case,block,arm,value]
                out.writerow(row); f.flush()
                print("SAMPLE " + ",".join(map(str,row)),flush=True)
print("COMPLETE",flush=True)
