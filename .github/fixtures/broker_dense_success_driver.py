import csv, glob, hashlib, os, re, subprocess

cpu = min(os.sched_getaffinity(0))
print(f"MEASURE_CPU {cpu}", flush=True)
binaries = {}
for arm in ("baseline", "candidate"):
    paths = [p for p in glob.glob(f"{os.environ['GITHUB_WORKSPACE']}/target-{arm}/release/deps/produce_dense_success-*") if os.path.isfile(p) and os.access(p, os.X_OK)]
    if len(paths) != 1:
        raise RuntimeError(f"{arm}: expected one executable, got {paths}")
    digest = hashlib.sha256(open(paths[0], "rb").read()).hexdigest()
    binaries[arm] = paths[0]
    print(f"BINARY arm={arm} path={paths[0]} sha256={digest}", flush=True)
if hashlib.sha256(open(binaries["baseline"], "rb").read()).digest() == hashlib.sha256(open(binaries["candidate"], "rb").read()).digest():
    raise RuntimeError("baseline and candidate binaries are identical")

cases = [(count, shape, 10) for count in (17,32) for shape in ("ascending","reversed","rotate")]
fields = ["elapsed_ns","setup_ns","shutdown_ns","verify_ns","cleanup_ns","verified_records","verified_partitions","read_bytes","sync_concurrency"]
def run(arm, case):
    count, shape, iterations = case
    env = dict(os.environ, PL_DENSE_COUNT=str(count), PL_DENSE_ITERATIONS=str(iterations), PL_DENSE_SHAPE=shape)
    cmd = ["taskset","-c",str(cpu),binaries[arm],"--exact","produce_dense_success_all_timing","--ignored","--nocapture"]
    try:
        p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired as e:
        print(f"FAIL_TIMEOUT arm={arm} case={case} stdout={e.stdout!r} stderr={e.stderr!r}",flush=True)
        raise
    if p.returncode:
        print(f"FAIL_PROCESS arm={arm} case={case} exit={p.returncode}\nSTDOUT\n{p.stdout}\nSTDERR\n{p.stderr}",flush=True)
        raise RuntimeError("all-success timing test failed")
    if "running 1 test" not in p.stdout or "1 passed; 0 failed" not in p.stdout:
        raise RuntimeError(f"unexpected test count arm={arm} case={case} output={p.stdout!r}")
    marker = "DENSE_SUCCESS "
    lines = [line[line.index(marker):] for line in p.stdout.splitlines() if marker in line]
    if len(lines) != 1:
        raise RuntimeError(f"missing measurement arm={arm} case={case} output={p.stdout!r}")
    data = dict(token.split("=",1) for token in lines[0].split()[1:])
    if data["shape"] != shape or int(data["count"]) != count or int(data["iterations"]) != iterations:
        raise RuntimeError(f"wrong case {data}")
    if int(data["verified_records"]) != count * (iterations + 1) or int(data["verified_partitions"]) != count:
        raise RuntimeError(f"wrong verification counts {data}")
    if int(data["read_bytes"]) != count * (24 + (iterations + 1) * (32 + 74)):
        raise RuntimeError(f"wrong raw byte count {data}")
    if int(data["sync_concurrency"]) != 4 or data["temp"] != os.environ["TMPDIR"]:
        raise RuntimeError(f"wrong runtime configuration {data}")
    return [int(data[field]) for field in fields]

with open("dense-success-samples.csv", "w", newline="") as f:
    out = csv.writer(f)
    out.writerow(["count","shape","iterations","block","arm",*fields])
    for case_id, case in enumerate(cases):
        for arm in ("baseline", "candidate"):
            row = [*case,-1,arm,*run(arm,case)]
            out.writerow(row); f.flush(); print("SAMPLE " + ",".join(map(str,row)),flush=True)
        for block in range(12):
            order = ("baseline","candidate") if (block + case_id) % 2 == 0 else ("candidate","baseline")
            for arm in order:
                row = [*case,block,arm,*run(arm,case)]
                out.writerow(row); f.flush(); print("SAMPLE " + ",".join(map(str,row)),flush=True)
print("COMPLETE",flush=True)
