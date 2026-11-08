from pathlib import Path
import json
import os
import signal
import subprocess
import sys
import time

ROOT = Path.cwd()
TEMP = Path(os.environ["RUNNER_TEMP"]) / "frame-deadline-pause"
TEMP.mkdir(parents=True, exist_ok=True)
TEST = "transport::frame_read_deadline_tests::read_deadline_stops_a_ready_frame_body"
BASE = "6298f6bfbed2ab98189d291ebebd2190a2ee75b6"

def interrupt(signum, _frame):
    raise SystemExit("qualification interrupted by signal " + str(signum))

signal.signal(signal.SIGTERM, interrupt)
signal.signal(signal.SIGINT, interrupt)

def checked(args, **kwargs):
    subprocess.run(args, check=True, **kwargs)

def instrument(work):
    path = work / "partitionline-broker/src/transport.rs"
    text = path.read_text()
    start = text.index("    struct SlowFrame {")
    end = text.index("    /// A legal frame that fills whatever spare capacity", start)
    section = text[start:end]
    needle = "            this.left -= n;\n            let start = std::time::Instant::now();"
    injection = """            this.left -= n;
            if this.left == 1024 * 1024 - 1024 {
                if let Some(dir) = std::env::var_os("PL_FRAME_PAUSE_DIR") {
                    let dir = std::path::PathBuf::from(dir);
                    std::fs::write(dir.join("entered"), b"entered").unwrap();
                    while !dir.join("release").exists() {
                        std::thread::yield_now();
                    }
                }
            }
            let start = std::time::Instant::now();"""
    if section.count(needle) != 1:
        raise RuntimeError("slow frame witness anchor did not match once")
    path.write_text(text[:start] + section.replace(needle, injection, 1) + text[end:])

def compile_test(work, arm):
    result = TEMP / (arm + "-cargo.jsonl")
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(TEMP / (arm + "-target"))
    with result.open("w") as out:
        checked(["cargo", "+stable", "test", "--locked", "--manifest-path",
                 str(work / "partitionline-broker/Cargo.toml"), "--lib",
                 "--no-run", "--message-format=json"], stdout=out, env=env, timeout=900)
    executables = []
    for line in result.read_text().splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if item.get("reason") == "compiler-artifact" and item.get("profile", {}).get("test") and "lib" in item.get("target", {}).get("kind", []) and item.get("executable"):
            executables.append(item["executable"])
    if len(executables) != 1:
        raise RuntimeError("expected exactly one library test executable: " + repr(executables))
    return executables[0]

def run_stopped(test_bin, arm):
    gate = TEMP / (arm + "-gate")
    gate.mkdir()
    log_path = TEMP / (arm + "-test.log")
    env = os.environ.copy()
    env["PL_FRAME_PAUSE_DIR"] = str(gate)
    with log_path.open("w") as log:
        proc = subprocess.Popen([test_bin, "--exact", TEST, "--nocapture"], stdout=log, stderr=subprocess.STDOUT, env=env)
        stopped = False
        try:
            deadline = time.monotonic() + 30
            while not (gate / "entered").exists():
                if proc.poll() is not None:
                    raise RuntimeError(arm + " test exited before the controlled poll")
                if time.monotonic() >= deadline:
                    raise RuntimeError(arm + " test did not enter the controlled poll")
                time.sleep(0.005)
            os.kill(proc.pid, signal.SIGSTOP)
            stopped = True
            time.sleep(0.2)
            (gate / "release").write_text("release")
            os.kill(proc.pid, signal.SIGCONT)
            stopped = False
            code = proc.wait(timeout=30)
        finally:
            if stopped:
                try:
                    os.kill(proc.pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
    data = log_path.read_text()
    print("PL_PAUSE", arm, "exit", code)
    print(data[-5000:])
    if arm == "baseline":
        if code != 101 or "deadline true" not in data or "test result: FAILED. 0 passed; 1 failed;" not in data:
            raise RuntimeError("baseline did not reproduce the old wall-time signature")
    elif code != 0 or "PL_READY_FRAME" not in data or "deadline=true" not in data or "test result: ok. 1 passed;" not in data:
        raise RuntimeError("candidate did not accept the correctly expired delayed poll")

for arm, ref in [("baseline", BASE), ("candidate", "HEAD")]:
    work = TEMP / (arm + "-work")
    checked(["git", "worktree", "add", "--detach", str(work), ref], cwd=ROOT)
    instrument(work)
    binary = compile_test(work, arm)
    run_stopped(binary, arm)

print("PL_PAUSE_CONTROL complete: only the spawned disposable test process was stopped, resumed and joined")
