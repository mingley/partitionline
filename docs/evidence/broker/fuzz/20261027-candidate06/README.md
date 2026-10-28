# Broker fuzz candidate 06

Stable Rust 1.99.1 built five coverage-instrumented default-feature targets at
`bdf28a1b987cbc73e12ef54ac9cc516e741b6c4a`, including the partial-offset and
Fetch notification repairs. All nine real supervisor controls passed inside
the pinned isolated runtime. Stored build metadata pins source files, compiler,
commands, log and executable bytes.

The fresh campaign started at 17:36 CDT on 27 October 2026. It failed after
2,704 seconds (about 45 minutes) with another `recover_epoch` timeout, exit 70.
All five workers were joined; the container exited 1 without OOM. No campaign
is currently running. This failed interval contributes no qualification time.

The retained 60-byte input is identical to the existing `one-entry` journal
seed. Three fresh container invocations of 100 exact-input replays all returned
0 with the original two-second budget. They do not reproduce or explain the
timeout and do not turn the failed campaign into a pass. The earlier timeout
and failed campaign remain published separately. No timeout increase, corpus
deletion or synchronization change was made.

`capture/` contains launch and final state observations, audit output, replay
commands/logs/input, full failed-campaign and supervisor-control archives with
verified per-file indexes, and the committed source archive. Deliberate invalid
TMPDIR failures in the control archive are expected infrastructure tests.
The live log copies under `launch-snapshot/` are dated prefixes, not complete
campaign logs; the failed-campaign archive is complete. `build/` retains the
actual instrumented executables and build receipt. Verify stored and reconstructed
bytes with `python3 benchmarks/runtime/verify-evidence-archive.py` and this directory.

The next step is to capture where the timeout spends its time without weakening
the timeout budget. Whole-broker behavioral checks and client conformance still
fail. Production readiness and fastest client/server performance remain unqualified.
