# Partial journal offset recovery

A 42-byte input retained by the failed epoch-recovery fuzz run contains an
incomplete entry header with offset bytes that contradict the expected offset.
The old code accepted it and truncated the journal from 42 bytes to 24. The
corrected code checks every available offset byte and returns `Corruption::Offset`
without changing the file. The epoch-recovery caller preserves the same bytes.

Two regressions failed before the production change. Both feature modes now
pass all 18 journal tests and five epoch-recovery tests. The tests cover every
partial offset length, nonzero expected offsets, valid torn-tail recovery,
append/reopen and complete-header corruption. Focused strict Clippy and formatting
pass with stable Rust 1.99.1.

The detached fuzz campaign failed after 3,223 seconds with an epoch-recovery
timeout (exit 70); all five workers were joined. Five host replays and five
container invocations of 100 replays did not reproduce that timeout. Crash
minimization returned 1 because the input did not fail again. The timeout remains
unexplained. This integrity repair does not establish a timeout fix or a completed
24-hour campaign. No campaign is running at this observation.

`capture/` includes the original input, failed campaign archive and per-file index,
before/after logs, before executable, replay/minimization records and syscall trace.
The first regression attempt had a compile error; the second ran and failed both
intended cases. `executables/` retains the corrected test executables; `source/`
retains the tested source. Verify stored and reconstructed bytes with
`python3 benchmarks/runtime/verify-evidence-archive.py` and this directory.
Production readiness and performance leadership remain unqualified.
