# Benchmark build profiles

KL09-62 compares benchmark builds on the current shared x86_64 host. It varies
LTO (none, thin, fat), codegen units (16 or 1), CPU target (x86-64 or native) and
PGO (thin LTO with the native target). It uses latest-stable Rust 1.99.0 and its
matching LLVM 23.1.1 tools. Library manifests, dependencies and defaults are
unchanged.

Each build records its exact Cargo command, explicit profile environment,
compiler identity and executable hashes. The measured runtime is always
current-thread. The null broker keeps the portable baseline executable for all
client profiles. Native bulk uses one owned Kafka 4.3.1 broker, fixed client and
broker CPU affinity, fresh topics and independent Java receipt verification.

All builds finish before comparison timing starts. Separate instrumented runs
train PGO on null produce, null fetch and native bulk. Every raw profile is
retained and hash-bound to the merged profile passed to the final compiler.
Training results are diagnostics, not comparison candidates.

The planned comparison has five interleaved repetitions per profile and cell,
plus five fresh portable-baseline repetitions. Null-broker cells retain their
20,000-record fixtures; native bulk retains eight million timed records and
10,000 warmup records. Actual durations and warmup observations are recorded.
These exploratory runs do not meet every frozen comparison-cell requirement,
control CPU frequency or establish performance leadership. Native resource
observations cover the whole workload; null-broker observations cover timed
work. Allocation instrumentation can affect contention.

`measure-build-profiles.py build` builds and retains the executables, then clears
its dedicated compilation directory. `matrix` assembles either the training or
comparison configurations. `merge` uses the matching stable `llvm-profdata` on
the retained training files. All stages require a clean source checkout, an
explicit source pin map, a new output directory and the source commit.

Pass the generated matrix to `measure-runtime-flavors.py --build-matrix`, with
`--bulk-only` for the native family. PGO training uses `--repetitions 1
--skip-reproduction`; comparison runs use the default five repetitions and
reproduction cohort. Each job writes its actual build specification alongside
its measurements. Existing runtime-flavor invocations keep their original
defaults.

Profile gains are specific to this host, workload and instrumentation. The
native target is not portable to every x86_64 machine. PGO may help its training
workloads and regress others. The evidence card stays open until the complete
matrix, report validation and reproduction are retained.
