# Benchmark build profiles

KL09-62 records an exploratory comparison on the shared x86_64 host on
October 7, 2026. It uses Rust 1.99.0 stable, LLVM 23.1.1 and locked Tokio 1.53.2.
The library manifests, dependencies and defaults were unchanged.

All six comparison builds used a current-thread runtime. Clients used CPUs 2
and 4; the Kafka 4.3.1 broker used CPUs 0 and 1. The null broker kept a private
copy of the same portable-baseline executable for every profile. CPU frequency
was uncontrolled, and source-preparation work continued on the shared host.
All compilation finished before comparison timing.

## Results

These are medians of five interleaved primary repetitions, in records/s.
`portable` means target-cpu=x86-64. All builds used optimization level 3,
debug=0 and strip=none. The profile names give LTO, codegen units and CPU target;
`pgo_use` uses thin LTO, 16 units and the native target.

| Build | Null produce | Null fetch | Native bulk |
| --- | ---: | ---: | ---: |
| none_16_portable | 74,585 | 102,039 | 227,899 |
| thin_16_portable | 75,071 | 111,881 | 214,206 |
| fat_16_portable | 75,430 | 109,141 | 226,657 |
| thin_1_portable | 74,789 | 106,143 | 228,958 |
| thin_16_native | 75,976 | 110,368 | 215,901 |
| pgo_use | 75,614 | 113,573 | 234,543 |

All throughput intervals against the disabled-LTO portable baseline include
zero. The matching one-factor comparisons are in
[analysis-02.json](study/analysis-02.json). Against thin LTO with the native
CPU target, PGO's paired median throughput change was −0.01% for null produce
(95% interval −9.28% to +4.58%), +1.31% for null fetch (+0.28% to +14.70%) and
+5.73% for native bulk (−1.74% to +14.99%).

Against that same non-PGO native build, PGO reduced measured null-produce CPU
cost by a paired median 15.61% (95% interval 5.67% to 21.01% lower) and
null-fetch CPU cost by 5.25% (2.11% to 33.14% lower). Native CPU cost's interval
includes zero. These observations describe the instrumented fixtures. They do
not establish a general profile choice or a performance ranking.

Each interval bootstraps the five matched percent changes 20,000 times, with
seed 962 plus the cell index. Intervals are per comparison, without a multiple
comparison adjustment. Five fresh baseline repetitions followed the primary
matrix: median throughput changed by +1.41% for null produce, +5.22% for null
fetch and −7.22% for native bulk. All observed values, resource measurements,
outcomes and intervals remain in the analysis.

## Training and checks

Seven builds passed: six initial builds including PGO instrumentation, then the
PGO-use build. Four executables per build, exact Cargo commands, explicit profile
environment, compiler identity and hashes are retained. Separate instrumented
runs trained on null produce, null fetch and native bulk. Four raw profiles
include the native configuration-print process; matching stable llvm-profdata
merged them into a profile containing 24,016 functions. Training is excluded
from the comparison. The PGO-use build emitted no compiler warnings.

There are 105 comparison/rerun results and three training results. Every one
has a waited report CLI exit of zero. All 36 native results pass the complete
JSON schema. The 72 null results fail it: their original bytes, successful
CLI receipts and actual schema errors are retained. KL09-73 tracks corrected
null output and full-schema enforcement. No full-schema pass is claimed for
those files.

Null runs target 20,000 records; fetched overfetch counts remain recorded.
Native runs acknowledge eight million timed records after 10,000 warmup
records. The Apache Java client checked every key, payload and partition offset:
280,350,000 records in the comparison and 8,010,000 in training. IDs are checked
within each warmup/timed phase. All original measured runs completed without
rejections, unknown outcomes or timeouts.

Validation rejected 144 actual native semantic/runtime mutations, 36 native
schema mutations and four changed matrix inputs. The latter are actual
analyzer CLI failures for a changed artifact hash, missing repetition,
instrumented comparison and changed build binding. A copied captured artifact
is a negative-control input, not another measurement. The study retains 363
waited command receipts, including five expected exit-1 controls. Both
owned brokers shut down with exit 143, joined their supervisors, left empty
process groups and released both ports. Each topic was deleted after readback.

A final merge check found that LLVM's `--failure-mode=all` accepts valid inputs
alongside a corrupted profile. That failed rejection is retained in
[integration/strict-merge-01](integration/strict-merge-01/summary.json).
The builder now uses `--failure-mode=any` and rejects a truncated captured input.
Strictly remerging all four valid profiles reproduced the exact bytes used by
the compiler, so the measurements and PGO input did not change.

Native resources cover the whole workload; null resources cover timed work.
Allocation instrumentation can affect costs. Bulk samples bound admission to
final flush rather than individual acknowledgments. The null fixtures have
short durations and do not satisfy every frozen comparison timing floor.
The benchmark's bounded cancellation barrier remains outside timing;
production shutdown completion is still KL02-12. Suite HOLD remains active.

## Source and archive

Compiler source is `999133586acfd88604222dc92c01c9eadd2fc6a5`. The executed
controller is `7667b6d5bfe39d867359282f26084b09aaae65e8`; all 184 compared
library, example, benchmark Rust and manifest files are byte-identical to the
compiler commit. Its pre-capture fixes bind result provenance to the executable
actually run and record the actual training repetition count. The final
analyzer is `8d32f523dd98611e8f503dcc5667553ab8107327`. Earlier preparation
and baseline-only analysis remain retained separately.

[archive-inventory.json](archive-inventory.json) maps original file hashes and
sizes to the published representations. Large files and executables use
deterministic gzip. Git publication scratch indexes and Python caches are
excluded. Kafka distributions, SDK archives and the LLVM tool installation are
external checksum-pinned inputs. Absolute paths record the original workspace;
a clone needs fresh paths and downloaded dependencies for execution.

Verify stored and reconstructed bytes from the repository root:

```bash
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/perf/build-profiles
```

`SHA256SUMS` covers all published files. Follow
[BUILD-PROFILES.md](../../../../benchmarks/runtime/BUILD-PROFILES.md) for a fresh
run. CPU targeting and training make any gain specific to this host and these
workloads. Controlled hardware and independent peer comparisons remain open.
