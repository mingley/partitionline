# Controller queue candidate qualification

This capsule tests a proposed change to queued controller input capacity. The
candidate is not adopted or qualified. Its base is commit
`45cbff24de76490594970fc962cd04e7be9fafb1`; `spec.json` pins its patches,
source archive, catalog and verification helpers. The supplied workflow pins
the driver and static controls separately. The hosted runner fetches only the
base Git trees and compares all archived bytes to their SHA-256 catalog and Git
blob IDs; it does not need to download the base blobs.

The intended invariant is at most `Q * L` retained Vec capacity across queued
input jobs, where Q is the queue count and L is the maximum request size.
It excludes active work, responses, socket buffers, allocator overhead and
caller futures. An over-reserved valid request is copied before admission; the
original and copied allocations coexist temporarily. An already-full queue
can therefore still incur up to a 64 MiB allocation and copy before `Busy`.
An oversized optional response spare is discarded. This is not a process-memory
bound or a measured performance improvement.

The qualification runs an exact baseline negative test and default/all-feature
positive controls, then broker-only all-target tests, formatting, strict Clippy,
Rustdoc, doctests and the broker inventory. A deterministic duplex stream
exercises the actual response-write/prefetch helper and then the real controller.
A separate real TCP test uses the real transport and controller, but deliberately
inflates the request buffer. Neither establishes that a naturally pipelined real
TCP connection reaches the controller with an over-reserved request. That remains
an open control. The cancellation and stop tests suspend pre-admission copying
with a controlled receiver; they do not prove that already-queued requests are
skipped or that a busy durable actor shuts down correctly. Existing shutdown-join
tests cover separate retry and failure behavior.

The workflow requires a disposable GitHub-hosted Linux x86_64 runner, a new
canonical output child of `RUNNER_TEMP`, and temp and checkout on the same
filesystem. It checks free space before setup and after checkout/toolchain setup.
Its supervised native commands sample free space against a 1.5 GiB floor, bound
logs and time, check command ownership and source hashes, and stop on failures.
Checkout and the toolchain setup action are checked before/after but are not
continuously monitored. Logs, reports and source/patch inputs are retained for
30 days, including failures. Source trees, Cargo targets and runtime temporary
files are not uploaded. A canceled job or infrastructure failure is not a pass.

The capsule has passed source-only patch replay, host/path negative controls,
and Git-tree source comparison. No Rust compilation, runtime test, hosted run,
allocation-pressure test or benchmark has run for this candidate. Positive
copy controls require the test host allocator to supply a capacity within L;
otherwise the candidate returns Allocation and the positive test is not met.
Independent review, a successful hosted run, real pipeline coverage, and
performance/load measurement remain necessary before considering adoption.
