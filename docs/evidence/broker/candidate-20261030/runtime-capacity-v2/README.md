# Broker runtime capacity qualification candidate

This source-only candidate tests the reviewed `Handle::propose` capacity
change on GitHub's Linux x86_64 runner. It uses latest stable Rust and the
existing broker qualification runner. Production broker files are copied from
commit `6fa8cdb3588d884580d01331e44df657c02f7670` into an isolated directory.
The archive includes the complete broker tree and its independent `Cargo.lock`.
The proposed workflow patch is inactive until explicitly applied.

The baseline starts from that exact source. A test-only overlay adds the two
reviewed tests and their fixture setup without changing the original production
`Handle::propose`. Its helper shims let the fixed-source tests compile against
the old implementation. A separate baseline test requires the old method to
return `Err(Deadline)` while every client permit is held. The original fixed
admission/WAL test must fail at its specific over-capacity `InvalidConfig`
assertion; another setup failure, a different panic or a missing test fails
the gate. The allocation-identity test must pass.

The runner reverses the test-only overlay, verifies the original source and
then applies the exact independently reviewed v2 patch. Both capacity tests
must pass in default and all-feature builds. The focused test requires the
invalid allocation to be rejected before admission, with no permit or envelope
peak and no change to the captured WAL state; it also checks a bounded proposal
and allocation identity. The original 29 runtime tests and their existing
20 ms fixture setting remain in place.

Both profiles run the existing 12 broker focused controls, three asynchronous
filesystem controls, all-target tests with the existing 700-pass minimum, and
three repeats of the existing Fetch deadline test. The plan also retains the
stale-leader exact control, source formatting, strict all-feature Clippy and
Rustdoc, both strict doctest profiles, the broker API inventory and its 55
negative controls. The existing runner owns child processes, checks source
hashes and modes around commands, and records resource and cleanup evidence.

The workflow checks the exact wrapper, static test and specification bytes
before executing them. The same workflow executes exactly five static controls
and rejects skipped or expected-failure tests. Its artifact retains the commands, test logs,
source-mode receipts, reports and reviewed inputs. `spec.json` pins the base
archive, complete file catalog, baseline overlay, v2 patch and review receipts.
The artifact excludes the extracted source tree.

The expected baseline failure and fixed passes have not been observed. No Rust
compilation, native tests, benchmark or hosted execution was performed for this
candidate. A successful run would qualify these specific controls only. The
change rejects a previously accepted `Vec` whose capacity exceeds the
configured command payload envelope; caller-owned inputs before admission,
allocator overhead, OS buffers and process RSS remain outside the accounting.
The related ControllerHandler queue accounting issue remains unresolved.

For review, `candidates-workflow.patch` applies to the current candidates
workflow, and its result is retained as `candidates-workflow-result.yml`. A
future import places this directory at
`docs/evidence/broker/candidate-20261030/runtime-capacity-v2` and applies the
workflow patch. The existing candidate concurrency setting does not cancel an
already running qualification. Review the exact final commit before starting a
hosted run.
