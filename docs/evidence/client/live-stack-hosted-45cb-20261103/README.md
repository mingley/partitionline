# Classic group stack diagnostic

This is a diagnostic candidate, not a fix or a compatibility result. It has not
been compiled or run against Kafka. It targets source commit
`45cbff24de76490594970fc962cd04e7be9fafb1` and the Kafka 4.1.2 image
`apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`.
It leaves the ordinary CI configuration and its deadlines unchanged.

The current unmodified live CI jobs for Kafka 4.1.2, 4.2.1, and 4.3.1 reported
stack overflow after logging `compatibility group phase: classic join`: jobs
125811643708, 125811643587, and 125811643614 in workflow 41604650418. Those
logs provide no crashing frame or last completed RPC. They do not establish the
cause. The pinned group, compatibility-test and harness sources match the
previously investigated `2645e96ce` versions.

Workflow 41674604055 then ran the shortened classic-only diagnostic once per
case on separately allocated runners. The `normal` and `caller_box` cases
reported runtime stack overflow; `stack_8m` and `library_box` returned from
classic join. The `stack_2m` case failed while rustc compiled rustls, before the
live test ran. The old collector passed `RUST_MIN_STACK` to the whole
harness, including rustc. The original run and artifacts remain separate evidence. None of
these observations establishes the failure's cause, proves library boxing fixes
the unchanged full test, or measures performance.

## Scope

`classic-only.patch` adds an unconditional return immediately after the first,
classic `join_group` succeeds. The original 150-second outer deadline and
20-second join deadline stay in place. `harness-classic-only.patch` preserves
the existing test command, adds an ownership token for the fresh broker, and
stops the harness immediately after that test returns. The later group, share,
transaction, ReadCommitted, rollback and independent history scenarios do not
run. These are separate source patches, applied only to a dedicated checkout.

The return changes the compiled caller, even when the join does not return, so
the diagnostic is not the same binary as the failing CI test. It may change
the future size, code generation, timing or stack usage. A pass or failure
does not prove the behavior or root cause of the unchanged test. The new
`PL_STACK_CLASSIC_JOIN_COMPLETE` marker only reports that this modified caller's
classic join returned; it is not a full compatibility result.

The matrix runs six cases on hosted Linux x86_64 jobs, at most two at a time:

- `normal`: no `RUST_MIN_STACK` override.
- `stack_2m`, `stack_8m`: request `RUST_MIN_STACK=2097152` and `8388608`.
  These values apply only to the test executable through Cargo's target runner;
  Cargo, rustc and build scripts do not receive `RUST_MIN_STACK`. A runner marker
  must appear in the test log or the requested stack case is inconclusive. The
  values are requests; the effective thread stack mapping is not measured.
- `caller_box`: box the join future in the test caller.
- `library_box`: box each private rejoin attempt. This adds an allocation per
  attempt and is an unmeasured hypothesis, not a proposed production fix.
- `phase_trace`: add logging to the group implementation. The extra logging
  can change timing and stack layout, so absence of a marker is not proof that
  execution did not enter a phase.

All cases use the same early return. Job failures remain failures; successful
collection or a successful early return qualifies neither production readiness
nor performance. Cases run on separately allocated hosts; they are not a
controlled performance comparison. The original unmodified CI is still needed
for an eventual fix.

## Run

Install the contents of `capsule/` at the matching repository paths, review and
commit them to the default branch. A push to `main` that changes the workflow
or this diagnostic directory starts one workflow run. Other pushes do not.
The workflow can also be dispatched manually:

```sh
gh workflow run classic-group-stack-diagnostic.yml --ref main
gh run list --workflow classic-group-stack-diagnostic.yml --limit 1
```

The driver checks the dedicated checkout's exact commit, cleanliness, source
hashes, patch hashes and expected postimages before creating a local diagnostic
commit. It refuses to run outside an Actions Linux x86_64 job or when free disk
space is below 1.5 GiB, and it stops a running child if that floor is crossed.
A 900-second outer collector limit bounds broker setup and compilation; the
150-second test and 20-second join limits are not enlarged. The child has core
dumps disabled and a 512 MiB per-file limit. Process ownership
uses the pinned `scripts/candidate-command-ownership.py`; on timeout it closes
the owned session and then removes only a container with the exact random name,
label and ID. This is not a security sandbox and abrupt host loss can prevent a
cleanup receipt. Each GitHub-hosted job has its own disposable runner.

The workflow retains `classic-stack-<case>-<run_id>-<attempt>` artifacts for
up to 30 days, subject to repository retention policy. They include
`receipt.json`, `subject.diff`, `harness.log`, and any broker/build/runtime logs
that were created. The receipt records the manifest and patch hashes, the
local diagnostic commit, observed outcome, cleanup, and, when exactly one test
executable is found, its hash after the process exits. A post-exit hash does not
prove the executable was unchanged while running. Rust and Kafka identities are
recorded by the harness when setup reaches those steps. Build outputs are not
uploaded. If setup fails before an output file is created, inspect the job log;
a missing artifact is not a passing test.

Source and patch hashes are in `subject-pins.json`; `SHA256SUMS` covers the
capsule files. The locally available disk had less than 1.5 GiB free when the
candidate was prepared, so no Cargo, Rust compiler, Docker, live Kafka, or
performance run was performed. The source-only control tests can be run with:

```sh
PL_STACK_SOURCE_CHECKOUT=/path/to/partitionline \
  python3 -B -m unittest -v test_stack_runner.py
```

The source checkout must contain the pinned commit. The local source-only tests
check the workflow trigger boundaries, patch application and postimages, the
early stop, unchanged timeout literals, runtime-only stack environment and runner behavior, failure classification,
disk guard and exact container cleanup ownership. They do not establish that the Rust patches build.
