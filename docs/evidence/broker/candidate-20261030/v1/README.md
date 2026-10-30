# Broker candidate qualification

This is an isolated candidate for replica epoch persistence, journal publication
permissions and synchronization, and five response-buffer growth fixes. It also
adds diagnostics for unexpected Raft proposal replies. Production source has not
adopted this candidate.

The local baseline build stopped at the 1.5 GiB disk guard before any test ran.
Its source inventory, compiler log, guard samples and receipts are retained in
`local-baseline-interrupted.tar.gz`. The interrupted build provides no behavioral
pass or expected-failure evidence.

The CI driver verifies the checked-out source pins and every member of the
baseline capsule before extracting to a separate directory. It requires all
seven named baseline controls to fail, then applies the exact patch retained in
`fixed-source.patch.gz` there.
The fixed source must pass twelve focused controls in each feature profile,
full default/all-feature all-target tests, formatting, strict Clippy, Rustdoc,
doctests and all 55 broker-inventory controls. Complete test executables are
retained as verified gzip files. Functional gate failures are retained while
later independent gates run; source/resource failures stop the lane.

`baseline-source.tar.gz` contains 5,982 regular files (11,033,212 source/fixture
bytes). All member bytes, sizes and modes have been read back. The 8-file fixed
patch has been reconstructed exactly. Eleven pure/mocked driver controls and
six small Python process-tree controls pass. These cover exact test identities,
archive and patch integrity, final resource checks, interrupted command cleanup,
and an actual optimized-Python hash-rejection subprocess. The process controls
include a child in another process group within the command's private session,
and a bystander that must remain alive. These preparation checks did not run
Cargo or Kafka tests. The original seven-control receipt is preserved alongside
the current preparation receipt.

The declared base is main `fe65ac7833fbaaa81331e31a182964c4ef3657a5`, followed by
the separately reviewed eleven stale inventory-digest replacements. The
candidate changes four digest bindings again only inside its extracted source;
API ranges and qualification flags remain unchanged.

CI uses latest stable Rust on x86_64, one Cargo job, incremental compilation off,
and zero test/dev debug information. This differs from ordinary CI debug
settings. The disk floor remains 1.5 GiB. Missing host OOM counters are recorded as
unavailable; partial counters are named explicitly. They are never filled with
invented zero values or used as a resource certificate. Observed OOM increases
or counter resets fail the lane.

The public Raft observer failure remains unresolved. A diagnostic pass alone
cannot prove it was repaired. This candidate does not establish replicated Kafka
partition data, authenticated leadership, a committed-watermark truncation guard,
native KRaft, production readiness, power-loss durability or a performance rank.
