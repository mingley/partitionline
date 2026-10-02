# KL11-12 durable fixed-membership elections

Validated source: **18194a1a3635ac82b6eaeb4958fd44a318911d32**, extracted by
`git archive` with no draft overlays. `final/source-verification.json` compares
all 302 recorded source/reference/fixture files with their exact Git objects.
Both toolchains recorded identical hashes and unchanged files after every gate.

Stable Rust 1.99.0 and Rust 1.85.0 each pass all eight retained commands:
broker package clean, locked default and all-feature library/election tests,
format check, strict all-target/all-feature Clippy, strict all-feature rustdoc,
and strict default/all-feature doctest compilations. Each test invocation passes
17 library cases and nine election integration cases. Of those, **four unit and
nine integration cases concern elections**; the other 13 library cases cover
existing journal/catalog behavior. There are zero executable doctests. This
card runs the election behavior, rather than all broker integration suites.
Exact argv, environment, compiler versions, durations and log hashes are in
`final/{stable,1.85.0}-results.json`. Builds use two jobs, no debug information or
incremental compilation, CPUs 0–2,4, and a dedicated target cleaned between lanes.

## Safety and bounds

The file-backed primitive binds local identity and a sorted, immutable set of
one to 64 nonnegative signed-32-bit voter IDs. Full term/vote/log-summary states
use the existing synchronous journal. Successful grants, higher-term replies
and outbound candidacy follow synchronized persistence. Within a term, a vote
cannot be cleared or changed. Recovery validates every checksummed state and
transition and resets volatile roles/grants/leader identity. A complete
unconfirmed append may be accepted after synchronized journal recovery.

Failed persistence poisons the election, removes volatile leadership and
disables all mutations and affirmative votes until reopening. Actual-file
tests exercise exhausted storage, torn-tail recovery, complete corruption and
checksummed semantic double votes. A private injected store separately tests
failure before writing versus a complete unconfirmed write; those tests isolate
the election consequence and rely on the journal's separately tested sync
contract. Higher-term request, response and leader-input paths also revoke old
volatile leadership when their checked deadline cannot be armed.

Terms, caller clock and deadline arithmetic are checked. Timers draw from a
positive inclusive interval bounded by 600,000 milliseconds using a seeded,
node-separated scheduling PRNG. It is not cryptographic, and bounded modulo
draws are not advertised as perfectly uniform. Persistence holds at most
65,536 full snapshots, including initialization; exhausting the configured
budget fails closed. The largest allowed journal is 22,020,120 bytes. The
implementation provides no automatic compaction.

## Actual histories and independent checks

Each toolchain retains 16 seeds (1–16), three voters, 112 clock steps per seed,
3–9 ms timers and a 512-state per-node budget. Node 1 is isolated at step 8
and rejoins at 36; node 3 is isolated at 60 and rejoins at 96. Node 2 restarts
at 48 and 84. Delivery includes seeded delay, reordering and replay of actual
requests/responses. An isolated test leader receives the replication-owner
`lose_quorum` callback. Local durable log summaries sometimes advance, without
pretending they are replicated or committed content.

Each retained trace has **8,523 events, 551 campaigns, 155 elections, 894 vote
denials and 32 restarts**, with all 48 actual final journals. The two traces
and all journal files are byte-identical. The independent Python state machine
imports no Rust decisions or scheduling PRNG. It validates causal durable
grants, strict distinct majorities, one elected leader per term, local stale
leader fencing, recovery, declared timer bounds, and every actual journal state
using an independently implemented bitwise CRC32C. It also requires supplied
test leader assertions to follow an actual recorded majority election.

All 12 synthetic counterexamples are rejected by the model and by its CLI
with exit code 1: double vote, stale log grant, stale leader acceptance,
duplicate election, campaign before persistence, forged majority, restart
rollback, log rollback, excessive timeout, leader without an actual election,
actual journal CRC corruption, and checksummed durable double vote. Each raw
failed trace, journal, mutation description, verdict and CLI output is retained
under `final/counterexamples/`. `final/oracle-cli-and-replay.json` records exact
commands and byte-identity hashes. These synthetic cases test the oracle;
they are not observed implementation failures.

`upstream/pins.json` records SHA-256 pins for the independently hashed original
Apache Kafka archives and 13 retained source/schema/license references for
each of 4.1.2, 4.2.1 and
4.3.1. `upstream-distinction.md` explains matching election properties and the
remaining KRaft differences. No Java election behavior or Kafka wire exchange
was executed for this card.

## Replay

Run from the repository using normal Python (without `-O`). The history
directory must not already exist. The commands below create an isolated source
tree and outputs, so a concurrently changing main checkout cannot affect them:

```sh
mkdir -p work/raft-replay-source
git archive 18194a1a3635ac82b6eaeb4958fd44a318911d32 | tar -x -C work/raft-replay-source
export CARGO_TARGET_DIR="$PWD/work/raft-replay-target"
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
PL_RAFT_HISTORY_DIR="$PWD/work/raft-replay-history" cargo +stable test --locked --manifest-path work/raft-replay-source/partitionline-broker/Cargo.toml --lib --test raft_election
python3 -B work/raft-replay-source/docs/evidence/broker/KL11-12/oracle.py work/raft-replay-history/history.jsonl --output work/raft-replay-oracle.json
python3 -B work/raft-replay-source/docs/evidence/broker/KL11-12/oracle-tests.py work/raft-replay-history/history.jsonl work/raft-replay-counterexamples
```

Repeat with Rust 1.85.0 and a fresh history directory. `validate-frozen.py`
retains the complete eight-command lane with explicit source SHA, source tree,
artifact directory, toolchain and target arguments. It is an evidence harness,
not part of the pinned crate source. `SHA256SUMS` covers this evidence tree.

## Failures and limits

`development/context.json` retains and distinguishes earlier candidate overlays,
a corrected upstream-path selector, a superseded counterexample that had not
actually changed its voter choice, and the higher-term timer-overflow review
correction. An initial frozen run failed to save the trace because its harness
passed a relative output path to the archived working directory; it passed 17
library and eight integration cases but has no successful final verdict.
The unchanged source was then validated completely using absolute paths after
package clean. All failed logs and the superseded raw fixture remain available.

Leader state is an election result, not a read/write lease or commit permission.
The replication owner must detect lost quorum and call `lose_quorum`; the
leader's timer does not itself provide that detection. `observe_leader` trusts
the caller's authenticated member assertion and performs a local term/identity
fence. It supplies no standalone remote-election proof or peer authentication.

Log summaries are caller-confirmed durable content, with an empty `(0,0)` or
positive term/index pair. They do not represent Kafka's nonempty epoch-zero
logs. There is no content replication, reconciliation/truncation, commit index,
snapshot, dynamic membership, directory identity, pre-vote, or Vote/Begin/End
wire API in this module. Checked `u64` terms/indexes need a separate validated
conversion to Kafka's signed wire domains and log-end offsets.

Journal ownership is enforced within this process; cross-process exclusive
ownership remains the caller's responsibility. CRC detects corruption rather
than authenticating data. Synchronous I/O belongs on a dedicated storage thread.
Physical power loss, network filesystems, non-Linux platforms, adversarial
network membership, real-time scheduler performance and full KRaft compatibility
are unqualified. Finite model histories and passing tests do not establish
production qualification.
