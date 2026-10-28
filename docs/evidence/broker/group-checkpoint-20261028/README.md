# Group coordinator checkpoint candidate

The consumer, share and streams coordinator journals previously stopped
accepting updates after a fixed number of snapshots. They could also exhaust
the journal's index or byte limit sooner. This candidate replaces the journal
with a synchronized, atomically renamed checkpoint before any of those limits
blocks the next state update. The temporary adjacent file is reserved for this
purpose; rollover can temporarily require space for the old journal and the
replacement. Failed storage operations poison the coordinator so recovery
must determine which complete state was retained.

The default-feature checkpoint test filter passed 12 tests. It exercised 40
heartbeats for each of three limit profiles in each coordinator, reopening and
checking assignment, member epoch, and session expiry during each run. Other
cases rejected misplaced or invalid checkpoints, checked process-local inode
ownership and a hostile temporary symlink, and started subprocesses that exited
before rename, after rename, and after directory sync. These process exits do
not simulate power loss or establish filesystem or hardware durability. Strict
Clippy passed for the broker library and tests.

The initial test attempts failed because the sparse checkout omitted targets
required to parse the parent manifest. The initial Clippy attempt failed on a
missing sparse-checkout fixture and two synchronous test I/O lints. Those logs
and subsequent passes are retained in `capture/`. The patch is
`capture/candidate.diff`. The tests ran with latest stable Rust, a two-job
build and offline cached dependencies; commands and compiler details are in
`capture/receipt.json`.

This is an uncommitted candidate based on commit `99868cad0`, not a pass for
the integrated main tree. Source and executable hashes were captured after the
runs; no pre-run input or executable hash was recorded. The archived source is
a post-run snapshot and the executable is not cryptographically bound to the
recorded build inputs. Run full broker tests and the all-feature checks on a
committed candidate before using these results as a release gate. The coordinator
still serializes its full state on heartbeats. Classic, offset and lifecycle
coordinators have separate finite-history limits and remain open.

Verify stored and reconstructed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/group-checkpoint-20261028
```
