# OIDC socket lease revocation ordering

The clean all-feature broker matrix at `de38cd3df` failed
`revocation_cancels_active_handler_and_waiting_handler_admission`: two handlers
started where the test expected one. The unchanged test also failed by itself;
its executable hash matched the matrix receipt. Neither failure recorded the
ordering of the two socket leases, so their exact cause remains unproven.

The candidate keeps the one-handler assertion when the waiting socket's lease
is revoked first. It adds tests for active-first revocation, both subjects
revoked while one inactive response is held, and two sockets authenticated with
identical bearer bytes. In the latter case both sockets must close and every
started handler must be cancelled; either one or two handlers may start before
both independently refreshed leases observe revocation. A direct transport unit
test holds the handler permit, revokes a queued operation's lease, and checks
that the operation ends before the permit is released. The Lease documentation
now states the per-socket refresh behavior.

On the isolated v2 candidate, the same-token case passed individually, the
complete OIDC session target passed 15 tests, and the direct permit unit test
passed one selected test. Strict all-feature Clippy passed for the broker
library and OIDC session target. All four valid receipts report stable source
inputs. The session test binary was freshly rebuilt from the candidate, and its
retained local copy hashes to
`40f4d3bbfe968be37e4d3fc0746b5bd10990a9058c66e372f2251e8b4b5c86cb`;
its listed tests include all four ordering cases. The unit binary was freshly
rebuilt and hashes to
`16e0f6428caef84e31f0f1890ccaf1c8103dc73929b124fe8ea170313b069ed6`.
The binaries are retained in `/workspace/work/goal-qualification-20261028/`,
not in this archive.

An earlier v1 session run reported 14 passes but its receipt recorded zero
free disk at completion; it is retained as resource-compromised evidence. The
v2 `unit-01` run selected zero tests because the shared Cargo target reused a
stale binary, and `clippy-01` did not demonstrate a fresh check; neither counts
as a pass. The final `unit-02` and `clippy-02` show actual compilation/checking.
Their 0.25-second disk monitor sampled no drop below the 2 GiB stop floor;
shorter dips could have been missed. The same-token and session runs also had
that monitor. The low-disk process snapshot remains in the local work directory.

The checks ran on x86_64 with stable Rust in the isolated dirty worktree based
on `de38cd3df`; the archived v2 patch and receipt input hashes identify the
candidate. They do not pin all external dependencies or machine state, and they
are not checks of a clean integrated commit. The tests show the stated local
lease behavior; they do not establish immediate cross-socket revocation, a
complete OAuth/OIDC security qualification, or a performance claim.

## Integrated working-tree checks

The candidate was also tested in the main working tree at base
`b24b0502e66acf3ed5a93c40529c2f19181b4dd7`, with the five OIDC files
modified and two evidence directories untracked. The unit test selected one
case and passed; the complete OIDC session target passed 15 tests; strict
all-feature library and session-target Clippy passed. All three receipts
record the same before/after source hashes, HEAD and Git status. These are
checks of a dirty integrated tree, not a clean commit or a full broker suite.

Cargo rebuilt the unit and session executables from the main worktree. Their
receipt hashes are `cb2c2ed06ea4645165003f4565aaf442fd8570cf32d7e19ca13354f36f8383b5`
and `3adfc079f207512a7d5864cd9741cab868a292dfc10f14bf67e6113170edca3c`,
respectively. These integrated executables were recorded by hash only; target
paths may later be overwritten. The disk monitor used a 2.3 GiB start guard
and a 2 GiB stop floor. The lowest 0.25-second sample across the three runs
was 2,537,582,592 bytes; shorter dips could have been missed.

Verify archived bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/oidc-revocation-order-20261028
```
