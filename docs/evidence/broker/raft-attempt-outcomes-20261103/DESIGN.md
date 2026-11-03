# Typed Raft attempt outcomes

## Observations and scope

The c034 broker job 125729388284 reported an observer-prefix proposal whose
reply was `Node(NotLeader)`. The later 337 broker job 125734535528 reported a
distinct voter-removal reply with the same error. State files are independently
published and can be stale. These logs do not establish the cause of either
failure or show whether the operation was appended. The earlier fdaf job's
two bare `InvalidDigit` errors are separate and their causes are unknown.

`Node::propose` and `Node::remove_voter` check leadership before the requested
append, then append and synchronize, reconcile the election summary and
membership, and advance commit. The last step can return `NotLeader`, and
persistence or reconciliation may fail after a complete synchronized append.
The existing error value is therefore not an append outcome certificate.

## Contract

This isolated candidate adds `propose_attempt` and `remove_voter_attempt` to
the exclusive `Node` and to the private-peer runtime `Handle`. Existing methods
and public error enum variants remain available and retain their error
semantics. The new methods return a separate non-exhaustive `AttemptError`:

* `NotLeaderBeforeAppend` is produced only by the exclusive owner after its
  `poll` succeeds and the owner's explicit role/active-term check finds no
  current leadership. This invocation did not attempt its requested data or
  voter-configuration WAL append. Other callers, other owners and prior
  attempts are outside that statement. Polling can expire or abort a snapshot,
  expire feature state, revoke authority or change election state; the result
  does not promise absence of other local or durable effects.
* `Indeterminate(error)` covers all polling errors and every later error,
  including input validation, storage, failed summary/membership
  reconciliation, and `NotLeader` after append. Some of these are known by the
  implementation to precede an append; treating them conservatively keeps the
  retry boundary small.
* A successful proposal position is locally synchronized but is not a commit
  acknowledgment. A removal receipt with `committed=false` is not final.

The check and subsequent append run synchronously under one Node owner; there
is no other actor interleaving between them. The runtime carries the explicit
owner result in the one-shot response for this exact invocation. A caller must
actually receive the typed rejection. Timeouts, closed channels, dropped or
cancelled futures and all other lost responses yield no such certificate; an
already started write can complete. A runtime `Deadline` or `Closed` error is
`Indeterminate` even when a particular queued command was never executed.

The rejection is not a persistent or deduplicated outcome record. The safety
property is about the requested append never being attempted in the observed
owner execution; an old rejected attempt therefore does not reappear from this
owner's WAL on restart. This says nothing about another invocation which may
have written the same bytes, nor does it make a new remote leader authoritative
or authenticate its identity. A crash or response loss before the caller
receives the typed result is indeterminate. Existing storage assumptions about
fsync, recovery and lack of external rollback continue to apply.

No retries are added. A later retrying driver must have no earlier ambiguous or
concurrent attempt, refresh the actual leader, enforce one existing absolute
deadline and bounded work across all attempts, and stop on anything except an
observed typed rejection. A self-removal test must choose a newly confirmed
current leader as both the executor and removal target after a rejected stale
selection; merely using the old target on a new leader would instead remove a
follower. The original test histories and checks must continue to pass.

`add_voter` is intentionally excluded. It has an asynchronous negotiation and
catch-up sequence with additional side effects; this candidate makes no
whole-operation pre-append promise for it. Authentication and production
replication qualification remain separate requirements.

## Controls proposed in the patch

* A virtual-clock dynamic group accepts an uncommitted proposal, then loses its
  quorum lease. Typed proposal and removal both reject, the prior WAL and
  committed barrier remain unchanged, the legacy proposal still reports its
  existing error, and reopening retains the same prefix. Invalid backwards
  clock results are indeterminate. Existing bounded catch-up setup may make
  up to four exchanges per follower.
* A self-removal returns an uncommitted receipt; a second removal while that
  configuration is pending is indeterminate. Reopen retains the first change.
* A test deliberately extends the companion election file before a removal.
  The metadata append then synchronizes, the election-summary append detects
  the unexpected file length, and the result is indeterminate. The test
  restores exactly the byte-length damage it introduced, reopens and checks the
  previously appended removal. This repair is test setup, not an instruction
  for repairing damaged production files.
* A journal test hook returns an error after an actual proposal append sync.
  The Node poisons, returns indeterminate, and reopening finds the complete
  record. A subsequent attempt against the poisoned owner is indeterminate.
* The runtime control checks the separately typed follower rejection and a
  queued deadline through the public Handle. It then gates an actual owner
  proposal after execution, drops that response, and verifies the record is
  present before submitting a different proposal. The gated lost-response
  portion exercises the real command and delivery path but does not invoke the
  public Handle's timeout after execution; that case still requires a native
  test which gates a public call after the owner starts.

All controls are added tests; original deadlines, historical tests, operations
and assertions are unchanged. The candidate has not been compiled or run, so
the tests and recovery behavior remain expectations rather than evidence.

## Qualification still required

Independent static review, Rust compilation and formatting/lint checks, default
and all-feature native tests, stress/repetition of the exact original failing
three- and five-node histories, cancellation/timeout after public admission,
race coverage and measurement are required before adoption. No performance or
production-readiness claim follows from this patch. The original historical
failures must remain visible rather than being relabeled as fixed by this API.

The complete original log gzip files remain at:

* `broker-raft-fdaf-investigation-20261103/job-125726869343.log.gz`, SHA-256
  `10a681a835802543b63a4c8178c9cc0d831aa53c4f8e53b8c13ffe6a15db2902`.
* `broker-raft-fdaf-investigation-20261103/job-125729388284.log.gz`, SHA-256
  `69aa06c4039151c80bc682ad593ee0b4540a1794fef506188f6d9a646aaef8c7`.
* `broker-raft-fdaf-investigation-20261103/job-125734535528.log.gz`, SHA-256
  `7ed2f7485ce6762c686a93283bb35f5d4a3783d54dbd89262ddc9312a21057e6`.
