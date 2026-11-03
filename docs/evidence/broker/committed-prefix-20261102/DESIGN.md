# Committed-prefix protection for local replica reconciliation

Status: design and source review only. There is no trustworthy committed-prefix
authority wired to `replication::Log`. No production patch or Rust tests were
run. A numeric comparison against an invented or stale watermark would give a
false safety guarantee.

## Current boundaries

- `replication::Log` replays a contiguous, zero-based local event journal.
  A replication call finds the common prefix, can atomically persist a newer
  local epoch header, and may then journal a separate truncation and copy
  replacement records. Its `leader_epoch` is supplied by the caller.
- The log has no cluster/topic/partition identity, authenticated leader,
  membership configuration, committed watermark or connection to the broker's
  RF1 operator data path.
- `high_watermark::Durability` models ISR and offsets in a separate journal.
  Its replica progress and membership are caller-supplied, its in-memory
  watermark can advance before a corresponding commit event, and it is not
  bound to the replica's bytes or to network quorum acknowledgements.
  `recovery::EpochLog` has a persistent watermark but is another ID-only
  journal, not this replica log or an authority over it.

Neither independent model can certify a committed prefix of `replication::Log`.
The existing journal end offset is not a watermark: locally written records
may be uncommitted. Conversely, a legacy file with no watermark record must not
be assumed to have watermark zero.

## Authority prerequisite

Before production truncation, a higher-level partition owner must bind a
stable cluster identity, topic incarnation/UUID and partition number to the
replica journal. It must validate:

1. The current leader identity and epoch against authenticated controller or
   quorum state, including the membership/configuration generation.
2. A committed exclusive end `H` from the authority responsible for
   acknowledgements, bound to that partition incarnation and leader lineage.
   The authority must also attest that the elected leader contains the
   committed prefix, by a verified log-matching/election protocol or a
   content commitment with proofs. A caller-supplied integer, TLS identity
   alone or a separately constructed `Durability` value is insufficient.
3. Freshness and serialization: the old leader must be fenced from further
   commits before its final committed boundary is certified; the partition
   owner must serialize reconciliation with later commitment and leadership
   changes. A once-valid certificate can become stale while another leader
   is still able to acknowledge writes.

These components do not exist in the present local replica model. This work
must therefore remain unqualified until an actual authority can produce and
validate the witness. Test-only fabricated witnesses prove local mechanics,
not network or global durability.

## Two explicit transitions

Use separate actions under the partition owner's serialization boundary:

1. **Accept an authorized epoch.** Verify a current authority witness, then
   durably persist the higher epoch. On success, old epochs are fenced even if
   a later reconciliation detects a committed-prefix conflict. An arbitrary
   higher integer from an unauthenticated caller must not fence a valid leader.
   Rename or post-rename synchronization ambiguity poisons the replica.
2. **Reconcile data.** Against the accepted epoch and a fresh committed-prefix
   witness, compare local and leader records by offset, record epoch and exact
   bytes. Maintain a separately certified local prefix `C`, whose actual
   bytes have been proved to match committed leader lineage. If the first
   divergent local offset `cut` is below the previously certified `C`, reject
   before any truncation, journal append, reservation with externally visible
   effects, or *additional* epoch mutation. This rejection does not undo the
   separately successful authorized epoch acceptance.

Bind the accepted epoch to its leader and configuration identity. A different
leader for the same already accepted epoch must not silently replace that
binding. Persist the binding with the epoch, or revalidate it against a fresh
nonrollback authority before use after restart. The current four-byte epoch
header records only a number and cannot establish leader identity.

The current `replicate_from` mixes caller epoch acceptance with reconciliation.
A safe API must split or otherwise enforce these transitions. Keeping the old
unguarded method callable would bypass the guard. Requiring authority will
change public behavior and requires a migration path; silently leaving the old
method unsafe is not acceptable.

The existing public `Log::append(epoch, bytes)` also accepts higher epochs
directly and derives the accepted epoch from its newly appended record. In a
guarded model it must require an already-authorized epoch and a reconciled
certified prefix, or remain explicitly outside the safety claim. Otherwise it
bypasses the separate epoch and lineage transitions.

Offsets below global `H` are committed *in the certified leader lineage*.
That number does not make arbitrary local bytes at the same offsets committed.
Offsets below local `C` are both present and certified against that lineage.
A cut exactly at `C` can be allowed after source and authority validation;
a cut below `C` must be rejected. This applies when the leader has a
different record, has a shorter log, or is empty. A fresh, trusted `H = 0`
and `C = 0` can permit complete truncation; unknown or unavailable authority
must fail closed rather than be interpreted as zero.

For one partition incarnation, both the global committed end `H` and the
certified local prefix `C` must never regress. The global floor matters even
when it exceeds `C`: if a replica has observed `H = 10, C = 3`, a later
certificate `H = 5, C = 3` still omits previously committed offsets 5–9.
Reject it or establish an equivalent anti-rollback proof from the current
authority. Preserve the prior global floor and authority version across
restart, or require an independently durable nonrollback authority before
the replica can resume. Tracking only `C` does not establish this.

## Follower lag and partial copies

Distinguish the authoritative global committed end from a *verified* local
committed prefix. A follower may have local end `L < H` while catching up.
Its certified local boundary must satisfy `C <= min(H, L)`, but is not
necessarily equal to that minimum. Rejecting every certificate with `H > L`
would prevent valid catch-up.

For example, let a follower contain `[A, B_old]` with prior certified
`C = 1`, while the current certified leader contains `[A, B_new]` with
`H = 2`. Setting local `C = min(H, L) = 2` would falsely mark `B_old`
committed. Rejecting every `cut < H` would also prevent replacing that
uncommitted divergent suffix. After verifying the leader's committed lineage,
the follower may cut at `C = 1`, copy the real `B_new`, verify and sync it,
and then durably advance `C` to 2. It must never truncate `A`.

Newly copied bytes must come from a source whose committed lineage is actually
verified; only advance `C` over bytes verified and synchronized locally.
In particular, while `H > L`, the current public `Log::append` can write
arbitrary caller bytes at offsets below `H`. Such a write must be fenced until
the missing prefix is verified, or remain outside any claim that the local
prefix is committed. A numeric minimum cannot establish matching bytes.

If an authorized source is claimed to contain the entire committed prefix,
its end must cover `H`; otherwise reject it or use a separately specified
lagging-source/catch-up protocol with verifiable range proofs.

## Durable local state

A future guarded log can journal a versioned certified-local-prefix event in
the same checksummed event stream. It must bind the previously witnessed
global floor `H`, the actual verified local prefix `C`, and enough identity
to reject a different partition incarnation and older authority/configuration;
alternatively, a separate nonrollback authority must durably supply the
global floor. The proof or reference to a durable authority record needs an
explicit verification contract. Global `H` and verified local `C` are
distinct values. Older replica readers must reject the new event kind, never
ignore it. A guarded file with missing, malformed, mismatched or regressing
boundary state must fail closed.

CRC detects accidental damage; it does not authenticate a journal or prove
freshness. Restoring an older complete, internally valid journal can roll back
both `H` and `C` without a CRC failure. The protocol relies on the stated
filesystem/hardware synchronization guarantees and needs an independently
durable, nonrollback authority when such restoration is in scope. Before
advertising data as committed, accepting leadership, acknowledging a commit,
or reconciling destructively after restart, establish a fresh binding to that
authority. Raw diagnostic reads do not establish commitment.

Replay must distinguish historical legacy events from guarded operation:
legacy truncations may be replayed to reconstruct a preexisting file, but do
not prove that no committed data was lost before the trusted bootstrap. Once
the first certified local boundary `C` is present, any later journal
truncation below it is corrupt and must fail. An `H = 0, C = 0` initialization
needs verified authority; an empty file alone does not prove a remote partition
has no committed data. On restart, require current authority before committed
reads, leadership, acknowledgements or destructive reconciliation; the durable
local boundary is only a previously observed lower bound.

Persistence errors require the same poison/reopen discipline as record
appends: an error during write or sync can leave either a complete boundary
event or an incomplete tail. The caller must not acknowledge a commitment on
the strength of a failed boundary update. Replay accepts only a complete,
validated event and preserves monotonicity. A successful boundary advance must
be durable before it is relied upon for a matching acknowledgement or cut.

Watermark events consume journal bytes and index entries. Existing finite file,
entry, index and fetch limits must be checked before mutation and cannot be
raised to make the guard pass. A full one-entry journal needs an explicit
storage design or fails closed; it cannot silently accept an unrecorded guard.
Frequent advances also require bounded checkpointing or another durable
metadata scheme. Full-file epoch rewrites and their temporary disk usage remain
separate performance and durability concerns.

## Required controls

- Divergence below, exactly at and above certified local `C`; shorter and
  empty leaders; follower end below/equal/above global `H`; mixed record
  bytes and epochs. Include the `[A, B_old]` versus `[A, B_new]` example.
- A higher **authorized** epoch is durably fenced by its own transition even
  when the subsequent reconcile conflicts; old epochs are then refused.
  A higher untrusted claim has no authority to mutate the epoch. Reject a
  different leader for the same accepted epoch, including after restart.
- Unknown legacy watermark, trusted zero bootstrap, topic recreation with a
  different UUID, partition mismatch, stale/missing certificates, prior
  leader still able to commit, wrong source lineage and direct append while
  a certified prefix is missing. Include `H = 10, C = 3` followed by a
  stale `H = 5, C = 3` across both live operation and restart; restore an
  older complete, CRC-valid journal and require the nonrollback authority.
- Partial copied prefixes and retry budgets preserve all certified bytes;
  record offsets remain contiguous and are never guessed from arbitrary
  caller data. A real witness must cover every newly protected record, and
  `C` cannot advance merely because `H` or local end advanced.
- Journal replay of complete and incomplete boundary events, malformed or
  regressing boundaries, old-reader refusal, truncated guarded entries and
  every write/sync/rename failure and process-exit boundary. Check that a
  rejected conflict has no data or additional epoch change.
- Full file/index/fetch limits, no budget increases, one-entry legacy file,
  grouped write ordering, allocation failure, and poisoned-handle reads and
  writes. Retain all previous divergence and restart evidence.
- Quorum/ISR changes, leader failover, partitions, stale acknowledgements,
  replay and negative histories need an actual integrated implementation and
  independent peer verification. No purely local fake proves those claims.
