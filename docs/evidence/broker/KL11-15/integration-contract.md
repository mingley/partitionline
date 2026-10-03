This is the proposed integration contract for the separately claimed snapshot
stage. The image/store foundation does not complete KL11-15. The runtime owner
must implement and qualify the checked Install WAL and Node/actor hooks below.

`snapshot::Identity` exactly carries the replication configuration's bounded
cluster/topic strings, nonnegative partition and sorted distinct fixed voters.
It excludes the replica-local ID. Its bytes never become paths. The owner creates
all image paths from a nonzero caller-provided 16-byte generation; allocation is
explicit caller policy, with duplicate/conflicting generations rejected.

`Store::create(generation, base, &[Entry])` publishes an inactive complete image.
`base.index=N` is the inclusive last included one-based core index and equals the
number of canonical prefix records, including barriers. Apache snapshot offset N
is exclusive and last-contained offset is N-1; normalized term is wire epoch+1.
An empty custom prefix `(0,0)` has no claimed Apache epoch mapping. The image
contains every committed opaque prefix record rather than an invented metadata
schema. No compaction or discarded-history claim is made.

`Store::begin_receive(Descriptor)`, `receive_chunk(generation, exact_offset,
bytes)`, and `finish_receive(generation)` validate exact lengths, positive bounds,
canonical indices/terms/barriers, fixed identity, checksums and an explicit
completion seal. `Published::descriptor()` describes an inactive image;
`Store::load(generation)` independently validates it before exposing its bounded
decoded prefix. The Store owns one incoming stage and one outgoing reader;
`start_read/next_chunk/cancel_read` transfer only bounded sequential chunks.
`abort_receive` removes an unreceipted partial image. Reopening removes known
partials and counts all complete generations against disk quota, including inert
images without a WAL receipt. The old accepted image is never deleted here.

The authoritative WAL extension should use a new PLREPL01 operation, preserving
Init/Append/Truncate/Commit semantics and exact legacy `Node::open` behavior. A
separate explicit snapshot-enabled open must own Store and check the whole group
identity. An Install receipt must bind generation, full encoded byte length,
transfer checksum, base index/term, record/payload counts, previous local tail and
committed end, the retained suffix end, receiving current term and causal
authority. Remote installs additionally bind configured leader/follower,
positive outstanding sequence and advertised leader committed end >=N. A local
checkpoint binds its confirmed committed prefix rather than asserting a majority.
No receipt imports a source's current election term or vote.

Before mutation, the owner checks live prefix+suffix allocation bounds, stale or
future boundaries, configured leader/term admission and byte-exact overlap with
every existing committed record. It may discard a conflicting uncommitted suffix;
it may retain a suffix only after verifying its prefix correspondence. An install
must not reduce committed_end or replace committed bytes. Publication order is
image sync -> rename -> image-directory sync -> Install WAL sync -> existing
election-summary reconciliation -> visible success. A WAL receipt references only
an already verified complete image. Replay checks the receipt against that image
and the preceding causal WAL state, reconstructs the selected prefix plus suffix,
then replays later operations. Unreceipted published images remain inert.

The simplest bounded profile keeps canonical full-prefix records in memory after
install, with a diagnostic selected base N. Existing term_at/fetch indexing can
therefore preserve inclusive positions. Image plus later WAL suffix must yield
exactly the same opaque content, boundary and committed state on restart. This
does not compact the operation WAL or lift its configured lifetime budget.

The owner must fence any ambiguous image publication, WAL sync or reconciliation
failure until verified recovery. Queued cancellation skips work; cancellation
during fsync can lose a successful receipt to the caller. Actor admission covers
queued, canceled-but-retained and completed-unconsumed commands/results, plus one
in-flight operation. Charge old+replacement decoded state, borrowed-entry tables
or fallible payload copies, bounded transfer chunks, one validation buffer,
operation-WAL recovery scratch and Store's at most four file descriptors. Term or
leader change during a staged transfer invalidates its authority before Install.
Shutdown stops admission, cancels partial/read sessions and joins the owner.

Integration ownership proposal: rpc_reuse exclusively edits replication.rs and
its runtime test/fault hooks after root releases the frozen73 source; c_peer owns
snapshot.rs, tests/raft_snapshot.rs, snapshot fixtures and KL11-15 evidence. Root
owns module exports, manifests, shared inventory/gate hash refresh and publishing.
Any additional controller/election hook requires coordination. Full15 acceptance
requires real Node catch-up, receipt/replay/restart, retained old-image fault
histories and independent committed-content checks on stable and Rust1.85.
