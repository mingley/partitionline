# Replicated partition integration

Status: design, not implemented or qualified. Inspected source: commit
`53b5af47fd18898a68364d9b88edc181c225bfe4` on x86_64. The current ordinary
operator is an RF1 server. Its local fsync precedes acks=1 and acks=all, and
Fetch treats the local partition end as the high watermark. The separate
three-controller operator replicates catalog metadata only. The standalone
`replication::Log`, `recovery::EpochLog`, and `high_watermark::Durability`
models do not replicate the operator's Kafka batches, share an authoritative
committed offset, or accept network follower Fetch requests. They cannot be
combined by merely placing their APIs in the current handler.

## Required authority and data boundaries

* Commit topic UUID, partition, replica set, ISR, leader ID and leader epoch in
  the controller quorum before those values control a data partition. Include
  controller term and broker incarnation/registration epoch in every update;
  reject stale updates and stale replica progress. An in-memory placement
  table or data-store lock is not a cluster authority. Prove the behavior of
  quorum loss and broker lease expiry; an isolated former leader must eventually
  stop accepting writes and cannot acknowledge from stale ISR state.
* Persist the accepted partition leader epoch before a former leader/follower
  truncates, appends, acknowledges or serves requests in a new epoch. The
  epoch fence must survive an empty log and a crash between each durable step.
  Authorize the claiming broker independently: an integer epoch supplied by an
  unauthenticated or untrusted caller is not proof of leadership. Existing
  `replication::Log` derives its epoch from retained records and can forget it
  on empty or truncated logs; the separate untested fence draft addresses part
  of this but has an unresolved one-entry journal capacity problem.
* Store actual Kafka record batches in the existing partition store, retaining
  base offsets, leader epochs, CRC-protected bytes, segment boundaries and
  exclusive end offsets. Implement bounded suffix truncation and epoch-index
  recovery on those same files. Never remove a record below the committed high
  watermark during a clean election. If a required prefix or matching epoch is
  missing, fail closed and require a full recovery path; do not infer matching
  histories from equal end offsets. Unclean election, if implemented, must be
  explicitly configured and may lose data acknowledged by weaker guarantees.
* Replication Fetch is a separate bounded, authenticated broker-to-broker path.
  Validate topic ID, partition, replica membership, broker incarnation,
  current leader epoch and log start. Carry a byte limit, progress and
  deadlines; use Kafka first-batch progress rules where the negotiated version
  requires them. Reconcile divergence via epoch end offsets, with explicit
  recovery after interruption and lost replies. A follower reports an end only
  after the bytes meet the selected durability policy. Kafka acks semantics do
  not by themselves promise physical-media fsync; document the stronger local
  fsync policy if retained and compare performance at equal durability.

## Produce, visibility, and failover

* Serialize partition mutation and epoch changes under one owner. A Produce
  request validates leadership and required acks against one current placement.
  Reject `acks=all` before append with NOT_ENOUGH_REPLICAS if ISR is already
  below minISR. After append, distinguish ISR falling below minISR with
  NOT_ENOUGH_REPLICAS_AFTER_APPEND and handle deadline, fencing, lost reply and
  cancellation without rolling back a durable append. `acks=1` waits for the
  leader's selected local durability; `acks=0` returns no Kafka response and
  must still follow connection/error behavior.
* Track followers' epoch-bound durable end offsets, update ISR through a
  committed controller transition, and compute the exclusive high watermark
  only from eligible current-epoch ISR progress. Acks=all succeeds only after
  the full appended range is below that watermark and the required ISR
  condition still holds. Waiters cannot hold the storage actor while waiting
  for follower progress: bounded admission, deadlines and cancellation-safe
  ownership must leave progress and shutdown messages runnable.
* Fetch, ListOffsets and retention use an explicit committed watermark, not
  `Partition::next_offset`. Keep uncommitted leader suffixes invisible to
  ordinary consumers, apply isolation rules for transactions separately, and
  preserve Kafka's oversized-first-batch and response-version behavior. On
  recovery, reconstruct a safe watermark from durable state and current
  authority before opening reads; never advance it from an unverified follower
  report or expose a range later removed by a clean election.
* Bind producer retry/idempotence state, topic identity, partition deletion and
  metadata movement to the same ordered transitions. RF1 store offsets and
  `replication::Log` offsets currently represent different data structures;
  copying one model's IDs or fake records into the other does not satisfy this.

## Implementation order and evidence

1. Repair local replica recovery and epoch fencing first, including exact
   configured file/fetch/index limits, durable epoch adoption and compatible
   rollover. One extra epoch record is not enough: a valid one-entry journal
   must not be permanently filled by a no-op epoch change. A sidecar requires
   crash-safe publication and a downgrade protocol that prevents an older
   binary silently ignoring it. A compact journal representation needs atomic
   multi-entry publication for state larger than one entry. Resolve committed
   prefix truncation and poison-aware reads before exposing the API.
2. Add controller-owned replicated placement/registration and fencing to the
   existing quorum. Start with fixed membership and clean elections, and
   explicitly refuse unsupported protocol versions and unclean operations.
3. Implement epoch-aware append/fetch/truncate on the real partition store,
   then wire follower progress and the watermark to the broker actor. Keep
   internal protocol separate until it has bounded framing and authenticated
   peer identity. Turn on RF>1 only after crash/restart and epoch tests pass.
4. Exercise three actual broker processes with controlled disk and network
   failure, including stale leaders after process pause and clock/lease
   boundaries; capture every acknowledged record independently and verify it
   survives clean leader loss. Test ISR shrink/rejoin, minISR transitions
   before/after append, follower truncation at the committed boundary, lost
   acknowledgments, topic recreation, disk exhaustion, unknown replica IDs,
   cancellation and restarts at every publication point. Repeat against pinned
   Java and librdkafka peers for supported wire versions. Process exit tests do
   not establish power-loss durability.
5. Profile before optimization. The current local replica model compares the
   whole common prefix on every bounded fetch; one-record steps can perform
   quadratic total comparisons. A reusable cursor must include source and
   follower identities plus durable log generations and be invalidated on
   truncation, source replacement, restart, epoch changes and any operation
   that can change the compared prefix. Persistent epoch indices or verified
   immutable segment fingerprints can reduce work only after their collision,
   mutation and recovery semantics are specified and tested. No speed claim is
   established by the presence of a cursor or local microbenchmarks.

A complete production claim additionally requires TLS/SASL and authorization,
operator recovery/observability, bounded process-wide memory and sustained
fault/soak tests. The current x86-only local campaign can establish measured
workload-specific results; it cannot prove a universal fastest claim.
