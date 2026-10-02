# Election semantics and Apache KRaft distinctions

`upstream/pins.json` binds 13 retained files for each Apache Kafka 4.1.2,
4.2.1 and 4.3.1 release to its original archive SHA-256 and immutable source
commit. The source archives were hashed before selecting regular, bounded
members; LICENSE and NOTICE are retained. These are source references, not
executed Java behavior or cross-peer interoperability results.

The independently stated oracle uses the following election properties:

* A vote belongs to one term and candidate. In 4.3.1 `QuorumState.java:904`,
  an existing vote rejects a different replica. Our persisted vote cannot be
  cleared or changed within its term, including after restart.
* Candidate log freshness compares last epoch first, then offset.
  `server-common/.../OffsetAndEpoch.java:21` implements that order;
  `KafkaRaftClient.java:914` and `:921` compare the requester with the local
  end offset. Our last-entry summary compares term first, then index.
* `QuorumState.java:731` writes election state before switching memory;
  `FileQuorumStateStore.java:181` synchronizes the temporary file before its
  atomic-move operation. Our journal synchronizes a full state before a grant,
  higher-term acknowledgement or outbound campaign; any failed append disables
  protocol mutation until reopen. Journal recovery may retain a complete
  unconfirmed append and synchronizes recovered bytes before exposing them.
* `internals/EpochElection.java:44` tracks each fixed voter separately and
  `:85` requires a strict majority; `QuorumState.java:696` prohibits becoming
  leader without that majority. The Rust histories use actual peer responses,
  including losing, delayed, replayed and partitioned requests. The oracle
  requires a preceding actual durable peer vote before counting a grant and
  independently checks one elected leader per term.

The implementation deliberately differs from modern KRaft in several ways.
`VoteRequest.json:22` defines v2 pre-votes, while
`QuorumState.java:638` requires a prospective state before candidacy. This
primitive has real elections only; isolated candidates can increase terms.
Timeouts use a configurable inclusive positive millisecond interval, instead
of Apache's `[electionTimeout, 2*electionTimeout)` at `QuorumState.java:746`.
Directory identities and dynamic voter records are absent. Membership is
permanently bound to the local journal, with one to 64 nonnegative IDs.

Our terms and indexes are checked `u64` values. Apache's wire epochs are signed
32-bit values and its `LastOffset` is a signed 64-bit log end offset, not our
one-based last-entry index. `KafkaRaftClient.java:861` rejects a real vote whose
last epoch is at least its requested epoch; this local primitive permits an
equal log term but its own campaign always increments the term. A later wire
adapter must enforce Apache's field domains, cluster/topic/partition identity,
directory identity, version-specific validation and pre-vote behavior.
There is no implemented Vote/Begin/End RPC or advertised API here.

Apache persists its versioned quorum JSON and leader-related state. This
primitive uses a different bounded append journal and recovers volatile role
and leader identity as follower/unknown while retaining term and vote. The
trusted `observe_leader` input is a local term and identity fence, not an
authentication or remote-election proof. The history oracle checks that the
actual test sender previously won, which is an input contract of that test.

An election result alone does not authorize metadata commit, reads or writes.
Apache's `QuorumState.java:703` explicitly defers a new leader's high watermark
until a majority reaches the epoch start. This implementation has no commit
index, replication, content reconciliation, truncation, snapshots or lease.
The replication owner must call `lose_quorum`; a leader timer does not itself
detect quorum loss. These later contracts must supply that fencing. Model
agreement and the source comparison do not establish full KRaft compatibility
or production qualification.
