# KL11-59 bounded topic catalog

Source commit `7848d98b23490782d70eefb4496f0021cdccd807` adds a synchronous,
journal-backed topic catalog. Create and delete each append one atomic operation;
confirmed state changes only after successful synchronization. Validation,
configured bounds, and fallible state/name reservations occur before append.
The receipt identifies a catalog operation offset, not a Kafka partition offset.

The catalog accepts caller-supplied 128-bit topic IDs in big-endian order.
Apache's zero and one UUIDs are rejected. No random ID allocator, UUID version,
or variant policy is implemented. Deleted IDs remain permanently tombstoned
within a configured historical identity budget, so a deleted name can be
recreated only with a fresh ID. This prevents old identities from referring to
a new topic. Names remain case sensitive; live dot/underscore collisions are
rejected without changing stored names.

Apache Kafka 4.1.2, 4.2.1, and 4.3.1 sources and official distribution jars pin
the naming and identity semantics. Original LICENSE, NOTICE, Topic, Uuid,
controller replay logic, TopicRecord/RemoveTopicRecord schemas, and their topic
and UUID tests are retained in three source archives with original and retained
archive SHA256 plus per-file hashes in [the pins](upstream-source-pins.json).
[The independent Java oracle](apache-oracle.json) executed the actual Apache
Topic/Uuid classes: all 87 name validations, six collision assertions, and 12
reserved identity assertions passed. Three variants incorrectly expecting `..`
to be valid failed Apache assertions with exit 1. Exact Java source, class/jar
hashes, input/output bytes, commands, and toolchain are retained.

Rust uses the identical 29 name outputs from all three releases as a committed
fixture. The raw Apache validator accepts `__cluster_metadata`; this catalog
explicitly reserves that name locally for a future control plane. Permanent
non-reuse of caller-supplied tombstoned IDs is also an explicit local policy.
Other syntactically valid internal-topic names are permitted by this catalog;
their coordinator behavior is not implemented here.

Configured bounds cover simultaneously live topics, historical identities,
per-topic and total live partition counts, retained operations, cumulative
operation payload/replay bytes, and journal file bytes. Journal entry/index/fetch
bounds are tightened to this format and the operation limit. Replay fetches one
whole operation at a time. State retains at most the configured number of topic
identities, each with at most 249 name bytes, excluding allocator overhead.
Logical partition counts do not allocate partition journals or replica state.
Deletes also consume journal/operation/replay budgets; no compaction or reclamation
is implemented, and exhausted history budgets require an explicit future policy.

The enclosing journal uses its custom PLJRNL01/PLENTRY1 checksummed format.
Catalog payloads are custom PLTCAT01 bytes, not Kafka metadata-log records:

| Payload bytes | Field |
| --- | --- |
| 0–7 | ASCII `PLTCAT01` |
| 8 | Opcode: 1 create, 2 tombstone |
| 9–11 | Zero reserved flags |
| 12–27 | Big-endian 128-bit topic ID |
| 28–31 | Big-endian positive int32-domain partition count; zero on delete |
| 32–33 | Big-endian u16 name byte length; zero on delete |
| 34 onward | Original ASCII topic name on create |

Create payloads are 35–283 bytes; tombstones are exactly 34 bytes. Every catalog
entry occupies one logical journal record. Complete malformed flags, lengths,
names, IDs, partition counts, duplicate/conflicting history, and unknown/repeated
tombstones fail closed. A journal checksum failure or interior byte loss is never
treated as a successful truncated recovery. The tests enumerate all 65 nonempty
incomplete prefixes of a tombstone entry and confirm restoration of the preceding
whole catalog, followed by a successful replacement tombstone and restart.

Failed writes/synchronizations are ambiguous. Mutations are poisoned and reads
expose only the previous confirmed state; that state can differ from the eventual
recovered durable state. The two private fault tests inject a complete write with
a failed synchronization contract and check publication/replay behavior. They
are deterministic state-transition tests, not physical fsync failures. Real-file
tests also check changed-file poisoning and child-process exit without running
destructors, with complete history or an incomplete final tail. Process exit
does not simulate hardware power loss.

Final validation used an immutable archive of the exact source commit, including
the independent broker, clippy configuration, committed protocol fixtures, and
catalog name fixture. All 202 files were compared with their git objects before
tests and rechecked afterward. A dedicated clean target, two Cargo jobs,
incremental compilation disabled, and CPUs 0–2,4 were used. Stable and Rust 1.85
each passed 67 default and 82 all-feature broker tests, with zero failures or
ignored cases, strict all-target/all-feature Clippy, strict rustdoc, and format.
The catalog contributes 14 integration and two private fault cases. All-feature
counts include 15 existing TLS regressions. These are broker checks; unrelated
client changes are outside this source snapshot.

[The independent Python format reader](independent-format.json) interpreted the
actual journal emitted by each toolchain, verified both CRC32C layers and the
catalog history, and observed identical 302-byte files: three identities, two
live topics, eight live partitions, and the old identity tombstoned. Five mutated
files were rejected, including checksum-correct illegal-name, reused-ID,
record-count, and unknown-tombstone histories. This confirms the custom byte
format independently; it does not prove hardware durability or Kafka log compatibility.

The initial shared-tree all-target lint attempt encountered a sibling records
test's missing crate documentation and unused mutable binding; that failed log
is retained. A focused catalog lint attempt then identified helper unwraps and
`fs::read` calls restricted by the repository's async lint policy. Fixed fixture
helpers received narrow documented test-only unwrap allowances, and explicit
File/Read was used. That focused initial raw log was overwritten before evidence
archival; the failure and correction are recorded. All final exact-source gates
passed after these corrections.

Run the independent format check from the repository root:

```sh
python3 -B docs/evidence/broker/KL11-59/check-catalog-format.py
```

Use `archived-source-files.json` and the task evidence for exact snapshot/build
commands. The caller must exclusively own the configured trusted path; no topic
name becomes a path. Blocking I/O belongs on a storage thread. Process-local
ownership is inherited from the journal, with no cross-process lock. No metadata
wire handler, partition file creation/deletion, replica assignment, metadata
quorum, compaction, distributed durability, client interop, production, or
performance qualification is claimed.
