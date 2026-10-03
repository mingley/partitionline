# Dynamic membership design (development, not qualification)

Claimed base: `d21c5bdfaff0fb37d248793c27ce5a5b1a06a3b4`. Existing fixed
formats, snapshots and controller-v0 advertisement remain a separate regression
profile. This document describes pending implementation; no durable dynamic
runtime result is claimed here.

The dynamic owner keeps an immutable genesis group (cluster/topic/partition and
canonical initial voter descriptors), separate from the active and committed
configuration. Replica keys include a nonzero directory UUID. A bounded
configuration contains 1–64 voters, at most four listener endpoints each, 249-byte
names/hosts and an explicit kraft.version1 range. Configuration records count
against the existing live record/payload and operation WAL budgets. A changed
voter record must add or remove exactly one ID, retain every unaffected descriptor
and increment the configuration epoch; arbitrary jumps or replacing an existing
ID's directory reject.

The leader must have committed a current-term barrier and its prior configuration
before beginning another change. Addition first discovers the proposed owner's
actual declared capabilities through a source-generated directory/term/sequence
probe. This is a typed local exchange, not a Kafka ApiVersions frame. Endpoint
and finalized-feature requirements are checked before append. Catch-up follows
Apache's positive prior catch-up timestamp and last accepted fetch within one
hour, including its previous-leader-end recurrence; exact equality to today's
LEO is not substituted for the native predicate. The surrounding owner separately
rejects stale, regressing, forged or beyond-sent-end durable receipts.

A synchronized voter record activates the new set before commit. The new set's
strict majority, with no removed local self contribution, commits it. Only
committed completion yields a durable change receipt; timeout does not revert an
uncommitted change or allow a second one. A removed leader loses authority after
commit even if its caller's request has already timed out. All three actual
Apache bare-component probes leave isResignRequested=false after an expired
removal request later commits; local committed-removal fencing is an explicit
stronger safety policy. Those component outcomes do not qualify the full Apache
client/network behavior. Any additional narrower policy will also be recorded.

Observers (including a new directory with an existing ID) cannot campaign or
grant votes. The owner preserves its current term and already-cast vote across
configuration changes and snapshot installation. Dynamic election persistence
stores the voted directory as well as the numeric ID, preventing a re-added ID
with a different directory from obtaining another same-term vote.

Dynamic disk formats must be explicitly distinguished from legacy formats. The
content journal binds exact genesis/local identity. Each dynamic election state
binds the complete active configuration to its exact configuration-changing
content-WAL operation ordinal. On reopen the owner checks those references
against actual Append/Truncate/Install bytes and images using bounded random
journal reads, rather than retaining an unbounded decoded configuration history.
Truncate and Install receipts bind their recomputed resulting configuration;
rollback cannot remove a committed voter record. A canonical snapshot retains
all typed configuration records and the later suffix, so replay derives the
latest configuration while preserving local term/vote. The exact layout will be
published before the independent raw checker freezes.

No native modern KRaft advertisement, directory wire fields or autonomous peer
transport qualify through this typed work. Native wire remains under KL11-70;
the bounded actual TCP owner/runtime follows under KL11-76. The original
aggregate KL11-14 stays open until all children qualify.
