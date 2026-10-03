KL11-15 remains in progress. This source stage provides the bounded canonical
image, complete-transfer seal, inactive durable publication and chunk reader/store
foundation. It does not yet provide the authoritative Node Install WAL receipt,
snapshot-enabled replay, replication catch-up or actor lifecycle integration.

The image contains the complete bounded committed opaque prefix, including
barriers, for an exact transferable fixed cluster/topic/partition/voter identity.
Current election term/vote and local replica ID are absent. Every image path uses
only a nonzero fixed-width generation supplied by the owner. The custom format is
PLSNAP01 with independent header/body/footer/whole-transfer CRC32C checks; it is
not Apache's snapshot serialization and does not compact records or operation WAL.

Defaults bound records to 4096, individual payloads to 1 MiB, all opaque payloads
to 64 MiB, encoded images to 65 MiB, chunks to 4 MiB, complete generations to 8
and directory entries to 32. Complete inactive generations also consume the disk
quota. Another generation fails explicitly when the finite envelope is exhausted.
Store opens at most four file descriptors; old/replacement decoded state and
queued or unconsumed chunks require separate owner admission charges. One trusted
owner exclusively owns the directory; cross-process locking and hostile filesystem
race protection are not provided by this stage.

The foundation suite has nine behavioral tests and one subprocess helper on each
of stable and Rust 1.85. It covers canonical transfer/reopening, exact chunks,
complete rehashed hostile inputs, missing seal/trailing bytes, fixed identity,
generation/resource preflight, canceled reader, ambiguous rename failure, actual
process exit during transfer and real Apache opaque observation correspondence.
The owned source-only shadow excludes concurrent runtime work. Strict focused
Clippy passes on both toolchains. Development fixture-lint and MSRV lifetime-lint
failures remain retained with their corrections; these are not final immutable
whole-crate gates.

The independent Apache lane executes actual official writer/reader components
from three pinned releases. Its scope and boundary evidence are described in
`oracle/apache/README.md`. A separately pushed generator pin is needed before final
immutable qualification. Full card closure additionally requires the concrete
owner/WAL integration and independent durable history described in
`integration-contract.md`, with exact source and fault/replay checks on both
toolchains. No Kafka snapshot wire, dynamic membership or production claim is made.
