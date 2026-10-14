# KL01-27 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 80 Fetch v12, ListOffsets v5,
OffsetCommit v8 and OffsetFetch v8 bodies (16 per peer, 0 unsupported)
for topic `pl-offsets`. Two independent generations per peer matched
byte-for-byte. Rust decoded every body leftover-empty and rejected every
truncated prefix. OffsetCommit/OffsetFetch v8 bodies re-encoded to the
same Apache bytes.

Named cells follow hashed `VerifiableConsumer --reset-policy` /
`--enable-autocommit` and `@JsonPropertyOrder({"timestamp","name"})`:

- latest ListOffsets timestamp `-1` returns offset `1`
- OffsetFetch empty is committed offset `-1`; stored resume is `2`
- Fetch at 1 delivers `new`; Fetch at 2 delivers `after-commit`
- OffsetCommit stores next-offset `2` after latest and interval `1`
  after the first autocommit poll
- Events: latest `record_data` then `offsets_committed` 2; none-missing
  has no deliveries; none-resume stores 3; autocommit emits no
  `offsets_committed`

A loopback broker advertised LIST_OFFSETS max v5 and FETCH max v12.
Public Consumer replay on the 4.1.0 and 4.3.1 peers:

- `assign_partitions` + `AutoOffsetReset::Latest` delivered offset 1
  `new` and advanced position to 2
- `assign_partitions` + `AutoOffsetReset::None` failed with
  `auto.offset.reset=none`
- `assign` at 2 delivered `after-commit` and advanced position to 3
- `assign` at 0 delivered `first` and left position 1 (interval store
  target; close does not invent a later commit)

When the same jars are present, Apache parsers reverse-read five named
cells per peer (25 parses).

The registered `verifiable-consumer-reset-policies-3-9-1` and
`verifiable-consumer-autocommit-3-9-1` rows stay `local_consistency` at
`3e1803560e081d44f8e5ae4a3e4b449f69ff0a54`. No `cases.json` rows were
added. `records.rs` and `buf.rs` were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_verifiable_consumer_offsets.py` into a
new directory if the generator or pins change.
