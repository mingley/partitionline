# KL01-29 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 32 AlterShareGroupOffsets and
DeleteShareGroupOffsets v0 bodies (8 per speaking peer) plus 8
unsupported receipts on 3.9.1. Two independent generations per peer
matched byte-for-byte. Deadline remaining 174.057s of 180s.

Named cells:

- alter-ok: group/topic `pl-share`, partition 0, startOffset 7, topic
  id sixteen `0x01`, top-level error 0
- alter-invalid: top-level `INVALID_REQUEST` 42 at bytes 4–5, empty
  responses (9-byte leftover-empty body)
- delete-ok: topic-name-only request, topic id sixteen `0x01`, error 0
- delete-unknown: top-level `GROUP_ID_NOT_FOUND` 69 at bytes 4–5,
  empty responses

Rust leftover-empty decoded all 32 speaking bodies, rejected every
truncated prefix, and re-encoded to the same Apache bytes. 3.9.1 has
no invented `.bin` files.

A loopback broker advertised FIND_COORDINATOR max v0, ALTER max v0 and
DELETE max v0. Public Admin replay on the 4.1.0 and 4.3.1 peers:

- `alter_share_group_offsets` delivered error 0, topic `pl-share`,
  topic id `[1; 16]`, partition 0
- FindCoordinator spoke v0 with `key_type=0` (GROUP, not SHARE)
- `delete_share_group_offsets` delivered error 0 on the name-only topic
- `INVALID_REQUEST` 42 and `GROUP_ID_NOT_FOUND` 69 are the public
  bodies — not `NOT_COORDINATOR` retry loops

When the same jars are present, Apache parsers reverse-read four named
cells per speaking peer (16 parses).

The six registered `current-public-api-091/092-v0-*` rows stay
`not_run` at `f0ca05da9c04942b211e2f56ddc615b9860cdea2`. No
`cases.json` rows were added. `records.rs`, `buf.rs` and `admin.rs`
were not edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_share_offset_mutations.py` into a new
directory if the generator or pins change.
