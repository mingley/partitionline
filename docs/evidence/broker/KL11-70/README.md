# KL11-70 directory identity and pre-vote

Directory-configured controller owners advertise Vote 0–2 and Begin/End 0–1.
The default fixed profile still advertises only version 0, so the existing
201 Apache controller goldens and v0 regressions stay on that path.

## What passed

- Apache kafka-clients 4.1.2 / 4.2.1 / 4.3.1 encoded 12 request fixtures each
  (36 cases). Rust parsed and applied every frame, including stale-directory
  denials, PreVote grants that do not persist term or vote, leftover-empty
  tags, truncated UUIDs, and End successor-directory fencing.
- Rust `encode_vote_request` Vote v2 matches the Apache 4.3.1 encoder
  byte-for-byte for the binding vote fixture.
- Mixed-version fallback: a v0-only peer negotiates Vote 0; a 0–2 peer
  negotiates Vote 2 on the directory profile. The v0 profile still cannot
  select v1/v2.
- Dynamic typed pre-vote does not append the WAL or rewrite the election
  journal.
- Existing `raft_protocol` v0 suite (11 tests, 201 goldens) stayed green.

## Limits

This is not a production KRaft qualification. Native Fetch/FetchSnapshot,
autonomous controller networking, and dynamic-membership advertisement remain
separate. The world-class client objective stays open.
