# Legacy JoinGroup v0/v1 qualification

Classic JoinGroup now speaks v0 and v1 through the public
`ConsumerGroup::join` path. v0 omits `RebalanceTimeoutMs` and fills it
from the session timeout. v1 matches v2 on the request and omits
response `ThrottleTimeMs`. A present `group.instance.id` is refused
before any JoinGroup frame.

Pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 all
report `ApiKeys.JOIN_GROUP` 0-9. Two independent generations matched:
30 leftover-empty bodies and 10 static-membership
`UnsupportedVersionException` receipts. Rust reverse-parsed every body.

Local `tests/common` Mock public joins are labeled separately: 0-only
and 1-only join in one RPC; hidden and disjoint JOIN_GROUP advertisements
fail closed with zero JoinGroup frames. Those runs are not the Apache
oracle. A successful old-group lifecycle is not inferred from a field
roundtrip.

Latest-stable Rust 1.99 default and all-feature lib tests (1123 each),
`--test legacy_join_group` (8), existing v2-v8 cap and v5+ static tests,
strict clippy, formatting, and the protocol checker (JoinGroup v0-v9,
legacy_spoken v0/v1, zero unclassified) passed. `cases.json` rows were
not rewritten.

Commit/fetch gaps, live old-broker membership, and the world-class
client objective remain open.
