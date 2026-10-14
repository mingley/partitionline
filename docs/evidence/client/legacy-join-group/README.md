# Legacy JoinGroup v0/v1

Classic JoinGroup now speaks v0 and v1. v0 omits RebalanceTimeoutMs and
fills it from the session timeout (Java `maybeOverrideRebalanceTimeout`).
v1 matches v2 on the request and omits response ThrottleTimeMs. A present
`group.instance.id` on a v0/v1 peer is refused before any JoinGroup frame.

Pinned Apache kafka-clients 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 builders
and parsers produced 30 leftover-empty bodies and 10 static-membership
refusals. Local `tests/common` Mock public `ConsumerGroup::join` runs on
0-only, 1-only, hidden, and disjoint peers are labeled separately and are
not the Apache oracle.

v2–v9 cooperative and static behavior is unchanged. Commit and fetch
gaps stay on their own cards. This does not close the old-client campaign
or the world-class client objective. See
[qualification/README.md](qualification/README.md).
