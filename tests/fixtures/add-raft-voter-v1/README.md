# AddRaftVoter v1 fixtures

This directory intentionally contains no Apache serializer bytes.

Kafka 4.1.2 speaks AddRaftVoter v0 only. The pinned 4.1.0 generator
`tests/conformance/java/AddRaftVoterFixtures.java` encodes version 0. No
4.2 or 4.3 Kafka distribution was available in this workspace, so v1
`AckWhenCommitted` bytes are not checked in as Apache fixtures.

Rust self-roundtrips in `tests/add_raft_voter_v1.rs` are a client wire
check. They do not qualify a controller, a delayed commit, or a public
Java `ack=false` option. Java's public `AddRaftVoterOptions` exposes
`clusterId` only.
