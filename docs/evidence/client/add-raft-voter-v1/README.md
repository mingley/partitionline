# AddRaftVoter v1 acknowledgement

Client wire and mock-broker behavior for AddRaftVoter v1
`AckWhenCommitted`. This does not close KL05-40.

## What the client does

- v1 writes a single `0` or `1` byte after `Listeners` and before tagged
  fields. v0 omits that byte. Encoding `false` at v0 fails before any
  request byte is written.
- `AddRaftVoterOptions` defaults `ack_when_committed` to true. Java's
  public options expose cluster id only, so `false` is a labeled Rust
  extension, not a Java public option.
- Default true negotiates the highest of v0 and v1. A v0-only broker,
  including the Kafka 4.1 shape, still receives v0. Success on that path
  is labeled committed because v0 has no other mode. That label is the
  schema default. It is not a measurement of a 4.1 Raft handler.
- Explicit false requires v1. A broker whose maximum is 0 returns
  `Error::Unsupported` before an AddRaftVoter frame is recorded.
- The response schema is unchanged in v1. Decode always labels the body
  committed. `Admin::add_raft_voter` rewrites a successful false call to
  local append. Broker errors stay `Error::Broker` and are not success.
  A dropped response followed by `DUPLICATE_VOTER`, and a deadline that
  expires on a stalled peer, stay failures.

## What is not proven

No Kafka distribution, Docker daemon, or `kafka-server-start.sh` was
available here. A mock that returns as soon as it decodes the request
cannot delay commit or show that false completes after a local append
while true waits for a majority. Lost leader, lost response, and deadline
behavior on a real quorum are not measured. No production membership
change was performed.
