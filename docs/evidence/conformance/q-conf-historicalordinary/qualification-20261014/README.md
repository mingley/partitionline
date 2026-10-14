# KL01-23 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 40 ordinary Produce v9, Fetch v12,
ListOffsets v5 and Metadata v9 bodies (8 per peer, 0 unsupported) for the
25-record one-partition topic `pl-ordinary`. Two independent generations
per peer matched byte-for-byte. Rust decoded every body leftover-empty
and rejected every truncated prefix.

Apache-shaped verifiable JSON events follow hashed
`VerifiableProducer.java` / `VerifiableConsumer.java`
`@JsonPropertyOrder({"timestamp","name"})`: 25 `producer_send_success`
events, verbose `record_data` x25, and a manual commit at offset 25.

A loopback broker served the Apache Fetch response bytes and advertised
FETCH max v12. Public Consumer replay on the 4.1.0 and 4.3.1 peers
delivered offsets `0..24` and left `position()` at 25.

When the same jars are present, Apache parsers reverse-read four named
cells per peer (20 parses).

The three registered rows stay `local_consistency` at
`cb7e97d3b92a8555aea34d59266a2990c206395f`. No `cases.json` rows were
added. `records.rs` and `buf.rs` were not edited. KIP-848 and share-group
cells stay nonapplicable.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_historical_ordinary.py` into a new
directory if the generator or pins change.
