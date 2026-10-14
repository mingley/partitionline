# KL01-26 qualification 2026-10-14

Pinned Maven `kafka-clients` jars built 40 Produce v9 option-profile
bodies (8 per peer, 0 unsupported) for topic `pl-options`. Two
independent generations per peer matched byte-for-byte. Rust decoded
every body leftover-empty and rejected every truncated prefix.

Named cells follow hashed `VerifiableProducer.getValue` / `getKey` /
first-message create-time and `@JsonPropertyOrder({"timestamp","name"})`:
six records, throughput 5, value-prefix 42, repeating-keys 2, create-time
1650000000000. Keys are `0,1,0,1,0,1`. Values are `42.0`..`42.5`.

A loopback broker advertised PRODUCE max v9. Public Producer replay on
the 4.1.0 and 4.3.1 peers sent those keys, values and CreateTime
timestamps. Captured Produce requests leftover-empty decoded to the
same mapping.

When the same jars are present, Apache parsers reverse-read four named
cells per peer (20 parses).

The registered `verifiable-producer-options-3-9-1` row stays
`local_consistency` at `3e1803560e081d44f8e5ae4a3e4b449f69ff0a54`. No
`cases.json` rows were added. `records.rs` and `buf.rs` were not
edited.

Do not regenerate this snapshot in place. Re-run
`tests/conformance/java/generate_verifiable_producer_options.py` into a
new directory if the generator or pins change.
