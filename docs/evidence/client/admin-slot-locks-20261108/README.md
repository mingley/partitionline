# Standalone client-dependent lockfiles

Main commit
[`a0ad79d`](https://github.com/mingley/partitionline/commit/a0ad79df8a5d9872eacd1cd322b59d8d979f04e3)
added `hashbrown 0.17.1` to the client without updating its standalone
consumers. The [main CI run](https://github.com/mingley/partitionline/actions/runs/42492157950)
failed the instruction and allocation gates before measurement because
`benchmarks/codec/Cargo.lock` could not be updated under `--locked`.

This change updates the codec, runtime, sticky-partitioner, client fuzz,
broker interoperability and librdkafka interoperability lockfiles. Each adds
only `hashbrown 0.17.1` with the checksum from the root lock and the
`partitionline` dependency edge; existing resolved package entries are
unchanged. The isolated [candidate](https://github.com/mingley/partitionline/commit/c4c2e8d19af7059a73118f0a8d32e1048634ae91)
was based on main `adde42ac3e087bd891043e7ce5668d7e1badfd4a`.

The [first qualification](https://github.com/mingley/partitionline/actions/runs/42493434314)
checked the exact lock graph delta and passed stable `cargo check --locked`
for codec, runtime, sticky-partitioner, librdkafka interoperability and the
selected client record-batch fuzz target. The existing codec allocation gate
passed all three census tests. Its instruction-count gate was still running
when this note was recorded.

The broker interoperability peer resolved its lock and compiled the client,
but its own binary failed with two type errors in topic-name and nullable-key
comparisons. That is a separate source failure, not a passing locked build.
A [rebased qualification](https://github.com/mingley/partitionline/actions/runs/42493530806)
included the corrected roadmap links and a packed-crate adopter check; it was
still running when recorded. No Kafka comparison or production qualification
is implied by these checks.
