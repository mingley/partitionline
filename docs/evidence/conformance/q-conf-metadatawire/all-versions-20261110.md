# Metadata v0–v12 body comparison

This is a finite comparison of Metadata request and response bodies; it is not
a live Kafka test or complete conformance qualification. KL01-20 remains open.

The corpus contains 43 request/response pairs across every version from v0
through v12. It covers empty and named topic lists, null topic lists where
supported, selected topic and cluster authorization flags, an ID-only v12
request and unknown tagged fields on v9–v12. The response has one broker,
one topic, one partition, nonempty replica and ISR lists and selected
version-dependent fields. Unknown tags are read and discarded by the Rust
codec; the reverse path checks represented fields through Apache's parser.
Metadata v0 is a raw wire case and does not establish that the public Java
builder or this client negotiates v0.

The [generation run](https://github.com/mingley/partitionline/actions/runs/42868554847)
at `079968cfb0b59788ea719bc51f2b0a48e36a9856` passed all 43 body pairs
against pinned Apache client SDK 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 in
both default and all-feature Rust profiles, with Rust 1.99.1 and Temurin
21.0.13. Rust decoded Apache request and response bodies, then Apache parsed
and checked Rust's re-encodings. A deliberate v3 throttle change was rejected
in each direction on the 3.9.1 default lane. Formatting and strict
test-target Clippy passed. This generation run did not run the offline or
packaged-crate checks.

The [Apache pairs](../../../../tests/fixtures/metadata_wire/all-v0-v12-apache.txt)
and [Rust pairs](../../../../tests/fixtures/metadata_wire/all-v0-v12-rust.txt)
were extracted from the bounded run log and independently hashed:
`e1b124fbbd9447551d0fe69aadc911f43900ce22056f7c0c38efaf762449b3a2`
and
`c25e37b8c1ad8e5c6d3bb7e97e94c616f5bc9ab534401cfe3220f5a5fb3b97d0`.
The retained corpus and offline packaged-crate checks require a separate
exact-source run. The SDK artifact hashes are in the
[runner](../../../../tests/conformance/java/generate_metadata_boundaries.py).

The [earlier boundary record](boundaries-20261110.md) retains the eight-version
corpus, initial incorrect assumption about the 3.9.1 raw v13 writer, and an
ignored-bytecode package failure. Metadata v13 has a
[separate record](README.md). Other error and collection shapes, version
negotiation, unsupported-peer behavior beyond the recorded control and live
broker interactions remain open.
