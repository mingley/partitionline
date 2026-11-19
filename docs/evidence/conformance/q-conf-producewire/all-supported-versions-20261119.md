# Produce v3–v13 wire checks

The pinned Apache SDKs generated and read a selected valid uncompressed
request and response for each shared Produce version. Kafka 4.1.0, 4.1.2,
4.2.1 and 4.3.1 covered v3–v13. The 3.9.1 peer declares v0–v11 and
covered v3–v11, the intersection with the Rust body API.

Across the five peers and the two Rust feature profiles, 106
request/response pairs matched in each direction. The new cases cover
transaction fields, log start offsets, record errors, leader and endpoint
tags, and distinct v13 topic IDs with the same partition index. Both
languages rejected a changed v4 throttle field. The four original
v4/v6/v7/v12 pairs matched their retained bytes.

The 11-pair corpus has SHA-256
`fc165b845ee9c527f1fcdc48606b0e92e2ea75d63164369d84772dc806bbdeb1`.
The 3.9.1 subset has SHA-256
`85f5639a635f92e4d80afbe141f5b5ea4c21304fc4255ed35410853e9f4a4925`.
[Corpus](../../../../tests/fixtures/produce_wire/apache-v3-through-v13.txt)

Source `a378c4c03f453087f39ceddb46c8a635c183ef2d` passed on x86_64 with stable
Rust 1.100.0 and Temurin 21.0.13. Default and all features passed in
the checkout and an extracted crate. Formatting and strict test Clippy
also passed. The first full pass of the body and package checks had eight
formatting differences.
[Final run](https://github.com/mingley/partitionline/actions/runs/44636092080) ·
[Initial full run](https://github.com/mingley/partitionline/actions/runs/44635263726) ·
[Corpus generation](https://github.com/mingley/partitionline/actions/runs/44634688693)

KL01-18 stays open for upstream case applicability, additional shapes,
compression, negotiation and live transaction behavior.
