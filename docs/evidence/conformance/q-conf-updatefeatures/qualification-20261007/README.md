# UpdateFeatures: bounded wire and public checks

A v0 validation-only request used to encode and dispatch a mutating request.
The retained before-fix tests failed: the public call succeeded and changed the
mock broker's finalized metadata version to 17. The encoder now returns
Unsupported before changing the caller's bytes. Default and all-feature builds
pass both regression tests.

The qualified source is `06d3f769eebd54d8156e97d721b51b8c2e7647f8`, built with
stable Rust 1.99.0. Each of Apache SDKs 4.1.2, 4.2.1 and 4.3.1 generated 101
request/response bodies twice with identical bytes. Each build decoded those
bodies, rejected their strict prefixes, and wrote bodies which the actual SDK
independently parsed and compared by known fields. Across both builds, that is
606 reverse parses. Rust skips unknown tagged fields and preserves numeric
error/upgrade codes on the wire; this differs from Java enum lookup.

On each matching Kafka broker, both public clients ran seven cases: a same-level
update with validation enabled and disabled, an unknown feature with validation
enabled and disabled, a mixed request, an empty name, and an empty update
collection. Feature names and error codes match; finalized features remain
unchanged. That is 84 caller cases across both builds. All six brokers were
joined after shutdown; their groups emptied and all twelve ports rebound.

This is partial qualification. Controller migration, retry deadlines, complete
source-case applicability, error counts and invalid value/deletion behavior
remain open. KL01-28 stays in progress. Existing conformance cases and their
denominators retain their dispositions.

The archive manifest maps every retained file to its original path and hash.
Large files use deterministic gzip; stored and decoded hashes were checked.
Every setup failure remains, including compile failures and incorrect Java
helper assumptions. They are not passing regressions. Two exact source snapshots
retain the failing test source and the qualified compiler/controller source.
SDK JARs and Kafka distributions are external pinned inputs. Upstream downloads,
URLs/hashes, native bindings, commands, output and process receipts are retained.
