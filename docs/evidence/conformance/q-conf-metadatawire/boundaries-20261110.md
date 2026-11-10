# Metadata boundary body comparison

This is a finite comparison of Metadata request and response bodies; it is not
a live Kafka test or complete conformance qualification. KL01-20 remains open.

The corpus covers versions 0, 1, 3, 8, 9, 10, 11 and 12. The 28 request and
response pairs cover empty and named topic lists, null topic lists where
supported, selected topic and cluster authorization flags, an ID-only v12
request and unknown tagged fields on v9–v12. The response has one broker,
one topic, one partition, nonempty replica/ISR lists and selected
version-dependent fields. Unknown tags are read and discarded by the Rust
codec; the reverse path checks the represented fields through Apache's parser.
Metadata v0 is a raw wire case and does not establish that the public Java
builder or this client negotiates v0.

[Run 42865336772](https://github.com/mingley/partitionline/actions/runs/42865336772)
at `7dfb39bc3dbe0986f76b12eb0463836842a0b896` passed the 28-pair matrix
against pinned Apache client SDK 3.9.1, 4.1.0, 4.1.2, 4.2.1 and 4.3.1 in
both default and all-feature Rust profiles. Rust decoded the Apache request
and response pairs, then Apache parsed and checked Rust's re-encodings.
Deliberately changing a v3 throttle field was rejected in each direction on
the 3.9.1 default lane. The retained Apache and Rust corpora were extracted
from the bounded log and independently hashed:
`b00f610bf0d41ac345eff8d92a496422b3c1302291475f10f134f4680590249e`
and
`558655733879dfc273f8180b096cef288a88b8ad39cc9597b6bf635f7ce355cd`.
The [Apache bodies](../../../../tests/fixtures/metadata_wire/boundaries-apache.txt)
and [Rust bodies](../../../../tests/fixtures/metadata_wire/boundaries-rust.txt)
are in the package test fixtures; uploaded artifacts were not separately
inspected.

The [initial run](https://github.com/mingley/partitionline/actions/runs/42864907776)
failed before the Rust comparison because a control incorrectly expected the
3.9.1 generated DTO to reject a raw v13 write. That SDK declares Metadata v12
as its maximum, but its raw request writer accepted v13. Declared API range
and generated writer behavior are separate facts; the permissive write does
not qualify v13 or make 3.9.1 a v13 response oracle. Formatting and
test-target Clippy passed on that initial attempt.

The [offline candidate run](https://github.com/mingley/partitionline/actions/runs/42866367751)
at `1cfd8ba6f666fbf9e36c5053e15f5a9e8bf26e1b` passed the Apache matrix,
retained-corpus comparison, both checkout offline profiles, formatting and
test-target Clippy. Clean packaging failed before creating the crate: importing
the shared Python helper created an ignored `__pycache__` file under the
package's included test paths. A Git status check did not list that ignored
file, but Cargo detected it as uncommitted. The runner now disables bytecode
writes before importing the helper; this change and the packaged-crate tests
require a separate run.

Versions 2, 4, 5, 6 and 7, other nested shapes, more error codes,
transactional behavior and live broker interactions remain open. See the
[separate v13 record](README.md) for that version.
