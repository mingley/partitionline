# Live Registry lookup

The x86_64 workflow pins the Linux image
`docker.redpanda.com/redpandadata/redpanda:v26.2.2@sha256:a9dffb84f388a7a2d4ab1aaa541cd0b9a484f0f5c9b72cb54c551953352769ec`.
Each lane starts a fresh loopback listener. Curl registers a named Avro
reference and two versions of a schema. The Rust client checks their
fields through live lookups. The workflow records the image, platform
and runtime version. Cleanup removes the owned container and volumes.
[Live workflow](../../../../.github/workflows/schema-registry-live.yml)

The default and all-feature lanes passed on stable Rust 1.100.0 at
`5276ca31df4b8ed67e54e0732abd528db3702061`. The test fetches the schema by
ID, exact subject version and latest version, resolves its reference, and
checks a typed missing-subject error. After curl confirms that latest has
changed on the server, the cloned Rust client returns its cached version
until the cache clock advances.
[Live run](https://github.com/mingley/partitionline/actions/runs/44614652080)

Formatting, strict test Clippy and the all-feature companion suite
passed with the same test blob. The suite passed 153 tests and doctests.
The live test ran separately in both feature profiles. The initial
formatting run found line wraps that were corrected before this check.
[Formatting attempt](https://github.com/mingley/partitionline/actions/runs/44612236028)
[Qualification checks](https://github.com/mingley/partitionline/actions/runs/44614983002)

This covers the named Avro lookup and cache cases over plain HTTP. Live
TLS, credentials, other schema formats and a combined Registry and Kafka
round trip remain open.
