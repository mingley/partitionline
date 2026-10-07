# UpdateFeatures preparation

KL01-28 remains open. This directory records an API57 source inventory and an
uncompiled Java fixture helper for Apache Kafka 4.1.2, 4.2.1 and 4.3.1.
No Rust tests, SDK calls or broker calls have qualified this cohort.

The inventory identifies 36 API-specific upstream test methods. The proposed
fixture set has 101 bodies per SDK across versions 0, 1 and 2, with bounded
counts, two deterministic generations, source and class hashes, and a five-minute
overall generation budget. Public options, error handling and controller retry
execution still need implementation and checks.

Source inspection identified a possible validation-only safety issue: the
Rust v0 encoder omits the flag, and the upstream schema has no v0 validation
field. Actual SDK rejection and a Rust public caller test must precede a fix
and qualification. No Rust source was edited during the compiler-profile timing.

Both preparation source stages and their hashes are retained. Candidate source
URLs and successful downloads are listed with SHA-256 checksums; missing test
file responses remain recorded. This is preparation, not a conformance report.
