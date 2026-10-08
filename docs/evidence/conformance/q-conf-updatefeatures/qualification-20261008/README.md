# UpdateFeatures qualification

The declared API57 v0–v2 cohort passes on default and all-feature builds with
Apache Kafka SDKs and brokers 4.1.2, 4.2.1 and 4.3.1. The checks cover 48
parameter invocations from 36 source methods, 606 independently parsed Rust
bodies, 84 native public caller cases, 84 bounded SDK TCP peers, 72 source-case
bodies and 60 malformed bodies. All owned peers joined and their ports rebound.

Five additional compatibility fixes follow the earlier v0 validation-only fix:
one operation deadline with decreasing wire budgets, Java feature-name trim
rules, rejection of null nonnullable fields, success rows for empty successful
responses, and preservation of per-feature controller errors. Actual old-source
failures and every failed preparation remain under `study/`.

Current Java public Admin fails before dispatch when only v0 is available. Rust
supports the v0 AllowDowngrade representation; each actual SDK independently
parses its public request and constructs its response. `applicability.json`
records this projection and all source-method mappings. No Java public v0
success is claimed.

Controller checks compiled source `9eb1bbba82bc997d314deadfd4bd48fa76ea4a09`.
Final checks and native runs compiled `2be86c09ba171dcef41e5dd50a24d2aef7aaf3c0`.
Only fixture I/O changed between them. Product code and existing caller tests are
byte-identical; `study/core-and-caller-source-identity-01.json` records the check.
Both final builds pass selected tests, formatting and strict Clippy on stable
Rust 1.99.0. These finite checks do not establish production readiness or speed.

The [earlier partial archive](../qualification-20261007/README.md) is unchanged.
`archive-manifest.json` maps every retained file to its original and stored hashes.
Large files and executables use deterministic gzip. Kafka distributions and SDK
jars are external checksum-pinned inputs. Verify the mapping with
`python3 -B verify-evidence.py`, then run `sha256sum -c SHA256SUMS` here.
