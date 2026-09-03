# Security

## Threat model

| Trust | Assumption |
|---|---|
| Application | The process using this crate is trusted. Config (bootstrap, credentials, TLS material) comes from the operator. |
| Network | On plaintext listeners, an on-path attacker can read and modify traffic. Use TLS (`TlsConfig`) for confidentiality and integrity in transit. |
| Broker | A compromised or malicious broker can return arbitrary protocol bytes. The client must not panic, UB, or silently invent offsets from truncated/malformed frames. Broker authz (ACLs) is enforced by the cluster, not this client. |
| Dependencies | Default features stay pure Rust (no librdkafka, OpenSSL, libzstd, Cyrus SASL). Supply-chain risk is crates.io Rust crates only. |

This crate forbids `unsafe_code`. That removes a class of memory-safety bugs
inside the client, not broker or network trust problems.

## Auth and transport

- **TLS:** `rustls` + `ring`. Custom CA PEM or Mozilla roots; optional mTLS.
- **SASL:** PLAIN, SCRAM-SHA-256/512 (pure Rust), OAUTHBEARER, OIDC token
  endpoint over HTTP(S) with rustls.
- **Not in default features:** GSSAPI / Kerberos (C), zstd (typically C).

Prefer SCRAM or OIDC over PLAIN. Prefer TLS (or SASL_SSL) on any network you
do not fully control.

## Reporting

Report security issues privately to the repository owner (see GitHub security
advisories when enabled). Do not open a public issue with exploit details.

## Verification posture

- Mock protocol tests exercise encode/decode extensively.
- Real-broker smoke (`scripts/ci-broker-smoke.sh`) checks live produce/fetch
  against Apache Kafka in CI when Docker is available.
- Dependency advisories: CI `audit` job (`cargo audit`).
- Decode allocation guards: `get_array_len` / tagged-field counts reject
  lengths greater than remaining buffer bytes (untrusted broker DoS).
- Adversarial decode smoke: `tests/fuzz_decode_smoke.rs` (pseudo-random
  blobs must not panic). Dedicated cargo-fuzz targets remain optional.