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

## Credential redaction

`Debug` for [`Sasl`](../src/config.rs), [`OidcConfig`](../src/protocol/oidc.rs),
[`TlsConfig`](../src/net.rs), and the producer/consumer/admin configs that embed
them redacts passwords, OIDC `client_secret`, and mTLS private key PEMs as
`<redacted>` (CA/cert PEMs print only a byte-length placeholder).

OIDC token-endpoint and OAUTHBEARER authenticate `Error` strings omit IdP/broker
response bodies (status-only / fixed message) so `Display`/`Debug` cannot echo
`client_secret` or token material from failure payloads.

Metrics snapshots (`ProducerMetrics` / `ConsumerMetrics` / `ShareMetrics` /
`AdminMetrics`) expose counters, latency, and topic names only — not credentials.
Optional `tracing` instruments `skip(self)` (and `skip(self, rec)` on produce) so
configs holding secrets are not recorded as span fields; recorded fields are
limited to topic / protocol names. This is a KL-06 honesty slice for
log/span/error/metrics dumps — it does **not** cover credential rotation/outage
recovery, and topic names remain operator-chosen (do not put secrets in topic names).

Mock coverage: `tests/credential_redact.rs`.

## Auth recovery (current behavior)

OIDC `client_credentials` is a **one-shot** fetch on each new SASL authenticate
(`fetch_client_credentials_token`). The client does **not** parse `expires_in`,
does **not** cache/refresh tokens mid-connection, and does **not** act on broker
`session_lifetime_ms`. A dropped TCP/TLS connection re-runs full SASL (and may
re-fetch OIDC); that is reconnect re-auth, not proactive rotation.

Token-endpoint outages fail closed: non-200 → `Error::Protocol` with
`oidc token endpoint HTTP {status}` only; a hung IdP surfaces `Error::Timeout`
bounded by the caller's request timeout. There is **no** OIDC-level retry/backoff
on the HTTP fetch (KL-06 rotation/outage recovery remains open).

Unit coverage: `fetch_token_rejects_http_503_fail_closed`,
`fetch_token_hang_times_out_fail_closed` in `src/protocol/oidc.rs`.

## Reporting

Report security issues privately to the repository owner (see GitHub security
advisories when enabled). Do not open a public issue with exploit details.

## Verification posture

- Mock protocol tests exercise encode/decode extensively.
- Real-broker smoke (`scripts/ci-broker-smoke.sh`) checks live produce/fetch
  against Apache Kafka in CI when Docker is available.
- Auth smoke (`scripts/ci-auth-smoke.sh`) boots an isolated KRaft broker with
  SASL_SSL + PLAIN + SCRAM-SHA-256/512 + OAUTHBEARER (Kafka unsecured JWT validator),
  produces via `examples/sasl` and `examples/oauth` (rustls), and checks
  that TLS-without-SASL fails closed. Soft-skips without Java/openssl/Kafka
  unless `REQUIRE_AUTH=1`.
- Dependency advisories: CI `audit` job (`cargo audit`) and `deny` job
  (`cargo deny` via `deny.toml` / `scripts/ci-deny.sh`). Mock TLS uses the
  `openssl` CLI (not `rcgen`), so `RUSTSEC-2026-0009` (`time`) is not ignored.
- Supply-chain bans: `rdkafka` / `rdkafka-sys`, OpenSSL, `native-tls`,
  `zstd-sys`, and archived `rustls-pemfile` are denied so C Kafka / TLS /
  zstd cannot land quietly.
- TLS PEM: `rustls-pki-types::pem::PemObject` (no `rustls-pemfile`).
- Decode allocation guards: `get_array_len` / tagged-field counts reject
  lengths greater than remaining buffer bytes (untrusted broker DoS).
- Adversarial decode smoke: `tests/fuzz_decode_smoke.rs` (pseudo-random
  blobs must not panic).
- libFuzzer targets under `fuzz/` (`decode_fetch_response`,
  `decode_produce_response`, `decode_metadata_response`,
  `decode_record_batches`); CI `fuzz-smoke` runs a short wall-clock budget.