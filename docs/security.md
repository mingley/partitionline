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

KL06-07 extends this to every auth lifecycle path (initial authenticate,
KIP-368 reauthentication, OIDC refresh, TLS rotation):

- SaslAuthenticate failures keep the broker code but drop the
  broker-controlled `error_message`, replacing it with a fixed
  `sasl {mechanism} authentication failed` label — a hostile broker cannot
  get echoed passwords/tokens into `Error::Broker`.
- SCRAM `e=` values are bounded to known RFC 5802/RFC 7677 tokens
  (anything else → `scram server error: unrecognized`); malformed
  attributes fail as `scram bad attr` without echoing the offending bytes.
- `OidcConfig::token_url` with `user:pass@` userinfo is rejected before any
  connect (`oidc token_url must not contain userinfo`) so the password
  cannot leak into DNS, the `Host` header, or `Debug`; `Debug` strips
  userinfo while keeping scheme/host/path visible.
- Failing SASL/OIDC/TLS flows are asserted under a capturing `tracing`
  subscriber: no span/event field may carry password, token, secret, or key
  material, and rotated `TlsConfig`s stay redacted.

Sanitized errors stay actionable: broker codes, OIDC HTTP statuses, and
known SCRAM tokens are preserved; only unbounded third-party bytes are
masked.

Metrics snapshots (`ProducerMetrics` / `ConsumerMetrics` / `ShareMetrics` /
`AdminMetrics`) expose counters, latency, and topic names only — not credentials.
Optional `tracing` instruments `skip(self)` (and `skip(self, rec)` on produce) so
configs holding secrets are not recorded as span fields; recorded fields are
limited to topic / protocol names. This is a KL-06 honesty slice for
log/span/error/metrics dumps — it does **not** cover credential rotation/outage
recovery, and topic names remain operator-chosen (do not put secrets in topic names).

Mock coverage: `tests/credential_redact.rs`.

## Auth recovery (current behavior)

OIDC `client_credentials` runs on each new SASL authenticate
(`fetch_client_credentials_token`). The client does **not** parse `expires_in`,
does **not** cache/refresh tokens mid-connection, and does **not** act on broker
`session_lifetime_ms`. A dropped TCP/TLS connection re-runs full SASL (and may
re-fetch OIDC); that is reconnect re-auth, not proactive rotation.

Token-endpoint responses: non-200 → `Error::Protocol` with
`oidc token endpoint HTTP {status}` only (no IdP body). A hung IdP surfaces
`Error::Timeout` bounded by the caller's request timeout. Transient failures
(HTTP 5xx, I/O, timeout) get **bounded** retries (3 attempts, short exponential
backoff) inside that same timeout; HTTP 4xx fails immediately. Mid-connection refresh / rotation / outage soak still open (KL-06).

For the complete token acquisition, expiry, proactive refresh, reconnect, and broker-requested reauthentication ownership contract, see [docs/auth-refresh.md](auth-refresh.md).

Unit coverage: `fetch_token_rejects_http_503_fail_closed`,
`fetch_token_hang_times_out_fail_closed`,
`fetch_token_retries_transient_503_then_succeeds`,
`fetch_token_does_not_retry_http_401` in `src/protocol/oidc.rs`.

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