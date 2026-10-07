# Security

## Threat model

| Trust | Assumption |
|---|---|
| Application | The process using this crate is trusted. Config (bootstrap, credentials, TLS material) comes from the operator. |
| Network | On plaintext listeners, an on-path attacker can read and modify traffic. Use TLS (`TlsConfig`) for confidentiality and integrity in transit. |
| Broker | A compromised or malicious broker can return arbitrary protocol bytes. The client must not panic, UB, or silently invent offsets from truncated/malformed frames. Broker authz (ACLs) is enforced by the cluster, not this client. |
| Dependencies | Review direct and transitive dependencies, including native code used by Ring and unsafe code in compression libraries. See `deny.toml` and the audit checks. |

This crate forbids `unsafe_code`. That removes a class of memory-safety bugs
inside the client, not broker or network trust problems. Dependencies are not
covered: the default gzip backend, zlib-rs, uses `unsafe` and SIMD internally
and decodes broker-supplied bytes. Build with `default-features = false` to use
miniz_oxide instead, which forbids `unsafe`; its gzip CRC-32 still comes from
`crc32fast`, which uses `unsafe` SIMD ([gzip-backend.md](gzip-backend.md)).

## Auth and transport

- **TLS:** `rustls` + `ring`. Custom CA PEM or Mozilla roots; optional mTLS.
- **SASL:** PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, OIDC token
  endpoint over HTTP(S) with rustls.
- **Unfinished authentication:** GSSAPI / Kerberos.

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
limited to topic and protocol names. These checks cover logs, spans, errors,
and metrics. Rotation and outage recovery have separate lifecycle tests.
Topic names are operator-chosen; keep secrets out of them.

Mock coverage: `tests/credential_redact.rs`.

## Token acquisition and lifecycle

OIDC connection opens share token acquisition for the same endpoint, credentials,
and TLS configuration. The shared cache holds at most 64 configurations; each is
limited to 64 KiB of URL, credentials, server name, and PEM input. New distinct
configurations evict the least recently used idle entry. If every slot is active
or still finishing cancellation, acquisition returns `Error::QueueFull`.

Concurrent opens share one fetch. Sequential opens reuse a valid cached token.
Idle cache entries start no refresh work. When the last active caller leaves,
any unfinished acquisition is cancelled. Tokens past their expiry/skew limit
must be acquired again; terminal authentication failures stay closed.

For application-owned proactive refresh, use `OidcTokenManager`. Manager clones
share acquisition and refresh. Dropping the last clone cancels both, including
an in-flight fetch. Cancelling one caller leaves other manager owners intact.
Broker session renewal is separate from token refresh; see
[token and session lifecycle](auth-refresh.md) for the integration contract.

Token endpoint errors report the HTTP status without response bodies. Transient
5xx, I/O, and timeout failures receive up to three attempts within the original
request timeout. HTTP 4xx fails immediately. Lifecycle tests cover expiry,
outages, cancellation, and redaction; sustained external-service qualification
remains separate.

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
