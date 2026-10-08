# Authentication Refresh and Reauthentication Contract (KL06-01)

## 1. Scope and Objective

This specification defines the ownership hierarchy, state machine, concurrency rules, timing calculations, and error-handling semantics for token acquisition, expiry, proactive background refresh, connection reconnect, and broker-requested reauthentication across `partitionline`.

**Boundary and Status:**
- This document satisfies card **KL06-01** (specification only).
- **No runtime Rust source changes are made in KL06-01.**
- All proposed configuration and trait additions in this document are **PROPOSED** designs for future cards (`KL06-02+`).
- This document strictly separates the currently landed one-shot bounded OIDC retry from the missing proactive token refresh, token caching, and `session_lifetime_ms` handling.

---

## 2. Current Implementation Baseline (Code Trace & Citations)

Every citation below has been verified against the current source in this worktree:

### 2.1. OIDC Token Endpoint (`src/protocol/oidc.rs`)
- **Configuration & Redaction:** [`OidcConfig`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L22) stores `token_url`, `client_id`, `client_secret`, and optional TLS configuration [`TlsConfig`](../src/net.rs). Its [`fmt::Debug`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L32) implementation redacts `client_secret` as `"<redacted>"`.
- **Landed One-Shot Bounded Retry:** [`fetch_client_credentials_token`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L85) executes up to [`OIDC_FETCH_ATTEMPTS = 3`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L75) attempts, starting with backoff [`OIDC_RETRY_BACKOFF_START = 20ms`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L77) doubling per attempt, bounded by the caller's overall `request_timeout` deadline.
- **Classification:** [`is_transient_oidc_error`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L106) treats `Error::Timeout`, `Error::Io`, and HTTP 5xx (parsed via [`oidc_http_status`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L121)) as transient. Non-transient errors (such as HTTP 4xx client errors) fail immediately without retrying.
- **Network Roundtrip:** [`fetch_client_credentials_token_once`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L126) parses the URL via [`parse_http_url`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L207), establishes a raw TCP/TLS connection, encodes HTTP Basic Auth credentials via [`basic_auth`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L286), and invokes [`token_http_roundtrip`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L324) and [`read_http_response`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L335).
- **Secret-Redacted Errors:** Non-200 HTTP responses return `Error::protocol(format!("oidc token endpoint HTTP {status}"))` ([`src/protocol/oidc.rs:167`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L167)) without copying the IdP response body, preventing token or secret echo in logs.
- **Token Extraction:** [`access_token_from_json`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L377) parses only the `"access_token"` string. **It does not parse `"expires_in"`, `"token_type"`, or any lifetime metadata.**

### 2.2. SASL Wire Protocol & Handshake (`src/protocol/sasl.rs`)
- **Version Negotiation:** [`apply_api_keys`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L48) extracts `SaslHandshake` (v0–v1) and `SaslAuthenticate` (v0–v2) versions from broker `ApiVersions`. [`spoken_sasl_versions`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L58) enforces these ranges.
- **Encoders & Decoders:**
  - `SaslHandshake`: [`encode_sasl_handshake_request`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L80), [`decode_sasl_handshake_response`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L94).
  - `SaslAuthenticate`: [`encode_sasl_authenticate_request`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L166), [`decode_sasl_authenticate_request`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L180), [`encode_sasl_authenticate_response`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L194), and [`decode_sasl_authenticate_response`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L219).
- **Session Lifetime Decoding:** [`decode_sasl_authenticate_response`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L219) decodes `(error_code, error_message, auth_bytes, session_lifetime_ms)` (returning `session_lifetime_ms` at line 227 on v1+).
- **Dropped Lifetime In OAUTHBEARER:** [`authenticate_oauthbearer_token`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L434) sends `SaslHandshake` with mechanism `"OAUTHBEARER"` and `SaslAuthenticate` with `client_initial(token)`. At line 466:
  ```rust
  let (code, msg, bytes, _) = decode_sasl_authenticate_response(&mut body.clone(), auth_version)?;
  ```
  **The returned `session_lifetime_ms` is ignored (`_`).**
- **Authentication Dispatcher:** [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L493) checks that at most one mechanism is configured. When `sasl_oidc` is present, it directly calls `super::oidc::fetch_client_credentials_token(oidc, timeout).await?` (line 527) and passes the resulting token to `authenticate_oauthbearer_token`. **Every call fetches a fresh token over HTTP.**

### 2.3. Connection Transport (`src/net.rs`)
- **Structure:** [`BrokerConn`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L629) holds the active TCP or TLS stream, correlation counters, and version caches.
- **Connection Factory:** [`BrokerConn::connect_tls`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L673) and [`BrokerConn::connect_tls_any`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L656).
- **Correlation Partitioning:** Regular request correlation uses [`next_correlation`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L720). SASL request correlation uses [`next_sasl_correlation`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L729) (dedicated negative range to prevent desynchronization).
- **Lifetime & Idleness:** [`BrokerConn::idle_expired`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L868) detects idle timeout; [`BrokerConn::is_closed`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L734) and [`BrokerConn::close`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L739) manage terminal disconnection.
- **Deadlines:** [`roundtrip_sasl_deadline`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L993) and [`roundtrip_sasl`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L1034) bind write and read under a single absolute deadline guard.

### 2.4. Subsystem Authentication Entrypoints Trace

Each client subsystem authenticates via [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L493) on every newly opened connection:

1. **Producer (`src/producer.rs`):**
   - *Bootstrap connection:* [`Producer::new`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L976) connects via [`BrokerConn::connect_tls_any`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L1015), negotiates versions, applies keys, and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L1029).
   - *Per-broker & coordinator connections:* [`open_conn`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L1955) calls `BrokerConn::connect_tls` (line 1956), negotiates versions (line 1959), applies keys (line 1961), and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L1962).
   - *Call sites:* `open_conn` is invoked by [`discover_typed_coord`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L1974) (bootstrap hop at line 1986, coordinator at line 2018), and by [`run_partition_broker_worker`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/producer.rs#L2378) on initial connect (line 2399) and upon reconnection/failure recovery (line 2681).

2. **Consumer (`src/consumer.rs`):**
   - *Bootstrap connection:* [`Consumer::new`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1289) connects via [`BrokerConn::connect_tls_any`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1297), negotiates versions, applies keys, and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1310).
   - *Bootstrap reconnect:* [`Consumer::reconnect_bootstrap`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1640) opens a connection and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1653).
   - *Per-node connections:* [`Consumer::open_node_conn`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1840) connects via `BrokerConn::connect_tls`, negotiates versions, applies keys, and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1852).
   - *Call sites:* `open_node_conn` is invoked during metadata refreshes ([`src/consumer.rs:1679`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1679), [`src/consumer.rs:2185`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L2185)) and partition leader connections in `connect_leader` ([`src/consumer.rs:1818`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1818)).

3. **Admin Client (`src/admin.rs`):**
   - *Bootstrap connection:* [`Admin::new`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L3004) connects via [`BrokerConn::connect_tls_any`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L3007), negotiates versions, and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L3019).
   - *Per-node connections:* [`Admin::open_node_conn`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L6684) connects via `BrokerConn::connect_tls`, negotiates versions, applies keys, and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L6697).
   - *Call sites:* `open_node_conn` is invoked during bootstrap reconnect ([`src/admin.rs:6615`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L6615)) and leader dispatch in `leader_conn` ([`src/admin.rs:6662`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/admin.rs#L6662)).

4. **Group Coordinator (`src/group.rs`):**
   - *Coordinator connection:* [`open_coord_with_find_version`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/group.rs#L2878) connects via `BrokerConn::connect_tls` (line 2882), negotiates versions, applies keys, and calls [`sasl::authenticate`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/group.rs#L2923).
   - *Call sites:* Invoked by [`open_coord`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/group.rs#L2874), [`discover_coord_inner`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/group.rs#L2828) (lines 2832, 2864), and [`coord_roundtrip`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/group.rs#L2942) upon idle expiration (line 2951) and broken socket retry (line 2964).

5. **Share Group Coordinator & Worker (`src/share.rs`):**
   - *Group membership:* [`ShareGroup::join_list`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/share.rs#L603) creates an internal [`Consumer`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1289) (line 608, authenticating bootstrap at line 1310) and calls [`discover_coord`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/group.rs#L2828) (line 619, authenticating coordinator at `src/group.rs:2923`).
   - *Heartbeat task:* [`ShareGroup::spawn_heartbeat`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/share.rs#L1610) re-discovers and connects the coordinator upon idle/failure via [`discover_coord`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/share.rs#L1669), authenticating via `src/group.rs:2923`.
   - *Record fetch and acknowledge:* [`ShareGroup::poll`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/share.rs#L1152) routes calls through `self.consumer.roundtrip_node(...)`, which delegates node connections to [`Consumer::open_node_conn`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/consumer.rs#L1840) (authenticating via `src/consumer.rs:1852`).

---

## 3. Separation of Concerns: Landed vs Missing Behaviors

| Dimension | Landed Behavior (Current Worktree) | Missing Behavior (Target Specification) |
|---|---|---|
| **OIDC HTTP Fetch** | Synchronous, 3-attempt bounded retry with exponential backoff on transient errors ([`src/protocol/oidc.rs:80`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L80)). | Shared token provider caching valid tokens across connections. |
| **Token Caching** | **None.** Every broker connection establishment triggers a separate OIDC HTTP POST ([`src/protocol/sasl.rs:527`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L527)). | Centralized token cache shared by all client connections. |
| **Token Expiry** | Not parsed. [`access_token_from_json`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L377) extracts only `"access_token"`. | Parse `"expires_in"`, track absolute expiration, compute refresh window. |
| **Proactive Refresh** | **None.** Token is never refreshed mid-connection or while idle. | Background asynchronous refresh before token expiry. |
| **Broker Session Lifetime** | Discarded (`_`) in [`authenticate_oauthbearer_token`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L466). | Track session lifetime from `SaslAuthenticate` v1+; schedule re-auth. |
| **In-Flight Coalescing** | N concurrent connections make N parallel HTTP requests (thundering herd). | Single-flight deduplication: concurrent requests coalesce onto one fetch. |
| **Mid-Connection Auth** | Drop connection and reconnect from scratch. | Proactive re-authentication or graceful session renewal prior to expiry. |

---

## 4. Ownership Hierarchy and State Machine

### 4.1. Ownership Hierarchy
1. **Client Instance (`Producer` / `Consumer` / `Admin` / `ShareGroup`):**
   - Owns exactly one shared `TokenProvider` instance (wrapped in `Arc`).
   - All internal broker connections (`BrokerConn`) borrow or clone a reference to this provider.
2. **Shared `TokenProvider` (Credential Owner):**
   - Owns the active token, issued time, calculated expiry, refresh task handle, and in-flight deduplication synchronization.
   - Responsible for proactive refresh scheduling, jitter calculation, clock skew guards, and backoff during IdP outages.
3. **Connection (`BrokerConn`):**
   - Owns its transport socket, correlation state, and session boundary (`session_expiry: Option<Instant>`).
   - Does NOT own the token lifecycle; queries `TokenProvider` when authenticating or re-authenticating.

### 4.2. Token Lifecycle States
- **`Uninitialized`:** No token has been acquired yet.
- **`Acquiring`:** First token acquisition is in-flight. Connection attempts wait on this task.
- **`Active (Valid)`:** Token is valid and fresh ($T_{\text{now}} < T_{\text{refresh}}$).
- **`Refreshing`:** Token is still valid, but proactive refresh is executing in the background ($T_{\text{refresh}} \le T_{\text{now}} < T_{\text{expiry}}$).
- **`Degraded (Valid Old Token)`:** Background refresh failed with a transient error, but the existing token is still valid ($T_{\text{now}} < T_{\text{expiry}} - \Delta_{\text{skew}}$).
- **`Expired`:** Token validity has lapsed ($T_{\text{now}} \ge T_{\text{expiry}} - \Delta_{\text{skew}}$). No connection may use this token.
- **`FailedClosed`:** Terminal failure (e.g., HTTP 401/403 or invalid credentials). Subsequent connection attempts fail immediately until reconfiguration.

### 4.3. State Transition Matrix

| Current State | Trigger / Event | Next State | Token Delivered to Callers | Action / Side Effect |
|---|---|---|---|---|
| `Uninitialized` | Connection requests token | `Acquiring` | Blocked on acquisition | Spawn single-flight OIDC HTTP fetch. |
| `Acquiring` | HTTP 200 + valid token & `expires_in` | `Active` | Delivers new token | Store token, record $T_{\text{expiry}}$, arm refresh timer at $T_{\text{refresh}}$. |
| `Acquiring` | Transient failure (5xx/Timeout) after retries | `Uninitialized` | None (`Err(Timeout/Io/Protocol)`) | Wake waiters with error. |
| `Acquiring` | Non-transient failure (HTTP 4xx) | `FailedClosed` | None (`Err(Protocol)`) | Wake waiters with error. Terminal state. |
| `Active` | Refresh timer fires ($T_{\text{refresh}}$ reached) | `Refreshing` | Delivers cached valid token | Spawn background refresh task; do NOT block caller connections. |
| `Refreshing` | HTTP 200 + new token & `expires_in` | `Active` | Delivers new token | Atomically swap token, cancel retry backoff, re-arm refresh timer. |
| `Refreshing` | Transient failure (5xx/Timeout) | `Degraded` | Delivers cached valid token | Schedule retry with exponential backoff + jitter before $T_{\text{expiry}}$. |
| `Refreshing` | Non-transient failure (HTTP 4xx) | `Expired` | None | Clear cached token. Invalidate active token. |
| `Degraded` | Retry succeeds (HTTP 200) | `Active` | Delivers new token | Atomically swap token, return to normal cadence. |
| `Degraded` | Time reaches $T_{\text{expiry}} - \Delta_{\text{skew}}$ | `Expired` | None | Discard old token; reject new connections. |
| `Expired` | Connection requests token | `Acquiring` | Blocked on acquisition | Attempt immediate synchronous fetch with standard retry budget. |

---

## 5. Valid-Old-Token Behavior During Refresh Failure

When proactive refresh fails, the client must safely maximize availability without violating security invariants:

1. **Validity Window:** An old token remains usable if and only if:
   $$T_{\text{now}} + \Delta_{\text{skew}} < T_{\text{expiry}}$$
2. **New Connections During Refresh Outage:**
   - As long as the old token is within its validity window, **new connection attempts continue to use the valid old token**.
   - Connections must not be blocked or failed prematurely while the existing token is cryptographically and temporally valid.
3. **Existing Connections:**
   - Established TCP/TLS connections remain operational until their negotiated `session_lifetime_ms` expires or the broker closes them.
4. **Hard Expiration Ceiling:**
   - Once $T_{\text{now}} + \Delta_{\text{skew}} \ge T_{\text{expiry}}$, the old token is discarded immediately.
   - Any connection attempt after this threshold must block on a fresh acquisition or fail closed with `Error::Timeout` / `Error::Protocol`.
   - **Under no circumstances may an expired token be presented to a broker.**
5. **Cancellation & Abort Safety:**
   - If a background refresh task is dropped or cancelled, the active token in the cache remains valid and intact. Cancellation never wipes a valid token.

---

## 6. Timing, Clock Skew, Jitter, and Deadlines

### 6.1. Expiry and Refresh Calculation
- **Token Lifetime ($T_{\text{lifetime}}$):** Derived directly from the IdP JSON field `"expires_in"` (in seconds).
- **Clock Skew Allowance ($\Delta_{\text{skew}}$):** Fixed at **60 seconds** (or $0.10 \times T_{\text{lifetime}}$ if lifetime $< 120\text{s}$). Protects against desynchronization between client, IdP, and broker clocks.
- **Refresh Buffer ($\Delta_{\text{buffer}}$):** Minimum headroom before expiration, set to $\min(60\text{s}, 0.20 \times T_{\text{lifetime}})$.
- **Proactive Refresh Point ($T_{\text{refresh}}$):**
  $$T_{\text{refresh}} = T_{\text{issued}} + \max\left(0.80 \times T_{\text{lifetime}},\, T_{\text{lifetime}} - \Delta_{\text{skew}} - \Delta_{\text{buffer}}\right)$$

### 6.2. Jitter Contract
To prevent a fleet-wide thundering herd against the IdP when thousands of clients authenticate at cluster start:
- Apply uniform random jitter $\Delta_{\text{jitter}}$ subtracted from $T_{\text{refresh}}$:
  $$\Delta_{\text{jitter}} \sim \text{Uniform}(0,\, \min(30\text{s},\, 0.10 \times T_{\text{lifetime}}))$$
  $$T_{\text{scheduled}} = T_{\text{refresh}} - \Delta_{\text{jitter}}$$

### 6.3. Outage Backoff Schedule
If proactive refresh fails in state `Refreshing` or `Degraded`:
- Initial retry delay: 1 second.
- Exponential doubling up to a maximum delay of 30 seconds:
  $$\text{delay}_n = \min\left(30\text{s},\, 1\text{s} \times 2^{n-1}\right) \pm 20\% \text{ jitter}$$
- Clamped ceiling: The next retry delay is always clamped so that it fires before the absolute expiration deadline:
  $$\text{delay}_{\text{effective}} = \min\left(\text{delay}_n,\, (T_{\text{expiry}} - \Delta_{\text{skew}} - T_{\text{now}}) \times 0.5\right)$$

### 6.4. Absolute Deadlines
- Every single OIDC HTTP request is bound by an absolute deadline derived from `request_timeout` (as in [`src/protocol/oidc.rs:85`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L85) and [`src/net.rs:947`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L947)).
- Cumulative retries for an initial synchronous acquisition cannot exceed the caller's request deadline.
- Background refresh tasks have a maximum execution budget equal to `request_timeout` to ensure unresponsive IdPs do not leak tasks.

---

## 7. Concurrency and In-Flight Coalescing (Stampede Prevention)

In a producer or consumer with many partitions, up to dozens of broker connections may be created concurrently:
1. **Single-Flight Coalescing:**
   - The `TokenProvider` must use an in-flight synchronization guard (such as a shared future or `tokio::sync::watch` channel).
   - If $N$ broker connections request a token while in state `Uninitialized`, `Acquiring`, or `Expired`, exactly **one** OIDC HTTP request is initiated.
   - The remaining $N - 1$ callers await the completion of that single in-flight request.
2. **Read Path Concurrency:**
   - When a token is in state `Active`, reading the token for a new connection must be non-blocking and wait-free (e.g., using `ArcSwap` or `parking_lot::RwLock`).
   - Background refresh executes in a separate Tokio task and does not acquire write locks until the new token is ready for atomic pointer swap.

---

## 8. Broker-Requested Reauthentication & Session Lifetime (KIP-368)

### 8.1. Protocol Semantics
- Kafka 2.2.0+ (KIP-368) adds `session_lifetime_ms` (INT64) to `SaslAuthenticateResponse` v1 and v2 ([`src/protocol/sasl.rs:207`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L207), [`decode_sasl_authenticate_response`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/sasl.rs#L219)).
- `session_lifetime_ms == 0`: Broker does not enforce session expiration for this connection.
- `session_lifetime_ms > 0`: Broker will close the connection or reject requests after this duration from initial authentication.

### 8.2. Connection Session Expiry
For each connection, the effective session deadline is:
$$T_{\text{conn\_session\_end}} = T_{\text{auth\_time}} + \text{Duration::from\_millis}(\text{session\_lifetime\_ms})$$
The effective connection lifetime is bounded by the minimum of the token expiration and the broker's session lifetime:
$$T_{\text{conn\_expiry}} = \min(T_{\text{token\_expiry}},\, T_{\text{conn\_session\_end}})$$

### 8.3. Reauthentication Execution
1. **Timing:** Reauthentication must be initiated at:
   $$T_{\text{reauth}} = T_{\text{auth\_time}} + 0.85 \times \text{Duration::from\_millis}(\text{session\_lifetime\_ms})$$
2. **Strategy Preference:**
   - **In-Place Re-auth (Ideal):** On an idle connection or coordinated quiet period, send `SaslAuthenticate` request over the existing connection with a freshly refreshed token.
   - **Graceful Reconnect (Safe Fallback):** If in-flight requests prevent safe in-place re-auth or broker does not support mid-connection re-auth, allow pending requests to drain, close the connection gracefully (`BrokerConn::close`), and reconnect.
3. **Broker Disconnect / Auth Error:**
   - If a broker returns `SASL_AUTHENTICATION_FAILED` (code 58) or `ILLEGAL_SASL_STATE` (code 34) on a live connection, the connection must be marked closed immediately ([`BrokerConn::close`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/net.rs#L739)).
   - Do NOT reuse the closed socket. Trigger a background token refresh check, apply reconnect backoff with jitter, and reconnect from scratch.

---

## 9. Security, Diagnostics, and Redaction Invariants

1. **Strict Prohibition of Plaintext Fallback:**
   - Under no circumstances may an authentication failure, token expiration, IdP outage, or broker rejection cause the client to fall back to unauthenticated `PLAINTEXT` communication.
   - If SASL/OIDC is configured, communication MUST fail closed if credentials cannot be acquired or validated.
2. **Secret-Bearing Diagnostics Ban:**
   - Access tokens, client secrets, HTTP Authorization headers, and raw JSON payloads must never appear in:
     - `fmt::Display` or `fmt::Debug` of errors ([`src/protocol/oidc.rs:167`](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/src/protocol/oidc.rs#L167)).
     - Tracing events, span fields, or log lines (all config structs must use `skip(self)` or redact secrets).
     - Metric labels, counter tags, or telemetry payloads.
   - Error messages must only convey sanitized HTTP status codes (e.g. `oidc token endpoint HTTP 401`) and Kafka error codes.

---

## 10. Proposed Configuration and Provider API (KL06-02+)

*Note: This section is a proposed API contract for future implementation cards. It is not implemented in KL06-01.*

```rust
// PROPOSED FOR FUTURE IMPLEMENTATION (DO NOT IMPLEMENT IN KL06-01)

/// Token metadata returned from OIDC token endpoint or custom provider.
#[derive(Clone, Debug)]
pub struct TokenData {
    /// Bearer access token string (redacted in Debug).
    token: String,
    /// Absolute expiration timestamp.
    expires_at: std::time::Instant,
}

/// Pluggable asynchronous token provider trait.
#[async_trait::async_trait]
pub trait TokenProvider: Send + Sync {
    /// Retrieve a valid token, refreshing proactively if near expiration.
    async fn token(&self, timeout: std::time::Duration) -> Result<String>;
}

/// Extended configuration options for OIDC refresh behavior.
#[derive(Clone, Debug)]
pub struct OidcRefreshConfig {
    /// Clock skew allowance (default: 60s).
    pub clock_skew: std::time::Duration,
    /// Proactive refresh ratio (default: 0.80).
    pub refresh_ratio: f64,
    /// Minimum headroom buffer before expiry (default: 60s).
    pub min_buffer: std::time::Duration,
    /// Maximum jitter added/subtracted (default: 30s).
    pub max_jitter: std::time::Duration,
}
```

This specification provides the immutable architectural baseline for cards `KL06-02` (token metadata parsing), `KL06-03` (proactive background refresh without stampedes), and `KL06-04` (in-place reauthentication and session lifetime handling).
