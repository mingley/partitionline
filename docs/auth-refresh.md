# Token and session lifecycle

This page is the contract for OIDC token acquisition, refresh, and Kafka SASL
session lifetime. [`OidcTokenManager`](../src/protocol/oidc.rs) implements the
token rules below. Headings **4.2**, **6.1**, and **6.3** keep the numbers the
implementation comments cite. Credential redaction is in
[security.md](security.md).

## What runs on a connection

| Path | Behavior |
|---|---|
| First SASL authentication | PLAIN, SCRAM, or OAUTHBEARER runs `SaslHandshake` and then `SaslAuthenticate`. OIDC calls `shared_client_credentials_token` and sends the access token as OAUTHBEARER. |
| Shared OIDC cache | Connections with the same token URL, client id, client secret, and TLS material share one manager. The cache holds at most 64 configurations. A configuration larger than 64 KiB is refused. A full cache evicts the least recently used idle entry, or returns `Error::QueueFull` when every entry is still in use. |
| Owned `OidcTokenManager` | An application that constructs the manager gets background refresh. The shared connect-time cache does not: an idle token does not start a timer. |
| SASL session lifetime | A successful `SaslAuthenticate` stores `session_lifetime_ms` on the connection. A positive value expires at authentication time plus that duration. Reauthentication is due at 85% of the lifetime. Zero or negative clears the session timer. |
| Sending after expiry | `send_deadline` refuses with a protocol error once the SASL session has expired. A pipelined connection closes and fails queued requests with the same error. |
| Mid-connection reauthentication | `reauthenticate` sends `SaslAuthenticate` again and does not repeat `SaslHandshake`. It covers PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and OAUTHBEARER. SaslAuthenticate v0 is refused. The producer, consumer, admin, and group loops do not call it on a timer. A caller invokes the connection's reauthenticate methods, or reconnects. |

OIDC reauthentication uses the one-shot fetch, not the shared cache. The
one-shot fetch tries three times inside the caller's timeout. HTTP 5xx, I/O,
and timeout wait 20 ms and then double that wait. HTTP 4xx fails on the first
response. The error text is the HTTP status. The identity-provider body is not
copied into the error.

A response without `expires_in` is kept for 365 days and does not schedule
background refresh.

## Who owns the token

An owned manager is the credential owner: the current token, its expiry, the
in-flight fetch, and the refresh task. Dropping the last manager clone cancels
acquisition and refresh, including a fetch that has already started. Cancelling
one caller leaves the other owners intact.

A connection stores the SASL session deadline. It does not store the OIDC
token. Connect-time OIDC asks the shared cache. A caller that wants background
refresh keeps an `OidcTokenManager` (or any [`TokenProvider`](../src/protocol/oidc.rs))
and passes that token into reauthentication.

The shared cache is demand-driven. It still coalesces concurrent misses onto
one HTTP request, and it still returns a cached token that has not reached
`expiry - skew`. It does not wake up to refresh while nobody is asking.

## 4.2 Token lifecycle states

| State | Meaning |
|---|---|
| `Uninitialized` | No token has been acquired. |
| `Acquiring` | A fetch is in flight. Other callers wait on that fetch. |
| `Active` | The token is inside its lifetime and before the refresh point. |
| `Refreshing` | Background refresh is running. Callers still receive the current token while it is valid. |
| `Degraded` | Refresh failed with a transient error and the current token is still valid. |
| `Expired` | `now >= expiry - skew`. Callers must not present this token. |
| `FailedClosed` | A non-transient failure, such as HTTP 4xx. Later calls fail with the stored protocol error until the manager is replaced. |

Transitions the manager actually performs:

| From | Event | To |
|---|---|---|
| `Uninitialized` or `Expired` | A caller asks for a token | `Acquiring`, one HTTP fetch |
| `Acquiring` | HTTP 200 | `Active`. A background refresh is armed only when `expires_in` was present and this manager has automatic refresh. |
| `Acquiring` | Transient failure and no cached token | `Uninitialized` |
| `Acquiring` | HTTP 4xx or another non-transient failure | `FailedClosed` |
| `Active` | `now` reaches the refresh point and automatic refresh is on | `Refreshing` |
| `Refreshing` | HTTP 200 with `expires_in` | `Active`, and the next refresh is armed |
| `Refreshing` | HTTP 200 without `expires_in` | `Active`, and no further refresh is armed |
| `Refreshing` | Transient failure while the old token is still valid | `Degraded`, then a retry |
| `Refreshing` | Transient failure after `expiry - skew` | `Expired` |
| `Refreshing` | Non-transient failure | `FailedClosed`, and the cached token is dropped |
| `Degraded` | A later attempt succeeds | `Active` |
| Any state except `FailedClosed` | The last owner is dropped | Background work is cancelled. A still-valid token stays `Active`; otherwise the state is `Uninitialized`. |

Cancellation of a refresh does not wipe a token that is still valid.

## 6.1 Expiry and refresh calculation

`expires_in` is the token lifetime in seconds, measured from the moment the
response is accepted.

Skew for a lifetime shorter than 120 seconds is 10% of that lifetime.
Otherwise skew is the configured clock skew, at most the lifetime. The
default clock skew is 60 seconds.

The buffer is the smaller of the configured minimum buffer and 20% of the
lifetime. The default minimum buffer is 60 seconds.

The refresh point, before jitter, is:

```text
refresh = issued + max(refresh_ratio * lifetime, lifetime - skew - buffer)
```

`refresh_ratio` defaults to 0.80.

Jitter is a uniform draw from zero up to the smaller of `max_jitter` and 10%
of the lifetime. The default `max_jitter` is 30 seconds. The scheduled instant
is `refresh - jitter`, and it is never earlier than `issued`.

Owned managers arm that instant when `expires_in` is present. The shared
connect-time cache does not.

## 6.3 Outage backoff schedule

When refresh fails and the old token is still valid, the next delay is:

```text
base = min(30s, 1s * 2^(attempt - 1))
delay = base * (0.80 + 0.40 * jitter_factor)
delay = min(delay, 0.5 * (expiry - skew - now))
delay = max(delay, 10ms)
```

`attempt` starts at 1, so the first retry waits about one second.
`jitter_factor` is in the range 0 to 1. The refresh loop passes 0.50, which
makes the multiplier 1 and leaves the delay at `base` before the expiry clamp.
The function returns no delay once `now` has reached `expiry - skew`.

Each refresh HTTP call uses `OidcRefreshConfig::request_timeout` (default 10
seconds). The one-shot connect fetch uses the caller's timeout instead, and
its own 20 ms doubling budget, capped by that same deadline.

## SASL session lifetime

KIP-368 adds `session_lifetime_ms` on SaslAuthenticate v1 and v2.

| Value | Effect |
|---|---|
| `0` or negative | The broker is not expiring this connection. The client clears the session timer. |
| Positive | Session end is authentication time plus that many milliseconds. Reauthentication is due at 85% of the way there. |

`reauthenticate` is the in-place attempt: `SaslAuthenticate` on the open
connection, with no new handshake. A pipelined connection quiesces in-flight
application requests, runs that exchange on reserved SASL correlation ids, and
then resumes. Requests that arrive during the pause stay queued.

If reauthentication fails, or the session has already expired, the connection
is closed. The client does not fall back to a socket without SASL. The next
successful authentication records a new lifetime.

Authentication records the lifetime. Later `roundtrip` calls refuse the write
once the session has expired. Producer, consumer, admin, and group loops do
not schedule the 85% reauthentication themselves.

## Types

| Type | Role |
|---|---|
| `OidcTokenManager` | Cache, single-flight fetch, and optional background refresh. `Debug` prints the state and redacts the token. |
| `OidcRefreshConfig` | Skew, refresh ratio, minimum buffer, maximum jitter, and refresh request timeout. |
| `TokenData` | Access token plus `expires_at`. `Debug` prints the token as `<redacted>`. |
| `TokenProvider` | `token` and `token_data` for a caller-supplied provider. `OidcTokenManager` implements it. |
| `TokenLifecycleState` | The seven states in section 4.2. |

## Secrets

Access tokens, client secrets, `Authorization` headers, and identity-provider
bodies stay out of `Display`, `Debug`, tracing fields, and metrics. OIDC
failures report the HTTP status. SASL failures keep the Kafka error code and
replace the broker's error string with a fixed label. The checks live in
[security.md](security.md).
