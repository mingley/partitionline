# Broker SASL reauthentication

A listener grants a SASL session lifetime only when the profile opts in.
The default is zero. A zero lifetime means the connection cannot renew.

```rust,ignore
let profile = profile.with_reauthentication(Duration::from_millis(30_000))?;
```

`with_reauthentication` accepts a whole number of milliseconds from 1 through
86,400,000 (24 hours). A fractional millisecond is rejected. A lifetime that
short can expire before a client finishes renewal. Accepting the value does
not mean every client can renew inside that window.

OAuth also caps the granted lifetime at the current token, signing key, and
revocation lease. The granted value is the smaller of the profile maximum and
the time left on that lease. An outage does not extend either limit.

The example `oidc_live_probe` reads `session_lifetime_ms` from its JSON config.
Zero, the default, leaves renewal off. The maximum is the same 24 hours. The
ready event reports the configured maximum. Each successful SaslAuthenticate
response carries the lifetime that was actually granted, measured from the
moment the proof completed.

## What a renewal sends

Renewal is SaslHandshake version 1, then SaslAuthenticate version 1 or 2, on
the same connection.

| Request | Result |
|---|---|
| SaslHandshake v1 while authenticated, same mechanism, session still live | Renewal starts. Byte and round counters reset, including this Handshake frame. |
| SaslHandshake v0 while authenticated | Error 34 (`ILLEGAL_SASL_STATE`). The socket closes. |
| A different mechanism name | Error 33 when the name is not offered, otherwise 58 (`SASL_AUTHENTICATION_FAILED`). |
| SaslAuthenticate v0 during renewal | Error 34. The socket closes. |
| Initial legacy or Authenticate v0 clients | Lifetime in the response is 0. Those clients cannot renew. |

The mechanism, authenticated name, and authority family must stay the same.
For OAuth, the authority includes the issuer, so a renewal has to present that
same issuer. A new password generation, signing key, or token is allowed when
it proves that same identity.

An OAuth authorization identity is empty or equal to the signed subject. A
credential principal is not an OAuth administrator, and an OAuth subject is
not a credential administrator. Those allowlists stay separate.

## Deadline

The deadline is fixed when the renewal Handshake arrives. It is the earlier of
the configured authentication timeout and the previous session expiry. A later
token does not move that deadline. Proof rounds and the final response write
share it.

The preauth byte and round counters reset once for that Handshake and then
accumulate again. Frame size, credential workers, handler time, and connection
limits still apply.

The previous identity is kept only to compare the new proof. Request dispatch
does not see a renewed identity until the final success response has been
written inside the old deadline. OAuth validation takes a new lease while the
previous lease is still held. Both permits count against the authority
manager's active-lease limit. If that admission fails, authentication fails.

Authentication frames and bearer tokens stay in zeroizing buffers. Errors use
fixed labels. They do not include the token, password, or principal.

## Expiry and failure

An expired session closes its connection. That includes an idle read, a
partial read, a handler waiting for admission, a handler that is still
running, and a blocked write.

A handler may already have admitted durable work. That work keeps its storage
contract. The transport checks the clock again after the handler returns and
before it writes the length prefix and body. A handler that returns after the
deadline is discarded. Bytes the socket has already accepted cannot be taken
back, so expiry during a partial write closes the connection and does not
install the renewed identity. This is not a hard real-time send guarantee.

A failed or unsupported renewal closes the socket after the bounded error
response it is allowed to send. An invalid OAuth token still gets one
`invalid_token` challenge with the socket open. A following request that is
not SaslAuthenticate closes it.

Connection shutdown waits for the connection's worker. Renewal does not reset
the delivery, read, write, or shutdown timeouts.

## Tests

These session tests cover the listener policy:

- `renewal_lifetime_wire_layout_matches_literal_independent_vectors`
- `renewal_policy_accepts_only_positive_whole_milliseconds_through_one_day`
- `renewal_authority_is_pending_until_final_reply_write_and_cannot_outlive_old_expiry`
- `renewal_budgets_reset_once_include_handshake_and_keep_absolute_deadline`

Socket-level cases live in `partitionline-broker/tests/sasl_sessions.rs` and
`partitionline-broker/tests/oidc_sessions.rs`. Those tests are the broker's
own renewal checks. They are not a cross-client qualification against Java or
librdkafka.
