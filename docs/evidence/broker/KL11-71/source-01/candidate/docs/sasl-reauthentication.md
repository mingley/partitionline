SASL listeners keep lifetime zero unless the caller explicitly enables renewal:

```rust,ignore
let profile = profile.with_reauthentication(Duration::from_millis(30_000))?;
```

The policy accepts whole milliseconds from 1 through 86,400,000. Tiny valid
lifetimes can expire before a client completes renewal; acceptance of the policy
does not imply that every SDK can renew in that interval. The finite normal
`oidc_live_probe` binary accepts the JSON option `session_lifetime_ms` with the same
maximum. Its default zero disables the policy. Its ready event reports the
configured maximum; each successful SaslAuthenticate response is authoritative
for the actual granted interval, measured from proof completion without a grace
extension for delayed writing or reading. OAuth grants are also bounded by the current
signed-token, key and revocation lease. No outage extends those limits.

Renewal uses SaslHandshake v1 followed by SaslAuthenticate v1 or v2, on the same
owned socket. Initial legacy/raw and Authenticate v0 clients keep lifetime zero
and cannot renew. The mechanism, authenticated name and authority family must
remain identical. An OAuth identity also retains the exact issuer. Fresh
credentials, signing-key generations and tokens can prove the same identity.
An OAuth authorization identity must still be empty or equal to its signed
subject. A credential principal never acquires OAuth administrator authority,
and an OAuth subject never acquires credential administrator authority.

Each connection handles one request and writes its response before reading the
next request. This ordering drains an application reply before the renewal
Handshake. The server admits no application requests or ApiVersions exchange
during renewal. It does not create another connection, transport admission or
handler queue. The deadline captured when the complete Handshake arrives bounds
global handler admission, all proof rounds and response writing. It is the
minimum of the configured authentication timeout and previous session expiry.
Receiving another token never resets that deadline. Cumulative incoming authentication
bytes include frame prefixes and the Handshake; rounds and bytes reset once per
accepted renewal. Existing frame, credential worker, handler and connection
ceilings continue to apply.

The previous authority remains live through the final proof response write.
The renewed identity is unavailable for dispatch until that write completes
within the old deadline. OAuth renewal can temporarily hold two managed leases;
both count against the authority manager's active-lease limit. Insufficient
lease admission fails authentication. All owned authentication frames and bearer
inputs retain their zeroizing buffers. Errors contain fixed labels without
token, password or principal data.

An expired session closes its owned connection, including idle/partial reads,
handler admission, active handler futures and blocked writes. A handler may have
already admitted durable work before cancellation; its storage durability
contract continues, and the expired connection cannot receive that result.
Failed or unsupported renewal closes the socket after any permitted bounded
error response. OAuth's existing invalid-token challenge still requires its
single terminal acknowledgement. Connection shutdown joins the owned worker;
renewal does not reset delivery, read, write or shutdown timeouts.

This WORK implementation and its draft tests have not yet been compiled or run.
The Python codec controls check independent literal wire layouts only. Current
Java, native C and public Rust refresh/rotation/expiry/cancellation histories,
stable/MSRV behavior and resource gates remain necessary for KL11-71 closure.
