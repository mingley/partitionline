This is an independent synthetic HTTPS authority for OIDC integration probes.
It generates an ephemeral test CA, a hostname-verified localhost certificate,
two RSA signing generations and random test client/control credentials. Private
material remains in a scratch directory with restrictive permissions. Public
receipts contain hashes, endpoint/status events and case counts; they omit live
compact bearers, private keys and credential values.

`issuer.py --scratch /workspace/work/UNIQUE --port 0` starts the authority and
prints its issuer URL, public CA DER path and credential file paths. The issuer
is `https://localhost:PORT/issuer`. Discovery, keys, token and introspection routes
are beneath that issuer. Client-credentials acquisition and RFC7662 introspection
use HTTP Basic with client ID `partitionline-probe` and the scratch client secret.
Introspection binds active status to the exact issued issuer, subject, audience
and expiry. Keys rotate as whole generations. `/control` uses separate test-only
Basic credentials and accepts `generation`, `ttl`, `outage` and `revoke_sha256`.
The latter receives a hash, never a bearer. Route outages return real HTTP503.

The authority has finite request slots, token/event counts, body sizes and socket
timeouts. It serves only synthetic test identities and RS256 live tokens. The
separate JWT fixture/official SDK lane covers ES256 and negative signed schemas.
No third-party IdP or production authority behavior follows from this harness.

`check-issuer.py --scratch /workspace/work/UNIQUE --output RESULT.json` executes
actual trusted/untrusted HTTPS acquisition, independently verifies signatures
with OpenSSL's public-key verifier, checks revocation/rotation/outages and joins
the server. Its first development execution passed15 cases and20 HTTP requests.
Final qualification must execute the exact pushed sources. Actual Rust broker,
Java/C/Rust acquisition, session expiry, authority freshness and cancellation
tests remain separate required KL11-37 work.
