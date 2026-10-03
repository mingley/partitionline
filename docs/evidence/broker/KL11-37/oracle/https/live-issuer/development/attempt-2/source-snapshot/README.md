This additive issuer module preserves the exact original independent synthetic
HTTPS issuer from main `6e498e3054180449520767ac040270bb1ba0e61b`. Its preserved
source is `original-issuer.py`; `original-provenance.json` pins its Git object,
SHA256, byte length and modes. Importing `live-issuer.py` verifies that original
SHA256 before loading it. The repository's original issuer has not been edited.

The module exports the original `prepare(directory)` and a derived
`Server(directory, port=0)`. The live driver imports those APIs and owns server
startup and shutdown; this derivative has no additional standalone daemon.
The HTTPS CA, localhost leaf certificate, two RSA signing keys and random
credentials are generated in a separate private mode0700 directory. Original
TLS verification and timeout/handler/body/response bounds are retained.

Authenticated `/control` accepts only generation0/1, TTL1..300, a bounded
predeclared outage-route set, an exact previously issued revocation hash, and
the `token_policy` enum. Control JSON duplicate keys, malformed/deep input,
unknown fields, bad types, unavailable issued hashes and empty controls are
rejected. All fields are validated before any mutation. Successful controls,
issued tokens and endpoint events have independent limits256/256/4096.

Each issuance is signed by actual OpenSSL RS256. Policies are fixed:

| Policy | Exact change from valid |
| --- | --- |
| valid | issuer, audience partitionline, subject issuer-probe, protected typ at+jwt |
| wrong_issuer | issuer suffix /untrusted |
| wrong_audience | audience untrusted-partitionline |
| expired | iat now-120, exp now-60, positive OAuth expires_in60 |
| future_nbf | nbf now+TTL+60 |
| wrong_typ | protected typ access+jwt |
| missing_typ | protected typ absent |
| missing_subject | subject absent |

`state.issued[sha256(compact_token)]` stores exact signed claims for existing
driver compatibility. `state.issued_metadata[hash]` adds `policy`, `generation`,
issued/expiry times and optional public subject. Public metadata contains only
predeclared policies, counters and issued hashes. Arbitrary claims cannot be
submitted. Unknown HTTP paths are recorded as `unknown`, without request text.

Introspection is bound to the exact issued-token hash and returns the exact
claims only when that issuance is unrevoked and unexpired. It intentionally
does not perform a second issuer/audience/subject/typ/nbf validator. A signed
negative policy may therefore be active under this synthetic authority while
the broker rejects its JWT. SDK provider validation may reject an expired or
missing-subject JWT before SASL; only an actual observed SASL/broker result may
be labeled broker authentication rejection.

`check-live-issuer.py` executes real trusted HTTPS controls/acquisition/
introspection and independently verifies every policy signature using the
corresponding OpenSSL public key. It includes modified signatures, wrong keys,
untrusted TLS, failed Basic authentication, malformed/duplicate controls,
atomic invalid combined controls, exact issued-hash revocation, outage and
positive recovery. Its separate exhaustion objects inject explicit component
preconditions to check limits, without claiming256 actual issuances or control
executions. Reports exclude credentials, private keys and compact tokens.

The WORK development component receipts establish no SDK, broker, SASL or
live-provider lifecycle result. No Cargo or broker/SDK process is launched by
the check. The public output directory and private scratch must be disjoint.
