# KL11-35: verified TLS/mTLS, handshake bounds and rotation

The optional broker `tls` feature adds an explicitly TLS-only listener. It uses
stock Rustls verification with the explicit ring provider. Required mTLS rejects
missing, untrusted, expired and wrong-purpose client credentials before request
dispatch. There is no custom insecure verifier, optional-client-cert policy,
protocol sniffing or plaintext fallback. The existing explicit plaintext
constructor and optional handler reply behavior remain available.

`Acceptor::new` accepts owned certificate-chain, private-key and client-trust DER.
It checks positive bounds before parsing, parses every supplied certificate,
constructs the required-client verifier, and verifies identity/key matching.
Server certificate trust, time and hostname are verified by clients. The
constructor does not infer the operator's server trust/name policy from client
trust roots. Server-only TLS does not authenticate the client.

| Configurable limit | Default | Supported positive maximum |
| --- | --- | --- |
| Identity/presented client chain count | 8 | 32 |
| One certificate DER | 16 KiB | 64 KiB |
| Private-key DER | 16 KiB | 64 KiB |
| Client trust anchor count | 64 | 256 |
| Combined identity/key/trust DER | 1 MiB | 4 MiB |
| Presented peer chain DER | Same total-byte cap | 4 MiB |
| Absolute handshake deadline | 10s | 24h |

Pending handshakes and established connections occupy the same transport task
cap. The handshake deadline begins at socket admission and does not reset on
partial progress. Shutdown cancellation surrounds both handshake and framed
exchange, drops their socket/future and joins the worker. Cancelling a shutdown
await can be retried; dropping the transport requests cleanup while the runtime
continues running. Explicit shutdown observes joined completion.

Incoming handshake parsing uses Rustls's own pinned 65,535-byte handshake and
deframer growth bounds. The adapter's smaller configurable peer chain/count
bounds apply after successful verification and before metadata copies or handler
dispatch, not before Rustls parses the handshake. The parser limits and default
non-producing ticket implementation are source-audited in
`rustls-source-audit.json`; hostile record and handshake headers are tested.

Rotation validates a replacement fully before atomically publishing a new
generation through Tokio watch. A socket captures its generation at admission.
New admissions use the new identity and client trust. Previously admitted
handshakes and established sessions retain their old generation; established
sessions continue after trust removal and are not retroactively reverified or
disconnected. Certificate validity is checked at handshake time, not on each
request. Operators needing immediate session revocation must close those
sessions; this rotation API does not provide selective disconnection.
Invalid DER/key/trust updates leave the previous generation intact, including
for fresh connections. Stateful resumption, TLS tickets and early data are
disabled, so new sockets cannot reuse an old authenticated session.

The compatible `Handler::handle_with_peer` default forwards to `handle`.
Overrides receive remote address, configuration generation and the authenticated
client leaf plus peer-presented chain. Additional presented certificates are not
separate authenticated identities or a guaranteed minimal verified path.
Server-only TLS has no authenticated client certificate. This metadata makes
no subject/SAN parsing, ACL principal or authorization decision.

## Exact source and final validation

All final checks use immutable committed source
`39bd19a2f824a2c25b847eecfd3b3484aa5ca338`, archived with the broker and its Clippy
configuration. Uncommitted parallel protocol files are excluded. Both result
matrices pin all 54 archived files by SHA256 and verified that the snapshot
remained unchanged. The broker package was cleaned before each toolchain lane
while keeping the dedicated external dependency target directory.

Stable rustc 1.99.0 and MSRV rustc 1.85.0 each pass these exact workflow gates:

| Gate | Each toolchain |
| --- | --- |
| Format check | Pass |
| Default, all targets | 42 tests pass; TLS tests feature-excluded |
| All features, all targets | 57 tests pass, including 15 TLS tests |
| Strict all-target, all-feature Clippy | Pass with `-D warnings` |
| Strict all-feature rustdoc | Pass with `RUSTDOCFLAGS=-D warnings` |
| Strict default doctest build | Pass; 0 doctests |
| Strict all-feature doctest build | Pass; 0 doctests |

There are 14 final workflow check invocations, 198 total test executions, 30 TLS
test executions and zero final failures. The two package-clean preparations are
recorded separately. `stable-results.json` and `1.85.0-results.json` contain
source/toolchain/OpenSSL versions, exact commands, exits, CPU affinity (0-2,4)
and sanitized logs. These logs are also attributed by the KL11-58 CI evidence.

The 15 TLS tests cover eager limit/DER/key/trust rejection; TLS 1.2 and 1.3;
verified intermediate chain and preserved metadata; anonymous server-only TLS;
four mTLS credential failures with healthy-peer isolation; wrong server name,
expiry and trust; no plaintext fallback; idle and progressing slow handshakes;
hostile TLS lengths; shared connection admission and joined shutdown; cancelled
shutdown retry and transport drop; identity/trust rotation with existing-session
continuity and fresh sessions after invalid updates; pre-rotation pending
handshake continuity; post-verification peer chain cap; independent OpenSSL peers.

Each final toolchain run actually invokes OpenSSL for six connections: valid
mTLS over TLS 1.2 and TLS 1.3, plus missing, untrusted, expired and wrong-purpose
client identities. That is 12 independent peer handshakes in the final matrices.
The peer uses `-verify_return_error -verify_hostname localhost` and checks exact
framed application bytes. Closing a stream without `close_notify` can make
OpenSSL report a nonzero EOF exit after a valid reply; application-byte success
is not inferred from its process exit. Each child has a bounded process deadline
and is killed/joined on early helper failure. Rejected peers emit no application
response and never dispatch a handler.

The 42 retained fixture files were independently generated with OpenSSL 3.5.7.
The command list and exact hashes are in `fixtures.json`; eight standalone
OpenSSL certificate verification cases agree (three valid, five invalid) in
`openssl-fixture-results.json`. Leaf certificates use fixed 2020–2040 validity;
expired leaves end in 2020. Roots use the recorded generation date. Private keys
are **public test fixtures only**, never deployment credentials. Generation
recreates equivalent profiles with fresh random keys, not identical bytes; use
the retained fixtures for exact replay.

## Dependency/default proof

The unpublished independent broker activates no TLS dependency by default.
`dependency-results.json` pins the activated Linux normal/build graph, package
checksums, licenses, declared MSRVs and features. Its default 15 package versions
are preserved against claim baseline
`36bca0a9`; the optional TLS graph has 29 packages. Principal TLS pins are:

| Package | Pin | Features | Declared MSRV |
| --- | --- | --- | --- |
| rustls | 0.23.45 | ring, std, tls12 | 1.71 |
| tokio-rustls | 0.26.6 | ring, tls12 | 1.71 |
| ring | 0.17.14 | alloc, default, dev_urandom_fallback | 1.66 |
| rustls-webpki | 0.103.15 | alloc, ring, std | 1.71 |
| rustls-pki-types | 1.15.1 | alloc, default, std | 1.60 |

The graph also includes zeroize 1.9.0 (declared MSRV 1.85) and ring's cc build
tool. Ring uses approved bundled native cryptography; this is not a claim of a
fully pure-Rust cryptographic graph. No OpenSSL, aws-lc or native Kafka package is
activated. OpenSSL is an external test peer only. Existing libc remains 0.2.189.
Core dependency, feature, target, workspace and patch sections and Cargo.lock are
unchanged. Core Cargo.toml bytes differ only through an independently committed
package include exclusion for broker conformance files; the proof records this
metadata difference rather than claiming byte identity.

To replay, set installed Rust paths and an external dedicated target directory,
then run the evidence runner for each toolchain:

```sh
python3 docs/evidence/broker/KL11-35/run-validation.py stable --source-sha 39bd19a2f824a2c25b847eecfd3b3484aa5ca338 --snapshot-dir /path/to/new/scratch-snapshot
python3 docs/evidence/broker/KL11-35/run-validation.py 1.85.0 --source-sha 39bd19a2f824a2c25b847eecfd3b3484aa5ca338 --snapshot-dir /path/to/new/scratch-snapshot
python3 docs/evidence/broker/KL11-35/verify-fixtures.py
```

The runner requires external `CARGO_TARGET_DIR`, applies jobs=1/incremental=0
and strict rustdoc flags, and inherits toolchain paths. Restrict CPU affinity
externally as needed. It rejects a reused snapshot containing changed/additional
files. The dependency proof runner takes the same source/snapshot arguments.

## Development failures and limitations

`development-results.json` records all development validation failures: one
missing test-helper `?` compile failure; two raw-rejection assertions corrected
to allow fatal TLS alerts; one redundant-question-mark Clippy failure; an
all-target lint invocation affected by a sibling's unexported protocol module;
and an overly broad full-manifest byte-equality assertion in the evidence helper.
Corrected scratch and shared developmental results are recorded separately. A
temporary libc 0.2.190 lock selection was restored before the final source
commit. None of these preliminary checks is substituted for the final matrices.

Handlers and destructors must cooperate with Tokio; synchronous blocking cannot
be preempted. There is no whole-process RSS budget, load/DoS qualification,
certificate revocation/OCSP policy, selective existing-session revocation,
dynamic expiry teardown, TLS close-notify guarantee, key zeroization audit,
cross-platform cryptographic qualification, Kafka authentication mechanism,
authorization, production security claim or broker readiness claim. Certificate
DER and caller allocations, Rustls buffers and OS socket buffers are distinct
from the bounded transport request/task counters. This card establishes only
the documented TLS listener, verification, admission, deadline and rotation
behavior for the exact tested source and dependencies.
