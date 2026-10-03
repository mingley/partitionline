This source-only preparation composes the production OIDC manager and socket
implementation with the actual broker router. The new additive example is
`partitionline-broker/examples/oidc_live_probe.rs`; its lifecycle-corrected
92d4f881 source passed ten narrow stable/MSRV check/strict/formatting commands
against the immutable2dc90b47 tree plus that single overlay. Those commands
did not build or run the normal server executable. The frozen six socket
source files are unchanged.

`broker-config.template.json` names public issuer/transport endpoints and paths
to ephemeral credential/key files. Replace the synthetic `ISSUER_PORT` marker
and select a free nonzero broker port. Start root's independently pinned
`oracle/https/issuer.py` authority, retain only public hashes/counters, and convert
its ephemeral leaf certificate/key to DER/PKCS8 for the production TLS acceptor.
Private CA, leaf/signing keys, client/control credentials and live bearer values
stay in the restrictive scratch directory or memory; they never enter evidence.

The metadata profile advertises API3v0..13,17v0..1,18v0..4,19v2..4,20v1..6,
36v0..2. The explicit read-write profile adds actual ordinary Produce0v3..13,
Fetch1v4..6 and ListOffsets2v1..3. The router has a real bounded catalog and
ordinary RF1 local-fsync partition journal; it does not support groups,
transactions, idempotence, replication or reauthentication. The seeded public
topic is `oidc-probe`, partition0. At most four live topics/partitions and four
open data stores fit the configured limits; SDK peers must delete their own
topics or share a predeclared complete public history rather than exceeding this
budget. The cumulative catalog identity/history limits remain finite.

The example prints finite JSON lines: ready API/profile metadata, typed
issuer/subject application dispatch with API/correlation/key generation, and
joined connection/task counters. It never prints SASL frames, HTTP headers,
bearers, keys or credential values. The driver must disable core dumps, retain
safe typed failure classes instead of arbitrary exception text, and write the
stop file to join listener, authority and router. The served lifetime defaults
to240 seconds and cannot exceed300; this does not assert cancellation of a
blocked operating-system DNS/file-system call. Root's source-pinned issuer and
owned process work must also join.

Avro prepares the three official Apache4.1.2/4.2.1/4.3.1 Java login-callback
clients, with the upstream implementation delegated unchanged. Native uses
unchanged librdkafka2.15 library SHA8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc.
That library was built without curl: its standard OAUTHBEARER callback will use
the peer's bounded libcurl HTTPS client-credentials fetch. This is genuine
callback-provider integration, not native built-in OIDC retrieval. The public
Rust peer uses existing partitionline OidcConfig/Sasl::oidc and explicit CA
trust, without replacing its token provider with a static bearer.

Predeclare positive fresh HTTPS token acquisition and metadata, ordinary public
record IDs and complete manual fetch histories where the read-write mode is
used, expiry/key rotation/revocation/outage/recovery and reconnect. Disable
idempotence/transactions/automatic offset commit and use manual assign/seek.
Observe token endpoint/introspection counts and issuer key generations to
distinguish a new token from SDK reuse of an unexpired cached token. Same-client
and fresh-client phases are distinct. Lifetime0 means reconnect, not renewed
mid-connection proof. Signed negative schemas remain bound to the independent
JWT/oracle fixtures and separately configured real-time SDK components.

Only offline standalone Rust lockfile resolution and the explicitly scoped
stdlib/process/Git infrastructure controls have run for this preparation. No
peer compile, SDK/socket, or broker live work has run. The complete merged
broker61 command run (51 qualification gates plus10 maintenance commands) and live peers must run against an actual
pushed source pin. Cargo's all-targets example test-harness ELF is excluded
from daemon qualification: a separate actual `cargo build --example
oidc_live_probe --no-default-features --features oidc --offline --locked`
receipt must bind the normal executable to that same source pin.

`run-live.py`, `live-config.template.json` and `lifecycle-contract.json` give
one driver/configuration contract for all peers. The operator fills exact
source, executable, SDK JAR/library and normal-build hashes before execution;
placeholders are not proof. The driver imports root's independently frozen
issuer implementation and sends actual trusted HTTPS controls. The default
template uses the independently signed-policy derivative at
`oracle/https/live-issuer/source/live-issuer.py`, SHA256
`3a1447f2c59cd9f49cc88ca9eb99f332c52285ebc7185df03e2ff4c6fd492d08`;
the original issuer stays preserved separately. Every step and
expected result is declared before the run. Acquisition failure before client
construction is separate from an authenticated public operation failure.
Cross-peer write/read steps compare every returned field against a ledger of
predeclared synthetic record identities, including ordered duplicate/null/
empty headers. Restart uses the same real durable catalog/data state.

The published `rust-peer` manifest uses the relative path to the same complete
immutable repository tree. Its standalone Cargo.lock was resolved offline in
a temporary identical manifest against immutable403, with only that client
path changed; the manifest/lock/origin/stdout/stderr hashes are retained. The
future actual pushed archive must include this lockfile and pass stable/MSRV
`--offline --locked` build and strict checks before live execution.
The C peer likewise needs strict compile and executable/runtime bindings.
Neither draft has been compiled or run. Source parse checks are explicitly
reported without promoting them to compiler or SDK/runtime qualification.

`own-live.py` is a Linux subreaper process owner. Its300s total deadline includes
monitoring and reserved graceful/group/reaping cleanup; it tracks the atomic
PID/starttime/PGID registry and independently observed descendants. Every
cleanup attempt is bounded and independent, and forced signals, observation
faults, deadline overruns, unresolved ownership, or survivors fail the result.
Linux proc/signal/exec/wait calls are not forcibly cancellable. Five real
stdlib/file/proc/pipe/Git controls and six real new-session process controls
exercise infrastructure only; their counts do not qualify OAuth or KafkaSDKs.
The initial and second reviewed attempts remain preserved separately.
