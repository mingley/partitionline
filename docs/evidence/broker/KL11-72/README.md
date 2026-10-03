KL11-72 keeps three kinds of independent evidence separate:

- `oracle/` executes the actual pinned Apache 4.1.2, 4.2.1 and 4.3.1 generated
  request/response serializers, parsers, headers, error constants and library
  error-response methods. The 79 fixture bodies are identical across releases.
  It rechecks the immutable KL11-66 public SCRAM vectors before generating
  Authenticate tokens and transient Alter/bootstrap SaltedPassword values.
- `peers/AdminPolicyOracle.java` executes the actual pinned Apache metadata
  jar's `ScramImage.describe` and `ScramControlManager.alterCredentials` methods,
  using actual client/server-common jars. This object-level execution checks
  controller/image policy; it is not an Apache network broker or authorization
  run. Three releases agree on the 14 request cases (42 executions).
- `peers/server/`, `SaslSocketPeer.java`, `sasl-native-peer.c` and `run-live.py`
  prepare an evidence-only composition of the actual Rust transport, verified
  TLS acceptor, SASL profile, durable credential store and metadata router.
  The final immutable source is `db002076bb5a19ed4a84cf8a60bce949c1d9cd84`.
  Positive live/session/admin/restart gates passed. Two isolated native Describe
  error-cleanup processes remain failed SDK completions, recorded separately.

The oracle-only source is pinned at
`8cf2516e9c9727f8a7fa45da5d066e301aa35d02`. Two fresh immutable runs each
compiled all three SDKs with `-Xlint:all -Werror`, generated and replayed 237
complete header/body/frame cases, rejected 237 one-byte truncations, and
observed 237 valid-prefix/trailing-byte acceptances. Both runs reverified
96 critical Git objects and the original KL11-66 crypto fixture. Results live
in `oracle/final-8cf2516e-{01,02}/results.json`.

The actual Apache image returns one result per unique Describe username;
every duplicated username gets error 92. Its controller rejects any second
Alter change for the same username, including a different SCRAM algorithm,
with error 92. The independent audit found both differences in an early Rust
draft and the implementation worker corrected them before freeze. Direct
trusted `Store::mutate` bootstrap can still atomically import two algorithms;
that is a separate interface from the Kafka Alter request.

These distinctions must remain explicit in final listener evidence:

- Apache generated parsers leave trailing bytes unread. The Rust listener
  consumes whole input with one explicit API 51 v0 dialect: one redundant
  empty terminal tag emitted by the pinned native writer/finalizer. Other
  tails remain rejected. The initial strict candidate's actual native Alter
  failure and its numeric-only framing/digest trace remain retained.
- Actual Apache controller methods accept the empty-salt and one-byte
  SaltedPassword fixture cases. The bounded Rust listener requires at least
  16 salt bytes and exactly the selected hash output length, and rejects them
  with error 93. Its configured resource caps are local policy.
- The TLS profile permits PLAIN and both SCRAM algorithms. The explicit
  plaintext profile permits SCRAM only. TLS-only PLAIN is a local default
  listener policy, not a claim that Apache universally forbids plaintext PLAIN.
- Authenticate v1/v2 lifetime is zero. Reauthentication and full KIP-368 are
  outside this foundation; no positive lifetime or reauthentication is claimed.
- The metadata composition advertises only its actual eight supported API
  ranges. DescribeCluster API 60 is absent. Actual native Metadata 13 and
  Describe 50 already worked; the failed Alter was a trailing-tag divergence,
  with native envelope error -195, rather than a discovery requirement.

The live peers use real entropy for client/server nonce exchanges. Fixed
salts, nonces, passwords, proofs and SaltedPassword values in fixture files are
deliberately public synthetic vectors. Runtime peers never print auth tokens,
proofs, passwords or verifier keys. Java failures retain exception classes
and stack locations without SASL exception messages, which can embed tokens;
native log text is discarded with a recorded suppressed-message count.

The separate harness workspace adds no published client or broker runtime
dependencies. Native librdkafka 2.15.0/OpenSSL is an external test peer. Its
exact source tree, selected headers/crypto/admin code, actual shared-library
hash and real runtime version are checked independently. The Rust harness
imports actual Apache-derived transient SaltedPassword and persists only the
canonical salt/iteration/StoredKey/ServerKey representation. The independent
Python journal audit validates CRC32C and offsets, derives every historical
verifier from the public fixture passwords, checks exact typed payload
consumption, and rejects raw/hex/base64 password or SaltedPassword persistence.
It does not log live proof bytes; canonical payload structure has no proof field.

All development attempts remain retained. The first native strict compile
failed because the pinned vendor header declares a qualified integer return;
the corrected command treats that external header as a system header while
keeping strict own-source warnings. A later Java draft used an indexed lookup
on an Apache keyed collection and failed strict compilation; its correction
uses the real keyed `find` API. Neither failed draft is final qualification.
The harness's first locked build rejected its draft lock file; an offline
Rust 1.85 resolution now retains the broker's existing libc version. A peer
self-review corrected shared native callback counters to C11 atomics before
live execution. Independent listener review also found an authenticated
control-frame limit bypass; the worker's four new socket regressions failed
against the earlier source and passed after enforcing the control cap before
header parsing or work. Post-proof buffers retain the separate transport cap.
Credential snapshot generations are compared within a running process; durable
cross-restart generation monotonicity is not claimed.
The native 106-byte public Alter packet has SHA256
`10f53b45dc952f8b9a821b8b312761ee3990a7c2f705a92fcf740a3cb20acbcd`.
All three actual Apache serializers and ScramFormatter independently reproduce
those exact bytes as their canonical 105-byte frame plus one empty terminal
tag. The development observer hashes only that public synthetic API 51 frame,
forwards bytes unchanged, and is absent from the final live driver. The final
native create/rotate/delete/restart flow now completes against the corrected
immutable source, with the packet sent unchanged.

The final live run records 10 clean peer processes and 2,872 checked assertions:
710 session assertions for each of three Java releases; Java admin/restart/default
denial phases check 213/84/63 assertions; native sessions/admin/restart/default
denial check 170/103/65/44. All 187 accepted connections join across three owned
listener lifecycles with no worker failures. The independent journal audit checks
all nine durable entries, historical verifier derivation, deletion and restart,
and absence of password or SaltedPassword in raw/hex/base64 forms. Denied requests
under an empty admin allowlist preserve the final journal hash.

The pinned native SDK has a separate error-path ownership bug: Describe's parser
assigns static or stack error text to an owned result field, and event destruction
frees it. Two actual Describe error 31 processes parse the correct error and emit
the pre-cleanup marker, then abort with SIGABRT. Both cells remain **failed SDK
completions**. The official Apache 4.3.1 generated parser independently consumes
both actual server error responses fully and confirms empty results and a null
message. Native unauthorized Alter error 31 completes cleanly. There is no native
library patch, event-cleanup bypass, packet rewriting or server-response workaround.
The final verdict is explicitly mixed, rather than an all-peer completion pass.

The original `zstd_decision` worker produced the independent external SDK oracles,
peers and listener review. After that worker stopped, the coordinator delegated
the final orchestration follow-up to `consumer_lookups`, the production implementer.
`peers/source-freeze-05.json` and `development-handoff-01/` preserve this attribution
and the earlier drafts. Expected crypto/schema values still come from pinned
external libraries; this final orchestration is not an independent reviewer sign-off.

All 20,137 Git objects match the live snapshot before and after. The full Rust
matrix remains separately bound to `2db175c7`; nine SASL/source/test/manifest inputs
are identical, while six independent replication paths changed before the final
live commit. The entire broker tree is not claimed identical. Final command/log
hashes, source binding, exact counts, failures and requirement coverage are in
`evidence.json`, `peers/final-db002076-live/validation.json` and `SHA256SUMS`.
No broker production-readiness or comparative-performance claim is made.
