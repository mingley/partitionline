# Bounded SASL credential and mechanism foundation

This card implements optional PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 server
primitives. It does not connect them to Kafka request dispatch, TLS connections,
reauthentication or admin APIs. Those connection and live-peer gates remain
KL11-36. No Kafka SASL wire API is advertised by this change.

`Service` owns salted verifier generations. Only algorithm, salt, iterations,
StoredKey and ServerKey can be imported or exported as credentials. Deriving a
verifier does not store it; callers explicitly replace an immutable generation.
An admitted SCRAM session captures its generation, so rotation changes new
admissions while an existing exchange verifies against its original keys.

PLAIN verifies a transferred password against these salted keys rather than
persisting plaintext. It prefers the SHA-256 verifier when both algorithms
are stored for the identity, otherwise SHA-512. Kafka-compatible password handling uses raw UTF-8 without
SASLprep. SCRAM usernames are ASCII to match the pinned Apache SASLNAME parser;
PLAIN identities can be UTF-8. Tests independently pin UTF-8 password acceptance,
Unicode SCRAM username rejection and Unicode PLAIN identity acceptance. RFC
coverage means the literal published ASCII proof and exact AuthMessage rules;
it does not establish general SASLprep conformance.

The supported SCRAM profile uses GS2 `n`, an empty or self authzid, exact binding
to that GS2 header, unique attributes and no unsupported mandatory extension.
Unknown optional extensions are ignored semantically but remain in the exact
AuthMessage. Failed first messages are terminal and finishing consumes the
session on every outcome. Independent upstream parser differences, including
Apache's short-proof retry and acceptance of unbound extension bytes, are
retained in [the oracle evidence](oracle/README.md), without inventing upstream
rejection expectations.

## Bounds and cancellation

| Resource | Default | Configurable hard ceiling or range |
| --- | ---: | ---: |
| Message bytes | 8,192 | 1–65,536 |
| Identity bytes | 256 | 1–1,024 |
| Password bytes | 4,096 | 1–16,384 |
| Maximum salt bytes | 64 | 32–1,024; imported salt minimum 16 |
| Combined nonce bytes | 256 | 64–4,096 |
| PBKDF2 iterations | 16,384 | 4,096–1,000,000 |
| Stored identity/algorithm entries | 1,024 | 1–4,096 |
| Admitted operations/sessions | 32 | 1–256 |
| Queued or running blocking jobs | 4 | 1–64 |

Messages also have at most 16 unique attributes. Generated salts and nonce
suffixes each use 32 bytes from `getrandom::fill`; the nonce suffix is encoded
without padding. Credential access uses short Tokio `RwLock::try_read` or
`try_write` sections and reports `Busy` instead of blocking the executor.

Derivation, PLAIN PBKDF2 and final proof verification execute in `spawn_blocking`.
Both admission pools fail fast and do not create an unbounded authentication
queue. Cancellation drops the caller's admission immediately. An already-running
PBKDF2 is not preemptible: its worker permit and owned secret remain held until
actual completion. A queued cancelled job checks its flag before doing work; a
running job checks again before returning a result. Cancelled callers receive
no success. Deterministic tests hold a real blocking worker and the blocking
queue, abort the caller, verify the separate permit counts, and prove subsequent
work cannot exceed the worker bound.

Owned application password, proof, SaltedPassword, ClientKey, signature and
credential-key buffers use `zeroize::Zeroizing`. The enabled RustCrypto zeroize
features scrub HMAC/SHA state and `CtOutput` output. The verifier compares fixed
hash outputs through `digest::CtOutput::Eq`, which uses `ctutils::CtEq`; tests
cover equal, first/middle/last mismatch and invalid lengths for both hashes.
This is an API/source guarantee, not a statistical timing proof. It does not
hide unknown-user or malformed-message timing. PBKDF2's internal raw U arrays and HMAC key-pad/hash temporaries,
compiler/register copies, caller-made input copies and external persistence are
outside an all-memory scrubbing guarantee. The API makes no such guarantee.
Debug and error text redact credential, identity, password, proof and nonce data.

## Reproduction

`run-rust-qa.py` archives the exact pushed source and runs stable/Rust 1.85
default, SASL-only and all-feature test suites, formatting, strict all-target Clippy and
strict rustdoc. It records source and log hashes, exact commands, exits and
test counts. CPU affinity defaults to 0,1; debug information and incremental
compilation are disabled to keep disposable build caches bounded. Set the
appropriate `CARGO_HOME`, `RUSTUP_HOME` and `PATH` for the installed toolchains.

```sh
python3 -B docs/evidence/broker/KL11-66/run-rust-qa.py \
  --repo /absolute/partitionline --source-sha EXACT_PUSHED_SOURCE_SHA \
  --work /absolute/new/qa-output --target /absolute/disposable-target
```

The separate Java runner replays real Apache server/client/parser outcomes from
three pinned releases, literal RFC bytes and raw AuthMessage extension references.
Development logs retain the initial missing zeroize allocation feature, the
repository's prohibited standard lock, the incomplete shadow fixture archive,
the environment selection error and ENOSPC rather than replacing failed checks.
`check-dependency-graph.py` compares actual locked active Linux default graphs
from the baseline and frozen source; other-platform activation is not asserted.
Final source pins, full Rust counts, graph checks and oracle results are in the
companion JSON and the card's plan evidence.
