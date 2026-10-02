# Independent Apache and RFC mechanism oracle

This oracle executes actual Apache Kafka `ScramSaslServer`, `ScramSaslClient`,
`ScramMessages`, `ScramFormatter` and `PlainSaslServer` classes from the pinned
4.1.2, 4.2.1 and 4.3.1 distribution jars. It imports no Rust implementation or
generated Rust outcomes. The SHA-256 of each jar, its embedded release/commit,
the inherited SHA-512 distribution provenance, the generator and compiled
classes are recorded before execution. Jar provenance is independently retained
by KL11-04; the runner verifies those committed pins rather than inventing new
ones. The SLF4J API dependency is also SHA-256 pinned.

Each release executes 48 SCRAM server transcript cases (both SHA-256/SHA-512),
36 parser cases, 12 PLAIN cases, one literal RFC 7677 proof/signature vector and
12 actual Apache client proof decisions, and two reference cases that use actual
Apache cryptographic primitives to bind optional extensions as RFC 5802 requires:
**111 cases per release, 333 total**. The five generated tables and detailed
outcomes are byte-identical across all
three releases. Three deliberate variants alter the published RFC client proof
and each exits 1 with an independent Apache proof assertion failure. Partial
outputs and stderr from those failed variants are preserved. They are deliberate
oracle checks, not observed Rust implementation failures.

`references/pins.json` records the independently checked original source
archive hashes and 12 retained source/license files per release. The originals
are pinned by `tests/conformance/broker/api-matrix.json`. The retained RFC 4616,
5802 and 7677 texts come from an immutable `pingidentity/ldapsdk` mirror commit;
their original publisher URLs, mirror commit, URLs and SHA-256 hashes are in
`references/rfc/pins.json`. This is an explicitly identified mirror, not a
claim of direct publisher retrieval. The runner independently extracts all four
messages from retained RFC 7677 section 3 and checks the literal fixture bytes
against them. Apache verifies that published proof and server signature.

## Fixture contract

The five tables are published under `partitionline-broker/tests/fixtures/sasl/`.
The first line is a normal tab-separated header. Hex encodes message bytes,
including NUL, malformed UTF-8 and complete GS2/proof/signature messages.

* `apache-scram.tsv`: name, mechanism, password_hex, salt_hex, iterations,
  client_first_hex, server_first_hex, client_final_hex, server_final_hex,
  stored_key_hex, server_key_hex, server_accepted.
* `rfc-scram-sha256.tsv`: the same schema, one literal published SHA-256 vector.
* `rfc-scram-extensions.tsv`: the same schema, two derived references with exact
  optional-extension bytes bound by RFC 5802 AuthMessage, using Apache's public
  HMAC/client-key/XOR functions. Here `server_accepted=true` denotes the expected
  RFC result, not an actual Apache server result. Actual Apache rejection of
  these proofs is retained separately under `rfc-raw-*` in `outcomes.tsv`.
* `apache-messages.tsv`: name, kind, message_hex, accepted. `accepted` describes
  the actual Apache parser constructor only; it does not claim authentication.
* `apache-plain.tsv`: name, message_hex, server_accepted,
  authenticated_identity_hex. The callback accepts only public synthetic users
  `user` and `escape,user=ok`, both with password `pencil`, plus `用户` with
  public synthetic password `päss💫`.

For SCRAM the password column denotes the stored synthetic user's password;
the `canonical-wrong-password` client deliberately derives its proof with a
different password. Empty later messages mean the server rejected an earlier
phase. `server_accepted` records the first final attempt, before the diagnostic
retry described below. Successful rows contain the actual server signature and
authenticated identity is checked by Java. Only salt, iteration, stored key and
server key are exported as credential derivatives; salted password and client
key are not fixture fields. Passwords in these public fixtures are test data,
not production credential persistence.

The Java fixture hook replaces the Apache formatter's `SecureRandom` with
public deterministic bytes using reflection. Apache still generates its nonce
string, parses the exchanged messages, retrieves its generated credential,
verifies the proof and emits its signature. The hook gives exact replay across
releases. It is confined to this test generator and establishes no production
entropy, randomness or replay safety property.

## Canonical and stricter policy cases

`canonical-` rows record ordinary compatible messages or authentication and
format failures. `hardening-` rows record an actual Apache outcome that the
bounded broker deliberately handles more strictly. Expected Rust rejection of
those rows is a documented broker policy, not a fabricated Apache rejection.
In particular:

* Apache accepts mandatory `m` in a client-first message and reconstructs a bare
  message without it. The bounded broker rejects unsupported mandatory fields.
* Apache parsers accept duplicate extension fields. The bounded broker rejects
  duplicate attributes, including duplicates of required fields.
* Apache accepts a client-final channel-binding value different from the GS2
  header if a valid proof is recomputed for that value. The bounded broker
  requires base64 of the exact GS2 header for its supported non-PLUS mechanism.
* Apache reconstructs a client-final message without optional extensions when
  building the proof input, so an added final extension can be accepted without
  being bound to that proof. The bounded broker ignores the optional field's
  semantics as RFC 5802 section 7 requires, while retaining the exact original
  message bytes in AuthMessage. Thus it rejects the unresigned Apache-accepted
  fixture and accepts a correctly resigned extension. The new extension
  references supply independent expected proofs and server signatures; they
  are derived vectors, distinct from the literal published RFC 7677 example.
* These Apache server classes enforce a 4,096 iteration floor but do not enforce
  their advertised 16,384 maximum in this exchange. The fixture at 16,385 is
  accepted by Apache and rejected by the bounded broker's configured work limit.

`canonical-proof-short` initially throws `IllegalArgumentException` in Apache,
leaving its final-receive state available for a subsequent valid proof. The
retained `outcomes.tsv` shows `accepted=false` for the malformed initial attempt
and `valid_retry_after_failure=true` for that later valid attempt. This does not
mean the malformed proof authenticated anyone. The bounded Rust mechanism's
terminal failed-state policy rejects all such retries.

The actual client oracle independently checks a valid exchange, corrupted server
signature, server error, empty signature, nonce mismatch and insufficient
iterations for each hash algorithm. Server-first/server-final parser fixtures
are retained Java/client references; the broker server handles client-first and
client-final messages. Full successful server outputs remain covered by complete
transcripts.

Apache's `ScramFormatter.normalize` encodes UTF-8 directly. Both hashes accept
the `canonical-utf8-password` row with ASCII username `user` and password
`päss💫`. Its ASCII-only SCRAM SASLNAME parser rejects the separate
`canonical-utf8-username` row (`用户`/`päss💫`) before issuing a challenge.
The PLAIN server accepts that valid UTF-8 username/password through its synthetic
callback. These are authentic Kafka raw-UTF-8 behavior and parser-limit checks;
this comparison does not establish general SASLprep behavior or RFC Unicode
preparation conformance. Its SCRAM parser supports only the `n`
GS2 flag. SHA-512 proof outputs come from Kafka's pinned mechanism definition;
the literal published RFC 7677 vector is SHA-256. Broader channel binding,
delegation tokens, extension negotiation, Kafka framing, TLS/channel lifecycle,
network sessions and broker/client interoperability remain outside this oracle.

## Replay and provenance

Run normal Python without `-O`, with the pinned jars available:

```sh
python3 -B docs/evidence/broker/KL11-66/oracle/run-oracle.py \
  --jars /workspace/work/broker-wire/jars \
  --output /absolute/new/output-directory \
  --classes /absolute/new/classes-directory \
  --fixtures /absolute/source/partitionline-broker/tests/fixtures/sasl \
  --source-sha EXACT_PUSHED_SOURCE_SHA
```

Output/class directories must not exist. Existing published fixtures are
byte-compared, never overwritten. Compiler commands use `jdk.compiler`,
`-Xlint:all -Werror`, and CPU affinity 0–2,4. `results.json` records every argv,
exit status, raw stdout/stderr hash, jar/source/class hash and successful/failed
run. The SLF4J missing-binder warning is retained; it selects the upstream no-op
logger and does not invalidate mechanism assertions.

`development-matrix/` is explicitly precommit candidate evidence, with actual
HEAD/dirty status and generator hashes; it is not claimed as final pushed source.
Frozen validation is retained separately after the coordinator pushes the
source and fixtures. Finite mechanism cases and independent proofs do not imply
full Kafka SASL connection support or production qualification.
