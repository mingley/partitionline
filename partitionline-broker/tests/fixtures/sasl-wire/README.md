These 79 public test vectors come from the actual Apache Kafka 4.1.2, 4.2.1
and 4.3.1 generated serializers, request/response header classes, parsers and
`getErrorResponse` methods. All three releases produced identical bytes.
The generator and upstream schemas/classes are retained under
`docs/evidence/broker/KL11-72/oracle/`; final source-pinned execution receipts
will distinguish development results from frozen validation.

`apache-wire.tsv` contains request/response kind, case name, API/version,
header version, signed correlation ID, expected constructed error code,
body hex, header plus body hex, and the complete four-byte-length frame hex.
Request error code -1 means no error expectation is encoded by the request.
The frame prefix is the signed big-endian length of the following payload.
The covered versions are Handshake 17 v0–1, Authenticate 36 v0–2, Describe
User SCRAM Credentials 50 v0, and Alter User SCRAM Credentials 51 v0.
Authenticate v1/v2 successful fixtures advertise lifetime 0.

The canonical SCRAM messages and independently checked StoredKey/ServerKey
come from the immutable KL11-66 public crypto fixture. Alter SaltedPassword
is computed by the actual Apache ScramFormatter. Fixture credentials,
proofs, nonces and salts are deliberately public synthetic test data;
these files are not redacted runtime logs or persisted credential journals.
`apache-bootstrap.tsv` separately supplies actual Apache-derived transient
SaltedPassword imports for the live harness's public user/admin/Unicode test
identities. These inputs do not become the persisted verifier representation.

`apache-errors.tsv` records actual Apache error constants.
`apache-parser-outcomes.tsv` records each executed generated parser's
one-byte truncation rejection and valid-prefix/trailing-byte acceptance.
Apache parsers leave the trailing byte unread. The Rust listener consumes
whole input, with one explicit API 51 v0 compatibility case: one redundant
empty terminal tag from the pinned librdkafka 2.15.0 writer/finalizer. Other
tails remain rejected. The authentic native sources and numeric-only rejected
frame trace are retained under `docs/evidence/broker/KL11-72/peers/`.
Constructed error
responses and parser acceptance are not Apache broker runtime outcomes.
Successful live listener interoperability requires the separately retained
Java/native C histories against an immutable Rust source snapshot.

`native-alter-canonical.frame.hex` and
`native-alter-redundant-empty-tag.frame.hex` independently reproduce a public
synthetic native Alter request with the actual Apache serializers/crypto.
The 106-byte latter frame's SHA256 matches the genuine librdkafka TLS-write
digest; only its numeric shape and digest were logged. Its import value is
deliberately public fixture data, with no live SASL proof/password in these frames.
