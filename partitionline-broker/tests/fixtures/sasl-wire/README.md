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
Apache parsers leave the trailing byte unread. The Rust listener's whole
input rejection is a separate, stricter framing policy. Constructed error
responses and parser acceptance are not Apache broker runtime outcomes.
Successful live listener interoperability requires the separately retained
Java/native C histories against an immutable Rust source snapshot.
