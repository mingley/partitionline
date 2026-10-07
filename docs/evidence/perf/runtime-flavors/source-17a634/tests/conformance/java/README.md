# Apache Kafka Protocol Fixture Generator (KL01-03)

This directory contains the reproducible wire-protocol fixture generator for
partitionline protocol conformance tests.

## Purpose

To ensure honest, independent verification of partitionline codecs against reference
Apache Kafka wire representations, fixtures are generated directly by pinned Apache
Kafka client message serialization implementations, completely independent of
partitionline encoders.

Rust tests consume the committed binary and JSON fixtures offline without requiring
Java or network access.

## Upstream Pins

- **Apache Kafka Version**: `3.9.1`
- **Upstream Tag**: `3.9.1` (`ce24d9b6aedca74f53c26f1ad2b2cc8ad950a03c`)
- **Maven Artifact**: `org.apache.kafka:kafka-clients:3.9.1`
- **Secondary Reference Pin**: `4.1.0` (`080a42c343d919971985102a38c82dc6da0623d5`)

See `pins.json` for exact artifact coordinates and verification hashes.

## Smoke Fixture

The smoke fixture is:
- **API**: Produce (API Key 0)
- **Version**: 9 (flexible version with compact arrays, strings, and tagged fields)
- **Request**: `smoke_produce_v9_request.bin`
- **Response**: `smoke_produce_v9_response.bin`
- **Metadata**: `smoke_produce_v9.json`

## Running the Generator

Use the repository runner script:

```bash
bash scripts/generate-protocol-fixtures.sh
```

To verify that existing committed fixtures match the generator output byte-for-byte:

```bash
bash scripts/generate-protocol-fixtures.sh --verify
```

## AddRaftVoter v0 oracle (KL05-20)

`AddRaftVoterFixtures.java` is a standalone offline generator for fresh defaults,
nullable fields, populated identity/endpoints and unknown tagged fields. It
checks that the loaded classpath jar is exactly the supplied jar, with SHA-256
`180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb`, from the
Apache `apache/kafka:4.1.0` distribution. This is a distribution pin, separate
from the Maven hash in `pins.json`. Tag object
`080a42c343d919971985102a38c82dc6da0623d5` resolves to source commit
`13f70256db3c994c590e5d262a7cc50b9e973204`.

With JDK 21, the pinned jar and SLF4J API 1.7.36 available, run:

```bash
mkdir -p /tmp/add-raft-fixtures
javac -cp "$KAFKA_CLIENT_JAR" -d /tmp/add-raft-fixtures tests/conformance/java/AddRaftVoterFixtures.java
java -cp "/tmp/add-raft-fixtures:$KAFKA_CLIENT_JAR:$SLF4J_API_JAR" AddRaftVoterFixtures "$KAFKA_CLIENT_JAR" tests/fixtures/protocol_oracles --verify
```

Omit `--verify` to generate the four fixture cells. The generator verifies
Apache self-roundtrips, emits binary hashes and performs no network request or
quorum mutation. Rust tests consume all committed bytes offline, including
truncation, null-required-field and unsupported-version checks. Unknown tags
are skipped when decoding; Rust encoders emit no unknown tags.

## RemoveRaftVoter v0 oracle (KL05-21)

`RemoveRaftVoterFixtures.java` uses the same pinned distribution jar, JDK and
source commit as the addition oracle. It covers fresh defaults, nullable fields,
populated cluster/voter/directory identities and unknown tagged fields. The
request has no TimeoutMs field. Compile and verify independently with:

```bash
javac -cp "$KAFKA_CLIENT_JAR" -d /tmp/add-raft-fixtures tests/conformance/java/RemoveRaftVoterFixtures.java
java -cp "/tmp/add-raft-fixtures:$KAFKA_CLIENT_JAR:$SLF4J_API_JAR" RemoveRaftVoterFixtures "$KAFKA_CLIENT_JAR" tests/fixtures/protocol_oracles --verify
```

The generator performs no network I/O or membership mutation. Rust checks all
four cells offline, with truncation and unsupported-version failures.

## DescribeLogDirs v1–v5 oracle (KL05-22)

`DescribeLogDirsFixtures.java` uses Apache 4.3.1's message size/write/read
implementations. The jar must be the supplied loaded classpath file and match
SHA-256 `dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e`
from `apache/kafka:4.3.1` distribution image digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
Tag object `a07059eb9b5bac1bfdbb1e74313f2fae4ca20fd9` peels to source commit
`26b251a451ce941d3d7a55e6487bcb7f16b5ad48`. This pin is separate from the
older Maven and distribution pins above. With JDK 21 (verified with 21.0.12.1):

```bash
mkdir -p /tmp/log-dir-fixtures
javac -cp "$KAFKA_CLIENT_JAR" -d /tmp/log-dir-fixtures tests/conformance/java/DescribeLogDirsFixtures.java
java -cp "/tmp/log-dir-fixtures:$KAFKA_CLIENT_JAR" DescribeLogDirsFixtures "$KAFKA_CLIENT_JAR" tests/fixtures/protocol_oracles --verify
```

Omit `--verify` to generate 24 cells and 72 binary/metadata files. Fresh Apache
request defaults contain an empty collection; explicit null remains distinct.
Cases cover empty/populated/error/throttle/Unicode data, v3 top-level errors,
v4 volume sizes, v5 true/false cordoned states and unknown tags at every nested
level. The writer deliberately sets IsCordoned true on older versions to prove
its ignorable-field omission and reader false default. Version projections are
applied after serialization, preserving original unknown tags. Rust decodes
all cells, reproduces canonical bytes exactly, skips unknown tags, and rejects
truncation/unsupported versions. This generator performs no network operation.

## ListOffsets v11 oracle (KL05-13)

`ListOffsetsV11Fixtures.java` uses the same source, distribution jar hash and
JDK pin as the DescribeLogDirs oracle above. Apache 4.3.1 `ListOffsetsRequest`
adds `EARLIEST_PENDING_UPLOAD_TIMESTAMP=-6` and a `forConsumer` flag requiring
v11. The request/response fields and defaults are identical to v10; this is a
new operation selector, not a new field. Compile and verify with:

```bash
mkdir -p /tmp/list-offsets-fixtures
javac -cp "$KAFKA_CLIENT_JAR" -d /tmp/list-offsets-fixtures tests/conformance/java/ListOffsetsV11Fixtures.java
java -cp "/tmp/list-offsets-fixtures:$KAFKA_CLIENT_JAR" ListOffsetsV11Fixtures "$KAFKA_CLIENT_JAR" tests/fixtures/protocol_oracles --verify
```

Six cells cover fresh defaults, empty arrays, both isolation levels, v10+
TimeoutMs, selectors -6 through -1, zero/i64-max timestamp boundaries, error
31/78 sentinels, and unknown tags at every level. Apache self-read and v10/v11
identical serialization verify the layout; emitting -6 syntax at v10 is only a
layout comparison and does not qualify that selector on older servers. Rust
canonical bytes match Apache exactly. These are synthetic wire responses, not
an observed tiered-storage upload boundary. No broker or network is contacted.
