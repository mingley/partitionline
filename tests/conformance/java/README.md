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
