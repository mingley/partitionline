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
