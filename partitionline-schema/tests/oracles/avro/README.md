# Offline Apache Avro oracle

`generate.py` uses **Apache Avro Python 1.12.1** on **CPython 3.12.14**. It
requires the exact `avro-1.12.1-py2.py3-none-any.whl` with SHA-256
`970475dd6457924533966fe761be607c759d5a48390cc8fbed472f7c9a8868f2`, verifies
every installed peer Python source against that wheel, and records source
checksums in the fixture `manifest.json`. The peer is Apache-2.0 licensed. It
is an external fixture-generation dependency, never a Rust dependency.

Use an isolated environment outside the checkout; obtain the pinned wheel from
PyPI, then install that local wheel with `pip install --no-deps`. The generator
checks version, wheel and installed source pins before producing any fixture.

```sh
python3.12 -m venv /tmp/partitionline-avro-peer
/tmp/partitionline-avro-peer/bin/pip download --no-deps --dest /tmp \
  avro==1.12.1
/tmp/partitionline-avro-peer/bin/pip install --no-deps \
  /tmp/avro-1.12.1-py2.py3-none-any.whl
/tmp/partitionline-avro-peer/bin/python -B \
  partitionline-schema/tests/oracles/avro/generate.py \
  --wheel /tmp/avro-1.12.1-py2.py3-none-any.whl \
  --output partitionline-schema/tests/fixtures/avro --verify
```

The peer emits Avro binary datums with Confluent's five-byte known-schema-ID
header. Both schema root JSON and separate named-reference JSON are retained.
Writer and reader use separate Apache `Names` tables and resolve
`common.Metadata` independently. The evolved reader reorders fields, promotes
int to long, adds string and null-union defaults, and evolves the referenced
record with a boolean default. Cases include null/present string unions,
UTF-8, int32 min/max and the empty primitive-null datum. Apache's reader also
proves int-to-string and required-field-without-default failures; its parser
proves missing references fail. These are valid schemas with incompatible
resolution, rather than corrupted payload fixtures.

For the reverse direction, export actual Rust adapter output outside the
checkout, then read it using Apache's actual `DatumReader`:

```sh
PL_AVRO_ORACLE_OUTPUT=/tmp/partitionline-avro-rust \
  cargo test --locked --manifest-path partitionline-schema/Cargo.toml \
  --features avro --test avro
# Repeat the generator command with:
# --verify --rust-output /tmp/partitionline-avro-rust
```

Every Rust frame must contain the known writer ID and exact raw datum payload.
Apache decodes that actual output using both original and evolved readers,
checks values/defaults and complete byte consumption, and requires equality
with independently produced peer bytes. Five reverse datum cases are checked.
The reverse tests use a small fixture-only Rust codec with explicitly bounded
fields; it accepts only the checked-in schemas and is not a production Avro
JSON parser/serialization library. Rust malformed/budget tests qualify adapter
failures only. No live Schema Registry or Kafka interoperability is claimed.
