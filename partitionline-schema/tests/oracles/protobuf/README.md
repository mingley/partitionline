# Offline Confluent Protobuf oracle

`generate.py` builds **Confluent Schema Registry 8.1.0** `MessageIndexes.java`
from commit `039a339d824debade488573769ad17b0d9975643` and uses its actual
`toByteArray` / `readFrom` methods. Its Apache `ByteUtils` dependency comes from
the recorded **Apache Kafka 4.1.0 Docker distribution** jar, not the differently
hashed Maven artifact. Source and loaded jar checksums are enforced before any
fixture generation. JDK **21.0.12.1** and Google **protoc 3.21.12** are pinned.
The source's Confluent Community License is recorded; it is fetched externally,
not vendored, shipped in the crate or added as a runtime dependency.

Obtain the source from the immutable
[upstream file](https://github.com/confluentinc/schema-registry/blob/039a339d824debade488573769ad17b0d9975643/protobuf-provider/src/main/java/io/confluent/kafka/schemaregistry/protobuf/MessageIndexes.java).
Supply the verified source, jar and tools as explicit local paths. The
generator checks their pins, builds into a temporary directory and cleans it.
The reproduction driver currently uses a Unix Java classpath; its recorded run
is Linux, not Windows qualification.

```sh
python3 -B partitionline-schema/tests/oracles/protobuf/generate.py \
  --source /path/to/MessageIndexes.java \
  --kafka-jar /path/to/kafka-clients-4.1.0-distribution.jar \
  --java-home /path/to/jdk-21.0.12.1 --protoc /path/to/protoc-3.21.12 \
  --output partitionline-schema/tests/fixtures/protobuf --verify
```

For both directions, retain actual Rust adapter output outside the checkout:

```sh
PL_PROTOBUF_ORACLE_OUTPUT=/tmp/partitionline-protobuf-rust \
  cargo test --locked --manifest-path partitionline-schema/Cargo.toml --test protobuf
# Repeat the generator command with:
# --verify --rust-output /tmp/partitionline-protobuf-rust
```

The Java reader verifies schema ID, path and exact payload boundary in the Rust
frames. Google protoc decodes the **actual Rust-emitted payload**, and its result
must match Google's original decode. The reverse direction decodes the pinned
Google payload and Confluent header through Rust framing and fixture-only
descriptor codecs, then compares exact re-encoded bytes. The nested message
uses imported `common.Metadata`. These minimal test codecs are not a production
serialization library. The numeric-boundary path does not exist in the example
descriptor and qualifies framing only. Malformed/boundary tests exercise Rust's
structured failures; they do not claim reference acceptance of invalid data.

`manifest.json` retains source/tool pins, fixture SHA-256s, inputs and per-cell
scope. No Kafka broker or live Schema Registry is needed or claimed.
