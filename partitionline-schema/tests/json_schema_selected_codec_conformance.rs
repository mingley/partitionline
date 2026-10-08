#![cfg(feature = "json-schema")]
#[path = "support/selected_json_schema.rs"]
mod selected_json_schema;

use partitionline_schema::json_schema::{
    Adapter, AdapterError, Error, Limits, Reference, Schema, DIALECT_URI,
};
use selected_json_schema::{Failure, SelectedJsonSchema};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

const WRITER: &str =
    include_str!("../../tests/fixtures/json_schema_selected_codec/writer.schema.json");
const READER: &str =
    include_str!("../../tests/fixtures/json_schema_selected_codec/reader.schema.json");
const INTEGER: &str =
    include_str!("../../tests/fixtures/json_schema_selected_codec/integer.schema.json");
const WRITER_RECORD: &str =
    include_str!("../../tests/fixtures/json_schema_selected_codec/writer-record.schema.json");
const READER_RECORD: &str =
    include_str!("../../tests/fixtures/json_schema_selected_codec/reader-record.schema.json");
const REFERENCES: &[Reference<'_>] = &[Reference {
    uri: "urn:partitionline:selected-json:integer",
    dialect_uri: DIALECT_URI,
    json: INTEGER,
}];
fn schema(json: &str) -> Schema<'_> {
    Schema {
        dialect_uri: DIALECT_URI,
        json,
        references: REFERENCES,
    }
}
fn limits() -> Limits {
    Limits::new(4096, 16 * 1024, 8).unwrap()
}
fn selected(
    writer: Schema<'_>,
    reader: Schema<'_>,
) -> Result<Adapter<SelectedJsonSchema>, AdapterError<Failure>> {
    Adapter::new(42, writer, reader, SelectedJsonSchema::default(), limits())
}
// Synchronous conformance fixtures and bounded peer outputs use no executor.
#[allow(clippy::disallowed_methods)]
fn read(name: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../tests/fixtures/json_schema_selected_codec")
            .join(name),
    )
    .unwrap()
}
#[allow(clippy::disallowed_methods)]
fn emit(name: &str, bytes: &[u8]) {
    if let Some(path) = std::env::var_os("PL_JSON_SCHEMA_SELECTED_OUTPUT") {
        let path = PathBuf::from(path);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(name), bytes).unwrap();
    }
}

#[test]
fn production_validator_reads_independent_frames_and_emits_reverse_checked_values() {
    let scalar = selected(schema(WRITER), schema(READER)).unwrap();
    let record = selected(schema(WRITER_RECORD), schema(READER_RECORD)).unwrap();
    let manifest: Value = serde_json::from_slice(&read("manifest.json")).unwrap();
    for case in manifest["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let adapter = if case["profile"] == "record" {
            &record
        } else {
            &scalar
        };
        let frame = read(&format!("{name}.frame.bin"));
        if case["valid"] == true {
            let expected: Value =
                serde_json::from_slice(&read(&format!("{name}.payload.json"))).unwrap();
            let decoded = adapter.decode(&frame).unwrap();
            assert_eq!(decoded, expected, "{name}");
            let encoded = adapter.encode(&decoded).unwrap();
            assert_eq!(adapter.decode(&encoded).unwrap(), expected);
            emit(&format!("{name}.frame.bin"), &encoded);
            emit(
                &format!("{name}.decoded.json"),
                &serde_json::to_vec(&decoded).unwrap(),
            );
        } else {
            let result = adapter.decode(&frame);
            assert!(
                matches!(
                    result,
                    Err(AdapterError::Codec(Failure::Instance | Failure::Json))
                ),
                "{name}: {result:?}"
            );
            emit(
                &format!("{name}.disposition.json"),
                b"{\"rejected\":true}\n",
            );
        }
    }
    assert_eq!(
        record
            .decode(&record.encode(&json!({"id":7})).unwrap())
            .unwrap(),
        json!({"id":7})
    );
}

#[test]
fn writer_and_reader_validation_are_both_executed_without_coercion() {
    let incompatible = Schema {
        json: r#"{"type":"integer","minimum":0}"#,
        dialect_uri: DIALECT_URI,
        references: &[],
    };
    let adapter = selected(schema(WRITER), incompatible).unwrap();
    let frame = adapter.encode(&json!(-1)).unwrap(); // writer accepts; reader rejects
    assert!(matches!(
        adapter.decode(&frame),
        Err(AdapterError::Codec(Failure::Instance))
    ));
    assert!(matches!(
        adapter.encode(&json!("123")),
        Err(AdapterError::Codec(Failure::Instance))
    ));
    let adapter = selected(schema(WRITER), schema(READER)).unwrap();
    assert_eq!(
        adapter
            .decode(&adapter.encode(&Value::Null).unwrap())
            .unwrap(),
        Value::Null
    );
}

#[test]
fn dialect_missing_reference_cycles_and_unsupported_schema_work_fail_closed() {
    let unknown = Schema {
        dialect_uri: "https://example.invalid/dialect",
        ..schema(WRITER)
    };
    assert!(matches!(
        selected(unknown, schema(READER)),
        Err(AdapterError::Input(Error::UnsupportedDialect))
    ));
    let missing = Schema {
        references: &[],
        ..schema(WRITER)
    };
    assert!(matches!(
        selected(missing, schema(READER)),
        Err(AdapterError::Codec(Failure::MissingReference))
    ));
    let uri = "urn:partitionline:selected-json:cycle";
    let text = r#"{"$id":"urn:partitionline:selected-json:cycle","$ref":"urn:partitionline:selected-json:cycle"}"#;
    let references = [Reference {
        uri,
        json: text,
        dialect_uri: DIALECT_URI,
    }];
    let cyclic = Schema {
        json: text,
        references: &references,
        dialect_uri: DIALECT_URI,
    };
    assert!(matches!(
        selected(cyclic, schema(READER)),
        Err(AdapterError::Codec(Failure::CyclicReference))
    ));
    let pattern = Schema {
        json: r#"{"type":"string","pattern":".*"}"#,
        references: &[],
        dialect_uri: DIALECT_URI,
    };
    assert!(matches!(
        selected(pattern, schema(READER)),
        Err(AdapterError::Codec(Failure::ProfileLimit))
    ));
}

#[test]
fn numeric_precision_and_value_expansion_outside_the_profile_are_explicit_errors() {
    let adapter = selected(schema(WRITER), schema(READER)).unwrap();
    for payload in [
        "1e-999",
        "18446744073709551615",
        "0.123456789",
        "1000000000000.00000001e-6",
    ] {
        let frame = partitionline_schema::json_schema::encode(42, payload, limits()).unwrap();
        assert!(
            matches!(
                adapter.decode(&frame),
                Err(AdapterError::Codec(Failure::ProfileLimit))
            ),
            "{payload}"
        );
    }
    assert!(matches!(
        adapter.encode(&Value::String("x".repeat(4092))),
        Err(AdapterError::Codec(Failure::ProfileLimit))
    ));
    let deep = "[".repeat(17) + "0" + &"]".repeat(17);
    let frame = partitionline_schema::json_schema::encode(42, &deep, limits()).unwrap();
    assert!(matches!(
        adapter.decode(&frame),
        Err(AdapterError::Codec(Failure::ProfileLimit))
    ));
}
