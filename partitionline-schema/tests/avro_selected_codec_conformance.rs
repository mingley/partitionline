#![cfg(feature = "avro")]
#[path = "support/selected_avro.rs"]
mod selected_avro;

use apache_avro::types::Value;
use partitionline_schema::avro::{Adapter, AdapterError, Error, Limits, Reference, Schema};
use selected_avro::{json, Failure, SelectedAvro, ALLOCATION_LIMIT};
use std::path::{Path, PathBuf};

const WRITER: &str = include_str!("fixtures/avro/writer.avsc");
const READER: &str = include_str!("fixtures/avro/reader.avsc");
const WRITER_REFERENCES: &[Reference<'_>] = &[Reference {
    name: "common.Metadata",
    json: include_str!("fixtures/avro/metadata-v1.avsc"),
}];
const READER_REFERENCES: &[Reference<'_>] = &[Reference {
    name: "common.Metadata",
    json: include_str!("fixtures/avro/metadata-v2.avsc"),
}];
fn limits() -> Limits {
    Limits::new(4096, 16 * 1024, 8).unwrap()
}
fn selected(
    writer: Schema<'_>,
    reader: Schema<'_>,
) -> Result<Adapter<SelectedAvro>, AdapterError<Failure>> {
    Adapter::new(42, writer, reader, SelectedAvro::default(), limits())
}
fn event_adapter(reader: &str) -> Adapter<SelectedAvro> {
    selected(
        Schema {
            json: WRITER,
            references: WRITER_REFERENCES,
        },
        Schema {
            json: reader,
            references: READER_REFERENCES,
        },
    )
    .unwrap()
}
// Synchronous integration tests; no async runtime runs here.
#[allow(clippy::disallowed_methods)]
fn fixture(name: &str, suffix: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../tests/fixtures/avro_selected_codec")
            .join(format!("{name}.{suffix}")),
    )
    .unwrap()
}
fn fixture_json(name: &str, suffix: &str) -> serde_json::Value {
    serde_json::from_slice(&fixture(name, suffix)).unwrap()
}
fn event(input: &serde_json::Value) -> Value {
    Value::Record(vec![
        (
            "id".into(),
            Value::Int(input["id"].as_i64().unwrap() as i32),
        ),
        (
            "metadata".into(),
            Value::Record(vec![(
                "source".into(),
                Value::String(input["metadata"]["source"].as_str().unwrap().into()),
            )]),
        ),
        (
            "note".into(),
            match input["note"].as_str() {
                None => Value::Union(0, Box::new(Value::Null)),
                Some(note) => Value::Union(1, Box::new(Value::String(note.into()))),
            },
        ),
    ])
}
// Write offline peer receipts outside any async runtime.
#[allow(clippy::disallowed_methods)]
fn emit(name: &str, frame: &[u8], resolved: &Value) {
    if let Some(directory) = std::env::var_os("PL_AVRO_SELECTED_OUTPUT") {
        let path = PathBuf::from(directory);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(format!("{name}.frame.bin")), frame).unwrap();
        std::fs::write(
            path.join(format!("{name}.reader.json")),
            serde_json::to_vec(&json(resolved)).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn production_codec_reads_java_frames_and_emits_identical_writer_frames() {
    let evolved = event_adapter(READER);
    let original = selected(
        Schema {
            json: WRITER,
            references: WRITER_REFERENCES,
        },
        Schema {
            json: WRITER,
            references: WRITER_REFERENCES,
        },
    )
    .unwrap();
    for name in ["null", "present", "negative", "intmax"] {
        let input = fixture_json(name, "writer.json");
        let expected = fixture_json(name, "reader.json");
        let peer = fixture(name, "frame.bin");
        let resolved = evolved.decode(&peer).unwrap();
        assert_eq!(
            json(&resolved),
            expected,
            "{name}: peer writer -> Rust evolved reader"
        );
        let output = evolved.encode(&event(&input)).unwrap();
        assert_eq!(output, peer, "{name}: actual production writer bytes");
        assert_eq!(json(&original.decode(&peer).unwrap()), input);
        emit(name, &output, &resolved);
    }
}

#[test]
fn primitive_schemas_and_numeric_promotions_use_the_production_resolver() {
    let cells = [
        ("root-null", "\"null\"", "\"null\"", Value::Null),
        (
            "boolean",
            "\"boolean\"",
            "\"boolean\"",
            Value::Boolean(true),
        ),
        ("int-long", "\"int\"", "\"long\"", Value::Int(i32::MIN)),
        (
            "long-double",
            "\"long\"",
            "\"double\"",
            Value::Long(9_007_199_254_740_993),
        ),
        ("float-double", "\"float\"", "\"double\"", Value::Float(1.5)),
    ];
    for (name, writer, reader, value) in cells {
        let codec = selected(
            Schema {
                json: writer,
                references: &[],
            },
            Schema {
                json: reader,
                references: &[],
            },
        )
        .unwrap();
        let peer = fixture(name, "frame.bin");
        let output = codec.encode(&value).unwrap();
        assert_eq!(output, peer, "{name}");
        let resolved = codec.decode(&peer).unwrap();
        assert_eq!(json(&resolved), fixture_json(name, "reader.json"), "{name}");
        emit(name, &output, &resolved);
    }
}

#[test]
fn incompatible_readers_missing_defaults_and_unresolved_or_mislabeled_references_fail() {
    let frame = fixture("present", "frame.bin");
    for reader in [
        include_str!("fixtures/avro/incompatible.avsc"),
        include_str!("fixtures/avro/required.avsc"),
    ] {
        assert!(matches!(
            event_adapter(reader).decode(&frame),
            Err(AdapterError::Codec(Failure::Avro(_)))
        ));
    }
    let no_references = Schema {
        json: WRITER,
        references: &[],
    };
    assert!(matches!(
        selected(no_references, no_references),
        Err(AdapterError::Codec(Failure::Avro(_)))
    ));
    let mislabeled = [Reference {
        name: "different.Metadata",
        json: WRITER_REFERENCES[0].json,
    }];
    assert!(matches!(
        selected(
            Schema {
                json: WRITER,
                references: &mislabeled
            },
            Schema {
                json: READER,
                references: READER_REFERENCES
            }
        ),
        Err(AdapterError::Codec(Failure::Policy(
            "reference full name mismatch"
        )))
    ));
}

#[test]
fn truncated_invalid_union_trailing_and_invalid_utf8_have_structured_failures() {
    let codec = event_adapter(READER);
    let frame = fixture("present", "frame.bin");
    for length in 5..frame.len() {
        assert!(matches!(
            codec.decode(&frame[..length]),
            Err(AdapterError::Codec(Failure::Policy("truncated datum")))
        ));
    }
    let mut union = fixture("null", "frame.bin");
    *union.last_mut().unwrap() = 4;
    assert!(matches!(
        codec.decode(&union),
        Err(AdapterError::Codec(Failure::Avro(_)))
    ));
    let mut trailing = frame;
    trailing.push(0);
    assert!(matches!(
        codec.decode(&trailing),
        Err(AdapterError::Input(Error::PayloadLengthMismatch { .. }))
    ));
    let string = Schema {
        json: "\"string\"",
        references: &[],
    };
    let strings = selected(string, string).unwrap();
    assert!(matches!(
        strings.decode(&[0, 0, 0, 0, 42, 2, 255]),
        Err(AdapterError::Codec(Failure::Avro(_)))
    ));
    assert!(matches!(
        strings.encode(&Value::Int(1)),
        Err(AdapterError::Codec(Failure::Avro(_)))
    ));
}

#[test]
fn input_frame_and_codec_resource_budgets_are_separate() {
    let codec = event_adapter(READER);
    let mut unknown = fixture("present", "frame.bin");
    unknown[4] = 43;
    assert!(matches!(
        codec.decode(&unknown),
        Err(AdapterError::Input(Error::UnknownSchema { .. }))
    ));
    let mut magic = fixture("present", "frame.bin");
    magic[0] = 1;
    assert!(matches!(
        codec.decode(&magic),
        Err(AdapterError::Input(Error::Header(_)))
    ));
    assert!(matches!(
        codec.decode(&vec![0; 4097]),
        Err(AdapterError::Input(Error::TooLarge))
    ));
    let string = Schema {
        json: "\"string\"",
        references: &[],
    };
    let strings = selected(string, string).unwrap();
    assert!(matches!(
        strings.encode(&Value::String("x".repeat(ALLOCATION_LIMIT + 1))),
        Err(AdapterError::Codec(Failure::Policy(
            "aggregate scalar bytes"
        )))
    ));
    // Exactly accepted scalar bytes still need their varint/header; streaming count catches this.
    assert!(matches!(
        strings.encode(&Value::String("x".repeat(ALLOCATION_LIMIT))),
        Err(AdapterError::Codec(Failure::Avro(_)))
    ));
    // Wire length 4097, before allocating/reading the advertised scalar body.
    assert!(matches!(
        strings.decode(&[0, 0, 0, 0, 42, 0x82, 0x40]),
        Err(AdapterError::Codec(Failure::Avro(_)))
    ));
    let schema_bytes = " ".repeat(16 * 1024);
    assert!(matches!(
        selected(
            Schema {
                json: &schema_bytes,
                references: &[]
            },
            string
        ),
        Err(AdapterError::Input(Error::SchemaBytesExceeded))
    ));
}

#[test]
fn production_parser_is_not_tied_to_fixture_names_and_profile_rejects_recursion_and_collections() {
    let json = r#"{"type":"record","name":"Counter","namespace":"unrelated","fields":[{"name":"enabled","type":"boolean"},{"name":"count","type":"long"}]}"#;
    let schema = Schema {
        json,
        references: &[],
    };
    let codec = selected(schema, schema).unwrap();
    let value = Value::Record(vec![
        ("enabled".into(), Value::Boolean(false)),
        ("count".into(), Value::Long(i64::MAX)),
    ]);
    assert_eq!(codec.decode(&codec.encode(&value).unwrap()).unwrap(), value);
    for json in [
        r#"{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}"#,
        r#"{"type":"array","items":"long"}"#,
        r#"{"type":"map","values":"string"}"#,
        r#"{"type":"long","logicalType":"timestamp-micros"}"#,
    ] {
        let schema = Schema {
            json,
            references: &[],
        };
        assert!(matches!(
            selected(schema, schema),
            Err(AdapterError::Codec(Failure::Policy(_)))
        ));
    }
    let deeply_nested = format!("{}\"null\"{}", "[".repeat(33), "]".repeat(33));
    assert!(matches!(
        selected(
            Schema {
                json: &deeply_nested,
                references: &[]
            },
            schema
        ),
        Err(AdapterError::Codec(Failure::Policy("schema JSON nesting")))
    ));
}
