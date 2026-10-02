#![cfg(feature = "json-schema")]

use partitionline_schema::json_schema::{
    decode, encode, Adapter, AdapterError, Codec, Dialect, Error, Limits, Reference, Schema,
    Selection, DIALECT_URI, PROFILE,
};
use std::cell::Cell;
use std::io::Write;
use std::rc::Rc;

const WRITER: &str = include_str!("fixtures/json_schema/writer.schema.json");
const READER: &str = include_str!("fixtures/json_schema/reader.schema.json");
const INT64: &str = include_str!("fixtures/json_schema/int64.schema.json");
const INCOMPATIBLE: &str = include_str!("fixtures/json_schema/incompatible.schema.json");
const TRUE_SCHEMA: &str = include_str!("fixtures/json_schema/true.schema.json");
const FALSE_SCHEMA: &str = include_str!("fixtures/json_schema/false.schema.json");
const RESOURCE: &str = "urn:partitionline:json-schema:int64";
const REFERENCES: &[Reference<'static>] = &[Reference {
    uri: RESOURCE,
    dialect_uri: DIALECT_URI,
    json: INT64,
}];

struct Case {
    name: &'static str,
    payload: &'static str,
    frame: &'static [u8],
    valid: bool,
    incompatible_valid: bool,
}
include!("fixtures/json_schema/cases.rs");

#[derive(Debug, Clone, PartialEq, Eq)]
enum CodecError {
    Schema,
    MissingReference,
    Json,
    Instance,
    InternalLimit,
    Forced,
}

#[derive(Clone, Copy)]
enum Kind {
    NullableInt64,
    Nonnegative,
    Accept,
    Reject,
}

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    Resolve,
    HugeLength,
    PartialWrite,
    InvalidUtf8,
    PartialRead,
}

// Fixture-only serializer/validator. It recognizes exactly the checked-in
// schemas and implements their scalar numeric constraints using i128 integer
// arithmetic. It is not a production JSON parser or JSON Schema engine.
struct FixtureCodec {
    writer: Kind,
    reader: Kind,
    calls: Rc<Cell<usize>>,
    fault: Fault,
}

fn schema(json: &'static str) -> Schema<'static> {
    Schema {
        dialect_uri: DIALECT_URI,
        json,
        references: if json == WRITER || json == READER {
            REFERENCES
        } else {
            &[]
        },
    }
}

fn prepare(input: Schema<'_>) -> Result<Kind, CodecError> {
    if input.json == WRITER || input.json == READER {
        if input.references.len() != 1
            || input.references[0].uri != RESOURCE
            || input.references[0].json != INT64
        {
            return Err(CodecError::MissingReference);
        }
        Ok(Kind::NullableInt64)
    } else if input.json == INCOMPATIBLE {
        Ok(Kind::Nonnegative)
    } else if input.json == TRUE_SCHEMA {
        Ok(Kind::Accept)
    } else if input.json == FALSE_SCHEMA {
        Ok(Kind::Reject)
    } else {
        Err(CodecError::Schema)
    }
}

fn decimal_integer(payload: &str) -> Result<Option<i128>, CodecError> {
    if payload.len() > 128 {
        return Err(CodecError::InternalLimit);
    }
    let payload = payload.trim_matches([' ', '\t', '\r', '\n']);
    if payload == "null" {
        return Ok(None);
    }
    let (negative, unsigned) = match payload.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, payload),
    };
    let mut exponents = unsigned.split(['e', 'E']);
    let mantissa = exponents.next().ok_or(CodecError::Json)?;
    let exponent = match exponents.next() {
        Some(text) => {
            let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(CodecError::Json);
            }
            text.parse::<i32>().map_err(|_| CodecError::Instance)?
        }
        None => 0,
    };
    if exponents.next().is_some() {
        return Err(CodecError::Json);
    }
    let mut pieces = mantissa.split('.');
    let integer = pieces.next().ok_or(CodecError::Json)?;
    let fraction = pieces.next();
    if pieces.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|b| b.is_ascii_digit())
        || (integer.len() > 1 && integer.starts_with('0'))
        || fraction.is_some_and(|s| s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(CodecError::Json);
    }
    let fraction = fraction.unwrap_or("");
    let mut value = 0i128;
    for digit in integer.bytes().chain(fraction.bytes()) {
        value = value
            .checked_mul(10)
            .and_then(|n| n.checked_add(i128::from(digit - b'0')))
            .ok_or(CodecError::Instance)?;
    }
    let scale = i64::from(exponent) - fraction.len() as i64;
    if value != 0 {
        if scale >= 0 {
            let power = u32::try_from(scale).map_err(|_| CodecError::Instance)?;
            value = value
                .checked_mul(10i128.checked_pow(power).ok_or(CodecError::Instance)?)
                .ok_or(CodecError::Instance)?;
        } else {
            let power = u32::try_from(-scale).map_err(|_| CodecError::Instance)?;
            let divisor = 10i128.checked_pow(power).ok_or(CodecError::Instance)?;
            if value % divisor != 0 {
                return Err(CodecError::Instance);
            }
            value /= divisor;
        }
    }
    Ok(Some(if negative { -value } else { value }))
}

impl Codec for FixtureCodec {
    type Value = String;
    type Error = CodecError;

    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        _: Limits,
    ) -> Result<(), Self::Error> {
        self.calls.set(self.calls.get() + 1);
        if matches!(self.fault, Fault::Resolve) {
            return Err(CodecError::Forced);
        }
        self.writer = prepare(writer)?;
        self.reader = prepare(reader)?;
        Ok(())
    }

    fn encoded_len(&self, value: &String) -> Result<usize, Self::Error> {
        self.calls.set(self.calls.get() + 1);
        Ok(if matches!(self.fault, Fault::HugeLength) {
            usize::MAX
        } else {
            value.len()
        })
    }

    fn encode_into(&self, value: &String, out: &mut [u8]) -> Result<usize, Self::Error> {
        self.calls.set(self.calls.get() + 1);
        if matches!(self.fault, Fault::InvalidUtf8) {
            out.fill(0xff);
        } else {
            out.copy_from_slice(value.as_bytes());
        }
        Ok(if matches!(self.fault, Fault::PartialWrite) {
            out.len().saturating_sub(1)
        } else {
            out.len()
        })
    }

    fn validate(&self, payload: &str, selection: Selection) -> Result<(), Self::Error> {
        self.calls.set(self.calls.get() + 1);
        let value = decimal_integer(payload)?;
        let kind = match selection {
            Selection::Writer => self.writer,
            Selection::Reader => self.reader,
        };
        match (kind, value) {
            (Kind::Accept, _) | (Kind::NullableInt64, None) => Ok(()),
            (Kind::NullableInt64, Some(n)) if i64::try_from(n).is_ok() => Ok(()),
            (Kind::Nonnegative, Some(n)) if n >= 0 => Ok(()),
            _ => Err(CodecError::Instance),
        }
    }

    fn decode(&self, payload: &str) -> Result<(String, usize), Self::Error> {
        self.calls.set(self.calls.get() + 1);
        let consumed = if matches!(self.fault, Fault::PartialRead) {
            payload.len().saturating_sub(1)
        } else {
            payload.len()
        };
        Ok((payload.to_owned(), consumed))
    }
}

fn codec(calls: Rc<Cell<usize>>, fault: Fault) -> FixtureCodec {
    FixtureCodec {
        writer: Kind::Reject,
        reader: Kind::Reject,
        calls,
        fault,
    }
}

fn fixture_adapter(reader: &'static str, fault: Fault) -> (Adapter<FixtureCodec>, Rc<Cell<usize>>) {
    let calls = Rc::new(Cell::new(0));
    let adapter = Adapter::new(
        42,
        schema(WRITER),
        schema(reader),
        codec(calls.clone(), fault),
        Limits::default(),
    )
    .unwrap();
    (adapter, calls)
}

#[test]
fn independent_schema_and_numeric_fixtures() {
    let (adapter, _) = fixture_adapter(READER, Fault::None);
    let (incompatible, _) = fixture_adapter(INCOMPATIBLE, Fault::None);
    let output = std::env::var_os("JSON_SCHEMA_RUST_OUTPUT").map(std::path::PathBuf::from);
    if let Some(path) = &output {
        std::fs::create_dir_all(path).unwrap();
    }
    assert_eq!(CASES.len(), 27);
    assert_eq!(CASES.iter().filter(|case| case.valid).count(), 10);
    for case in CASES {
        let encoded = adapter.encode(&case.payload.to_owned());
        assert_eq!(encoded.is_ok(), case.valid, "encode {}", case.name);
        let decoded = adapter.decode(case.frame);
        assert_eq!(decoded.is_ok(), case.valid, "decode {}", case.name);
        assert_eq!(
            incompatible.decode(case.frame).is_ok(),
            case.incompatible_valid,
            "reader constraints {}",
            case.name
        );
        if case.valid {
            let actual = encoded.unwrap();
            assert_eq!(actual, case.frame, "independent bytes {}", case.name);
            assert_eq!(decoded.unwrap(), case.payload, "unchanged {}", case.name);
            if let Some(path) = &output {
                std::fs::File::create(path.join(format!("{}.frame.bin", case.name)))
                    .unwrap()
                    .write_all(&actual)
                    .unwrap();
            }
        }
    }
}

#[test]
fn explicit_profile_and_dialect_reject_unknowns() {
    assert_eq!(PROFILE, "partitionline.json-schema.draft2020-12.v1");
    assert_eq!(Dialect::from_uri(DIALECT_URI), Ok(Dialect::Draft202012));
    assert_eq!(Dialect::Draft202012.uri(), DIALECT_URI);
    for uri in [
        "",
        "http://json-schema.org/draft-07/schema#",
        "https://example.invalid/schema",
    ] {
        assert_eq!(Dialect::from_uri(uri), Err(Error::UnsupportedDialect));
        let calls = Rc::new(Cell::new(0));
        let wrong = Schema {
            dialect_uri: uri,
            ..schema(WRITER)
        };
        let result = Adapter::new(
            42,
            wrong,
            schema(READER),
            codec(calls.clone(), Fault::None),
            Limits::default(),
        );
        assert!(matches!(
            result,
            Err(AdapterError::Input(Error::UnsupportedDialect))
        ));
        assert_eq!(calls.get(), 0);
    }
}

#[test]
fn reference_dialect_and_duplicate_uri_fail_before_codec() {
    let wrong = [Reference {
        dialect_uri: "http://json-schema.org/draft-07/schema#",
        ..REFERENCES[0]
    }];
    let duplicates = [REFERENCES[0], REFERENCES[0]];
    for (references, expected) in [
        (&wrong[..], Error::UnsupportedDialect),
        (&duplicates[..], Error::DuplicateReference),
    ] {
        let calls = Rc::new(Cell::new(0));
        let writer = Schema {
            references,
            ..schema(WRITER)
        };
        let result = Adapter::new(
            42,
            writer,
            schema(READER),
            codec(calls.clone(), Fault::None),
            Limits::default(),
        );
        assert!(matches!(result, Err(AdapterError::Input(error)) if error == expected));
        assert_eq!(calls.get(), 0);
    }
}

fn schema_bytes(schema: Schema<'_>) -> usize {
    schema.json.len()
        + schema.dialect_uri.len()
        + schema
            .references
            .iter()
            .map(|r| r.uri.len() + r.dialect_uri.len() + r.json.len())
            .sum::<usize>()
}

#[test]
fn aggregate_schema_and_reference_limits_include_both_selections_and_uris() {
    let bytes = schema_bytes(schema(WRITER)) + schema_bytes(schema(READER));
    let exact = Limits::new(1024, bytes, 2).unwrap();
    assert_eq!(exact.max_schema_bytes(), bytes);
    assert_eq!(exact.max_references(), 2);
    assert!(Adapter::new(
        42,
        schema(WRITER),
        schema(READER),
        codec(Rc::new(Cell::new(0)), Fault::None),
        exact
    )
    .is_ok());
    for (limits, expected) in [
        (
            Limits::new(1024, bytes - 1, 2).unwrap(),
            Error::SchemaBytesExceeded,
        ),
        (
            Limits::new(1024, bytes, 1).unwrap(),
            Error::ReferenceLimitExceeded,
        ),
    ] {
        let calls = Rc::new(Cell::new(0));
        let result = Adapter::new(
            42,
            schema(WRITER),
            schema(READER),
            codec(calls.clone(), Fault::None),
            limits,
        );
        assert!(matches!(result, Err(AdapterError::Input(error)) if error == expected));
        assert_eq!(calls.get(), 0);
    }
}

#[test]
fn invalid_limit_configuration_and_hard_ceilings() {
    for (frame, schema_bytes, refs) in [
        (5, 1, 0),
        (64 * 1024 * 1024 + 1, 1, 0),
        (6, 0, 0),
        (6, 64 * 1024 * 1024 + 1, 0),
        (6, 1, 1025),
    ] {
        assert_eq!(
            Limits::new(frame, schema_bytes, refs),
            Err(Error::InvalidLimits)
        );
    }
    assert!(Limits::new(6, 1, 0).is_ok());
    assert!(Limits::new(64 * 1024 * 1024, 64 * 1024 * 1024, 1024).is_ok());
}

#[test]
fn empty_schema_inputs_fail_before_codec() {
    let empty_reference = [Reference {
        uri: " \t",
        ..REFERENCES[0]
    }];
    for writer in [
        Schema {
            json: " \n",
            ..schema(WRITER)
        },
        Schema {
            references: &empty_reference,
            ..schema(WRITER)
        },
    ] {
        let calls = Rc::new(Cell::new(0));
        let result = Adapter::new(
            42,
            writer,
            schema(READER),
            codec(calls.clone(), Fault::None),
            Limits::default(),
        );
        assert!(matches!(
            result,
            Err(AdapterError::Input(Error::EmptySchemaInput))
        ));
        assert_eq!(calls.get(), 0);
    }
}

#[test]
fn missing_reference_and_mismatched_schema_are_codec_errors() {
    let missing = Schema {
        references: &[],
        ..schema(WRITER)
    };
    for (writer, expected) in [
        (missing, CodecError::MissingReference),
        (
            Schema {
                json: r#"{"$schema":"http://json-schema.org/draft-07/schema#"}"#,
                ..schema(WRITER)
            },
            CodecError::Schema,
        ),
    ] {
        let result = Adapter::new(
            42,
            writer,
            schema(READER),
            codec(Rc::new(Cell::new(0)), Fault::None),
            Limits::default(),
        );
        assert!(matches!(result, Err(AdapterError::Codec(error)) if error == expected));
    }
}

#[test]
fn null_is_a_document_and_default_annotation_does_not_rewrite_it() {
    let (adapter, _) = fixture_adapter(READER, Fault::None);
    assert_eq!(
        adapter
            .decode(&adapter.encode(&"null".to_owned()).unwrap())
            .unwrap(),
        "null"
    );
    let header_only = [0, 0, 0, 0, 42];
    assert_eq!(
        decode(&header_only, Limits::default()),
        Err(Error::EmptyPayload)
    );
    assert!(matches!(
        adapter.decode(&header_only),
        Err(AdapterError::Input(Error::EmptyPayload))
    ));
}

#[test]
fn known_writer_id_is_checked_before_validation_or_utf8() {
    let (adapter, calls) = fixture_adapter(READER, Fault::None);
    let baseline = calls.get();
    assert!(matches!(
        adapter.decode(&[0, 0, 0, 0, 43, 0xff]),
        Err(AdapterError::Input(Error::UnknownSchema {
            expected: 42,
            got: 43
        }))
    ));
    assert_eq!(calls.get(), baseline);
}

#[test]
fn framing_size_utf8_and_empty_document_checks() {
    let limits = Limits::new(6, 1024, 0).unwrap();
    assert_eq!(limits.max_frame_bytes(), 6);
    let frame = encode(42, "0", limits).unwrap();
    assert_eq!(decode(&frame, limits).unwrap().payload, b"0");
    assert_eq!(encode(42, "null", limits), Err(Error::TooLarge));
    assert_eq!(
        decode(&[0, 0, 0, 0, 42, b'0', b'0'], limits),
        Err(Error::TooLarge)
    );
    assert_eq!(
        decode(&[0, 0, 0, 0, 42, 0xff], limits),
        Err(Error::InvalidUtf8)
    );
    assert_eq!(encode(42, "", limits), Err(Error::EmptyPayload));
    assert_eq!(encode(42, " ", limits), Err(Error::EmptyPayload));
    assert!(matches!(
        decode(&[1, 0, 0, 0, 42, b'0'], limits),
        Err(Error::Header(_))
    ));
    assert!(matches!(decode(&[0, 0, 0], limits), Err(Error::Header(_))));
    // Low-level framing does not promise syntax or schema validation.
    assert_eq!(
        decode(
            &encode(42, "NaN", Limits::default()).unwrap(),
            Limits::default()
        )
        .unwrap()
        .payload,
        b"NaN"
    );
}

#[test]
fn codec_size_overflow_stops_before_serialization() {
    let (adapter, calls) = fixture_adapter(READER, Fault::HugeLength);
    assert_eq!(
        adapter.encode(&"0".to_owned()),
        Err(AdapterError::Input(Error::TooLarge))
    );
    assert_eq!(calls.get(), 2); // preparation and length only
}

#[test]
fn partial_write_invalid_utf8_and_partial_read_are_structured_failures() {
    let (partial, _) = fixture_adapter(READER, Fault::PartialWrite);
    assert_eq!(
        partial.encode(&"1.0".to_owned()),
        Err(AdapterError::Input(Error::PayloadLengthMismatch {
            expected: 3,
            got: 2
        }))
    );
    let (utf8, _) = fixture_adapter(READER, Fault::InvalidUtf8);
    assert_eq!(
        utf8.encode(&"0".to_owned()),
        Err(AdapterError::Input(Error::InvalidUtf8))
    );
    let (read, _) = fixture_adapter(READER, Fault::PartialRead);
    assert_eq!(
        read.decode(&encode(42, "1.0", Limits::default()).unwrap()),
        Err(AdapterError::Input(Error::PayloadLengthMismatch {
            expected: 3,
            got: 2
        }))
    );
}

#[test]
fn both_validation_selections_run_before_decode_and_failures_stop_early() {
    let (adapter, calls) = fixture_adapter(READER, Fault::None);
    let frame = encode(42, "0", Limits::default()).unwrap();
    assert_eq!(adapter.decode(&frame), Ok("0".to_owned()));
    assert_eq!(calls.get(), 4); // resolve, writer, reader, decode
    let (reject, calls) = fixture_adapter(INCOMPATIBLE, Fault::None);
    assert_eq!(
        reject.decode(&encode(42, "null", Limits::default()).unwrap()),
        Err(AdapterError::Codec(CodecError::Instance))
    );
    assert_eq!(calls.get(), 3); // no decode after reader failure
    let (invalid, calls) = fixture_adapter(READER, Fault::None);
    assert!(invalid
        .decode(&encode(42, "NaN", Limits::default()).unwrap())
        .is_err());
    assert_eq!(calls.get(), 2); // no reader validation/decode after writer failure
}

#[test]
fn boolean_schemas_and_resolution_failure_remain_explicit() {
    for (root, accepted) in [(TRUE_SCHEMA, true), (FALSE_SCHEMA, false)] {
        let adapter = Adapter::new(
            42,
            schema(root),
            schema(root),
            codec(Rc::new(Cell::new(0)), Fault::None),
            Limits::default(),
        )
        .unwrap();
        assert_eq!(adapter.encode(&"0".to_owned()).is_ok(), accepted);
    }
    let result = Adapter::new(
        42,
        schema(WRITER),
        schema(READER),
        codec(Rc::new(Cell::new(0)), Fault::Resolve),
        Limits::default(),
    );
    assert!(matches!(
        result,
        Err(AdapterError::Codec(CodecError::Forced))
    ));
}

#[test]
fn oversized_frame_fails_before_any_codec_operation() {
    let calls = Rc::new(Cell::new(0));
    let adapter = Adapter::new(
        42,
        schema(WRITER),
        schema(READER),
        codec(calls.clone(), Fault::None),
        Limits::new(6, 4096, 2).unwrap(),
    )
    .unwrap();
    assert_eq!(
        adapter.encode(&"null".to_owned()),
        Err(AdapterError::Input(Error::TooLarge))
    );
    let before = calls.get();
    assert_eq!(
        adapter.decode(&encode(42, "null", Limits::default()).unwrap()),
        Err(AdapterError::Input(Error::TooLarge))
    );
    assert_eq!(calls.get(), before);
}
