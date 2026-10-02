#![cfg(feature = "avro")]

use partitionline_schema::avro::{
    decode, encode, Adapter, AdapterError, Codec, Error, Limits, Reference, Schema,
};
use std::cell::Cell;
use std::rc::Rc;

const WRITER: &str = include_str!("fixtures/avro/writer.avsc");
const READER: &str = include_str!("fixtures/avro/reader.avsc");
const METADATA_V1: &str = include_str!("fixtures/avro/metadata-v1.avsc");
const METADATA_V2: &str = include_str!("fixtures/avro/metadata-v2.avsc");
const INCOMPATIBLE: &str = include_str!("fixtures/avro/incompatible.avsc");
const REQUIRED: &str = include_str!("fixtures/avro/required.avsc");
const REF_V1: [Reference<'static>; 1] = [Reference {
    name: "common.Metadata",
    json: METADATA_V1,
}];
const REF_V2: [Reference<'static>; 1] = [Reference {
    name: "common.Metadata",
    json: METADATA_V2,
}];

#[derive(Debug, PartialEq, Eq)]
enum CodecError {
    InvalidSchema,
    MissingReference,
    Incompatible,
    MissingDefault,
    Truncated,
    IntegerOverflow,
    FieldLimit,
    InvalidUnion,
    InvalidUtf8,
    AllocationFailed,
    ForcedFailure,
}

#[derive(Debug, PartialEq, Eq)]
struct Event {
    id: i64,
    source: String,
    note: Option<String>,
    active: Option<bool>,
    status: Option<String>,
    tag: Option<String>,
}

#[derive(Clone, Copy)]
enum ReaderKind {
    Original,
    Evolved,
    Incompatible,
    MissingDefault,
}

// A deliberately fixture-only codec: it recognizes exactly the checked-in
// schemas, then implements their binary datum layout and resolution rules.
// It is not a general-purpose JSON parser or production Avro serializer.
struct EventCodec {
    reader: ReaderKind,
    calls: Rc<Cell<usize>>,
}
impl Codec for EventCodec {
    type Value = Event;
    type Error = CodecError;
    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        _: Limits,
    ) -> Result<(), CodecError> {
        self.calls.set(self.calls.get() + 1);
        if writer.json != WRITER {
            return Err(CodecError::InvalidSchema);
        }
        if writer.references.len() != 1
            || writer.references[0].name != "common.Metadata"
            || writer.references[0].json != METADATA_V1
        {
            return Err(CodecError::MissingReference);
        }
        self.reader = match reader.json {
            WRITER => ReaderKind::Original,
            READER => ReaderKind::Evolved,
            INCOMPATIBLE => ReaderKind::Incompatible,
            REQUIRED => ReaderKind::MissingDefault,
            _ => return Err(CodecError::InvalidSchema),
        };
        let expected = if matches!(self.reader, ReaderKind::Evolved) {
            METADATA_V2
        } else {
            METADATA_V1
        };
        if reader.references.len() != 1
            || reader.references[0].name != "common.Metadata"
            || reader.references[0].json != expected
        {
            return Err(CodecError::MissingReference);
        }
        Ok(())
    }
    fn encoded_len(&self, value: &Event) -> Result<usize, CodecError> {
        if i32::try_from(value.id).is_err() {
            return Err(CodecError::IntegerOverflow);
        }
        if value.source.len() > 256 || value.note.as_ref().is_some_and(|note| note.len() > 256) {
            return Err(CodecError::FieldLimit);
        }
        Ok(long_len(value.id)
            + long_len(value.source.len() as i64)
            + value.source.len()
            + 1
            + value
                .note
                .as_ref()
                .map_or(0, |note| long_len(note.len() as i64) + note.len()))
    }
    fn encode_into(&self, value: &Event, output: &mut [u8]) -> Result<usize, CodecError> {
        self.calls.set(self.calls.get() + 1);
        let mut position = 0;
        put_long(value.id, output, &mut position);
        put_string(&value.source, output, &mut position);
        put_long(i64::from(value.note.is_some()), output, &mut position);
        if let Some(note) = &value.note {
            put_string(note, output, &mut position);
        }
        Ok(position)
    }
    fn decode(&self, bytes: &[u8]) -> Result<(Event, usize), CodecError> {
        self.calls.set(self.calls.get() + 1);
        // Apache Avro rejects these cases during datum resolution; construction
        // still parses valid writer and reader schemas successfully.
        match self.reader {
            ReaderKind::Incompatible => return Err(CodecError::Incompatible),
            ReaderKind::MissingDefault => return Err(CodecError::MissingDefault),
            _ => {}
        }
        let mut position = 0;
        let id = take_long(bytes, &mut position)?;
        if i32::try_from(id).is_err() {
            return Err(CodecError::IntegerOverflow);
        }
        let source = take_string(bytes, &mut position)?;
        let note = match take_long(bytes, &mut position)? {
            0 => None,
            1 => Some(take_string(bytes, &mut position)?),
            _ => return Err(CodecError::InvalidUnion),
        };
        let evolved = matches!(self.reader, ReaderKind::Evolved);
        Ok((
            Event {
                id,
                source,
                note,
                active: evolved.then_some(true),
                status: evolved.then(|| "new".into()),
                tag: None,
            },
            position,
        ))
    }
}

fn zigzag(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}
fn long_len(value: i64) -> usize {
    let mut n = zigzag(value);
    let mut len = 1;
    while n >= 128 {
        n >>= 7;
        len += 1;
    }
    len
}
fn put_long(value: i64, output: &mut [u8], position: &mut usize) {
    let mut n = zigzag(value);
    while n >= 128 {
        output[*position] = (n as u8 & 127) | 128;
        *position += 1;
        n >>= 7;
    }
    output[*position] = n as u8;
    *position += 1;
}
fn put_string(value: &str, output: &mut [u8], position: &mut usize) {
    put_long(value.len() as i64, output, position);
    output[*position..*position + value.len()].copy_from_slice(value.as_bytes());
    *position += value.len();
}
fn take_long(bytes: &[u8], position: &mut usize) -> Result<i64, CodecError> {
    let mut value = 0u64;
    for index in 0..10 {
        let byte = *bytes.get(*position).ok_or(CodecError::Truncated)?;
        *position += 1;
        if index == 9 && byte > 1 {
            return Err(CodecError::IntegerOverflow);
        }
        value |= u64::from(byte & 127) << (7 * index);
        if byte < 128 {
            return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
        }
    }
    Err(CodecError::IntegerOverflow)
}
fn take_string(bytes: &[u8], position: &mut usize) -> Result<String, CodecError> {
    let length =
        usize::try_from(take_long(bytes, position)?).map_err(|_| CodecError::IntegerOverflow)?;
    if length > 256 {
        return Err(CodecError::FieldLimit);
    }
    let end = position
        .checked_add(length)
        .ok_or(CodecError::IntegerOverflow)?;
    let text = std::str::from_utf8(bytes.get(*position..end).ok_or(CodecError::Truncated)?)
        .map_err(|_| CodecError::InvalidUtf8)?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(length)
        .map_err(|_| CodecError::AllocationFailed)?;
    owned.push_str(text);
    *position = end;
    Ok(owned)
}

fn schema(json: &'static str, references: &'static [Reference<'static>]) -> Schema<'static> {
    Schema { json, references }
}
fn adapter(
    reader: &'static str,
    refs: &'static [Reference<'static>],
    limits: Limits,
) -> (Adapter<EventCodec>, Rc<Cell<usize>>) {
    let calls = Rc::new(Cell::new(0));
    let codec = EventCodec {
        reader: ReaderKind::Original,
        calls: calls.clone(),
    };
    (
        Adapter::new(
            42,
            schema(WRITER, &REF_V1),
            schema(reader, refs),
            codec,
            limits,
        )
        .unwrap(),
        calls,
    )
}

type Fixture = (
    &'static str,
    i64,
    &'static str,
    Option<&'static str>,
    &'static [u8],
    &'static [u8],
);
fn fixtures() -> [Fixture; 4] {
    [
        (
            "null",
            1,
            "reference.avsc",
            None,
            include_bytes!("fixtures/avro/null.frame.bin"),
            include_bytes!("fixtures/avro/null.payload.bin"),
        ),
        (
            "present",
            300,
            "producer",
            Some("hello 雪"),
            include_bytes!("fixtures/avro/present.frame.bin"),
            include_bytes!("fixtures/avro/present.payload.bin"),
        ),
        (
            "negative",
            i64::from(i32::MIN),
            "雪",
            Some(""),
            include_bytes!("fixtures/avro/negative.frame.bin"),
            include_bytes!("fixtures/avro/negative.payload.bin"),
        ),
        (
            "intmax",
            i64::from(i32::MAX),
            "boundary",
            Some("large"),
            include_bytes!("fixtures/avro/intmax.frame.bin"),
            include_bytes!("fixtures/avro/intmax.payload.bin"),
        ),
    ]
}

// Opt-in test-only synchronous artifact retention; normal tests never write.
#[allow(clippy::disallowed_methods)]
fn retain(name: &str, frame: &[u8], payload: &[u8]) {
    if let Some(directory) = std::env::var_os("PL_AVRO_ORACLE_OUTPUT") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{name}.frame.bin")), frame).unwrap();
        std::fs::write(directory.join(format!("{name}.payload.bin")), payload).unwrap();
    }
}

#[test]
fn apache_peer_framing_borrows_exact_avro_datum() {
    for (_, _, _, _, frame, payload) in fixtures() {
        let message = decode(frame, Limits::default()).unwrap();
        assert_eq!(message.schema_id, 42);
        assert_eq!(message.payload, payload);
        assert_eq!(message.payload.as_ptr(), frame[5..].as_ptr());
        assert_eq!(encode(42, payload, Limits::default()).unwrap(), frame);
    }
}

#[test]
fn independent_writer_reader_resolution_references_defaults_nulls_bidirectional() {
    for (reader, refs, evolved) in [
        (WRITER, REF_V1.as_slice(), false),
        (READER, REF_V2.as_slice(), true),
    ] {
        let (adapter, calls) = adapter(reader, refs, Limits::default());
        assert_eq!(calls.get(), 1); // schema selection happens only once.
        for (name, id, source, note, bytes, payload) in fixtures() {
            let expected = Event {
                id,
                source: source.into(),
                note: note.map(str::to_owned),
                active: evolved.then_some(true),
                status: evolved.then(|| "new".into()),
                tag: None,
            };
            assert_eq!(adapter.decode(bytes).unwrap(), expected);
            let frame = adapter.encode(&expected).unwrap();
            assert_eq!(frame, bytes);
            assert_eq!(decode(&frame, Limits::default()).unwrap().payload, payload);
            retain(name, &frame, &frame[5..]);
        }
        assert_eq!(calls.get(), 9);
    }
}

#[test]
fn incompatible_change_and_missing_default_preserve_codec_failures() {
    for (reader, error) in [
        (INCOMPATIBLE, CodecError::Incompatible),
        (REQUIRED, CodecError::MissingDefault),
    ] {
        let (adapter, _) = adapter(reader, &REF_V1, Limits::default());
        assert_eq!(
            adapter.decode(fixtures()[0].4),
            Err(AdapterError::Codec(error))
        );
    }
}

#[test]
fn unresolved_reference_and_invalid_schema_fail_construction() {
    for (writer, reader, error) in [
        (
            schema(WRITER, &[]),
            schema(READER, &REF_V2),
            CodecError::MissingReference,
        ),
        (
            schema(WRITER, &REF_V1),
            schema(READER, &REF_V1),
            CodecError::MissingReference,
        ),
        (
            schema("invalid JSON", &[]),
            schema(READER, &REF_V2),
            CodecError::InvalidSchema,
        ),
    ] {
        let codec = EventCodec {
            reader: ReaderKind::Original,
            calls: Rc::new(Cell::new(0)),
        };
        assert!(
            matches!(Adapter::new(42, writer, reader, codec, Limits::default()), Err(AdapterError::Codec(got)) if got == error)
        );
    }
}

#[test]
fn unknown_writer_and_oversized_input_never_invoke_decoder() {
    let (adapter, calls) = adapter(READER, &REF_V2, Limits::new(16, 4096, 2).unwrap());
    assert_eq!(
        adapter.decode(&encode(999, b"\xff", Limits::default()).unwrap()),
        Err(AdapterError::Input(Error::UnknownSchema {
            expected: 42,
            got: 999
        }))
    );
    assert_eq!(
        adapter.decode(&[0; 17]),
        Err(AdapterError::Input(Error::TooLarge))
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn schema_text_and_reference_counts_fail_before_resolution() {
    for limits in [
        Limits::new(16, 16, 2).unwrap(),
        Limits::new(16, 4096, 1).unwrap(),
    ] {
        let calls = Rc::new(Cell::new(0));
        let codec = EventCodec {
            reader: ReaderKind::Original,
            calls: calls.clone(),
        };
        let expected = if limits.max_references() == 1 {
            Error::ReferenceLimitExceeded
        } else {
            Error::SchemaBytesExceeded
        };
        assert!(
            matches!(Adapter::new(42, schema(WRITER, &REF_V1), schema(READER, &REF_V2), codec, limits), Err(AdapterError::Input(got)) if got == expected)
        );
        assert_eq!(calls.get(), 0);
    }
}

#[test]
fn duplicate_or_empty_reference_inputs_fail_before_codec() {
    let duplicate = [Reference {
        name: "common.Metadata",
        json: METADATA_V1,
    }; 2];
    let empty_name = [Reference {
        name: " ",
        json: METADATA_V1,
    }];
    let empty_json = [Reference {
        name: "common.Metadata",
        json: "\n",
    }];
    for (input, expected) in [
        (
            Schema {
                json: WRITER,
                references: &duplicate,
            },
            Error::DuplicateReference,
        ),
        (
            Schema {
                json: WRITER,
                references: &empty_name,
            },
            Error::EmptySchemaInput,
        ),
        (
            Schema {
                json: WRITER,
                references: &empty_json,
            },
            Error::EmptySchemaInput,
        ),
        (
            Schema {
                json: "\t",
                references: &[],
            },
            Error::EmptySchemaInput,
        ),
    ] {
        let calls = Rc::new(Cell::new(0));
        let codec = EventCodec {
            reader: ReaderKind::Original,
            calls: calls.clone(),
        };
        assert!(
            matches!(Adapter::new(42, input, schema(READER, &REF_V2), codec, Limits::default()), Err(AdapterError::Input(got)) if got == expected)
        );
        assert_eq!(calls.get(), 0);
    }
}

#[test]
fn exact_combined_schema_budget_and_limit_configuration_boundaries() {
    let bytes = WRITER.len()
        + READER.len()
        + 2 * "common.Metadata".len()
        + METADATA_V1.len()
        + METADATA_V2.len();
    let limits = Limits::new(64, bytes, 2).unwrap();
    assert_eq!(limits.max_frame_bytes(), 64);
    assert_eq!(limits.max_schema_bytes(), bytes);
    assert_eq!(limits.max_references(), 2);
    adapter(READER, &REF_V2, limits);
    for (frame, schema_bytes, refs) in [
        (4, 1, 0),
        (64 * 1024 * 1024 + 1, 1, 0),
        (5, 0, 0),
        (5, 64 * 1024 * 1024 + 1, 0),
        (5, 1, 1025),
    ] {
        assert_eq!(
            Limits::new(frame, schema_bytes, refs),
            Err(Error::InvalidLimits)
        );
    }
    assert!(Limits::new(5, 1, 0).is_ok());
    assert_eq!(
        encode(42, b"", Limits::new(5, 1, 0).unwrap())
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        encode(42, b"a", Limits::new(5, 1, 0).unwrap()),
        Err(Error::TooLarge)
    );
}

#[test]
fn generic_header_errors_are_structured() {
    for size in 0..5 {
        assert_eq!(
            decode(&[0; 5][..size], Limits::default()),
            Err(Error::Header(
                partitionline_schema::DecodeError::Truncated { got: size }
            ))
        );
    }
    assert_eq!(
        decode(&[1, 0, 0, 0, 42], Limits::default()),
        Err(Error::Header(partitionline_schema::DecodeError::BadMagic {
            got: 1
        }))
    );
}

#[test]
fn truncated_malformed_and_trailing_datums_cannot_succeed() {
    let (adapter, _) = adapter(READER, &REF_V2, Limits::default());
    for (_, _, _, _, frame, _) in fixtures() {
        for length in 5..frame.len() {
            assert!(adapter.decode(&frame[..length]).is_err());
        }
        let mut trailing = frame.to_vec();
        trailing.push(0);
        assert_eq!(
            adapter.decode(&trailing),
            Err(AdapterError::Input(Error::PayloadLengthMismatch {
                expected: trailing.len() - 5,
                got: frame.len() - 5
            }))
        );
    }
    for (payload, error) in [
        (&[0, 0, 4][..], CodecError::InvalidUnion),
        (&[0, 1][..], CodecError::IntegerOverflow),
        (&[0, 2, 0xff, 0][..], CodecError::InvalidUtf8),
        (&[0, 0x82, 4][..], CodecError::FieldLimit),
        (&[0xff; 10][..], CodecError::IntegerOverflow),
    ] {
        let frame = encode(42, payload, Limits::default()).unwrap();
        assert_eq!(adapter.decode(&frame), Err(AdapterError::Codec(error)));
    }
}

#[test]
fn oversized_writer_values_fail_before_payload_encoding() {
    let (adapter, calls) = adapter(READER, &REF_V2, Limits::new(8, 4096, 2).unwrap());
    let mut value = Event {
        id: 0,
        source: "large".into(),
        note: None,
        active: None,
        status: None,
        tag: None,
    };
    assert_eq!(
        adapter.encode(&value),
        Err(AdapterError::Input(Error::TooLarge))
    );
    value.source = "x".repeat(257);
    assert_eq!(
        adapter.encode(&value),
        Err(AdapterError::Codec(CodecError::FieldLimit))
    );
    value.id = i64::MAX;
    assert_eq!(
        adapter.encode(&value),
        Err(AdapterError::Codec(CodecError::IntegerOverflow))
    );
    assert_eq!(calls.get(), 1);
}

struct ContractCodec {
    size: usize,
    written: usize,
    consumed: usize,
    fail_encode: bool,
    calls: Rc<Cell<usize>>,
}
impl Codec for ContractCodec {
    type Value = ();
    type Error = CodecError;
    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        _: Limits,
    ) -> Result<(), CodecError> {
        if writer.json != "\"null\"" || reader.json != "\"null\"" {
            return Err(CodecError::InvalidSchema);
        }
        Ok(())
    }
    fn encoded_len(&self, _: &()) -> Result<usize, CodecError> {
        Ok(self.size)
    }
    fn encode_into(&self, _: &(), _: &mut [u8]) -> Result<usize, CodecError> {
        self.calls.set(self.calls.get() + 1);
        if self.fail_encode {
            Err(CodecError::ForcedFailure)
        } else {
            Ok(self.written)
        }
    }
    fn decode(&self, _: &[u8]) -> Result<((), usize), CodecError> {
        Ok(((), self.consumed))
    }
}
fn contract(codec: ContractCodec) -> Adapter<ContractCodec> {
    let schema = Schema {
        json: "\"null\"",
        references: &[],
    };
    Adapter::new(42, schema, schema, codec, Limits::default()).unwrap()
}

#[test]
fn independent_primitive_null_is_an_empty_datum_in_both_directions() {
    let adapter = contract(ContractCodec {
        size: 0,
        written: 0,
        consumed: 0,
        fail_encode: false,
        calls: Rc::new(Cell::new(0)),
    });
    let frame = include_bytes!("fixtures/avro/root-null.frame.bin");
    adapter.decode(frame).unwrap();
    let emitted = adapter.encode(&()).unwrap();
    assert_eq!(emitted, frame);
    retain("root-null", &emitted, b"");
}

#[test]
fn overflow_failed_encoder_and_wrong_reported_lengths_never_return_success() {
    for (size, written, fail, expected) in [
        (usize::MAX, 0, false, AdapterError::Input(Error::TooLarge)),
        (1, 0, true, AdapterError::Codec(CodecError::ForcedFailure)),
        (
            1,
            0,
            false,
            AdapterError::Input(Error::PayloadLengthMismatch {
                expected: 1,
                got: 0,
            }),
        ),
        (
            1,
            2,
            false,
            AdapterError::Input(Error::PayloadLengthMismatch {
                expected: 1,
                got: 2,
            }),
        ),
    ] {
        let calls = Rc::new(Cell::new(0));
        let adapter = contract(ContractCodec {
            size,
            written,
            consumed: 0,
            fail_encode: fail,
            calls: calls.clone(),
        });
        assert_eq!(adapter.encode(&()), Err(expected));
        assert_eq!(calls.get(), usize::from(size != usize::MAX));
    }
    let adapter = contract(ContractCodec {
        size: 0,
        written: 0,
        consumed: 1,
        fail_encode: false,
        calls: Rc::new(Cell::new(0)),
    });
    assert_eq!(
        adapter.decode(&[0, 0, 0, 0, 42]),
        Err(AdapterError::Input(Error::PayloadLengthMismatch {
            expected: 0,
            got: 1
        }))
    );
}

#[test]
fn bounded_datum_sweep_never_panics_and_success_reencodes_canonically() {
    let (adapter, _) = adapter(READER, &REF_V2, Limits::new(64, 4096, 2).unwrap());
    let mut state = 0x4156_524f_5045_4552u64;
    for size in 0..59 {
        for _ in 0..100 {
            let mut payload = vec![0; size];
            for byte in &mut payload {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            let frame = encode(42, &payload, Limits::new(64, 4096, 2).unwrap()).unwrap();
            if let Ok(value) = adapter.decode(&frame) {
                let canonical = adapter.encode(&value).unwrap();
                assert_eq!(adapter.decode(&canonical).unwrap(), value);
            }
        }
    }
}
