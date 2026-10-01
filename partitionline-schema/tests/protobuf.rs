use partitionline_schema::protobuf::{decode, encode, Adapter, AdapterError, Codec, Error, Limits};
use std::cell::Cell;
use std::rc::Rc;

#[test]
fn independent_confluent_and_google_frames_preserve_payload_boundary() {
    for (frame, payload, path) in [
        (
            include_bytes!("fixtures/protobuf/first.frame.bin").as_slice(),
            include_bytes!("fixtures/protobuf/first.payload.bin").as_slice(),
            vec![0],
        ),
        (
            include_bytes!("fixtures/protobuf/outer.frame.bin").as_slice(),
            include_bytes!("fixtures/protobuf/outer.payload.bin").as_slice(),
            vec![1],
        ),
        (
            include_bytes!("fixtures/protobuf/nested.frame.bin").as_slice(),
            include_bytes!("fixtures/protobuf/nested.payload.bin").as_slice(),
            vec![1, 1],
        ),
    ] {
        let decoded = decode(frame, Limits::default()).unwrap();
        assert_eq!(decoded.schema_id, 42);
        assert_eq!(decoded.indexes, path);
        assert_eq!(decoded.payload, payload);
        assert_eq!(
            encode(42, &path, payload, Limits::default()).unwrap(),
            frame
        );
        // Generic framing cannot distinguish the message-index bytes from data.
        assert_ne!(
            partitionline_schema::decode(frame).unwrap().payload,
            payload
        );
    }
}

#[test]
fn rejects_malformed_varints_negative_fields_and_truncation() {
    for (indexes, error) in [
        (vec![], Error::TruncatedIndexes),
        (vec![0x80], Error::TruncatedIndexes),
        (vec![0x80; 5], Error::InvalidVarint),
        (vec![0xff, 0xff, 0xff, 0xff, 0x10], Error::InvalidVarint),
        (vec![1], Error::NegativeLength),
        (vec![2], Error::TruncatedIndexes),
        (vec![2, 1], Error::NegativeIndex),
    ] {
        let mut frame = vec![0, 0, 0, 0, 42];
        frame.extend(indexes);
        assert_eq!(decode(&frame, Limits::default()), Err(error));
    }
}

#[test]
fn canonical_special_zero_and_explicit_zero_path_are_both_readable() {
    let special = encode(42, &[0], &[], Limits::default()).unwrap();
    assert_eq!(special, [0, 0, 0, 0, 42, 0]);
    let explicit = [0, 0, 0, 0, 42, 2, 0];
    assert_eq!(
        decode(&special, Limits::default()),
        decode(&explicit, Limits::default())
    );
    // Reference ByteUtils permits a nonminimal positive varint.
    let nonminimal = [0, 0, 0, 0, 42, 0x82, 0, 0x80, 0];
    assert_eq!(
        decode(&special, Limits::default()),
        decode(&nonminimal, Limits::default())
    );
}

#[test]
fn independent_multibyte_index_fixture_covers_fifth_byte_boundary() {
    let bytes = include_bytes!("fixtures/protobuf/varints.frame.bin");
    let message = decode(bytes, Limits::default()).unwrap();
    assert_eq!(message.indexes, [0, 64, 8192, i32::MAX as u32]);
    assert_eq!(
        encode(42, &message.indexes, &[], Limits::default()).unwrap(),
        bytes
    );
    for length in 5..bytes.len() {
        assert!(decode(&bytes[..length], Limits::default()).is_err());
    }
}

#[test]
fn limits_apply_before_index_or_output_allocation() {
    for (bytes, depth) in [(5, 1), (64 * 1024 * 1024 + 1, 1), (6, 0), (6, 1025)] {
        assert_eq!(Limits::new(bytes, depth), Err(Error::InvalidLimits));
    }
    let limits = Limits::new(8, 2).unwrap();
    assert_eq!(limits.max_frame_bytes(), 8);
    assert_eq!(limits.max_indexes(), 2);
    assert_eq!(encode(42, &[0], b"ab", limits).unwrap().len(), 8);
    assert_eq!(encode(42, &[0], b"abc", limits), Err(Error::TooLarge));
    assert_eq!(
        encode(42, &[0, 0, 0], b"", limits),
        Err(Error::IndexLimitExceeded)
    );
    assert_eq!(encode(42, &[], b"", limits), Err(Error::InvalidPath));
    assert_eq!(
        encode(42, &[u32::MAX], b"", limits),
        Err(Error::InvalidPath)
    );
    assert_eq!(decode(&[0; 9], limits), Err(Error::TooLarge));
    // Huge count with zero remaining data cannot reserve a huge Vec.
    assert_eq!(
        decode(
            &[0, 0, 0, 0, 42, 0xfe, 0xff, 0xff, 0xff, 0x0f],
            Limits::default()
        ),
        Err(Error::IndexLimitExceeded)
    );
    assert_eq!(
        decode(&[0, 0, 0, 0, 42, 4, 0], Limits::default()),
        Err(Error::TruncatedIndexes)
    );
}

#[test]
fn generic_header_errors_and_borrowed_payload_are_preserved() {
    for size in 0..5 {
        assert_eq!(
            decode(&[0; 5][..size], Limits::default()),
            Err(Error::Header(
                partitionline_schema::DecodeError::Truncated { got: size }
            ))
        );
    }
    assert_eq!(
        decode(&[1, 0, 0, 0, 42, 0], Limits::default()),
        Err(Error::Header(partitionline_schema::DecodeError::BadMagic {
            got: 1
        }))
    );
    let bytes = encode(42, &[0], b"abc", Limits::default()).unwrap();
    let message = decode(&bytes, Limits::default()).unwrap();
    assert_eq!(message.payload.as_ptr(), bytes[6..].as_ptr());
}

// Minimal fixture-only descriptor codecs, not a production serializer.
struct StringCodec {
    path: Vec<u32>,
    calls: Rc<Cell<usize>>,
}
impl Codec for StringCodec {
    type Message = String;
    type Error = &'static str;
    fn message_indexes(&self) -> &[u32] {
        &self.path
    }
    fn encoded_len(&self, message: &String) -> Result<usize, Self::Error> {
        if message.len() > 127 {
            return Err("fixture field too large");
        }
        Ok(if message.is_empty() {
            0
        } else {
            2 + message.len()
        })
    }
    fn encode_into(&self, message: &String, output: &mut [u8]) -> Result<(), Self::Error> {
        self.calls.set(self.calls.get() + 1);
        if message.is_empty() {
            return Ok(());
        }
        output[0] = 10;
        output[1] = message.len() as u8;
        output[2..].copy_from_slice(message.as_bytes());
        Ok(())
    }
    fn decode(&self, bytes: &[u8]) -> Result<String, Self::Error> {
        self.calls.set(self.calls.get() + 1);
        if bytes.is_empty() {
            return Ok(String::new());
        }
        if bytes.len() < 2
            || bytes[0] != 10
            || bytes[1] > 127
            || bytes.len() != 2 + bytes[1] as usize
        {
            return Err("malformed fixture string");
        }
        std::str::from_utf8(&bytes[2..])
            .map(str::to_owned)
            .map_err(|_| "invalid UTF-8")
    }
}

fn adapter(path: &[u32], limits: Limits) -> (Adapter<StringCodec>, Rc<Cell<usize>>) {
    let calls = Rc::new(Cell::new(0));
    let codec = StringCodec {
        path: path.to_vec(),
        calls: calls.clone(),
    };
    (Adapter::new(42, codec, limits).unwrap(), calls)
}

// Synchronous test-only artifact export; no executor or async task runs here.
#[allow(clippy::disallowed_methods)]
fn retain(name: &str, frame: &[u8], payload: &[u8]) {
    // Opt-in external output only; normal tests do not modify the checkout.
    if let Some(directory) = std::env::var_os("PL_PROTOBUF_ORACLE_OUTPUT") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{name}.frame.bin")), frame).unwrap();
        std::fs::write(directory.join(format!("{name}.payload.bin")), payload).unwrap();
    }
}

#[test]
fn caller_selected_string_codec_matches_google_in_both_directions() {
    for (name, path, value, bytes) in [
        (
            "first",
            vec![0],
            "hello",
            include_bytes!("fixtures/protobuf/first.frame.bin").as_slice(),
        ),
        (
            "outer",
            vec![1],
            "top-level",
            include_bytes!("fixtures/protobuf/outer.frame.bin").as_slice(),
        ),
        (
            "empty",
            vec![0],
            "",
            include_bytes!("fixtures/protobuf/empty.frame.bin").as_slice(),
        ),
    ] {
        let (adapter, _) = adapter(&path, Limits::default());
        assert_eq!(adapter.decode(bytes).unwrap(), value);
        let frame = adapter.encode(&value.to_string()).unwrap();
        assert_eq!(frame, bytes);
        retain(
            name,
            &frame,
            decode(&frame, Limits::default()).unwrap().payload,
        );
    }
}

#[test]
fn unknown_schema_and_wrong_message_never_enter_selected_codec() {
    let (adapter, calls) = adapter(&[0], Limits::default());
    let frame = encode(999, &[0], b"\xff", Limits::default()).unwrap();
    assert_eq!(
        adapter.decode(&frame),
        Err(AdapterError::Frame(Error::UnknownSchema {
            expected: 42,
            got: 999
        }))
    );
    let frame = encode(42, &[1, 1], b"\xff", Limits::default()).unwrap();
    assert_eq!(
        adapter.decode(&frame),
        Err(AdapterError::Frame(Error::UnexpectedMessage))
    );
    assert_eq!(calls.get(), 0);
}

#[test]
fn oversized_payloads_do_not_enter_encoder_and_codec_failures_remain_errors() {
    let (adapter, calls) = adapter(&[0], Limits::new(8, 2).unwrap());
    assert_eq!(
        adapter.encode(&"abc".to_string()),
        Err(AdapterError::Frame(Error::TooLarge))
    );
    assert_eq!(calls.get(), 0);
    assert_eq!(
        adapter.encode(&"x".repeat(128)),
        Err(AdapterError::Codec("fixture field too large"))
    );
    let frame = encode(42, &[0], b"\xff", Limits::default()).unwrap();
    assert_eq!(
        adapter.decode(&frame),
        Err(AdapterError::Codec("malformed fixture string"))
    );
    assert_eq!(calls.get(), 1);
}

// This small fixture codec resolves the imported common.Metadata descriptor.
// It deliberately handles only this test's bounded, complete field ordering.
#[derive(Debug, PartialEq, Eq)]
struct Envelope {
    id: u64,
    source: String,
    body: Vec<u8>,
}
struct EnvelopeCodec;
fn unsigned_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 128 {
        value >>= 7;
        len += 1;
    }
    len
}
fn put_unsigned(mut value: u64, bytes: &mut [u8], position: &mut usize) {
    while value >= 128 {
        bytes[*position] = (value as u8 & 127) | 128;
        *position += 1;
        value >>= 7;
    }
    bytes[*position] = value as u8;
    *position += 1;
}
fn take_unsigned(bytes: &[u8], position: &mut usize) -> Result<u64, &'static str> {
    let mut value = 0u64;
    for index in 0..10 {
        let byte = *bytes.get(*position).ok_or("truncated field")?;
        *position += 1;
        if index == 9 && byte > 1 {
            return Err("overflow field");
        }
        value |= u64::from(byte & 127) << (index * 7);
        if byte < 128 {
            return Ok(value);
        }
    }
    Err("overflow field")
}
fn take_field<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    tag: u8,
) -> Result<&'a [u8], &'static str> {
    if bytes.get(*position) != Some(&tag) {
        return Err("unexpected field");
    }
    *position += 1;
    let len = usize::try_from(take_unsigned(bytes, position)?).map_err(|_| "overflow length")?;
    if len > 127 {
        return Err("fixture field too large");
    }
    let end = position.checked_add(len).ok_or("overflow length")?;
    let field = bytes.get(*position..end).ok_or("truncated field")?;
    *position = end;
    Ok(field)
}
impl Codec for EnvelopeCodec {
    type Message = Envelope;
    type Error = &'static str;
    fn message_indexes(&self) -> &[u32] {
        &[1, 1]
    }
    fn encoded_len(&self, message: &Envelope) -> Result<usize, Self::Error> {
        let inner = 3 + unsigned_len(message.id) + message.source.len();
        if inner > 127 || message.body.len() > 127 {
            return Err("fixture field too large");
        }
        Ok(4 + inner + message.body.len())
    }
    fn encode_into(&self, message: &Envelope, bytes: &mut [u8]) -> Result<(), Self::Error> {
        let inner = 3 + unsigned_len(message.id) + message.source.len();
        let mut p = 0;
        for byte in [10, inner as u8, 8] {
            bytes[p] = byte;
            p += 1;
        }
        put_unsigned(message.id, bytes, &mut p);
        for byte in [18, message.source.len() as u8] {
            bytes[p] = byte;
            p += 1;
        }
        bytes[p..p + message.source.len()].copy_from_slice(message.source.as_bytes());
        p += message.source.len();
        for byte in [18, message.body.len() as u8] {
            bytes[p] = byte;
            p += 1;
        }
        bytes[p..].copy_from_slice(&message.body);
        Ok(())
    }
    fn decode(&self, bytes: &[u8]) -> Result<Envelope, Self::Error> {
        let mut p = 0;
        let metadata = take_field(bytes, &mut p, 10)?;
        let body = take_field(bytes, &mut p, 18)?;
        if p != bytes.len() || metadata.first() != Some(&8) {
            return Err("unexpected field");
        }
        let mut p = 1;
        let id = take_unsigned(metadata, &mut p)?;
        let source = take_field(metadata, &mut p, 18)?;
        if p != metadata.len() {
            return Err("unexpected field");
        }
        Ok(Envelope {
            id,
            source: std::str::from_utf8(source)
                .map_err(|_| "invalid UTF-8")?
                .to_owned(),
            body: body.to_vec(),
        })
    }
}

#[test]
fn nested_message_with_imported_reference_matches_google_bidirectionally() {
    let adapter = Adapter::new(42, EnvelopeCodec, Limits::default()).unwrap();
    let expected = Envelope {
        id: 300,
        source: "reference.proto".into(),
        body: b"hello".to_vec(),
    };
    let bytes = include_bytes!("fixtures/protobuf/nested.frame.bin");
    assert_eq!(adapter.decode(bytes).unwrap(), expected);
    let frame = adapter.encode(&expected).unwrap();
    assert_eq!(frame, bytes);
    retain(
        "nested",
        &frame,
        decode(&frame, Limits::default()).unwrap().payload,
    );
    for length in 8..bytes.len() {
        assert!(adapter.decode(&bytes[..length]).is_err());
    }
}

#[test]
fn bounded_deterministic_parser_sweep_preserves_every_success() {
    let limits = Limits::new(64, 8).unwrap();
    let mut state = 0x5052_4f54_4f42_5546u64;
    for size in 6..64 {
        for _ in 0..200 {
            let mut bytes = vec![0, 0, 0, 0, 42];
            for _ in 5..size {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.push(state as u8);
            }
            if let Ok(message) = decode(&bytes, limits) {
                let canonical =
                    encode(message.schema_id, &message.indexes, message.payload, limits).unwrap();
                assert_eq!(decode(&canonical, limits).unwrap(), message);
            }
        }
    }
    let frame = encode(42, &[0, 64, 8192, i32::MAX as u32], &[], Limits::default()).unwrap();
    retain("varints", &frame, &[]);
}

struct FailingCodec {
    length: usize,
    calls: Rc<Cell<usize>>,
}
impl Codec for FailingCodec {
    type Message = ();
    type Error = &'static str;
    fn message_indexes(&self) -> &[u32] {
        &[0]
    }
    fn encoded_len(&self, _: &()) -> Result<usize, Self::Error> {
        Ok(self.length)
    }
    fn encode_into(&self, _: &(), _: &mut [u8]) -> Result<(), Self::Error> {
        self.calls.set(self.calls.get() + 1);
        Err("encode failed")
    }
    fn decode(&self, _: &[u8]) -> Result<(), Self::Error> {
        Err("decode failed")
    }
}
#[test]
fn overflowing_codec_size_and_failed_encoder_cannot_return_partial_success() {
    let calls = Rc::new(Cell::new(0));
    let adapter = Adapter::new(
        42,
        FailingCodec {
            length: usize::MAX,
            calls: calls.clone(),
        },
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        adapter.encode(&()),
        Err(AdapterError::Frame(Error::TooLarge))
    );
    assert_eq!(calls.get(), 0);
    let adapter = Adapter::new(
        42,
        FailingCodec {
            length: 1,
            calls: calls.clone(),
        },
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        adapter.encode(&()),
        Err(AdapterError::Codec("encode failed"))
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn adapter_rejects_invalid_or_over_limit_selected_descriptors() {
    for (path, error) in [
        (vec![], Error::InvalidPath),
        (vec![u32::MAX], Error::InvalidPath),
        (vec![0, 1], Error::IndexLimitExceeded),
    ] {
        let codec = StringCodec {
            path,
            calls: Rc::new(Cell::new(0)),
        };
        assert!(
            matches!(Adapter::new(42, codec, Limits::new(6, 1).unwrap()), Err(got) if got == error)
        );
    }
    let codec = StringCodec {
        path: vec![1],
        calls: Rc::new(Cell::new(0)),
    };
    assert!(matches!(
        Adapter::new(42, codec, Limits::new(6, 1).unwrap()),
        Err(Error::TooLarge)
    ));
}
