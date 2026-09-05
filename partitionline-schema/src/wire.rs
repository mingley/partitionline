//! Confluent wire framing: magic `0` + BE `u32` schema id + payload.
//!
//! Spec: Confluent Schema Registry wire format (single-record embedding).

/// Confluent magic byte for schema-registry-framed payloads.
pub const MAGIC: u8 = 0;

/// Decoded Confluent-framed message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMessage<'a> {
    /// Schema id (big-endian u32 on the wire).
    pub schema_id: u32,
    /// Remaining payload bytes (codec-specific).
    pub payload: &'a [u8],
}

/// Decode failures for Confluent framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Buffer shorter than 1 + 4 byte header.
    Truncated {
        /// Bytes available.
        got: usize,
    },
    /// First byte was not [`MAGIC`].
    BadMagic {
        /// Observed magic byte.
        got: u8,
    },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Truncated { got } => {
                write!(f, "confluent wire truncated: need >= 5 bytes, got {got}")
            }
            DecodeError::BadMagic { got } => {
                write!(f, "confluent wire bad magic: expected {MAGIC}, got {got}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Encode `schema_id` + `payload` into Confluent framing.
pub fn encode(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(MAGIC);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode Confluent framing from `buf`.
pub fn decode(buf: &[u8]) -> Result<WireMessage<'_>, DecodeError> {
    if buf.len() < 5 {
        return Err(DecodeError::Truncated { got: buf.len() });
    }
    if buf[0] != MAGIC {
        return Err(DecodeError::BadMagic { got: buf[0] });
    }
    let schema_id = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    Ok(WireMessage {
        schema_id,
        payload: &buf[5..],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_payload() {
        let framed = encode(42, b"");
        assert_eq!(framed, vec![0, 0, 0, 0, 42]);
        let msg = decode(&framed).unwrap();
        assert_eq!(msg.schema_id, 42);
        assert_eq!(msg.payload, b"");
    }

    #[test]
    fn roundtrip_payload() {
        let framed = encode(0x0102_0304, b"avro-bytes");
        let msg = decode(&framed).unwrap();
        assert_eq!(msg.schema_id, 0x0102_0304);
        assert_eq!(msg.payload, b"avro-bytes");
    }

    #[test]
    fn rejects_bad_magic() {
        let mut framed = encode(1, b"x");
        framed[0] = 1;
        assert_eq!(decode(&framed), Err(DecodeError::BadMagic { got: 1 }));
    }

    #[test]
    fn rejects_truncated() {
        assert_eq!(decode(&[0, 0, 0]), Err(DecodeError::Truncated { got: 3 }));
    }
}
