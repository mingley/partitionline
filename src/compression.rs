//! Pure-Rust record-batch compression for
//! `kafka_protocol::records::RecordBatchEncoder::encode_with_custom_compression`.
//!
//! This is the hook that lets us stay off kafka-protocol's C `lz4` / `zstd` features.

use bytes::{Bytes, BytesMut};
use kafka_protocol::records::Compression as WireCompression;

use crate::error::{Error, Result};

/// Client-facing codec. Maps 1:1 onto the Kafka attributes bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression.
    #[default]
    None,
    /// gzip via `flate2` + `zlib-rs` (pure Rust).
    Gzip,
    /// Snappy via `snap` (pure Rust).
    Snappy,
    /// LZ4 frame via `lz4_flex` (pure Rust). Kafka magic-v2 / Java
    /// `KafkaLZ4BlockOutputStream`, not `lz4_flex::block` size-prefix.
    Lz4,
    /// Zstd decode via `ruzstd`. Encode is a documented gap: `ruzstd` is a decoder.
    Zstd,
}

impl Compression {
    /// Kafka attributes nibble.
    pub fn as_wire(self) -> WireCompression {
        match self {
            Self::None => WireCompression::None,
            Self::Gzip => WireCompression::Gzip,
            Self::Snappy => WireCompression::Snappy,
            Self::Lz4 => WireCompression::Lz4,
            Self::Zstd => WireCompression::Zstd,
        }
    }

    /// Compress record bytes for a magic-v2 batch.
    pub fn compress(self, src: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(src)),
            Self::Gzip => {
                use std::io::Write;
                let mut out = Vec::with_capacity(src.len() / 2 + 32);
                let mut enc = flate2::write::GzEncoder::new(&mut out, flate2::Compression::fast());
                enc.write_all(src).map_err(Error::from)?;
                enc.finish().map_err(Error::from)?;
                Ok(Bytes::from(out))
            }
            Self::Snappy => {
                let mut enc = snap::raw::Encoder::new();
                let out = enc.compress_vec(src).map_err(|e| Error::protocol(e))?;
                Ok(Bytes::from(out))
            }
            Self::Lz4 => {
                // Magic v2 = LZ4 frame (KIP-57 / Java KafkaLZ4BlockOutputStream),
                // magic 0x184D2204. `lz4_flex::block::compress` is a different
                // size-prefixed block codec and will not interop with Kafka.
                use std::io::Write;
                let mut enc =
                    lz4_flex::frame::FrameEncoder::new(Vec::with_capacity(src.len() / 2 + 32));
                enc.write_all(src).map_err(Error::from)?;
                let out = enc.finish().map_err(|e| Error::protocol(e))?;
                Ok(Bytes::from(out))
            }
            Self::Zstd => Err(Error::NotImplemented(
                "zstd produce: ruzstd 0.8 is decode-only; see PROTOCOL.md",
            )),
        }
    }

    /// Decompress record bytes from a magic-v2 batch.
    pub fn decompress(self, src: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(src)),
            Self::Gzip => {
                use std::io::Read;
                let mut dec = flate2::read::GzDecoder::new(src);
                let mut out = Vec::new();
                dec.read_to_end(&mut out).map_err(Error::from)?;
                Ok(Bytes::from(out))
            }
            Self::Snappy => {
                let mut dec = snap::raw::Decoder::new();
                let out = dec.decompress_vec(src).map_err(|e| Error::protocol(e))?;
                Ok(Bytes::from(out))
            }
            Self::Lz4 => {
                use std::io::Read;
                if src.len() >= 4 && src[0..4] == [0x04, 0x22, 0x4D, 0x18] {
                    let mut dec = lz4_flex::frame::FrameDecoder::new(src);
                    let mut out = Vec::new();
                    dec.read_to_end(&mut out).map_err(Error::from)?;
                    return Ok(Bytes::from(out));
                }
                // Accept a broker/Java frame we did not produce, or (last resort)
                // a size-prefixed block so we can detect our own old mistake.
                // Lab A is compression=none; a Java-broker fixture is not in-repo.
                match lz4_flex::block::decompress_size_prepended(src) {
                    Ok(out) => Ok(Bytes::from(out)),
                    Err(_) => lz4_flex::block::decompress(src, 64 * 1024 * 1024)
                        .map(Bytes::from)
                        .map_err(|e| Error::protocol(e)),
                }
            }
            Self::Zstd => {
                let mut dec =
                    ruzstd::decoding::StreamingDecoder::new(src).map_err(|e| Error::protocol(e))?;
                let mut out = Vec::new();
                std::io::Read::read_to_end(&mut dec, &mut out).map_err(Error::from)?;
                Ok(Bytes::from(out))
            }
        }
    }
}

/// Hook for `RecordBatchEncoder::encode_with_custom_compression`.
pub fn encode_hook(
    uncompressed: &mut BytesMut,
    dst: &mut impl bytes::BufMut,
    wire: WireCompression,
) -> anyhow::Result<()> {
    let codec = match wire {
        WireCompression::None => Compression::None,
        WireCompression::Gzip => Compression::Gzip,
        WireCompression::Snappy => Compression::Snappy,
        WireCompression::Lz4 => Compression::Lz4,
        WireCompression::Zstd => Compression::Zstd,
    };
    let out = codec
        .compress(uncompressed)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    dst.put_slice(&out);
    Ok(())
}

/// Hook for `RecordBatchDecoder::decode_with_custom_compression`.
pub fn decode_hook(compressed: &mut Bytes, wire: WireCompression) -> anyhow::Result<Bytes> {
    let codec = match wire {
        WireCompression::None => Compression::None,
        WireCompression::Gzip => Compression::Gzip,
        WireCompression::Snappy => Compression::Snappy,
        WireCompression::Lz4 => Compression::Lz4,
        WireCompression::Zstd => Compression::Zstd,
    };
    codec
        .decompress(compressed)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_is_kafka_magic_v2_frame_not_block() {
        let src = b"partitionline-lz4-roundtrip-partitionline-lz4-roundtrip";
        let c = Compression::Lz4.compress(src).unwrap();
        assert!(c.len() >= 4);
        assert_eq!(
            &c[..4],
            &[0x04, 0x22, 0x4D, 0x18],
            "LZ4 frame magic (little-endian 0x184D2204), Java KafkaLZ4BlockOutputStream"
        );
        let block = lz4_flex::block::compress(src);
        assert_ne!(
            &c[..],
            &block[..],
            "must not emit lz4_flex::block (silent Kafka interop fail)"
        );
        let back = Compression::Lz4.decompress(&c).unwrap();
        assert_eq!(&back[..], src);
        // No in-repo Java / live-broker LZ4 fixture. Lab A is compression=none.
        // Interop against a real broker is a later check, not a claimed pass.
    }

    #[test]
    fn gzip_roundtrip() {
        let src = vec![7u8; 2048];
        let c = Compression::Gzip.compress(&src).unwrap();
        let back = Compression::Gzip.decompress(&c).unwrap();
        assert_eq!(back, src);
    }
}
