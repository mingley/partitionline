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
    /// LZ4 via `lz4_flex` (pure Rust).
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
                // Kafka uses LZ4 block format (not frame) for magic v2.
                Ok(Bytes::from(lz4_flex::block::compress(src)))
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
                // kafka-protocol / Java use LZ4 block with an implicit size; we
                // try the raw block decoder. Fetch path also accepts lz4_flex frame.
                match lz4_flex::block::decompress_size_prepended(src) {
                    Ok(out) => Ok(Bytes::from(out)),
                    Err(_) => lz4_flex::decompress_size_prepended(src)
                        .or_else(|_| {
                            // Kafka's lz4 is often HC block without size prefix.
                            // Bound output to 64 MiB to avoid bombs.
                            lz4_flex::block::decompress(src, 64 * 1024 * 1024)
                        })
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
    fn lz4_roundtrip() {
        let src = b"partitionline-lz4-roundtrip-partitionline-lz4-roundtrip";
        let c = Compression::Lz4.compress(src).unwrap();
        assert_ne!(&c[..], src);
        let back = Compression::Lz4.decompress(&c).unwrap();
        assert_eq!(&back[..], src);
    }

    #[test]
    fn gzip_roundtrip() {
        let src = vec![7u8; 2048];
        let c = Compression::Gzip.compress(&src).unwrap();
        let back = Compression::Gzip.decompress(&c).unwrap();
        assert_eq!(back, src);
    }
}
