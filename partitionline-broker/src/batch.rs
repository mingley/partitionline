//! Kafka record-batch v2 header layout.
//!
//! Produce admission and codec normalization both read this prefix. Offsets
//! are from the first byte of the batch, which is the base offset. The CRC32-C
//! covers the bytes from [`ATTRIBUTES`] through the end of the batch. The
//! client codec uses the same positions (`Records::MAGIC_OFFSET` and
//! `RecordBatch::RECORD_BATCH_OVERHEAD` in the client crate).

/// Bytes in the v2 header, including the base offset and the length.
pub(crate) const LEN: usize = 61;
/// Base offset.
#[allow(
    dead_code,
    reason = "documents the first header field in the shared layout"
)]
pub(crate) const BASE_OFFSET: usize = 0;
/// Batch length. The length counts bytes after this field, so the framed
/// batch is twelve bytes larger than the value stored here.
pub(crate) const LENGTH: usize = 8;
/// Partition leader epoch.
pub(crate) const LEADER_EPOCH: usize = 12;
/// Magic byte. Version 2 is [`MAGIC_V2`].
pub(crate) const MAGIC: usize = 16;
/// CRC32-C of the bytes from [`ATTRIBUTES`] to the end of the batch.
pub(crate) const CRC: usize = 17;
/// Attribute bitfield. The low three bits are the compression codec.
pub(crate) const ATTRIBUTES: usize = 21;
/// Last offset delta.
pub(crate) const LAST_OFFSET_DELTA: usize = 23;
/// Base timestamp.
pub(crate) const BASE_TIMESTAMP: usize = 27;
/// Maximum timestamp.
pub(crate) const MAX_TIMESTAMP: usize = 35;
/// Producer id. A value of `-1` means the batch is not idempotent.
pub(crate) const PRODUCER_ID: usize = 43;
/// Producer epoch.
#[cfg(any(test, feature = "codecs"))]
pub(crate) const PRODUCER_EPOCH: usize = 51;
/// Base sequence.
#[allow(
    dead_code,
    reason = "readers reach this field through the sequential header cursor"
)]
pub(crate) const BASE_SEQUENCE: usize = 53;
/// Number of records declared by the batch.
pub(crate) const RECORD_COUNT: usize = 57;
/// Base offset plus the length field. The framed batch is this much larger
/// than the length stored at [`LENGTH`]. It is also the offset of [`LEADER_EPOCH`].
pub(crate) const PREFIX: usize = 12;
/// Length value stored in a header-only batch: [`LEN`] minus [`PREFIX`].
pub(crate) const HEADER_ONLY_LENGTH: i32 = (LEN - PREFIX) as i32;
/// Magic value for Kafka 0.11 and later batches.
pub(crate) const MAGIC_V2: u8 = 2;
/// Low three attribute bits. Codec 0 is uncompressed.
pub(crate) const COMPRESSION_MASK: u16 = 0x07;
/// Attribute bit for log-append time. Unset means create time.
pub(crate) const LOG_APPEND_TIME: u16 = 0x08;
/// Attribute bit for a transactional batch.
pub(crate) const TRANSACTIONAL: u16 = 0x10;
/// Attribute bit for a control batch.
pub(crate) const CONTROL: u16 = 0x20;
/// Attribute bit for a compaction delete horizon. The base timestamp is that horizon.
pub(crate) const DELETE_HORIZON: u16 = 0x40;
/// The seven defined v2 attribute bits. Bits above this mask are rejected.
pub(crate) const ATTRIBUTE_MASK: u16 = 0x7f;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_tile_the_header_without_gaps() {
        let fields = [
            (BASE_OFFSET, 8usize),
            (LENGTH, 4),
            (LEADER_EPOCH, 4),
            (MAGIC, 1),
            (CRC, 4),
            (ATTRIBUTES, 2),
            (LAST_OFFSET_DELTA, 4),
            (BASE_TIMESTAMP, 8),
            (MAX_TIMESTAMP, 8),
            (PRODUCER_ID, 8),
            (PRODUCER_EPOCH, 2),
            (BASE_SEQUENCE, 4),
            (RECORD_COUNT, 4),
        ];
        let mut at = 0usize;
        for (offset, size) in fields {
            assert_eq!(offset, at);
            at += size;
        }
        assert_eq!(at, LEN);
        assert_eq!(PREFIX, LEADER_EPOCH);
        assert_eq!(HEADER_ONLY_LENGTH as usize, LEN - PREFIX);
        assert_eq!(MAGIC_V2, 2);
        assert_eq!(
            COMPRESSION_MASK | LOG_APPEND_TIME | TRANSACTIONAL | CONTROL | DELETE_HORIZON,
            ATTRIBUTE_MASK
        );
    }
}
