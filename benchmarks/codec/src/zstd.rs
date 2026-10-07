//! Record-batch zstd workloads shared by timing and allocation tools.
use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch, write_record_batch_scratch, BatchHeader,
    CompressScratch, Compression, EncodeRecord, RecordBatch, ZstdLevel,
};

/// Four Kafka batch shapes, each repeated at high and low entropy.
pub const SHAPES: &[(&str, usize, usize, usize, usize)] = &[
    ("small-100", 8, 16, 100, 0),
    ("bulk-100", 512, 16, 100, 0),
    ("headers-1k", 32, 16, 1024, 2),
    ("large-64k", 8, 16, 65536, 0),
];

/// An immutable workload; construction and validation are outside timed operations.
pub struct Case {
    /// Shape, entropy and encoder level.
    pub name: String,
    /// Explicit encoder level; the decoder has no level setting.
    pub level: i32,
    /// Visible key, value and header bytes per batch.
    pub payload_bytes: usize,
    /// Encoded uncompressed record section, excluding Kafka's 61-byte batch header.
    pub section_bytes: usize,
    /// Source records and batch metadata.
    pub batch: RecordBatch,
    /// Header used by the producer's scratch-reusing encoder.
    pub header: BatchHeader,
    /// Checked zstd wire batch for decode measurements.
    pub wire: Bytes,
}

impl Case {
    /// Build the same deterministic records for every level of a shape.
    pub fn new(
        shape: &str,
        count: usize,
        key: usize,
        payload: usize,
        headers: usize,
        entropy: &str,
        level: i32,
    ) -> partitionline::Result<Self> {
        let records = crate::build_records(0xC0DEC, count, key, payload, entropy, headers);
        let payload_bytes = records
            .iter()
            .map(|r| {
                r.key.as_ref().map_or(0, Bytes::len)
                    + r.value.as_ref().map_or(0, Bytes::len)
                    + r.headers
                        .iter()
                        .map(|h| h.key.len() + h.value.as_ref().map_or(0, Bytes::len))
                        .sum::<usize>()
            })
            .sum();
        let plain = RecordBatch::from_records(records);
        let mut uncompressed = BytesMut::new();
        encode_record_batch(&mut uncompressed, &plain)?;
        let batch = plain.with_compression(Compression::Zstd);
        let header = BatchHeader {
            base_offset: batch.base_offset,
            partition_leader_epoch: batch.partition_leader_epoch,
            attributes: batch.attributes,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            count: i32::try_from(count)
                .map_err(|_| partitionline::Error::protocol("benchmark count overflow"))?,
        };
        let mut case = Self {
            name: format!("{shape}/{entropy}/level-{level}"),
            level,
            payload_bytes,
            section_bytes: uncompressed.len() - 61,
            batch,
            header,
            wire: Bytes::new(),
        };
        let mut scratch = case.scratch()?;
        let mut wire = BytesMut::new();
        case.encode(&mut wire, &mut scratch)?;
        case.wire = wire.freeze();
        case.verify()?;
        Ok(case)
    }

    /// Fresh encoder context. Levels are validated by the public client type.
    pub fn scratch(&self) -> partitionline::Result<CompressScratch> {
        let mut scratch = CompressScratch::with_caps(1024 * 1024, 1024 * 1024);
        scratch.set_zstd_level(ZstdLevel::new(self.level)?);
        Ok(scratch)
    }

    /// Write a batch using the actual producer's public encoding path.
    pub fn encode(
        &self,
        output: &mut BytesMut,
        scratch: &mut CompressScratch,
    ) -> partitionline::Result<()> {
        write_record_batch_scratch(
            output,
            &self.header,
            self.batch.records.iter().map(EncodeRecord::from_record),
            scratch,
        )
    }

    /// Decode the prepared wire bytes; cloning Bytes does not copy their backing allocation.
    pub fn decode(&self) -> partitionline::Result<RecordBatch> {
        let mut wire = self.wire.clone();
        let batch = decode_record_batch(&mut wire)?;
        if !wire.is_empty() {
            return Err(partitionline::Error::protocol("benchmark trailing bytes"));
        }
        Ok(batch)
    }

    /// Check fields and the compression ID before timing or counting allocations.
    pub fn verify(&self) -> partitionline::Result<()> {
        if self.wire.len() < 61
            || self.wire.get(22).map(|b| b & 7) != Some(4)
            || self.decode()? != self.batch
        {
            return Err(partitionline::Error::protocol(
                "zstd benchmark preflight failed",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_shapes_levels_entropy_and_reused_contexts_preserve_every_record() {
        for &(name, count, key, payload, headers) in SHAPES {
            for entropy in ["random", "text"] {
                for level in [1, 3, 19] {
                    let case =
                        Case::new(name, count, key, payload, headers, entropy, level).unwrap();
                    let mut scratch = case.scratch().unwrap();
                    let mut out = BytesMut::new();
                    for _ in 0..3 {
                        out.clear();
                        case.encode(&mut out, &mut scratch).unwrap();
                        assert_eq!(out.as_ref(), case.wire.as_ref());
                    }
                    assert_eq!(case.decode().unwrap().records.len(), count);
                    assert!(case.section_bytes >= case.payload_bytes);
                }
            }
        }
    }
}
