//! Deterministic synthetic log for Fetch (KL09-07).
//!
//! Record content derives purely from `(seed, partition, offset)`, so a
//! consumer can verify every ID and hash without shared state. Batches are
//! generated on demand from `(seed, partition, batch_index)`, giving a
//! bounded (configured size) but storage-free log.
//!
//! Compressed codecs are served as valid stored (uncompressed) frames:
//! gzip with stored deflate blocks, xerial snappy-java with literal-only
//! raw blocks, LZ4 with uncompressed blocks. Clients run their real
//! decoders; only the compression math (not representative entropy) is
//! absent.

/// gzip attribute bits.
pub const CODEC_GZIP: u8 = 1;
/// Snappy attribute bits.
pub const CODEC_SNAPPY: u8 = 2;
/// LZ4 attribute bits.
pub const CODEC_LZ4: u8 = 3;

/// Synthetic log configuration.
#[derive(Debug, Clone)]
pub struct SynthConfig {
    /// Seed for all IDs, hashes and filler.
    pub seed: u64,
    /// Records per partition (log size; fetches past it are empty).
    pub records_per_partition: u64,
    /// Records per batch (last batch may be short).
    pub records_per_batch: u32,
    /// Value size in bytes (at least 16: 8 ID + 8 hash).
    pub payload_bytes: usize,
    /// Headers per record.
    pub header_count: u32,
    /// Codec attribute bits (0 = none, 1 = gzip, 2 = snappy, 3 = lz4).
    pub codec: u8,
    /// Every Nth batch (1-based) is aborted; 0 disables aborts.
    pub abort_every: u64,
}

impl Default for SynthConfig {
    fn default() -> Self {
        Self {
            seed: 0x5EED_0001,
            records_per_partition: 10_000_000,
            records_per_batch: 500,
            payload_bytes: 100,
            header_count: 0,
            codec: 0,
            abort_every: 0,
        }
    }
}

impl SynthConfig {
    /// Number of batches in a full partition log (rounded up).
    #[must_use]
    pub fn batches_per_partition(&self) -> u64 {
        if self.records_per_batch == 0 {
            return 0;
        }
        self.records_per_partition
            .div_ceil(u64::from(self.records_per_batch))
    }
}

/// One step of splitmix64.
#[must_use]
pub fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Verification hash for `(seed, partition, offset)`.
#[must_use]
pub fn record_hash(seed: u64, partition: i32, offset: u64) -> u64 {
    let mixed = seed
        .wrapping_add(partition as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(offset);
    splitmix64(mixed)
}

/// Deterministic 128-bit topic id from the topic name (FNV-1a, two lanes).
#[must_use]
pub fn topic_id(name: &str) -> [u8; 16] {
    let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
    let mut h2: u64 = 0x8422_2325_cbf2_9ce4;
    for &b in name.as_bytes() {
        h1 = h1.wrapping_mul(0x100_0000_01b3).wrapping_add(u64::from(b));
        h2 = h2.wrapping_add(u64::from(b)).wrapping_mul(0x100_0000_01b3);
    }
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&h1.to_be_bytes());
    out[8..].copy_from_slice(&h2.to_be_bytes());
    out
}

/// CRC32-IEEE table (gzip trailer).
fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// CRC32-IEEE over `data` (gzip trailer checksum).
#[must_use]
pub fn crc32_ieee(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(crc32_table);
    let mut crc: u32 = 0xffff_ffff;
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xff) as usize;
        crc = table[index] ^ (crc >> 8);
    }
    !crc
}

const XXH_P1: u32 = 0x9E37_79B1;
const XXH_P2: u32 = 0x85EB_CA77;
const XXH_P3: u32 = 0xC2B2_AE3D;
const XXH_P4: u32 = 0x27D4_EB2F;
const XXH_P5: u32 = 0x1656_67B1;

fn xxh_round(acc: u32, input: u32) -> u32 {
    (acc.wrapping_add(input.wrapping_mul(XXH_P2)))
        .rotate_left(13)
        .wrapping_mul(XXH_P1)
}

/// XXH32 (seed 0) for the LZ4 frame header checksum.
#[must_use]
pub fn xxh32(data: &[u8]) -> u32 {
    let len = data.len();
    let mut hash = XXH_P5.wrapping_add(len as u32);
    let (chunks, mut tail) = data.as_chunks::<16>();
    if len >= 16 {
        let mut v1 = XXH_P1.wrapping_add(XXH_P2);
        let mut v2 = XXH_P2;
        let mut v3 = 0u32;
        let mut v4 = XXH_P1.wrapping_neg();
        for chunk in chunks {
            let l =
                |i: usize| u32::from_le_bytes([chunk[i], chunk[i + 1], chunk[i + 2], chunk[i + 3]]);
            v1 = xxh_round(v1, l(0));
            v2 = xxh_round(v2, l(4));
            v3 = xxh_round(v3, l(8));
            v4 = xxh_round(v4, l(12));
        }
        hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
    }
    while tail.len() >= 4 {
        let lane = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
        hash = hash
            .wrapping_add(lane.wrapping_mul(XXH_P3))
            .rotate_left(17)
            .wrapping_mul(XXH_P4);
        tail = &tail[4..];
    }
    for &b in tail {
        hash = hash
            .wrapping_add(u32::from(b).wrapping_mul(XXH_P5))
            .rotate_left(11)
            .wrapping_mul(XXH_P1);
    }
    hash ^= hash >> 15;
    hash = hash.wrapping_mul(XXH_P2);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(XXH_P3);
    hash ^= hash >> 16;
    hash
}

/// Append a zig-zag `varint`.
fn put_varint(out: &mut Vec<u8>, value: i32) {
    let mut v = ((value << 1) ^ (value >> 31)) as u32;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Append a zig-zag `varlong`.
fn put_varlong(out: &mut Vec<u8>, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Key layout: partition `i32` + offset `i64` + hash fragment `u32`.
pub const KEY_LEN: usize = 16;

/// Encode one record value: offset ID + hash + deterministic filler.
fn encode_value(out: &mut Vec<u8>, cfg: &SynthConfig, partition: i32, offset: u64) {
    let hash = record_hash(cfg.seed, partition, offset);
    out.extend_from_slice(&offset.to_be_bytes());
    out.extend_from_slice(&hash.to_be_bytes());
    let mut filler_len = cfg.payload_bytes.saturating_sub(16);
    let mut word = hash;
    while filler_len >= 8 {
        word = splitmix64(word);
        out.extend_from_slice(&word.to_be_bytes());
        filler_len -= 8;
    }
    if filler_len > 0 {
        word = splitmix64(word);
        out.extend_from_slice(&word.to_be_bytes()[..filler_len]);
    }
}

/// Encode one record key: partition + offset + hash fragment.
fn encode_key(out: &mut Vec<u8>, cfg: &SynthConfig, partition: i32, offset: u64) {
    let hash = record_hash(cfg.seed, partition, offset);
    out.extend_from_slice(&partition.to_be_bytes());
    out.extend_from_slice(&offset.to_be_bytes());
    out.extend_from_slice(&(hash as u32).to_be_bytes());
}

/// Encode the uncompressed records block for offsets `[base, base + count)`.
fn encode_records(out: &mut Vec<u8>, cfg: &SynthConfig, partition: i32, base: u64, count: u32) {
    let mut key_buf = Vec::with_capacity(KEY_LEN);
    let mut val_buf = Vec::with_capacity(cfg.payload_bytes);
    for delta in 0..count {
        let offset = base + u64::from(delta);
        let mut body = Vec::new();
        body.push(0); // attributes
        put_varlong(&mut body, 0); // timestamp delta
        put_varint(&mut body, delta as i32); // offset delta
        key_buf.clear();
        encode_key(&mut key_buf, cfg, partition, offset);
        put_varint(&mut body, key_buf.len() as i32);
        body.extend_from_slice(&key_buf);
        val_buf.clear();
        encode_value(&mut val_buf, cfg, partition, offset);
        put_varint(&mut body, val_buf.len() as i32);
        body.extend_from_slice(&val_buf);
        put_varint(&mut body, cfg.header_count as i32);
        for h in 0..cfg.header_count {
            let name = format!("h{h}");
            put_varint(&mut body, name.len() as i32);
            body.extend_from_slice(name.as_bytes());
            let hv =
                splitmix64(record_hash(cfg.seed, partition, offset).wrapping_add(u64::from(h)));
            put_varint(&mut body, 8);
            body.extend_from_slice(&hv.to_be_bytes());
        }
        put_varint(out, body.len() as i32);
        out.extend_from_slice(&body);
    }
}

/// Wrap `data` in a gzip member using stored (uncompressed) deflate blocks.
#[must_use]
pub fn gzip_stored(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 32);
    out.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x00]);
    out.extend_from_slice(&0u32.to_le_bytes()); // mtime
    out.push(0x00); // XFL
    out.push(0x00); // OS
    let mut chunks = data.chunks(65_535);
    // Always emit at least one block, even for empty input.
    let first = chunks.next().unwrap_or(&[]);
    let mut pending = first;
    loop {
        let next = chunks.next();
        let last = next.is_none();
        out.push(u8::from(last)); // BFINAL + BTYPE 00
        let len = pending.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(pending);
        match next {
            Some(chunk) => pending = chunk,
            None => break,
        }
    }
    out.extend_from_slice(&crc32_ieee(data).to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out
}

/// xerial snappy-java magic used by Kafka.
const SNAPPY_JAVA_MAGIC: &[u8] = &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

/// Wrap `data` in xerial snappy-java framing with literal-only raw blocks.
///
/// Kafka does not use Snappy framed format: the payload is the xerial
/// header plus `[u32be clen][raw snappy]` chunks. Each raw block is a
/// varint length followed by literal elements (no back-references).
#[must_use]
pub fn snappy_stored(data: &[u8]) -> Vec<u8> {
    // One raw stream per 256 KiB keeps chunks small.
    let mut out = Vec::with_capacity(data.len() + 64);
    out.extend_from_slice(SNAPPY_JAVA_MAGIC);
    out.extend_from_slice(&1u32.to_be_bytes()); // version
    out.extend_from_slice(&1u32.to_be_bytes()); // compatible
    let pieces: Vec<&[u8]> = if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(262_144).collect()
    };
    for piece in pieces {
        let mut raw = Vec::with_capacity(piece.len() + 8);
        let mut len = piece.len() as u32;
        // Varint uncompressed length.
        loop {
            let mut byte = (len & 0x7f) as u8;
            len >>= 7;
            if len != 0 {
                byte |= 0x80;
            }
            raw.push(byte);
            if len == 0 {
                break;
            }
        }
        // Literal elements of at most 65536 bytes (2-byte length form).
        for lit in piece.chunks(65_536) {
            let n = lit.len();
            if n <= 60 {
                raw.push(((n - 1) << 2) as u8);
            } else {
                raw.push((61 << 2) as u8);
                raw.extend_from_slice(&((n - 1) as u16).to_le_bytes());
            }
            raw.extend_from_slice(lit);
        }
        out.extend_from_slice(&(raw.len() as u32).to_be_bytes());
        out.extend_from_slice(&raw);
    }
    out
}

/// Wrap `data` in an LZ4 frame using uncompressed blocks only.
#[must_use]
pub fn lz4_stored(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 32);
    out.extend_from_slice(&[0x04, 0x22, 0x4d, 0x18]); // magic
    out.push(0x60); // FLG: v01, independent blocks, no checksums
    out.push(0x40); // BD: 64 KiB max block
    let header = [0x60u8, 0x40u8];
    out.push(((xxh32(&header) >> 8) & 0xff) as u8);
    for chunk in data.chunks(65_536) {
        let size = chunk.len() as u32 | 0x8000_0000; // uncompressed flag
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(chunk);
    }
    out.extend_from_slice(&0u32.to_le_bytes()); // end mark
    out
}

/// A generated batch plus its log coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynthBatch {
    /// Encoded batch bytes (records field already codec-wrapped).
    pub bytes: Vec<u8>,
    /// First offset.
    pub base_offset: u64,
    /// Record count.
    pub count: u32,
    /// Whether this batch is aborted (transactional bit + abort entry).
    pub aborted: bool,
    /// Producer id (`-1` for plain batches).
    pub producer_id: i64,
}

/// First producer id handed to aborted batches; each aborted batch takes
/// `ABORT_PID_BASE + batch_index` (unique per partition).
pub const ABORT_PID_BASE: i64 = 1_000_000;

/// Generate batch `batch_index` of `partition`, or `None` past log end.
#[must_use]
pub fn encode_batch(cfg: &SynthConfig, partition: i32, batch_index: u64) -> Option<SynthBatch> {
    use crate::crc32c;
    if cfg.records_per_batch == 0 {
        return None;
    }
    let base = batch_index.saturating_mul(u64::from(cfg.records_per_batch));
    if base >= cfg.records_per_partition {
        return None;
    }
    let remaining = cfg.records_per_partition - base;
    let count = u64::from(cfg.records_per_batch).min(remaining) as u32;
    let aborted = cfg.abort_every > 0 && (batch_index + 1).is_multiple_of(cfg.abort_every);
    let mut records = Vec::new();
    encode_records(&mut records, cfg, partition, base, count);
    let payload = match cfg.codec {
        CODEC_GZIP => gzip_stored(&records),
        CODEC_SNAPPY => snappy_stored(&records),
        CODEC_LZ4 => lz4_stored(&records),
        _ => records,
    };
    let attributes: i16 = i16::from(cfg.codec & 0x07) | if aborted { 0x10 } else { 0 };
    let producer_id = if aborted {
        ABORT_PID_BASE + (batch_index as i64)
    } else {
        -1
    };
    let mut out = Vec::with_capacity(61 + payload.len());
    out.extend_from_slice(&(base as i64).to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes()); // batch length placeholder
    out.extend_from_slice(&0i32.to_be_bytes()); // leader epoch
    out.push(2); // magic
    out.extend_from_slice(&0u32.to_be_bytes()); // crc placeholder
    let crc_start = out.len();
    out.extend_from_slice(&attributes.to_be_bytes());
    out.extend_from_slice(&(count as i32 - 1).to_be_bytes());
    let ts = 1_700_000_000_000i64 + base as i64;
    out.extend_from_slice(&ts.to_be_bytes());
    out.extend_from_slice(&ts.to_be_bytes());
    out.extend_from_slice(&producer_id.to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes()); // epoch
    out.extend_from_slice(&0i32.to_be_bytes()); // base sequence
    out.extend_from_slice(&(count as i32).to_be_bytes());
    out.extend_from_slice(&payload);
    let batch_len = (out.len() - 12) as i32;
    out[8..12].copy_from_slice(&batch_len.to_be_bytes());
    let crc = crc32c(&out[crc_start..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());
    Some(SynthBatch {
        bytes: out,
        base_offset: base,
        count,
        aborted,
        producer_id,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Read stored-deflate gzip members (test-only structural check).
    fn gunzip_stored(member: &[u8]) -> Vec<u8> {
        assert_eq!(&member[..3], &[0x1f, 0x8b, 0x08]);
        let flags = member[3];
        assert_eq!(flags & 0xe0, 0, "reserved gzip flags");
        let mut pos = 10;
        if flags & 0x04 != 0 {
            let xlen = u16::from_le_bytes([member[pos], member[pos + 1]]) as usize;
            pos += 2 + xlen;
        }
        if flags & 0x08 != 0 {
            while member[pos] != 0 {
                pos += 1;
            }
            pos += 1;
        }
        if flags & 0x10 != 0 {
            while member[pos] != 0 {
                pos += 1;
            }
            pos += 1;
        }
        if flags & 0x02 != 0 {
            pos += 2;
        }
        let mut out = Vec::new();
        loop {
            let head = member[pos];
            pos += 1;
            let last = head & 0x01 != 0;
            assert_eq!(head & 0x06, 0, "only stored blocks");
            let len = u16::from_le_bytes([member[pos], member[pos + 1]]) as usize;
            let nlen = u16::from_le_bytes([member[pos + 2], member[pos + 3]]);
            pos += 4;
            assert_eq!(nlen, !len as u16);
            out.extend_from_slice(&member[pos..pos + len]);
            pos += len;
            if last {
                break;
            }
        }
        assert_eq!(
            u32::from_le_bytes([
                member[pos],
                member[pos + 1],
                member[pos + 2],
                member[pos + 3]
            ]),
            crc32_ieee(&out)
        );
        assert_eq!(
            u32::from_le_bytes([
                member[pos + 4],
                member[pos + 5],
                member[pos + 6],
                member[pos + 7]
            ]),
            out.len() as u32
        );
        out
    }

    /// Read xerial snappy-java with literal-only raw blocks (test check).
    fn unsnappy_stored(frame: &[u8]) -> Vec<u8> {
        assert_eq!(&frame[..8], SNAPPY_JAVA_MAGIC);
        assert_eq!(&frame[8..12], &1u32.to_be_bytes());
        assert_eq!(&frame[12..16], &1u32.to_be_bytes());
        let mut pos = 16;
        let mut out = Vec::new();
        while pos < frame.len() {
            let clen =
                u32::from_be_bytes([frame[pos], frame[pos + 1], frame[pos + 2], frame[pos + 3]])
                    as usize;
            pos += 4;
            let block = &frame[pos..pos + clen];
            pos += clen;
            // Raw stream: varint length, then literal elements only.
            let mut bp = 0;
            let mut unc_len = 0u32;
            let mut shift = 0;
            loop {
                let byte = block[bp];
                bp += 1;
                unc_len |= u32::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            let start = out.len();
            while bp < block.len() {
                let tag = block[bp];
                bp += 1;
                assert_eq!(tag & 0x03, 0, "only literals");
                let len = match tag >> 2 {
                    v if v < 60 => v as usize + 1,
                    60 => {
                        let n = block[bp] as usize;
                        bp += 1;
                        n + 1
                    }
                    61 => {
                        let n = u16::from_le_bytes([block[bp], block[bp + 1]]) as usize;
                        bp += 2;
                        n + 1
                    }
                    v => panic!("unexpected literal length form {v}"),
                };
                out.extend_from_slice(&block[bp..bp + len]);
                bp += len;
            }
            assert_eq!(out.len() - start, unc_len as usize);
        }
        out
    }

    /// Read uncompressed-block LZ4 frames (test-only structural check).
    fn unlz4_stored(frame: &[u8]) -> Vec<u8> {
        assert_eq!(&frame[..4], &[0x04, 0x22, 0x4d, 0x18]);
        assert_eq!(frame[4], 0x60);
        assert_eq!(frame[5], 0x40);
        assert_eq!(frame[6], ((xxh32(&frame[4..6]) >> 8) & 0xff) as u8);
        let mut pos = 7;
        let mut out = Vec::new();
        loop {
            let size =
                u32::from_le_bytes([frame[pos], frame[pos + 1], frame[pos + 2], frame[pos + 3]]);
            pos += 4;
            if size == 0 {
                break;
            }
            assert_ne!(size & 0x8000_0000, 0, "only uncompressed blocks");
            let len = (size & 0x7fff_ffff) as usize;
            out.extend_from_slice(&frame[pos..pos + len]);
            pos += len;
        }
        assert_eq!(pos, frame.len());
        out
    }

    #[test]
    fn hash_vectors() {
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
        assert_eq!(xxh32(b""), 0x02CC_5D05);
        // Deterministic and input-sensitive.
        assert_eq!(record_hash(1, 0, 0), record_hash(1, 0, 0));
        assert_ne!(record_hash(1, 0, 0), record_hash(1, 0, 1));
        assert_ne!(record_hash(1, 0, 0), record_hash(2, 0, 0));
        assert_ne!(record_hash(1, 0, 0), record_hash(1, 1, 0));
        assert_ne!(topic_id("a"), topic_id("b"));
        assert_eq!(topic_id("t"), topic_id("t"));
    }

    #[test]
    fn stored_codecs_roundtrip_structurally() {
        // Exercise multi-block paths (> 64 KiB forces several blocks).
        let mut data = vec![0u8; 200_000];
        let mut word = 0x1234_5678_9abc_def0u64;
        for chunk in data.chunks_mut(8) {
            word = splitmix64(word);
            let bytes = word.to_be_bytes();
            let n = chunk.len().min(8);
            chunk[..n].copy_from_slice(&bytes[..n]);
        }
        assert_eq!(gunzip_stored(&gzip_stored(&data)), data);
        assert_eq!(unsnappy_stored(&snappy_stored(&data)), data);
        assert_eq!(unlz4_stored(&lz4_stored(&data)), data);
        assert_eq!(gunzip_stored(&gzip_stored(&[])), Vec::<u8>::new());
        assert_eq!(unsnappy_stored(&snappy_stored(&[])), Vec::<u8>::new());
        assert_eq!(unlz4_stored(&lz4_stored(&[])), Vec::<u8>::new());
    }

    #[test]
    fn batches_are_deterministic_and_bounded() {
        let cfg = SynthConfig {
            seed: 7,
            records_per_partition: 1200,
            records_per_batch: 500,
            payload_bytes: 100,
            header_count: 2,
            codec: 0,
            abort_every: 0,
        };
        assert_eq!(cfg.batches_per_partition(), 3);
        let b0 = encode_batch(&cfg, 0, 0).unwrap();
        assert_eq!(b0.base_offset, 0);
        assert_eq!(b0.count, 500);
        assert!(!b0.aborted);
        assert_eq!(b0.producer_id, -1);
        assert_eq!(encode_batch(&cfg, 0, 0).unwrap(), b0);
        assert_ne!(
            encode_batch(&cfg, 1, 0).unwrap().bytes,
            b0.bytes,
            "partition alters content"
        );
        let b2 = encode_batch(&cfg, 0, 2).unwrap();
        assert_eq!((b2.base_offset, b2.count), (1000, 200));
        assert!(encode_batch(&cfg, 0, 3).is_none());
    }

    #[test]
    fn abort_every_marks_batches() {
        let cfg = SynthConfig {
            records_per_partition: 10_000,
            records_per_batch: 100,
            abort_every: 4,
            ..SynthConfig::default()
        };
        for i in 0..8 {
            let batch = encode_batch(&cfg, 0, i).unwrap();
            assert_eq!(batch.aborted, (i + 1) % 4 == 0, "batch {i}");
            if batch.aborted {
                assert_eq!(batch.producer_id, ABORT_PID_BASE + i as i64);
            }
        }
    }
}
