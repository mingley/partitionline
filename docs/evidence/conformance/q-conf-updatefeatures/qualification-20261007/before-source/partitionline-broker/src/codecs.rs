//! Optional bounded normalization of ordinary compressed magic-2 batches.
//!
//! This foundation does not advertise codecs or append records. Entire encoded
//! outer batches, CRCs, counts and bounded codec headers are checked before
//! allocation/decoding; stream checksums and gzip member boundaries are checked
//! while decoding under the output/member budgets;
//! the existing ordinary-record validator then checks the entire normalized input.
//! Only compression bits, batch length and CRC change. All-uncompressed input is
//! borrowed. Errors drop all partial output; no partial admission token escapes.
//!
//! Standard gzip members (without optional header fields), raw/Xerial Snappy,
//! standard LZ4 frames and standard Zstandard frames are accepted. Concatenated
//! gzip/LZ4/Zstandard frames are supported within one batch. Dictionaries,
//! skippable/legacy frames and trailing bytes are rejected. Some framing rules
//! are stricter than Apache's parser; concatenated LZ4 is a supported extension.
//! Neither relation is a statement about upstream broker admission policy.
//!
//! Work is bounded by encoded/decoded bytes, records, batches and codec units.
//! Workspace admission accounts for the reserved output vector and conservative
//! pinned-backend scratch allowances; it is not an allocator/RSS guarantee. The
//! caller must separately bound retained input, results and concurrent calls.

use std::borrow::Cow;
use std::fmt;
use std::io::Read;

use crate::records;

const HEADER: usize = 61;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_WINDOW: usize = 8 * 1024 * 1024;
const MAX_WORKSPACE: usize = 256 * 1024 * 1024;
const MAX_ITEMS: usize = 1_000_000;
const XERIAL: &[u8; 16] = b"\x82SNAPPY\0\0\0\0\x01\0\0\0\x01";

/// Positive independent limits for one normalization operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    encoded_bytes: usize,
    batch_bytes: usize,
    batches: usize,
    records: usize,
    decoded_batch_bytes: usize,
    normalized_bytes: usize,
    window_bytes: usize,
    workspace_bytes: usize,
    units: usize,
}
impl Limits {
    /// Check positive byte/count limits and their hard ceilings.
    ///
    /// Encoded/normalized/decoded-batch limits include each 61-byte batch header.
    /// Window limit bounds gzip's 32 KiB history, LZ4's advertised block size and
    /// Zstandard's advertised window. Workspace includes output capacity plus
    /// backend scratch; codec units count gzip members, Snappy chunks and LZ4/
    /// Zstandard frames and blocks. Count limits are at most one million, byte
    /// limits 64 MiB, window 8 MiB and workspace 256 MiB.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent explicit resource budgets"
    )]
    pub fn new(
        encoded_bytes: usize,
        batch_bytes: usize,
        batches: usize,
        records: usize,
        decoded_batch_bytes: usize,
        normalized_bytes: usize,
        window_bytes: usize,
        workspace_bytes: usize,
        units: usize,
    ) -> Result<Self, Error> {
        if [
            encoded_bytes,
            batch_bytes,
            decoded_batch_bytes,
            normalized_bytes,
        ]
        .iter()
        .any(|&n| !(1..=MAX_BYTES).contains(&n))
            || [batches, records, units]
                .iter()
                .any(|&n| !(1..=MAX_ITEMS).contains(&n))
            || !(1..=MAX_WINDOW).contains(&window_bytes)
            || !(1..=MAX_WORKSPACE).contains(&workspace_bytes)
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            encoded_bytes,
            batch_bytes,
            batches,
            records,
            decoded_batch_bytes,
            normalized_bytes,
            window_bytes,
            workspace_bytes,
            units,
        })
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            encoded_bytes: 16 * 1024 * 1024,
            batch_bytes: 1024 * 1024,
            batches: 1024,
            records: 65536,
            decoded_batch_bytes: 1024 * 1024,
            normalized_bytes: 16 * 1024 * 1024,
            window_bytes: 4 * 1024 * 1024,
            workspace_bytes: 32 * 1024 * 1024,
            units: 65536,
        }
    }
}
/// A normalization resource budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// Caller-owned encoded input bytes.
    EncodedBytes,
    /// Complete encoded batch size.
    BatchBytes,
    /// Aggregate batch count.
    Batches,
    /// Aggregate declared record count.
    Records,
    /// Complete decoded batch size.
    DecodedBatchBytes,
    /// Aggregate complete normalized batch bytes.
    NormalizedBytes,
    /// Advertised codec window/block size, or gzip's fixed history.
    WindowBytes,
    /// Reserved output capacity plus admitted backend scratch.
    WorkspaceBytes,
    /// Aggregate frame/member/chunk/block count.
    Units,
}
/// Typed errors without retained payloads or backend-generated strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Invalid configuration.
    InvalidLimits,
    /// A configured resource budget was exceeded.
    BudgetExceeded(Budget),
    /// Encoded codec framing, checksum or stream is invalid/unsupported.
    MalformedCodec,
    /// Reserving the bounded output allocation failed.
    Allocation,
    /// Existing ordinary-record admission rejected the input.
    Records(records::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("invalid codec limits"),
            Self::BudgetExceeded(b) => write!(f, "codec budget {b:?} exceeded"),
            Self::MalformedCodec => f.write_str("invalid or unsupported codec framing"),
            Self::Allocation => f.write_str("codec output reservation failed"),
            Self::Records(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {}
impl From<records::Error> for Error {
    fn from(e: records::Error) -> Self {
        Self::Records(e)
    }
}
fn cap(actual: usize, limit: usize, budget: Budget) -> Result<(), Error> {
    if actual > limit {
        Err(Error::BudgetExceeded(budget))
    } else {
        Ok(())
    }
}
fn malformed<T>() -> Result<T, Error> {
    Err(Error::MalformedCodec)
}
fn be32(bytes: &[u8]) -> Result<i32, Error> {
    Ok(i32::from_be_bytes(
        bytes.try_into().map_err(|_| Error::MalformedCodec)?,
    ))
}
fn le32(bytes: &[u8]) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        bytes.try_into().map_err(|_| Error::MalformedCodec)?,
    ))
}
fn take<'a>(bytes: &'a [u8], at: &mut usize, n: usize) -> Result<&'a [u8], Error> {
    let end = at.checked_add(n).ok_or(Error::MalformedCodec)?;
    let result = bytes.get(*at..end).ok_or(Error::MalformedCodec)?;
    *at = end;
    Ok(result)
}
fn unit(units: &mut usize, limits: Limits) -> Result<(), Error> {
    *units += 1;
    cap(*units, limits.units, Budget::Units)
}

/// Fully admitted normalized bytes and counts; no partial result can be created.
#[derive(Debug)]
pub struct Normalized<'a> {
    bytes: Cow<'a, [u8]>,
    batches: usize,
    records: usize,
    headers: usize,
}
impl Normalized<'_> {
    /// Complete ordinary uncompressed bytes, already checked by `records::validate`.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Whether the caller's original bytes were borrowed without an output allocation.
    #[must_use]
    pub fn is_borrowed(&self) -> bool {
        matches!(self.bytes, Cow::Borrowed(_))
    }
    /// Fully validated batch count.
    #[must_use]
    pub fn batch_count(&self) -> usize {
        self.batches
    }
    /// Fully validated record count.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records
    }
    /// Fully validated aggregate header count.
    #[must_use]
    pub fn header_count(&self) -> usize {
        self.headers
    }
}

/// Normalize all batches and validate the entire result before returning admission.
///
/// `record_limits` independently enforces record/field/header budgets after decode.
/// Ordinary compressed producer metadata is checked before decoding. Any failure
/// drops the output and current decoder, including Zstandard's partial output.
/// This synchronous operation has byte/count work bounds, not a wall-clock timeout.
pub fn normalize(
    input: &[u8],
    limits: Limits,
    record_limits: records::Limits,
) -> Result<Normalized<'_>, Error> {
    cap(input.len(), limits.encoded_bytes, Budget::EncodedBytes)?;
    cap(
        HEADER,
        limits.decoded_batch_bytes,
        Budget::DecodedBatchBytes,
    )?;
    let mut cursor = 0;
    let mut batches = 0;
    let mut records = 0;
    let mut units = 0;
    let mut scratch = 0;
    let mut compressed = false;
    let mut reservation = 0usize;
    // No decoder or output allocation exists during this complete outer preflight.
    while cursor < input.len() {
        let at = cursor;
        let batch = batch(input, &mut cursor, limits)?;
        batches += 1;
        cap(batches, limits.batches, Budget::Batches)?;
        let codec = header(batch, at)?;
        let count = usize::try_from(be32(&batch[57..61])?).map_err(|_| Error::MalformedCodec)?;
        if count == 0 {
            return malformed();
        }
        records += count;
        cap(records, limits.records, Budget::Records)?;
        if codec != 0 {
            compressed = true;
            scratch = scratch.max(preflight(codec, &batch[HEADER..], limits, &mut units)?);
            reservation = reservation
                .saturating_add(limits.decoded_batch_bytes)
                .min(limits.normalized_bytes);
        } else {
            cap(
                batch.len(),
                limits.decoded_batch_bytes,
                Budget::DecodedBatchBytes,
            )?;
            reservation = reservation
                .saturating_add(batch.len())
                .min(limits.normalized_bytes);
        }
    }
    if !compressed {
        cap(
            input.len(),
            limits.normalized_bytes,
            Budget::NormalizedBytes,
        )?;
        return admitted(Cow::Borrowed(input), record_limits);
    }
    let capacity = reservation + 32; // pinned zstd decoder's writable slack
    cap(
        capacity + scratch,
        limits.workspace_bytes,
        Budget::WorkspaceBytes,
    )?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|_| Error::Allocation)?;
    cap(
        output.capacity() + scratch,
        limits.workspace_bytes,
        Budget::WorkspaceBytes,
    )?;
    cursor = 0;
    while cursor < input.len() {
        let batch = batch(input, &mut cursor, limits)?;
        let start = output.len();
        cap(
            start + HEADER,
            limits.normalized_bytes,
            Budget::NormalizedBytes,
        )?;
        output.extend_from_slice(&batch[..HEADER]);
        let codec = (batch[22] & 7) as usize;
        let allowance = limits
            .decoded_batch_bytes
            .saturating_sub(HEADER)
            .min(limits.normalized_bytes - output.len());
        let payload = &batch[HEADER..];
        match codec {
            0 => {
                cap(payload.len(), allowance, Budget::DecodedBatchBytes)?;
                output.extend_from_slice(payload);
            }
            1 => gzip(payload, &mut output, allowance, limits, &mut units)?,
            2 => snappy(payload, &mut output, allowance)?,
            3 => lz4(payload, &mut output, allowance, limits)?,
            4 => {
                zstd_rs::Decompressor::new()
                    .decompress(payload, None, allowance, &mut output)
                    .map_err(|e| {
                        if e == zstd_rs::Error::OutputLimit {
                            Error::BudgetExceeded(Budget::DecodedBatchBytes)
                        } else {
                            Error::MalformedCodec
                        }
                    })?;
            }
            _ => return malformed(),
        }
        let length = i32::try_from(output.len() - start - 12).map_err(|_| Error::MalformedCodec)?;
        output[start + 8..start + 12].copy_from_slice(&length.to_be_bytes());
        output[start + 22] &= !7;
        let crc = crc32c::crc32c(&output[start + 21..]);
        output[start + 17..start + 21].copy_from_slice(&crc.to_be_bytes());
    }
    admitted(Cow::Owned(output), record_limits)
}
fn admitted(bytes: Cow<'_, [u8]>, limits: records::Limits) -> Result<Normalized<'_>, Error> {
    let checked = records::validate(&bytes, limits)?;
    let (batches, records, headers) = (
        checked.batch_count(),
        checked.record_count(),
        checked.header_count(),
    );
    Ok(Normalized {
        bytes,
        batches,
        records,
        headers,
    })
}
fn batch<'a>(input: &'a [u8], at: &mut usize, limits: Limits) -> Result<&'a [u8], Error> {
    let prefix = input
        .get(*at..)
        .and_then(|b| b.get(..12))
        .ok_or(Error::MalformedCodec)?;
    let size = usize::try_from(be32(&prefix[8..])?).map_err(|_| Error::MalformedCodec)?;
    let size = size.checked_add(12).ok_or(Error::MalformedCodec)?;
    if size < HEADER {
        return malformed();
    }
    cap(size, limits.batch_bytes, Budget::BatchBytes)?;
    take(input, at, size)
}
fn header(b: &[u8], at: usize) -> Result<u8, Error> {
    if b[16] != 2 {
        return Err(Error::Records(records::Error::Invalid {
            at: at + 16,
            kind: records::Invalid::Magic,
        }));
    }
    let crc = u32::from_be_bytes(b[17..21].try_into().map_err(|_| Error::MalformedCodec)?);
    if crc32c::crc32c(&b[21..]) != crc {
        return Err(Error::Records(records::Error::Invalid {
            at: at + 17,
            kind: records::Invalid::Checksum,
        }));
    }
    let attrs = u16::from_be_bytes(b[21..23].try_into().map_err(|_| Error::MalformedCodec)?);
    if attrs & !0x7f != 0 || attrs & 7 > 4 {
        return malformed();
    }
    for (mask, feature) in [
        (0x20, records::Unsupported::Control),
        (0x10, records::Unsupported::Transactional),
        (0x40, records::Unsupported::DeleteHorizon),
        (8, records::Unsupported::LogAppendTime),
    ] {
        if attrs & mask != 0 {
            return Err(Error::Records(records::Error::Unsupported {
                at: at + 21,
                feature,
            }));
        }
    }
    let producer = i64::from_be_bytes(b[43..51].try_into().map_err(|_| Error::MalformedCodec)?);
    if producer >= 0 {
        return Err(Error::Records(records::Error::Unsupported {
            at: at + 43,
            feature: records::Unsupported::Idempotent,
        }));
    }
    if producer != -1 || b[51..57] != [255; 6] {
        return malformed();
    }
    Ok((attrs & 7) as u8)
}
fn preflight(codec: u8, payload: &[u8], limits: Limits, units: &mut usize) -> Result<usize, Error> {
    match codec {
        1 => {
            gzip_header(payload)?;
            cap(32768, limits.window_bytes, Budget::WindowBytes)?;
            // Pinned miniz_oxide inflater state/history/tables, with conservative margin.
            Ok(512 * 1024)
        }
        2 => {
            snappy_lengths(payload, limits, units)?;
            Ok(0)
        }
        3 => lz4_preflight(payload, limits, units),
        4 => {
            zstd_preflight(payload, limits, units)?;
            Ok(1024 * 1024)
        }
        _ => malformed(),
    }
}
fn gzip_header(b: &[u8]) -> Result<(), Error> {
    // Reject optional name/comment/extra/FHCRC rather than allocating header strings.
    if b.len() < 18 || b[..4] != [31, 139, 8, 0] {
        return malformed();
    }
    Ok(())
}
fn append_read(reader: &mut impl Read, output: &mut Vec<u8>, limit: usize) -> Result<(), Error> {
    let mut buffer = [0u8; 4096];
    loop {
        // One extra byte detects excess without allocating or accepting extra output.
        let n = reader
            .read(&mut buffer[..(limit - output.len()).saturating_add(1).min(4096)])
            .map_err(|_| Error::MalformedCodec)?;
        if n == 0 {
            return Ok(());
        }
        cap(n, limit - output.len(), Budget::DecodedBatchBytes)?;
        output.extend_from_slice(&buffer[..n]);
    }
}
fn gzip(
    mut src: &[u8],
    out: &mut Vec<u8>,
    allowance: usize,
    limits: Limits,
    units: &mut usize,
) -> Result<(), Error> {
    let end = out.len() + allowance;
    while !src.is_empty() {
        unit(units, limits)?;
        gzip_header(src)?;
        let mut decoder = flate2::bufread::GzDecoder::new(src);
        append_read(&mut decoder, out, end)?;
        let remaining = decoder.into_inner();
        if remaining.len() >= src.len() {
            return malformed();
        }
        src = remaining;
    }
    Ok(())
}
fn snappy_chunks<'a>(
    src: &'a [u8],
    mut f: impl FnMut(&'a [u8]) -> Result<(), Error>,
) -> Result<(), Error> {
    if src.starts_with(&XERIAL[..8]) {
        if !src.starts_with(XERIAL) {
            return malformed();
        }
        let mut at = 16;
        while at < src.len() {
            let size = usize::try_from(be32(take(src, &mut at, 4)?)?)
                .map_err(|_| Error::MalformedCodec)?;
            if size == 0 {
                return malformed();
            }
            f(take(src, &mut at, size)?)?;
        }
        Ok(())
    } else {
        f(src)
    }
}
fn snappy_lengths(src: &[u8], limits: Limits, units: &mut usize) -> Result<(), Error> {
    let mut length = HEADER;
    snappy_chunks(src, |chunk| {
        unit(units, limits)?;
        let n = snap::raw::decompress_len(chunk).map_err(|_| Error::MalformedCodec)?;
        length = length.checked_add(n).ok_or(Error::MalformedCodec)?;
        cap(
            length,
            limits.decoded_batch_bytes,
            Budget::DecodedBatchBytes,
        )
    })
}
fn snappy(src: &[u8], out: &mut Vec<u8>, allowance: usize) -> Result<(), Error> {
    let end = out.len() + allowance;
    snappy_chunks(src, |chunk| {
        let n = snap::raw::decompress_len(chunk).map_err(|_| Error::MalformedCodec)?;
        cap(n, end - out.len(), Budget::DecodedBatchBytes)?;
        let start = out.len();
        out.resize(start + n, 0);
        let actual = snap::raw::Decoder::new()
            .decompress(chunk, &mut out[start..])
            .map_err(|_| Error::MalformedCodec)?;
        if actual != n {
            return malformed();
        }
        Ok(())
    })
}
fn lz4_frame(src: &[u8], limits: Limits, units: &mut usize) -> Result<(usize, usize), Error> {
    unit(units, limits)?;
    if src.len() < 7 || src[..4] != [4, 34, 77, 24] {
        return malformed();
    }
    let flags = src[4];
    if flags & 0xc0 != 0x40 || flags & 3 != 0 || src[5] & 0x8f != 0 {
        return malformed();
    }
    let block = match (src[5] >> 4) & 7 {
        4 => 65536,
        5 => 262144,
        6 => 1048576,
        7 => 4194304,
        _ => return malformed(),
    };
    cap(block, limits.window_bytes, Budget::WindowBytes)?;
    let mut at = 6;
    if flags & 8 != 0 {
        let size = u64::from_le_bytes(
            take(src, &mut at, 8)?
                .try_into()
                .map_err(|_| Error::MalformedCodec)?,
        );
        if size > limits.decoded_batch_bytes.saturating_sub(HEADER) as u64 {
            return Err(Error::BudgetExceeded(Budget::DecodedBatchBytes));
        }
    }
    take(src, &mut at, 1)?; // actual header checksum verified by the backend
    loop {
        unit(units, limits)?;
        let n = le32(take(src, &mut at, 4)?)?;
        if n == 0 {
            break;
        }
        let size = (n & 0x7fff_ffff) as usize;
        if size == 0 || size > block {
            return malformed();
        }
        take(src, &mut at, size)?;
        if flags & 16 != 0 {
            take(src, &mut at, 4)?;
        }
    }
    if flags & 4 != 0 {
        take(src, &mut at, 4)?;
    }
    Ok((at, 3 * block + 65536))
}
fn lz4_preflight(mut src: &[u8], limits: Limits, units: &mut usize) -> Result<usize, Error> {
    let mut scratch = 0;
    while !src.is_empty() {
        let (n, current) = lz4_frame(src, limits, units)?;
        scratch = scratch.max(current);
        src = &src[n..];
    }
    Ok(scratch)
}
fn lz4(mut src: &[u8], out: &mut Vec<u8>, allowance: usize, limits: Limits) -> Result<(), Error> {
    let end = out.len() + allowance;
    // Re-scan boundaries without charging the preflighted units twice.
    let mut units = 0;
    while !src.is_empty() {
        let (n, _) = lz4_frame(src, limits, &mut units)?;
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&src[..n]);
        append_read(&mut decoder, out, end)?;
        src = &src[n..];
    }
    Ok(())
}
fn zstd_preflight(mut src: &[u8], limits: Limits, units: &mut usize) -> Result<(), Error> {
    while !src.is_empty() {
        unit(units, limits)?;
        let h = zstd_rs::FrameHeader::parse(src).map_err(|_| Error::MalformedCodec)?;
        if src[4] & 0x1b != 0 || h.dict_id != 0 {
            return malformed();
        }
        if h.window_size > limits.window_bytes as u64 {
            return Err(Error::BudgetExceeded(Budget::WindowBytes));
        }
        if h.content_size
            .is_some_and(|n| n > limits.decoded_batch_bytes.saturating_sub(HEADER) as u64)
        {
            return Err(Error::BudgetExceeded(Budget::DecodedBatchBytes));
        }
        let mut at = h.header_len;
        loop {
            unit(units, limits)?;
            let b = take(src, &mut at, 3)?;
            let value = u32::from_le_bytes([b[0], b[1], b[2], 0]);
            let ty = (value >> 1) & 3;
            let size = (value >> 3) as usize;
            if ty == 3 || size > 131072 || size as u64 > h.window_size {
                return malformed();
            }
            take(src, &mut at, if ty == 1 { 1 } else { size })?;
            if value & 1 != 0 {
                break;
            }
        }
        if h.checksum {
            take(src, &mut at, 4)?;
        }
        src = &src[at..];
    }
    Ok(())
}
