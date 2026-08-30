//! Classic and compact Kafka primitive codecs.
//!
//! [`size_of_unsigned_varint`], [`size_of_varint`], [`size_of_unsigned_varlong`],
//! and [`size_of_varlong`] are Java `ByteUtils.sizeOfUnsignedVarint` /
//! `sizeOfVarint` / `sizeOfUnsignedVarlong` / `sizeOfVarlong`. The unsigned
//! helpers take a signed value and reinterpret the bits, so `-1` is five
//! bytes (varint) or ten (varlong). A fifth unsigned-varint continuation byte
//! is Java `ByteUtils.illegalVarintException`; a tenth unsigned-varlong
//! continuation byte is `illegalVarlongException`. [`utf8_length`] is Java
//! `Utils.utf8Length`. [`to_32_bit_field`] / [`from_32_bit_field`] are Java
//! `Utils.to32BitField` / `from32BitField`. [`is_blank`] / [`replace_suffix`]
//! are Java `Utils.isBlank` / `replaceSuffix`. [`entries_with_prefix`] /
//! [`entries_with_prefix_matching`] are Java `Utils.entriesWithPrefix`.
//! [`is_equal_constant_time`] is Java `Utils.isEqualConstantTime`.
//! [`require`] / [`require_message`] are Java `Utils.require`.

use std::collections::{HashMap, HashSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

/// Convert `usize` to `i16` (classic string/array lengths).
pub fn i16_from_usize(n: usize) -> Result<i16> {
    i16::try_from(n).map_err(|_| Error::protocol("length exceeds i16"))
}

/// Convert `usize` to `i32` (classic bytes/array lengths).
pub fn i32_from_usize(n: usize) -> Result<i32> {
    i32::try_from(n).map_err(|_| Error::protocol("length exceeds i32"))
}

/// Convert `usize` to `u32` (compact lengths).
pub fn u32_from_usize(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::protocol("length exceeds u32"))
}

/// Convert `usize` to `i64`.
pub fn i64_from_usize(n: usize) -> Result<i64> {
    i64::try_from(n).map_err(|_| Error::protocol("length exceeds i64"))
}

/// Convert a non-negative `i16` length to `usize`.
pub fn usize_from_i16(n: i16) -> Result<usize> {
    usize::try_from(n).map_err(|_| Error::protocol("negative length"))
}

/// Convert a non-negative `i32` length to `usize`.
pub fn usize_from_i32(n: i32) -> Result<usize> {
    usize::try_from(n).map_err(|_| Error::protocol("negative length"))
}

/// Convert `u32` to `usize`.
pub fn usize_from_u32(n: u32) -> Result<usize> {
    usize::try_from(n).map_err(|_| Error::protocol("length exceeds usize"))
}

fn zigzag_i32(v: i32) -> u32 {
    u32::from_ne_bytes((v.wrapping_shl(1) ^ (v >> 31)).to_ne_bytes())
}

fn unzigzag_i32(n: u32) -> i32 {
    let hi = i32::from_ne_bytes((n >> 1).to_ne_bytes());
    let lo = if n & 1 == 0 { 0 } else { -1 };
    hi ^ lo
}

fn zigzag_i64(v: i64) -> u64 {
    u64::from_ne_bytes((v.wrapping_shl(1) ^ (v >> 63)).to_ne_bytes())
}

fn unzigzag_i64(n: u64) -> i64 {
    let hi = i64::from_ne_bytes((n >> 1).to_ne_bytes());
    let lo = if n & 1 == 0 { 0 } else { -1 };
    hi ^ lo
}

fn varint_byte_u32(v: u32, more: bool) -> u8 {
    let low = u8::try_from(v & 0x7f).unwrap_or(0);
    if more {
        low | 0x80
    } else {
        low
    }
}

fn varint_byte_u64(v: u64, more: bool) -> u8 {
    let low = u8::try_from(v & 0x7f).unwrap_or(0);
    if more {
        low | 0x80
    } else {
        low
    }
}

/// Fail if `buf` has fewer than `n` remaining bytes.
pub fn need<B: Buf>(buf: &B, n: usize) -> Result<()> {
    if buf.remaining() < n {
        Err(Error::protocol(format!(
            "need {n} bytes, have {}",
            buf.remaining()
        )))
    } else {
        Ok(())
    }
}

/// Encoded size of an unsigned varint (`u32` bits).
pub fn unsigned_varint_size(mut v: u32) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Encoded size of an unsigned varlong (`u64` bits).
pub fn unsigned_varlong_size(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Encoded size of a zigzag varint.
pub fn varint_size(v: i32) -> usize {
    unsigned_varint_size(zigzag_i32(v))
}

/// Encoded size of a zigzag varlong.
pub fn varlong_size(v: i64) -> usize {
    unsigned_varlong_size(zigzag_i64(v))
}

fn encoded_len_i32(n: usize) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// Java `ByteUtils.sizeOfUnsignedVarint` (signed bits as unsigned varint).
pub fn size_of_unsigned_varint(value: i32) -> i32 {
    encoded_len_i32(unsigned_varint_size(u32::from_ne_bytes(
        value.to_ne_bytes(),
    )))
}

/// Java `ByteUtils.sizeOfVarint` (zigzag).
pub fn size_of_varint(value: i32) -> i32 {
    encoded_len_i32(varint_size(value))
}

/// Java `ByteUtils.sizeOfUnsignedVarlong` (signed bits as unsigned varlong).
pub fn size_of_unsigned_varlong(value: i64) -> i32 {
    encoded_len_i32(unsigned_varlong_size(u64::from_ne_bytes(
        value.to_ne_bytes(),
    )))
}

/// Java `ByteUtils.sizeOfVarlong` (zigzag).
pub fn size_of_varlong(value: i64) -> i32 {
    encoded_len_i32(varlong_size(value))
}

/// Java `Utils.utf8Length`.
///
/// For a valid Unicode string this is the UTF-8 byte length. Java walks
/// UTF-16 code units; unpaired surrogates cannot appear in Rust `&str`.
#[must_use]
pub fn utf8_length(s: &str) -> i32 {
    encoded_len_i32(s.len())
}

/// Java `String.trim` emptiness: strip UTF-16 code units at or below U+0020.
fn java_trim_is_empty(s: &str) -> bool {
    s.trim_matches(|c: char| c <= '\u{20}').is_empty()
}

/// Java `Utils.isBlank`.
///
/// `None` is Java `null`. Java `String.trim` strips code units at or below
/// U+0020 (not Unicode White_Space), so NBSP is not blank.
#[must_use]
pub fn is_blank(s: Option<&str>) -> bool {
    s.is_none_or(java_trim_is_empty)
}

/// Java `Utils.replaceSuffix`.
///
/// When `s` does not end with `old_suffix`, this is [`Error::protocol`]
/// (`Expected string to end with … but string is …`).
pub fn replace_suffix(s: &str, old_suffix: &str, new_suffix: &str) -> Result<String> {
    match s.strip_suffix(old_suffix) {
        Some(stem) => Ok(format!("{stem}{new_suffix}")),
        None => Err(Error::protocol(format!(
            "Expected string to end with {old_suffix} but string is {s}"
        ))),
    }
}

/// Java `Utils.entriesWithPrefix(map, prefix)` (strip the prefix; omit keys
/// that equal the prefix).
#[must_use]
pub fn entries_with_prefix<V: Clone>(map: &HashMap<String, V>, prefix: &str) -> HashMap<String, V> {
    entries_with_prefix_matching(map, prefix, true, false)
}

/// Java `Utils.entriesWithPrefix(map, prefix, strip, allowMatchingLength)`.
#[must_use]
pub fn entries_with_prefix_matching<V: Clone>(
    map: &HashMap<String, V>,
    prefix: &str,
    strip: bool,
    allow_matching_length: bool,
) -> HashMap<String, V> {
    let mut result = HashMap::new();
    for (key, value) in map {
        let Some(rest) = key.strip_prefix(prefix) else {
            continue;
        };
        if !allow_matching_length && rest.is_empty() {
            continue;
        }
        let out_key = if strip { rest.to_string() } else { key.clone() };
        result.extend([(out_key, value.clone())]);
    }
    result
}

/// Java `Utils.isEqualConstantTime`.
///
/// `None` is Java `null`. Both `None` is true (Java `==`). When `second` is
/// empty, Java returns whether `first` is empty without scanning. Otherwise
/// every element of `first` is compared; indexes past `second` reuse the
/// first element of `second`. Timing depends only on the length of `first`.
#[must_use]
pub fn is_equal_constant_time(first: Option<&[u16]>, second: Option<&[u16]>) -> bool {
    match (first, second) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(a), Some(b)) => equal_constant_time_chars(a, b),
    }
}

fn equal_constant_time_chars(first: &[u16], second: &[u16]) -> bool {
    if std::ptr::eq(first, second) {
        return true;
    }
    if second.is_empty() {
        return first.is_empty();
    }
    let mut matches = first.len() == second.len();
    for (i, &ai) in first.iter().enumerate() {
        let bj = if i < second.len() {
            second.get(i)
        } else {
            second.first()
        };
        if bj.copied() != Some(ai) {
            matches = false;
        }
    }
    matches
}

/// Java `Utils.require(boolean)`.
///
/// Failure is [`Error::protocol`] (`requirement failed`).
pub fn require(requirement: bool) -> Result<()> {
    require_message(requirement, "requirement failed")
}

/// Java `Utils.require(boolean, String)`.
///
/// Failure is [`Error::protocol`] with `error_message`.
pub fn require_message(requirement: bool, error_message: &str) -> Result<()> {
    if requirement {
        Ok(())
    } else {
        Err(Error::protocol(error_message))
    }
}

fn check_range(i: i8) -> Result<u8> {
    if i > 31 {
        return Err(Error::protocol(format!("out of range: i>31, i = {i}")));
    }
    if i < 0 {
        return Err(Error::protocol(format!("out of range: i<0, i = {i}")));
    }
    Ok(u8::try_from(i).unwrap_or(0))
}

/// Java `Utils.to32BitField`.
///
/// Each value must be `0..=31`. Out of range is [`Error::protocol`]
/// (`out of range: i>31` / `i<0`).
pub fn to_32_bit_field(bytes: impl IntoIterator<Item = i8>) -> Result<i32> {
    let mut value = 0i32;
    for b in bytes {
        let shift = u32::from(check_range(b)?);
        value |= i32::from_ne_bytes((1u32 << shift).to_ne_bytes());
    }
    Ok(value)
}

/// Java `Utils.from32BitField`.
#[must_use]
pub fn from_32_bit_field(int_value: i32) -> HashSet<i8> {
    let mut result = HashSet::new();
    let mut itr = u32::from_ne_bytes(int_value.to_ne_bytes());
    let mut count: u8 = 0;
    while itr != 0 {
        if itr & 1 != 0 {
            result.extend([i8::try_from(count).unwrap_or(0)]);
        }
        itr >>= 1;
        count = count.saturating_add(1);
    }
    result
}

/// Java `ByteUtils.illegalVarintException`.
fn illegal_varint_exception(value: u32) -> Error {
    Error::protocol(format!(
        "Varint is too long, the most significant bit in the 5th byte is set, converted value: {value:x}"
    ))
}

/// Java `ByteUtils.illegalVarlongException`.
fn illegal_varlong_exception(value: u64) -> Error {
    Error::protocol(format!(
        "Varlong is too long, most significant bit in the 10th byte is set, converted value: {value:x}"
    ))
}

/// Write an unsigned varint (compact protocol lengths).
pub fn put_unsigned_varint(buf: &mut BytesMut, mut v: u32) {
    while v >= 0x80 {
        buf.put_u8(varint_byte_u32(v, true));
        v >>= 7;
    }
    buf.put_u8(varint_byte_u32(v, false));
}

/// Read an unsigned varint.
///
/// Java `ByteUtils.readUnsignedVarint`. A fifth continuation byte is
/// `illegalVarintException`.
pub fn get_unsigned_varint<B: Buf>(buf: &mut B) -> Result<u32> {
    let mut result = 0u32;
    for i in 0..4 {
        need(buf, 1)?;
        let b = buf.get_u8();
        result |= u32::from(b & 0x7f) << (i * 7);
        if b & 0x80 == 0 {
            return Ok(result);
        }
    }
    need(buf, 1)?;
    // Java `result |= (tmp = buffer.get()) << 28;` then `if (tmp < 0)`.
    let tmp = i8::from_ne_bytes([buf.get_u8()]);
    result |= u32::from_ne_bytes(i32::from(tmp).wrapping_shl(28).to_ne_bytes());
    if tmp < 0 {
        return Err(illegal_varint_exception(result));
    }
    Ok(result)
}

/// Write a zigzag varint (record lengths).
pub fn put_varint(buf: &mut BytesMut, v: i32) {
    put_unsigned_varint(buf, zigzag_i32(v));
}

/// Read a zigzag varint.
pub fn get_varint<B: Buf>(buf: &mut B) -> Result<i32> {
    Ok(unzigzag_i32(get_unsigned_varint(buf)?))
}

/// Write an unsigned varlong.
pub fn put_unsigned_varlong(buf: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        buf.put_u8(varint_byte_u64(v, true));
        v >>= 7;
    }
    buf.put_u8(varint_byte_u64(v, false));
}

/// Read an unsigned varlong.
///
/// Java `ByteUtils.readUnsignedVarlong`. A tenth continuation byte is
/// `illegalVarlongException`.
pub fn get_unsigned_varlong<B: Buf>(buf: &mut B) -> Result<u64> {
    let mut result = 0u64;
    for i in 0..9 {
        need(buf, 1)?;
        let b = buf.get_u8();
        result |= u64::from(b & 0x7f) << (i * 7);
        if b & 0x80 == 0 {
            return Ok(result);
        }
    }
    need(buf, 1)?;
    let b = buf.get_u8();
    result |= u64::from(b & 0x7f) << 63;
    if b & 0x80 != 0 {
        return Err(illegal_varlong_exception(result));
    }
    Ok(result)
}

/// Write a zigzag varlong.
pub fn put_varlong(buf: &mut BytesMut, v: i64) {
    put_unsigned_varlong(buf, zigzag_i64(v));
}

/// Read a zigzag varlong.
pub fn get_varlong<B: Buf>(buf: &mut B) -> Result<i64> {
    Ok(unzigzag_i64(get_unsigned_varlong(buf)?))
}

/// Write a classic nullable STRING (`i16` length, `-1` is null).
pub fn put_classic_nullable_string(buf: &mut BytesMut, s: Option<&str>) -> Result<()> {
    match s {
        None => buf.put_i16(-1),
        Some(s) => {
            buf.put_i16(i16_from_usize(s.len())?);
            buf.extend_from_slice(s.as_bytes());
        }
    }
    Ok(())
}

/// Read a classic nullable STRING.
pub fn get_classic_nullable_string<B: Buf>(buf: &mut B) -> Result<Option<String>> {
    need(buf, 2)?;
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let len = usize_from_i16(len)?;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| Error::protocol(e.to_string()))
}

/// Write a compact STRING (`unsigned varint` of `n+1`, `0` is null).
pub fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) -> Result<()> {
    match s {
        None => put_unsigned_varint(buf, 0),
        Some(s) => {
            let n = u32_from_usize(s.len())?
                .checked_add(1)
                .ok_or_else(|| Error::protocol("compact string overflow"))?;
            put_unsigned_varint(buf, n);
            buf.extend_from_slice(s.as_bytes());
        }
    }
    Ok(())
}

/// Read a compact STRING.
pub fn get_compact_string<B: Buf>(buf: &mut B) -> Result<Option<String>> {
    let n = get_unsigned_varint(buf)?;
    if n == 0 {
        return Ok(None);
    }
    let len = usize_from_u32(n - 1)?;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| Error::protocol(e.to_string()))
}

/// Write STRING: compact when `flexible`, otherwise classic nullable.
pub fn put_string(buf: &mut BytesMut, flexible: bool, s: Option<&str>) -> Result<()> {
    if flexible {
        put_compact_string(buf, s)?;
    } else {
        put_classic_nullable_string(buf, s)?;
    }
    Ok(())
}

/// Read STRING: compact when `flexible`, otherwise classic nullable.
pub fn get_string<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<String>> {
    if flexible {
        get_compact_string(buf)
    } else {
        get_classic_nullable_string(buf)
    }
}

/// Write compact BYTES (`unsigned varint` of `n+1`, `0` is null).
pub fn put_compact_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) -> Result<()> {
    match bytes {
        None => put_unsigned_varint(buf, 0),
        Some(b) => {
            let n = u32_from_usize(b.len())?
                .checked_add(1)
                .ok_or_else(|| Error::protocol("compact bytes overflow"))?;
            put_unsigned_varint(buf, n);
            buf.extend_from_slice(b);
        }
    }
    Ok(())
}

/// Read compact BYTES as a `Bytes` view when the buffer is frozen.
pub fn take_compact_bytes<B: Buf>(buf: &mut B) -> Result<Option<Bytes>> {
    let n = get_unsigned_varint(buf)?;
    if n == 0 {
        return Ok(None);
    }
    let len = usize_from_u32(n - 1)?;
    need(buf, len)?;
    Ok(Some(buf.copy_to_bytes(len)))
}

/// Read compact BYTES into a `Vec`.
pub fn get_compact_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>> {
    Ok(take_compact_bytes(buf)?.map(|b| b.to_vec()))
}

/// Write classic BYTES (`i32` length, `-1` is null).
pub fn put_classic_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) -> Result<()> {
    match bytes {
        None => buf.put_i32(-1),
        Some(b) => {
            buf.put_i32(i32_from_usize(b.len())?);
            buf.extend_from_slice(b);
        }
    }
    Ok(())
}

/// Length-prefixed classic bytes as a `Bytes` view.
///
/// When `buf` is `Bytes` (a frozen fetch frame), this is a refcount bump, not a
/// memcpy. Slice-backed test buffers still copy once.
pub fn take_classic_bytes<B: Buf>(buf: &mut B) -> Result<Option<Bytes>> {
    need(buf, 4)?;
    let len = buf.get_i32();
    if len < 0 {
        return Ok(None);
    }
    let len = usize_from_i32(len)?;
    need(buf, len)?;
    Ok(Some(buf.copy_to_bytes(len)))
}

/// Read classic BYTES into a `Vec`.
pub fn get_classic_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>> {
    Ok(take_classic_bytes(buf)?.map(|b| b.to_vec()))
}

/// Write BYTES: compact when `flexible`, otherwise classic.
pub fn put_bytes(buf: &mut BytesMut, flexible: bool, bytes: Option<&[u8]>) -> Result<()> {
    if flexible {
        put_compact_bytes(buf, bytes)?;
    } else {
        put_classic_bytes(buf, bytes)?;
    }
    Ok(())
}

/// Read BYTES: compact when `flexible`, otherwise classic.
pub fn get_bytes<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<Vec<u8>>> {
    if flexible {
        get_compact_bytes(buf)
    } else {
        get_classic_bytes(buf)
    }
}

/// Write an array length: compact `n+1` when `flexible`, otherwise `i32` (`-1` is null).
pub fn put_array_len(buf: &mut BytesMut, flexible: bool, len: Option<usize>) -> Result<()> {
    match len {
        None => {
            if flexible {
                put_unsigned_varint(buf, 0);
            } else {
                buf.put_i32(-1);
            }
        }
        Some(n) => {
            if flexible {
                let v = u32_from_usize(n)?
                    .checked_add(1)
                    .ok_or_else(|| Error::protocol("compact array overflow"))?;
                put_unsigned_varint(buf, v);
            } else {
                buf.put_i32(i32_from_usize(n)?);
            }
        }
    }
    Ok(())
}

/// Read an array length. `None` is a null array.
pub fn get_array_len<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<usize>> {
    if flexible {
        let n = get_unsigned_varint(buf)?;
        if n == 0 {
            Ok(None)
        } else {
            Ok(Some(usize_from_u32(n - 1)?))
        }
    } else {
        need(buf, 4)?;
        let n = buf.get_i32();
        if n < 0 {
            Ok(None)
        } else {
            Ok(Some(usize_from_i32(n)?))
        }
    }
}

/// Skip tagged fields (flexible protocol). Unknown tags are discarded.
pub fn skip_tagged_fields<B: Buf>(buf: &mut B) -> Result<()> {
    let n = get_unsigned_varint(buf)?;
    for _ in 0..n {
        let _tag = get_unsigned_varint(buf)?;
        let size = usize_from_u32(get_unsigned_varint(buf)?)?;
        need(buf, size)?;
        buf.advance(size);
    }
    Ok(())
}

/// Write tagged fields. `fields` must be strictly ascending by tag.
pub fn put_tagged_fields<T: AsRef<[u8]>>(buf: &mut BytesMut, fields: &[(u32, T)]) -> Result<()> {
    put_unsigned_varint(buf, u32_from_usize(fields.len())?);
    let mut prev: Option<u32> = None;
    for (tag, value) in fields {
        let tag = *tag;
        if prev.is_some_and(|p| tag <= p) {
            return Err(Error::protocol(
                "tagged fields must be in ascending tag order",
            ));
        }
        prev = Some(tag);
        let bytes = value.as_ref();
        put_unsigned_varint(buf, tag);
        put_unsigned_varint(buf, u32_from_usize(bytes.len())?);
        buf.extend_from_slice(bytes);
    }
    Ok(())
}

/// Read tagged fields. Tags must be strictly ascending.
pub fn get_tagged_fields<B: Buf>(buf: &mut B) -> Result<Vec<(u32, Bytes)>> {
    let n = get_unsigned_varint(buf)?;
    let mut out = Vec::with_capacity(usize_from_u32(n)?);
    let mut prev: Option<u32> = None;
    for _ in 0..n {
        let tag = get_unsigned_varint(buf)?;
        if prev.is_some_and(|p| tag <= p) {
            return Err(Error::protocol(
                "tagged fields must be in ascending tag order",
            ));
        }
        prev = Some(tag);
        let size = usize_from_u32(get_unsigned_varint(buf)?)?;
        need(buf, size)?;
        let mut bytes = vec![0u8; size];
        buf.copy_to_slice(&mut bytes);
        out.push((tag, Bytes::from(bytes)));
    }
    Ok(out)
}

/// Write an empty tagged-fields count (`0`).
pub fn put_empty_tagged_fields(buf: &mut BytesMut) {
    put_unsigned_varint(buf, 0);
}

/// Overwrite a previously reserved `i32` (record-batch length / CRC placeholders).
pub fn patch_i32(buf: &mut BytesMut, pos: usize, v: i32) -> Result<()> {
    let slot = buf
        .get_mut(pos..pos + 4)
        .ok_or_else(|| Error::protocol("short i32 patch slot"))?;
    slot.copy_from_slice(&v.to_be_bytes());
    Ok(())
}

/// Read `INT8`.
pub fn get_i8<B: Buf>(buf: &mut B) -> Result<i8> {
    need(buf, 1)?;
    Ok(buf.get_i8())
}

/// Read `INT16`.
pub fn get_i16<B: Buf>(buf: &mut B) -> Result<i16> {
    need(buf, 2)?;
    Ok(buf.get_i16())
}

/// Read `INT32`.
pub fn get_i32<B: Buf>(buf: &mut B) -> Result<i32> {
    need(buf, 4)?;
    Ok(buf.get_i32())
}

/// Read `INT64`.
pub fn get_i64<B: Buf>(buf: &mut B) -> Result<i64> {
    need(buf, 8)?;
    Ok(buf.get_i64())
}

/// Read `FLOAT64`.
pub fn get_f64<B: Buf>(buf: &mut B) -> Result<f64> {
    need(buf, 8)?;
    Ok(buf.get_f64())
}

/// Read `UINT32`.
pub fn get_u32<B: Buf>(buf: &mut B) -> Result<u32> {
    need(buf, 4)?;
    Ok(buf.get_u32())
}

/// Read `BOOLEAN` (`INT8 != 0`).
pub fn get_bool<B: Buf>(buf: &mut B) -> Result<bool> {
    Ok(get_i8(buf)? != 0)
}

/// Read a 16-byte UUID.
pub fn get_uuid<B: Buf>(buf: &mut B) -> Result<[u8; 16]> {
    need(buf, 16)?;
    let mut id = [0u8; 16];
    buf.copy_to_slice(&mut id);
    Ok(id)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    #[test]
    fn unsigned_varint_roundtrip() {
        for v in [0u32, 1, 127, 128, 300, 16_383, 16_384, u32::MAX] {
            let mut buf = BytesMut::new();
            put_unsigned_varint(&mut buf, v);
            assert_eq!(get_unsigned_varint(&mut &buf[..]).unwrap(), v);
        }
    }

    #[test]
    fn zigzag_varint_roundtrip() {
        for v in [0i32, 1, -1, 2, -2, 127, -128, i32::MAX, i32::MIN] {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, v);
            assert_eq!(get_varint(&mut &buf[..]).unwrap(), v);
        }
    }

    #[test]
    fn compact_string_null_empty() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, None).unwrap();
        put_compact_string(&mut buf, Some("")).unwrap();
        put_compact_string(&mut buf, Some("hi")).unwrap();
        let mut cur = &buf[..];
        assert_eq!(get_compact_string(&mut cur).unwrap(), None);
        assert_eq!(get_compact_string(&mut cur).unwrap().as_deref(), Some(""));
        assert_eq!(get_compact_string(&mut cur).unwrap().as_deref(), Some("hi"));
        assert_eq!(cur.remaining(), 0);
    }

    fn naive_size_of_unsigned_varint(value: i32) -> i32 {
        // Java ByteUtilsTest.testSizeOfUnsignedVarint loop (`>>>` 7).
        let mut v = u32::from_ne_bytes(value.to_ne_bytes());
        let mut bytes = 1i32;
        while (v & 0xffff_ff80) != 0 {
            bytes += 1;
            v >>= 7;
        }
        bytes
    }

    fn naive_size_of_varlong(value: i64) -> i32 {
        // Java ByteUtilsTest.testSizeOfVarlong zigzag then `>>>` 7.
        let mut v = zigzag_i64(value);
        let mut bytes = 1i32;
        while (v & 0xffff_ffff_ffff_ff80) != 0 {
            bytes += 1;
            v >>= 7;
        }
        bytes
    }

    fn assert_unsigned_varint_size_matches_encode(value: i32) {
        let mut buf = BytesMut::new();
        put_unsigned_varint(&mut buf, u32::from_ne_bytes(value.to_ne_bytes()));
        assert_eq!(
            size_of_unsigned_varint(value),
            i32::try_from(buf.len()).unwrap_or(i32::MAX),
            "unsigned varint {value}"
        );
    }

    fn assert_varint_size_matches_encode(value: i32) {
        let mut buf = BytesMut::new();
        put_varint(&mut buf, value);
        assert_eq!(
            size_of_varint(value),
            i32::try_from(buf.len()).unwrap_or(i32::MAX),
            "varint {value}"
        );
    }

    fn assert_varlong_size_matches_encode(value: i64) {
        let mut buf = BytesMut::new();
        put_varlong(&mut buf, value);
        assert_eq!(
            size_of_varlong(value),
            i32::try_from(buf.len()).unwrap_or(i32::MAX),
            "varlong {value}"
        );
    }

    fn assert_unsigned_varlong_size_matches_encode(value: i64) {
        let mut buf = BytesMut::new();
        put_unsigned_varlong(&mut buf, u64::from_ne_bytes(value.to_ne_bytes()));
        assert_eq!(
            size_of_unsigned_varlong(value),
            i32::try_from(buf.len()).unwrap_or(i32::MAX),
            "unsigned varlong {value}"
        );
    }

    #[test]
    fn varint_size_matches_encoded_len() {
        for v in [0i32, 1, -1, 2, -2, 127, -128, 16_383, i32::MAX, i32::MIN] {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, v);
            assert_eq!(buf.len(), varint_size(v), "varint {v}");
        }
        for v in [0i64, 1, -1, i64::MAX, i64::MIN] {
            let mut buf = BytesMut::new();
            put_varlong(&mut buf, v);
            assert_eq!(buf.len(), varlong_size(v), "varlong {v}");
        }
    }

    #[test]
    fn size_of_unsigned_varint_matches_java() {
        // ByteUtilsTest.assertUnsignedVarintSerde encodings.
        for v in [
            0,
            -1,
            1,
            63,
            -64,
            64,
            8191,
            -8192,
            8192,
            -8193,
            1_048_575,
            1_048_576,
            i32::MAX,
            i32::MIN,
        ] {
            assert_unsigned_varint_size_matches_encode(v);
            assert_eq!(size_of_unsigned_varint(v), naive_size_of_unsigned_varint(v));
        }
        assert_eq!(size_of_unsigned_varint(-1), 5);
        assert_eq!(size_of_unsigned_varint(i32::MIN), 5);
        assert_eq!(size_of_unsigned_varint(0), 1);
        for i in 0i32..10_000 {
            assert_eq!(
                size_of_unsigned_varint(i),
                naive_size_of_unsigned_varint(i),
                "{i}"
            );
        }
        let mut i = 0i32;
        while i < 100_000 {
            assert_eq!(
                size_of_unsigned_varint(i),
                naive_size_of_unsigned_varint(i),
                "{i}"
            );
            i += 13;
        }
        let mut pow = 1i32;
        while pow > 0 {
            assert_eq!(
                size_of_unsigned_varint(pow),
                naive_size_of_unsigned_varint(pow),
                "{pow}"
            );
            pow = pow.wrapping_shl(1);
        }
        assert_eq!(
            size_of_unsigned_varint(i32::MAX),
            naive_size_of_unsigned_varint(i32::MAX)
        );
    }

    #[test]
    fn size_of_varint_matches_java() {
        for v in [
            0,
            -1,
            1,
            63,
            -64,
            64,
            -65,
            8191,
            -8192,
            8192,
            -8193,
            1_048_575,
            -1_048_576,
            1_048_576,
            -1_048_577,
            134_217_727,
            -134_217_728,
            134_217_728,
            -134_217_729,
            i32::MAX,
            i32::MIN,
        ] {
            assert_varint_size_matches_encode(v);
        }
        assert_eq!(size_of_varint(-1), 1);
        assert_eq!(size_of_varint(i32::MIN), 5);
        assert_eq!(size_of_varint(i32::MAX), 5);
    }

    #[test]
    fn size_of_varlong_matches_java() {
        for v in [
            0,
            -1,
            1,
            63,
            -64,
            64,
            -65,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
            17_179_869_183,
            -17_179_869_184,
            17_179_869_184,
            -17_179_869_185,
            i64::MAX,
            i64::MIN,
        ] {
            assert_varlong_size_matches_encode(v);
            assert_eq!(size_of_varlong(v), naive_size_of_varlong(v), "{v}");
        }
        let mut l = 1i64;
        while l > 0 {
            assert_eq!(size_of_varlong(l), naive_size_of_varlong(l), "{l}");
            l = l.wrapping_shl(1);
        }
        assert_eq!(size_of_varlong(0), naive_size_of_varlong(0));
        assert_eq!(size_of_varlong(-1), 1);
        assert_eq!(size_of_varlong(i64::MAX), 10);
        assert_eq!(size_of_varlong(i64::MIN), 10);
    }

    #[test]
    fn size_of_unsigned_varlong_matches_java() {
        for v in [0i64, -1, 1, 63, -64, 64, i64::MAX, i64::MIN] {
            assert_unsigned_varlong_size_matches_encode(v);
        }
        assert_eq!(size_of_unsigned_varlong(-1), 10);
        assert_eq!(size_of_unsigned_varlong(0), 1);
        assert_eq!(size_of_unsigned_varlong(i64::MIN), 10);
        assert_eq!(size_of_unsigned_varlong(i64::MAX), 9);
    }

    #[test]
    fn take_classic_bytes_from_bytes_is_a_view() {
        let payload = [7u8; 32];
        let mut buf = BytesMut::new();
        put_classic_bytes(&mut buf, Some(&payload)).unwrap();
        let frozen = buf.freeze();
        let expected = frozen.slice(4..);
        let mut cur = frozen;
        let taken = take_classic_bytes(&mut cur).unwrap().unwrap();
        assert_eq!(&taken[..], &payload[..]);
        assert_eq!(taken.as_ptr(), expected.as_ptr());
        assert!(cur.remaining() == 0);
    }

    #[test]
    fn take_compact_bytes_from_bytes_is_a_view() {
        let payload = [9u8; 16];
        let mut buf = BytesMut::new();
        put_compact_bytes(&mut buf, Some(&payload)).unwrap();
        let frozen = buf.freeze();
        let prefix = unsigned_varint_size(u32_from_usize(payload.len()).unwrap() + 1);
        let expected = frozen.slice(prefix..);
        let mut cur = frozen;
        let taken = take_compact_bytes(&mut cur).unwrap().unwrap();
        assert_eq!(&taken[..], &payload[..]);
        assert_eq!(taken.as_ptr(), expected.as_ptr());
    }

    #[test]
    fn tagged_fields_roundtrip_and_require_ascending_tags() {
        let mut buf = BytesMut::new();
        put_tagged_fields(&mut buf, &[(0, &b"aa"[..]), (2, &b"bbb"[..])]).unwrap();
        let mut cur = &buf[..];
        let got = get_tagged_fields(&mut cur).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 0);
        assert_eq!(&got[0].1[..], b"aa");
        assert_eq!(got[1].0, 2);
        assert_eq!(&got[1].1[..], b"bbb");
        assert_eq!(cur.remaining(), 0);

        let mut bad = BytesMut::new();
        assert!(put_tagged_fields(&mut bad, &[(2, &b"x"[..]), (1, &b"y"[..])]).is_err());
    }

    #[test]
    fn take_classic_bytes_null_is_none() {
        let mut buf = BytesMut::new();
        put_classic_bytes(&mut buf, None).unwrap();
        assert_eq!(take_classic_bytes(&mut buf.freeze()).unwrap(), None);
    }

    #[test]
    fn utf8_length_matches_java_utils() {
        assert_eq!(utf8_length(""), 0);
        assert_eq!(utf8_length("a"), 1);
        assert_eq!(utf8_length("hello"), 5);
        assert_eq!(utf8_length("é"), 2);
        assert_eq!(utf8_length("€"), 3);
        assert_eq!(utf8_length("你"), 3);
        assert_eq!(utf8_length("😀"), 4);
        assert_eq!(utf8_length("a😀é"), 7);
    }

    #[test]
    fn to_32_bit_field_matches_java_utils() {
        assert_eq!(to_32_bit_field(std::iter::empty::<i8>()).unwrap(), 0);
        assert_eq!(to_32_bit_field([0i8]).unwrap(), 1);
        assert_eq!(to_32_bit_field([1i8]).unwrap(), 2);
        assert_eq!(to_32_bit_field([0i8, 1]).unwrap(), 3);
        assert_eq!(to_32_bit_field([31i8]).unwrap(), i32::MIN);
        assert_eq!(to_32_bit_field([0i8, 31]).unwrap(), i32::MIN | 1);
        let high = to_32_bit_field([32i8]).unwrap_err().to_string();
        assert!(high.contains("out of range: i>31, i = 32"), "{high}");
        let low = to_32_bit_field([-1i8]).unwrap_err().to_string();
        assert!(low.contains("out of range: i<0, i = -1"), "{low}");
        assert!(from_32_bit_field(0).is_empty());
        assert_eq!(from_32_bit_field(1), HashSet::from([0i8]));
        assert_eq!(from_32_bit_field(2), HashSet::from([1i8]));
        assert_eq!(from_32_bit_field(3), HashSet::from([0i8, 1]));
        assert_eq!(from_32_bit_field(i32::MIN), HashSet::from([31i8]));
        let bits = [0i8, 3, 7, 31];
        let packed = to_32_bit_field(bits).unwrap();
        assert_eq!(from_32_bit_field(packed), HashSet::from(bits));
    }

    #[test]
    fn is_blank_matches_java_utils() {
        assert!(is_blank(None));
        assert!(is_blank(Some("")));
        assert!(is_blank(Some(" ")));
        assert!(is_blank(Some("\t\n\r")));
        assert!(is_blank(Some("\0")));
        assert!(is_blank(Some(" \t \0 ")));
        assert!(!is_blank(Some("a")));
        assert!(!is_blank(Some(" a ")));
        assert!(
            !is_blank(Some("\u{00A0}")),
            "Java String.trim does not strip NBSP"
        );
        assert!(
            !is_blank(Some("\u{2000}")),
            "Java String.trim does not strip Unicode White_Space above U+0020"
        );

        assert_eq!(
            replace_suffix("foo.log", ".log", ".tmp").unwrap(),
            "foo.tmp"
        );
        assert_eq!(replace_suffix(".log", ".log", ".tmp").unwrap(), ".tmp");
        assert_eq!(replace_suffix("foo", "", ".tmp").unwrap(), "foo.tmp");
        let missing = replace_suffix("foo.log", ".tmp", ".bak")
            .unwrap_err()
            .to_string();
        assert!(
            missing.contains("Expected string to end with .tmp but string is foo.log"),
            "{missing}"
        );
    }

    #[test]
    fn entries_with_prefix_matches_java_utils() {
        let map = HashMap::from([
            ("foo.bar".to_string(), 1i32),
            ("foo".to_string(), 2),
            ("baz.qux".to_string(), 3),
            ("foo.baz".to_string(), 4),
        ]);
        assert_eq!(
            entries_with_prefix(&map, "foo."),
            HashMap::from([("bar".to_string(), 1), ("baz".to_string(), 4)])
        );
        assert_eq!(
            entries_with_prefix(&map, "foo"),
            HashMap::from([(".bar".to_string(), 1), (".baz".to_string(), 4)])
        );
        assert!(entries_with_prefix(&map, "nope").is_empty());
        assert_eq!(
            entries_with_prefix_matching(&map, "foo.", false, false),
            HashMap::from([("foo.bar".to_string(), 1), ("foo.baz".to_string(), 4)])
        );
        assert_eq!(
            entries_with_prefix_matching(&map, "foo", true, true),
            HashMap::from([
                (".bar".to_string(), 1),
                (String::new(), 2),
                (".baz".to_string(), 4)
            ])
        );
        assert_eq!(
            entries_with_prefix_matching(&map, "foo", false, true),
            HashMap::from([
                ("foo.bar".to_string(), 1),
                ("foo".to_string(), 2),
                ("foo.baz".to_string(), 4)
            ])
        );
        let empty_prefix = entries_with_prefix(&map, "");
        assert_eq!(empty_prefix.len(), 4);
        assert_eq!(empty_prefix.get("foo.bar"), Some(&1));
    }

    #[test]
    fn is_equal_constant_time_matches_java_utils() {
        assert!(is_equal_constant_time(None, None));
        assert!(!is_equal_constant_time(None, Some(&[])));
        assert!(!is_equal_constant_time(Some(&[]), None));
        assert!(is_equal_constant_time(Some(&[]), Some(&[])));
        assert!(!is_equal_constant_time(Some(&[1]), Some(&[])));
        assert!(!is_equal_constant_time(Some(&[]), Some(&[1])));

        let same = [1u16, 2, 3];
        assert!(is_equal_constant_time(Some(&same), Some(&same)));

        let a = [1u16, 2];
        let b = [1u16, 2];
        assert!(is_equal_constant_time(Some(&a), Some(&b)));
        assert!(!is_equal_constant_time(Some(&[1, 2]), Some(&[1, 3])));
        assert!(!is_equal_constant_time(Some(&[1, 2]), Some(&[1, 2, 3])));
        assert!(!is_equal_constant_time(Some(&[1, 2, 3]), Some(&[1, 2])));
        assert!(!is_equal_constant_time(Some(&[5, 5, 5]), Some(&[5])));
        assert!(is_equal_constant_time(Some(&[0xD800]), Some(&[0xD800])));
        assert!(!is_equal_constant_time(Some(&[0xD800]), Some(&[0xD801])));
    }

    #[test]
    fn require_matches_java_utils() {
        assert!(require(true).is_ok());
        let failed = require(false).unwrap_err().to_string();
        assert!(failed.contains("requirement failed"), "{failed}");
        assert!(require_message(true, "must be set").is_ok());
        let custom = require_message(false, "must be set")
            .unwrap_err()
            .to_string();
        assert!(custom.contains("must be set"), "{custom}");
        assert!(!custom.contains("requirement failed"), "{custom}");
    }

    #[test]
    fn invalid_varint_matches_java_byte_utils() {
        // ByteUtilsTest.testInvalidVarint: five 0xFF continuation bytes plus 0x01.
        let mut buf: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let msg = get_varint(&mut buf).unwrap_err().to_string();
        assert!(
            msg.contains(
                "Varint is too long, the most significant bit in the 5th byte is set, converted value: "
            ),
            "{msg}"
        );
        assert!(msg.contains("converted value: ffffffff"), "{msg}");
    }

    #[test]
    fn invalid_varlong_matches_java_byte_utils() {
        // ByteUtilsTest.testInvalidVarlong: ten 0xFF continuation bytes plus 0x01.
        let mut buf: &[u8] = &[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
        ];
        let msg = get_varlong(&mut buf).unwrap_err().to_string();
        assert!(
            msg.contains(
                "Varlong is too long, most significant bit in the 10th byte is set, converted value: "
            ),
            "{msg}"
        );
        assert!(msg.contains("converted value: ffffffffffffffff"), "{msg}");
    }
}
