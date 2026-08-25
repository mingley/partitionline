#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use crate::error::{Error, Result};

pub fn i16_from_usize(n: usize) -> Result<i16> {
    i16::try_from(n).map_err(|_| Error::protocol("length exceeds i16"))
}

pub fn i32_from_usize(n: usize) -> Result<i32> {
    i32::try_from(n).map_err(|_| Error::protocol("length exceeds i32"))
}

pub fn u32_from_usize(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::protocol("length exceeds u32"))
}

pub fn i64_from_usize(n: usize) -> Result<i64> {
    i64::try_from(n).map_err(|_| Error::protocol("length exceeds i64"))
}

pub fn usize_from_i16(n: i16) -> Result<usize> {
    usize::try_from(n).map_err(|_| Error::protocol("negative length"))
}

pub fn usize_from_i32(n: i32) -> Result<usize> {
    usize::try_from(n).map_err(|_| Error::protocol("negative length"))
}

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

pub fn unsigned_varint_size(mut v: u32) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

pub fn unsigned_varlong_size(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

pub fn varint_size(v: i32) -> usize {
    unsigned_varint_size(zigzag_i32(v))
}

pub fn varlong_size(v: i64) -> usize {
    unsigned_varlong_size(zigzag_i64(v))
}

pub fn put_unsigned_varint(buf: &mut BytesMut, mut v: u32) {
    while v >= 0x80 {
        buf.put_u8(varint_byte_u32(v, true));
        v >>= 7;
    }
    buf.put_u8(varint_byte_u32(v, false));
}

pub fn get_unsigned_varint<B: Buf>(buf: &mut B) -> Result<u32> {
    let mut result = 0u32;
    for i in 0..5 {
        need(buf, 1)?;
        let b = buf.get_u8();
        result |= u32::from(b & 0x7f) << (i * 7);
        if b & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(Error::protocol("unsigned varint overflow"))
}

pub fn put_varint(buf: &mut BytesMut, v: i32) {
    put_unsigned_varint(buf, zigzag_i32(v));
}

pub fn get_varint<B: Buf>(buf: &mut B) -> Result<i32> {
    Ok(unzigzag_i32(get_unsigned_varint(buf)?))
}

pub fn put_unsigned_varlong(buf: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        buf.put_u8(varint_byte_u64(v, true));
        v >>= 7;
    }
    buf.put_u8(varint_byte_u64(v, false));
}

pub fn get_unsigned_varlong<B: Buf>(buf: &mut B) -> Result<u64> {
    let mut result = 0u64;
    for i in 0..10 {
        need(buf, 1)?;
        let b = buf.get_u8();
        result |= u64::from(b & 0x7f) << (i * 7);
        if b & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(Error::protocol("unsigned varlong overflow"))
}

pub fn put_varlong(buf: &mut BytesMut, v: i64) {
    put_unsigned_varlong(buf, zigzag_i64(v));
}

pub fn get_varlong<B: Buf>(buf: &mut B) -> Result<i64> {
    Ok(unzigzag_i64(get_unsigned_varlong(buf)?))
}

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

pub fn put_string(buf: &mut BytesMut, flexible: bool, s: Option<&str>) -> Result<()> {
    if flexible {
        put_compact_string(buf, s)?;
    } else {
        put_classic_nullable_string(buf, s)?;
    }
    Ok(())
}

pub fn get_string<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<String>> {
    if flexible {
        get_compact_string(buf)
    } else {
        get_classic_nullable_string(buf)
    }
}

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

pub fn get_compact_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>> {
    let n = get_unsigned_varint(buf)?;
    if n == 0 {
        return Ok(None);
    }
    let len = usize_from_u32(n - 1)?;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(bytes))
}

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

pub fn get_classic_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>> {
    need(buf, 4)?;
    let len = buf.get_i32();
    if len < 0 {
        return Ok(None);
    }
    let len = usize_from_i32(len)?;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(bytes))
}

pub fn put_bytes(buf: &mut BytesMut, flexible: bool, bytes: Option<&[u8]>) -> Result<()> {
    if flexible {
        put_compact_bytes(buf, bytes)?;
    } else {
        put_classic_bytes(buf, bytes)?;
    }
    Ok(())
}

pub fn get_bytes<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<Vec<u8>>> {
    if flexible {
        get_compact_bytes(buf)
    } else {
        get_classic_bytes(buf)
    }
}

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

pub fn put_empty_tagged_fields(buf: &mut BytesMut) {
    put_unsigned_varint(buf, 0);
}

pub fn patch_i32(buf: &mut BytesMut, pos: usize, v: i32) -> Result<()> {
    let slot = buf
        .get_mut(pos..pos + 4)
        .ok_or_else(|| Error::protocol("short i32 patch slot"))?;
    slot.copy_from_slice(&v.to_be_bytes());
    Ok(())
}

pub fn get_i8<B: Buf>(buf: &mut B) -> Result<i8> {
    need(buf, 1)?;
    Ok(buf.get_i8())
}

pub fn get_i16<B: Buf>(buf: &mut B) -> Result<i16> {
    need(buf, 2)?;
    Ok(buf.get_i16())
}

pub fn get_i32<B: Buf>(buf: &mut B) -> Result<i32> {
    need(buf, 4)?;
    Ok(buf.get_i32())
}

pub fn get_i64<B: Buf>(buf: &mut B) -> Result<i64> {
    need(buf, 8)?;
    Ok(buf.get_i64())
}

pub fn get_u32<B: Buf>(buf: &mut B) -> Result<u32> {
    need(buf, 4)?;
    Ok(buf.get_u32())
}

pub fn get_bool<B: Buf>(buf: &mut B) -> Result<bool> {
    Ok(get_i8(buf)? != 0)
}

pub fn get_uuid<B: Buf>(buf: &mut B) -> Result<[u8; 16]> {
    need(buf, 16)?;
    let mut id = [0u8; 16];
    buf.copy_to_slice(&mut id);
    Ok(id)
}

#[cfg(test)]
mod tests {
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
}
