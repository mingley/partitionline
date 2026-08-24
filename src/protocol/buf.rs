use bytes::{Buf, BufMut, BytesMut};

use crate::error::{Error, Result};

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

pub fn put_unsigned_varint(buf: &mut BytesMut, mut v: u32) {
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
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
    put_unsigned_varint(buf, ((v << 1) ^ (v >> 31)) as u32);
}

pub fn get_varint<B: Buf>(buf: &mut B) -> Result<i32> {
    let n = get_unsigned_varint(buf)?;
    Ok(((n >> 1) as i32) ^ -((n & 1) as i32))
}

pub fn put_unsigned_varlong(buf: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
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
    put_unsigned_varlong(buf, ((v << 1) ^ (v >> 63)) as u64);
}

pub fn get_varlong<B: Buf>(buf: &mut B) -> Result<i64> {
    let n = get_unsigned_varlong(buf)?;
    Ok(((n >> 1) as i64) ^ -((n & 1) as i64))
}

pub fn put_classic_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        None => buf.put_i16(-1),
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

pub fn get_classic_nullable_string<B: Buf>(buf: &mut B) -> Result<Option<String>> {
    need(buf, 2)?;
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| Error::protocol(e.to_string()))
}

pub fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        None => put_unsigned_varint(buf, 0),
        Some(s) => {
            put_unsigned_varint(buf, s.len() as u32 + 1);
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

pub fn get_compact_string<B: Buf>(buf: &mut B) -> Result<Option<String>> {
    let n = get_unsigned_varint(buf)?;
    if n == 0 {
        return Ok(None);
    }
    let len = (n - 1) as usize;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| Error::protocol(e.to_string()))
}

pub fn put_string(buf: &mut BytesMut, flexible: bool, s: Option<&str>) {
    if flexible {
        put_compact_string(buf, s);
    } else {
        put_classic_nullable_string(buf, s);
    }
}

pub fn get_string<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<String>> {
    if flexible {
        get_compact_string(buf)
    } else {
        get_classic_nullable_string(buf)
    }
}

pub fn put_compact_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) {
    match bytes {
        None => put_unsigned_varint(buf, 0),
        Some(b) => {
            put_unsigned_varint(buf, b.len() as u32 + 1);
            buf.extend_from_slice(b);
        }
    }
}

pub fn get_compact_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>> {
    let n = get_unsigned_varint(buf)?;
    if n == 0 {
        return Ok(None);
    }
    let len = (n - 1) as usize;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(bytes))
}

pub fn put_classic_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) {
    match bytes {
        None => buf.put_i32(-1),
        Some(b) => {
            buf.put_i32(b.len() as i32);
            buf.extend_from_slice(b);
        }
    }
}

pub fn get_classic_bytes<B: Buf>(buf: &mut B) -> Result<Option<Vec<u8>>> {
    need(buf, 4)?;
    let len = buf.get_i32();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    need(buf, len)?;
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(bytes))
}

pub fn put_bytes(buf: &mut BytesMut, flexible: bool, bytes: Option<&[u8]>) {
    if flexible {
        put_compact_bytes(buf, bytes);
    } else {
        put_classic_bytes(buf, bytes);
    }
}

pub fn get_bytes<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<Vec<u8>>> {
    if flexible {
        get_compact_bytes(buf)
    } else {
        get_classic_bytes(buf)
    }
}

pub fn put_array_len(buf: &mut BytesMut, flexible: bool, len: Option<usize>) {
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
                put_unsigned_varint(buf, n as u32 + 1);
            } else {
                buf.put_i32(n as i32);
            }
        }
    }
}

pub fn get_array_len<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<usize>> {
    if flexible {
        let n = get_unsigned_varint(buf)?;
        if n == 0 {
            Ok(None)
        } else {
            Ok(Some((n - 1) as usize))
        }
    } else {
        need(buf, 4)?;
        let n = buf.get_i32();
        if n < 0 {
            Ok(None)
        } else {
            Ok(Some(n as usize))
        }
    }
}

pub fn skip_tagged_fields<B: Buf>(buf: &mut B) -> Result<()> {
    let n = get_unsigned_varint(buf)?;
    for _ in 0..n {
        let _tag = get_unsigned_varint(buf)?;
        let size = get_unsigned_varint(buf)? as usize;
        need(buf, size)?;
        buf.advance(size);
    }
    Ok(())
}

pub fn put_empty_tagged_fields(buf: &mut BytesMut) {
    put_unsigned_varint(buf, 0);
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
        put_compact_string(&mut buf, None);
        put_compact_string(&mut buf, Some(""));
        put_compact_string(&mut buf, Some("hi"));
        let mut cur = &buf[..];
        assert_eq!(get_compact_string(&mut cur).unwrap(), None);
        assert_eq!(get_compact_string(&mut cur).unwrap().as_deref(), Some(""));
        assert_eq!(get_compact_string(&mut cur).unwrap().as_deref(), Some("hi"));
        assert_eq!(cur.remaining(), 0);
    }
}
