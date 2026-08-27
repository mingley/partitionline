#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

pub const ACL_RESOURCE_TOPIC: i8 = 2;
pub const ACL_OPERATION_ALL: i8 = 2;
pub const ACL_PERMISSION_ALLOW: i8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    pub resource_type: i8,
    pub resource_name: String,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission: i8,
}

pub fn encode_create_acls_request(buf: &mut BytesMut, acls: &[AclBinding]) -> Result<()> {
    buf::put_array_len(buf, false, Some(acls.len()))?;
    for a in acls {
        buf.put_i8(a.resource_type);
        buf::put_classic_nullable_string(buf, Some(&a.resource_name))?;
        buf::put_classic_nullable_string(buf, Some(&a.principal))?;
        buf::put_classic_nullable_string(buf, Some(&a.host))?;
        buf.put_i8(a.operation);
        buf.put_i8(a.permission);
    }
    Ok(())
}

pub fn decode_create_acls_request<B: Buf>(buf: &mut B) -> Result<Vec<AclBinding>> {
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let resource_name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let principal = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let host = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let operation = buf::get_i8(buf)?;
        let permission = buf::get_i8(buf)?;
        out.push(AclBinding {
            resource_type,
            resource_name,
            principal,
            host,
            operation,
            permission,
        });
    }
    Ok(out)
}

pub fn encode_create_acls_response(buf: &mut BytesMut, errors: &[i16]) -> Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(errors.len()))?;
    for e in errors {
        buf.put_i16(*e);
        buf::put_classic_nullable_string(buf, None)?;
    }
    Ok(())
}

pub fn decode_create_acls_response<B: Buf>(buf: &mut B) -> Result<Vec<i16>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let e = buf::get_i16(buf)?;
        let _msg = buf::get_classic_nullable_string(buf)?;
        out.push(e);
    }
    Ok(out)
}

pub fn encode_describe_acls_request(buf: &mut BytesMut, resource_type: i8) -> Result<()> {
    buf.put_i8(resource_type);
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_classic_nullable_string(buf, None)?;
    buf.put_i8(ACL_OPERATION_ALL);
    buf.put_i8(ACL_PERMISSION_ALLOW);
    Ok(())
}

pub fn decode_describe_acls_request<B: Buf>(buf: &mut B) -> Result<i8> {
    let rt = buf::get_i8(buf)?;
    let _name = buf::get_classic_nullable_string(buf)?;
    let _prin = buf::get_classic_nullable_string(buf)?;
    let _host = buf::get_classic_nullable_string(buf)?;
    let _op = buf::get_i8(buf)?;
    let _perm = buf::get_i8(buf)?;
    Ok(rt)
}

pub fn encode_describe_acls_response(buf: &mut BytesMut, acls: &[AclBinding]) -> Result<()> {
    buf.put_i32(0);
    buf.put_i16(0);
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_array_len(buf, false, Some(acls.len()))?;
    for a in acls {
        buf.put_i8(a.resource_type);
        buf::put_classic_nullable_string(buf, Some(&a.resource_name))?;
        buf::put_array_len(buf, false, Some(1))?;
        buf::put_classic_nullable_string(buf, Some(&a.principal))?;
        buf::put_classic_nullable_string(buf, Some(&a.host))?;
        buf.put_i8(a.operation);
        buf.put_i8(a.permission);
    }
    Ok(())
}

pub fn decode_describe_acls_response<B: Buf>(buf: &mut B) -> Result<Vec<AclBinding>> {
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    let _msg = buf::get_classic_nullable_string(buf)?;
    if err != 0 {
        return Ok(Vec::new());
    }
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut out = Vec::new();
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let resource_name = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
        let an = buf::get_array_len(buf, false)?.unwrap_or(0);
        for _ in 0..an {
            let principal = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            let host = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
            let operation = buf::get_i8(buf)?;
            let permission = buf::get_i8(buf)?;
            out.push(AclBinding {
                resource_type,
                resource_name: resource_name.clone(),
                principal,
                host,
                operation,
                permission,
            });
        }
    }
    Ok(out)
}

pub fn encode_delete_acls_request(buf: &mut BytesMut, resource_type: i8) -> Result<()> {
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i8(resource_type);
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_classic_nullable_string(buf, None)?;
    buf.put_i8(ACL_OPERATION_ALL);
    buf.put_i8(ACL_PERMISSION_ALLOW);
    Ok(())
}

pub fn decode_delete_acls_request<B: Buf>(buf: &mut B) -> Result<i8> {
    let _n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let rt = buf::get_i8(buf)?;
    let _name = buf::get_classic_nullable_string(buf)?;
    let _prin = buf::get_classic_nullable_string(buf)?;
    let _host = buf::get_classic_nullable_string(buf)?;
    let _op = buf::get_i8(buf)?;
    let _perm = buf::get_i8(buf)?;
    Ok(rt)
}

pub fn encode_delete_acls_response(buf: &mut BytesMut, removed: i32) -> Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i16(0);
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_array_len(
        buf,
        false,
        Some(usize::try_from(removed.max(0)).unwrap_or(0)),
    )?;
    Ok(())
}

pub fn decode_delete_acls_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    let _n = buf::get_array_len(buf, false)?.unwrap_or(0);
    buf::get_i16(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_acls_not_controller_is_not_at_byte_four() {
        let mut buf = BytesMut::new();
        encode_create_acls_response(&mut buf, &[crate::error::NOT_CONTROLLER]).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + creation-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_acls_response(&mut cur).unwrap(),
            vec![crate::error::NOT_CONTROLLER]
        );
        assert!(
            !cur.has_remaining(),
            "CreateAcls v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn create_describe_acls_roundtrip() {
        let acl = AclBinding {
            resource_type: ACL_RESOURCE_TOPIC,
            resource_name: "t".into(),
            principal: "User:alice".into(),
            host: "*".into(),
            operation: ACL_OPERATION_ALL,
            permission: ACL_PERMISSION_ALLOW,
        };
        let mut buf = BytesMut::new();
        encode_create_acls_request(&mut buf, std::slice::from_ref(&acl)).unwrap();
        assert_eq!(
            decode_create_acls_request(&mut &buf[..]).unwrap(),
            vec![acl.clone()]
        );
        let mut resp = BytesMut::new();
        encode_describe_acls_response(&mut resp, std::slice::from_ref(&acl)).unwrap();
        assert_eq!(
            decode_describe_acls_response(&mut &resp[..]).unwrap(),
            vec![acl]
        );
    }
}
