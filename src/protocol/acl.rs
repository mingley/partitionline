//! CreateAcls, DescribeAcls, and DeleteAcls (api keys 29–31). Classic v0.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

/// ACL resource type: topic.
pub const ACL_RESOURCE_TOPIC: i8 = 2;
/// ACL operation: all.
pub const ACL_OPERATION_ALL: i8 = 2;
/// ACL permission: allow.
pub const ACL_PERMISSION_ALLOW: i8 = 3;

/// Kafka ACL resource type (`ResourceType` on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum AclResourceType {
    /// Protocol `UNKNOWN`.
    Unknown = 0,
    /// Matches any resource type on DescribeAcls / DeleteAcls.
    Any = 1,
    /// Topic.
    Topic = 2,
    /// Consumer group.
    Group = 3,
    /// Cluster.
    Cluster = 4,
    /// Transactional id.
    TransactionalId = 5,
    /// Delegation token.
    DelegationToken = 6,
}

impl From<AclResourceType> for i8 {
    fn from(ty: AclResourceType) -> Self {
        ty as i8
    }
}

/// Kafka ACL operation (`AclOperation` on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum AclOperation {
    /// Protocol `UNKNOWN`.
    Unknown = 0,
    /// Matches any operation on DescribeAcls / DeleteAcls.
    Any = 1,
    /// All operations.
    All = 2,
    /// Read.
    Read = 3,
    /// Write.
    Write = 4,
    /// Create.
    Create = 5,
    /// Delete.
    Delete = 6,
    /// Alter.
    Alter = 7,
    /// Describe.
    Describe = 8,
    /// Cluster action.
    ClusterAction = 9,
    /// Describe configs.
    DescribeConfigs = 10,
    /// Alter configs.
    AlterConfigs = 11,
    /// Idempotent write.
    IdempotentWrite = 12,
}

impl From<AclOperation> for i8 {
    fn from(op: AclOperation) -> Self {
        op as i8
    }
}

/// Kafka ACL permission type (`AclPermissionType` on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum AclPermission {
    /// Protocol `UNKNOWN`.
    Unknown = 0,
    /// Matches any permission on DescribeAcls / DeleteAcls.
    Any = 1,
    /// Deny.
    Deny = 2,
    /// Allow.
    Allow = 3,
}

impl From<AclPermission> for i8 {
    fn from(perm: AclPermission) -> Self {
        perm as i8
    }
}

/// One ACL binding for CreateAcls / DescribeAcls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    /// Kafka resource type (`ACL_RESOURCE_TOPIC`, [`AclResourceType::Topic`], …).
    pub resource_type: i8,
    /// Resource name (topic name, …).
    pub resource_name: String,
    /// Principal, for example `User:alice`.
    pub principal: String,
    /// Host filter (`*` is any).
    pub host: String,
    /// Operation (`ACL_OPERATION_ALL`, [`AclOperation::All`], …).
    pub operation: i8,
    /// Permission (`ACL_PERMISSION_ALLOW`, [`AclPermission::Allow`], …).
    pub permission: i8,
}

impl AclBinding {
    /// Allow every operation on `name` for `principal` from any host.
    #[must_use]
    pub fn allow(
        resource_type: impl Into<i8>,
        name: impl Into<String>,
        principal: impl Into<String>,
    ) -> Self {
        Self {
            resource_type: resource_type.into(),
            resource_name: name.into(),
            principal: principal.into(),
            host: "*".into(),
            operation: ACL_OPERATION_ALL,
            permission: ACL_PERMISSION_ALLOW,
        }
    }

    /// Allow every operation on `topic` for `principal` from any host.
    #[must_use]
    pub fn allow_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::allow(AclResourceType::Topic, topic, principal)
    }

    /// Host filter (`*` is any).
    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Operation (`AclOperation::All`, …).
    #[must_use]
    pub fn operation(mut self, operation: impl Into<i8>) -> Self {
        self.operation = operation.into();
        self
    }

    /// Permission (`AclPermission::Allow`, …).
    #[must_use]
    pub fn permission(mut self, permission: impl Into<i8>) -> Self {
        self.permission = permission.into();
        self
    }
}

/// Encode CreateAcls v0.
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

/// Decode CreateAcls v0.
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

/// Encode CreateAcls: throttle `0` plus per-binding error codes.
pub fn encode_create_acls_response(buf: &mut BytesMut, errors: &[i16]) -> Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(errors.len()))?;
    for e in errors {
        buf.put_i16(*e);
        buf::put_classic_nullable_string(buf, None)?;
    }
    Ok(())
}

/// Decode CreateAcls: per-binding error codes.
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

/// Encode DescribeAcls filtered by resource type (`Any` is 1).
pub fn encode_describe_acls_request(buf: &mut BytesMut, resource_type: i8) -> Result<()> {
    buf.put_i8(resource_type);
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_classic_nullable_string(buf, None)?;
    buf::put_classic_nullable_string(buf, None)?;
    buf.put_i8(ACL_OPERATION_ALL);
    buf.put_i8(ACL_PERMISSION_ALLOW);
    Ok(())
}

/// Decode DescribeAcls: resource type filter.
pub fn decode_describe_acls_request<B: Buf>(buf: &mut B) -> Result<i8> {
    let rt = buf::get_i8(buf)?;
    let _name = buf::get_classic_nullable_string(buf)?;
    let _prin = buf::get_classic_nullable_string(buf)?;
    let _host = buf::get_classic_nullable_string(buf)?;
    let _op = buf::get_i8(buf)?;
    let _perm = buf::get_i8(buf)?;
    Ok(rt)
}

/// Encode DescribeAcls with matching bindings.
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

/// Decode DescribeAcls bindings. Top-level error returns an empty list.
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

/// Encode DeleteAcls filtered by resource type.
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

/// Decode DeleteAcls: resource type filter.
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

/// Encode DeleteAcls: `removed` matching filters (mock count).
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

/// Decode DeleteAcls: first filter error code.
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
        let acl = AclBinding::allow_topic("t", "User:alice");
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

    #[test]
    fn allow_topic_is_all_allow_on_topic() {
        let acl = AclBinding::allow_topic("events", "User:alice");
        assert_eq!(acl.resource_type, i8::from(AclResourceType::Topic));
        assert_eq!(acl.resource_type, ACL_RESOURCE_TOPIC);
        assert_eq!(acl.resource_name, "events");
        assert_eq!(acl.principal, "User:alice");
        assert_eq!(acl.host, "*");
        assert_eq!(acl.operation, ACL_OPERATION_ALL);
        assert_eq!(acl.permission, ACL_PERMISSION_ALLOW);
        assert_eq!(acl.operation, i8::from(AclOperation::All));
        assert_eq!(acl.permission, i8::from(AclPermission::Allow));
    }
}
