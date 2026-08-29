//! CreateAcls, DescribeAcls, and DeleteAcls (api keys 29–31).
//!
//! v0 is classic with no pattern type. v1 adds ResourcePatternType /
//! PatternTypeFilter (KIP-73; default LITERAL). v2–v3 are flexible
//! (compact arrays/strings plus tagged fields). v3 is the same layout
//! (user resource type). Kafka 4.0 `validVersions` is `1-3` (v0 removed).
//! This crate speaks 0–3. v4+ is not spoken.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// ACL resource type: topic.
pub const ACL_RESOURCE_TOPIC: i8 = 2;
/// ACL operation: all.
pub const ACL_OPERATION_ALL: i8 = 2;
/// ACL permission: allow.
pub const ACL_PERMISSION_ALLOW: i8 = 3;
/// Resource pattern type: any (DescribeAcls / DeleteAcls filters).
pub const ACL_PATTERN_ANY: i8 = 1;
/// Resource pattern type: literal (CreateAcls default).
pub const ACL_PATTERN_LITERAL: i8 = 3;
/// Resource pattern type: prefixed.
pub const ACL_PATTERN_PREFIXED: i8 = 4;

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
    /// User (CreateAcls / DescribeAcls / DeleteAcls v3+).
    User = 7,
}

impl From<AclResourceType> for i8 {
    fn from(ty: AclResourceType) -> Self {
        ty as i8
    }
}

/// Kafka ACL resource pattern type (`ResourcePatternType` on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum AclPatternType {
    /// Protocol `UNKNOWN`.
    Unknown = 0,
    /// Matches any pattern type on DescribeAcls / DeleteAcls.
    Any = 1,
    /// Match filter (DescribeAcls / DeleteAcls).
    Match = 2,
    /// Literal name.
    Literal = 3,
    /// Prefixed name.
    Prefixed = 4,
}

impl From<AclPatternType> for i8 {
    fn from(ty: AclPatternType) -> Self {
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
    /// Pattern type (`ACL_PATTERN_LITERAL`, [`AclPatternType::Literal`], …).
    /// Omitted on the wire at v0; decode fills LITERAL.
    pub pattern_type: i8,
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
            pattern_type: ACL_PATTERN_LITERAL,
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

    /// Resource pattern type (`AclPatternType::Literal`, …).
    #[must_use]
    pub fn pattern_type(mut self, pattern_type: impl Into<i8>) -> Self {
        self.pattern_type = pattern_type.into();
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

/// `true` when CreateAcls / DescribeAcls / DeleteAcls `version` is flexible.
///
/// v0–v1 are classic. v2 is the first flexible version. v3 is the same
/// layout. Kafka 4.0 `validVersions` is `1-3`. This crate speaks 0–3.
/// v4+ is not spoken.
fn acl_api_flexible(version: i16) -> Result<bool> {
    match version {
        0 | 1 => Ok(false),
        2 | 3 => Ok(true),
        other => Err(Error::protocol(format!(
            "CreateAcls/DescribeAcls/DeleteAcls version {other} is not implemented"
        ))),
    }
}

fn skip_delete_acl_filter<B: Buf>(buf: &mut B, version: i16, flexible: bool) -> Result<()> {
    let _ = buf::get_i8(buf)?;
    let _ = buf::get_string(buf, flexible)?;
    if version >= 1 {
        let _ = buf::get_i8(buf)?;
    }
    let _ = buf::get_string(buf, flexible)?;
    let _ = buf::get_string(buf, flexible)?;
    let _ = buf::get_i8(buf)?;
    let _ = buf::get_i8(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(())
}

fn skip_delete_matching_acl<B: Buf>(buf: &mut B, version: i16, flexible: bool) -> Result<()> {
    let _ = buf::get_i16(buf)?;
    let _ = buf::get_string(buf, flexible)?;
    let _ = buf::get_i8(buf)?;
    let _ = buf::get_string(buf, flexible)?;
    if version >= 1 {
        let _ = buf::get_i8(buf)?;
    }
    let _ = buf::get_string(buf, flexible)?;
    let _ = buf::get_string(buf, flexible)?;
    let _ = buf::get_i8(buf)?;
    let _ = buf::get_i8(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(())
}

/// Encode CreateAcls v0–3 (classic through v1; flexible from v2).
pub fn encode_create_acls_request(
    buf: &mut BytesMut,
    version: i16,
    acls: &[AclBinding],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(acls.len()))?;
    for a in acls {
        buf.put_i8(a.resource_type);
        buf::put_string(buf, flexible, Some(&a.resource_name))?;
        if version >= 1 {
            buf.put_i8(a.pattern_type);
        }
        buf::put_string(buf, flexible, Some(&a.principal))?;
        buf::put_string(buf, flexible, Some(&a.host))?;
        buf.put_i8(a.operation);
        buf.put_i8(a.permission);
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode CreateAcls (all creations).
pub fn decode_create_acls_request<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<AclBinding>> {
    let flexible = acl_api_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let resource_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pattern_type = if version >= 1 {
            buf::get_i8(buf)?
        } else {
            ACL_PATTERN_LITERAL
        };
        let principal = buf::get_string(buf, flexible)?.unwrap_or_default();
        let host = buf::get_string(buf, flexible)?.unwrap_or_default();
        let operation = buf::get_i8(buf)?;
        let permission = buf::get_i8(buf)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(AclBinding {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// Encode CreateAcls: throttle `0` plus per-binding error codes.
pub fn encode_create_acls_response(buf: &mut BytesMut, version: i16, errors: &[i16]) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(errors.len()))?;
    for e in errors {
        buf.put_i16(*e);
        buf::put_string(buf, flexible, None)?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode CreateAcls: per-binding error codes.
pub fn decode_create_acls_response<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<i16>> {
    let flexible = acl_api_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let e = buf::get_i16(buf)?;
        let _msg = buf::get_string(buf, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(e);
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// Encode DescribeAcls filtered by resource type (`Any` is 1).
///
/// v1+ sends [`ACL_PATTERN_ANY`] so literal and prefixed bindings match.
pub fn encode_describe_acls_request(
    buf: &mut BytesMut,
    version: i16,
    resource_type: i8,
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf.put_i8(resource_type);
    buf::put_string(buf, flexible, None)?;
    if version >= 1 {
        buf.put_i8(ACL_PATTERN_ANY);
    }
    buf::put_string(buf, flexible, None)?;
    buf::put_string(buf, flexible, None)?;
    buf.put_i8(ACL_OPERATION_ALL);
    buf.put_i8(ACL_PERMISSION_ALLOW);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DescribeAcls: resource type filter.
pub fn decode_describe_acls_request<B: Buf>(buf: &mut B, version: i16) -> Result<i8> {
    let flexible = acl_api_flexible(version)?;
    let rt = buf::get_i8(buf)?;
    let _name = buf::get_string(buf, flexible)?;
    if version >= 1 {
        let _pattern = buf::get_i8(buf)?;
    }
    let _prin = buf::get_string(buf, flexible)?;
    let _host = buf::get_string(buf, flexible)?;
    let _op = buf::get_i8(buf)?;
    let _perm = buf::get_i8(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(rt)
}

/// Encode DescribeAcls with matching bindings.
pub fn encode_describe_acls_response(
    buf: &mut BytesMut,
    version: i16,
    acls: &[AclBinding],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(0);
    buf::put_string(buf, flexible, None)?;
    buf::put_array_len(buf, flexible, Some(acls.len()))?;
    for a in acls {
        buf.put_i8(a.resource_type);
        buf::put_string(buf, flexible, Some(&a.resource_name))?;
        if version >= 1 {
            buf.put_i8(a.pattern_type);
        }
        buf::put_array_len(buf, flexible, Some(1))?;
        buf::put_string(buf, flexible, Some(&a.principal))?;
        buf::put_string(buf, flexible, Some(&a.host))?;
        buf.put_i8(a.operation);
        buf.put_i8(a.permission);
        if flexible {
            buf::put_empty_tagged_fields(buf);
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DescribeAcls bindings. Top-level error returns an empty list.
pub fn decode_describe_acls_response<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<AclBinding>> {
    let flexible = acl_api_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    let _msg = buf::get_string(buf, flexible)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::new();
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let resource_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pattern_type = if version >= 1 {
            buf::get_i8(buf)?
        } else {
            ACL_PATTERN_LITERAL
        };
        let an = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..an {
            let principal = buf::get_string(buf, flexible)?.unwrap_or_default();
            let host = buf::get_string(buf, flexible)?.unwrap_or_default();
            let operation = buf::get_i8(buf)?;
            let permission = buf::get_i8(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            out.push(AclBinding {
                resource_type,
                resource_name: resource_name.clone(),
                pattern_type,
                principal,
                host,
                operation,
                permission,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    if err != 0 {
        return Ok(Vec::new());
    }
    Ok(out)
}

/// Encode DeleteAcls filtered by resource type.
///
/// v1+ sends [`ACL_PATTERN_ANY`] so literal and prefixed bindings match.
pub fn encode_delete_acls_request(
    buf: &mut BytesMut,
    version: i16,
    resource_type: i8,
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(1))?;
    buf.put_i8(resource_type);
    buf::put_string(buf, flexible, None)?;
    if version >= 1 {
        buf.put_i8(ACL_PATTERN_ANY);
    }
    buf::put_string(buf, flexible, None)?;
    buf::put_string(buf, flexible, None)?;
    buf.put_i8(ACL_OPERATION_ALL);
    buf.put_i8(ACL_PERMISSION_ALLOW);
    if flexible {
        buf::put_empty_tagged_fields(buf);
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DeleteAcls: first filter resource type.
pub fn decode_delete_acls_request<B: Buf>(buf: &mut B, version: i16) -> Result<i8> {
    let flexible = acl_api_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut rt = 0i8;
    if n > 0 {
        rt = buf::get_i8(buf)?;
        let _name = buf::get_string(buf, flexible)?;
        if version >= 1 {
            let _pattern = buf::get_i8(buf)?;
        }
        let _prin = buf::get_string(buf, flexible)?;
        let _host = buf::get_string(buf, flexible)?;
        let _op = buf::get_i8(buf)?;
        let _perm = buf::get_i8(buf)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        for _ in 1..n {
            skip_delete_acl_filter(buf, version, flexible)?;
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(rt)
}

/// Encode DeleteAcls: first-filter error plus matching bindings.
pub fn encode_delete_acls_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    matching: &[AclBinding],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(1))?;
    buf.put_i16(error_code);
    buf::put_string(buf, flexible, None)?;
    buf::put_array_len(buf, flexible, Some(matching.len()))?;
    for a in matching {
        buf.put_i16(0);
        buf::put_string(buf, flexible, None)?;
        buf.put_i8(a.resource_type);
        buf::put_string(buf, flexible, Some(&a.resource_name))?;
        if version >= 1 {
            buf.put_i8(a.pattern_type);
        }
        buf::put_string(buf, flexible, Some(&a.principal))?;
        buf::put_string(buf, flexible, Some(&a.host))?;
        buf.put_i8(a.operation);
        buf.put_i8(a.permission);
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DeleteAcls: first filter error code.
pub fn decode_delete_acls_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let flexible = acl_api_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut err = 0i16;
    if n > 0 {
        err = buf::get_i16(buf)?;
        let _msg = buf::get_string(buf, flexible)?;
        let mn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        for _ in 0..mn {
            skip_delete_matching_acl(buf, version, flexible)?;
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        for _ in 1..n {
            let _ = buf::get_i16(buf)?;
            let _ = buf::get_string(buf, flexible)?;
            let extra = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            for _ in 0..extra {
                skip_delete_matching_acl(buf, version, flexible)?;
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
        }
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_acls_not_controller_is_not_at_byte_four() {
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_create_acls_response(&mut buf, version, &[crate::error::NOT_CONTROLLER])
                .unwrap();
            let b4 = buf.get(4).copied().unwrap();
            let b5 = buf.get(5).copied().unwrap();
            assert_ne!(
                i16::from_be_bytes([b4, b5]),
                crate::error::NOT_CONTROLLER,
                "throttle + creation-array length must not look like error 41 at v{version}"
            );
            let mut cur = &buf[..];
            assert_eq!(
                decode_create_acls_response(&mut cur, version).unwrap(),
                vec![crate::error::NOT_CONTROLLER]
            );
            assert!(
                !cur.has_remaining(),
                "CreateAcls v{version} NOT_CONTROLLER must be leftover-empty"
            );
        }
    }

    #[test]
    fn create_describe_delete_acls_roundtrip_v0_to_v3() {
        let acl = AclBinding::allow_topic("t", "User:alice");
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_create_acls_request(&mut buf, version, std::slice::from_ref(&acl)).unwrap();
            let mut cur = &buf[..];
            assert_eq!(
                decode_create_acls_request(&mut cur, version).unwrap(),
                vec![acl.clone()]
            );
            assert!(
                !cur.has_remaining(),
                "CreateAcls v{version} request must be leftover-empty"
            );

            let mut resp = BytesMut::new();
            encode_describe_acls_response(&mut resp, version, std::slice::from_ref(&acl)).unwrap();
            let mut cur = &resp[..];
            assert_eq!(
                decode_describe_acls_response(&mut cur, version).unwrap(),
                vec![acl.clone()]
            );
            assert!(
                !cur.has_remaining(),
                "DescribeAcls v{version} response must be leftover-empty"
            );

            let mut del = BytesMut::new();
            encode_delete_acls_request(&mut del, version, ACL_RESOURCE_TOPIC).unwrap();
            let mut cur = &del[..];
            assert_eq!(
                decode_delete_acls_request(&mut cur, version).unwrap(),
                ACL_RESOURCE_TOPIC
            );
            assert!(
                !cur.has_remaining(),
                "DeleteAcls v{version} request must be leftover-empty"
            );

            let mut delr = BytesMut::new();
            encode_delete_acls_response(&mut delr, version, 0, std::slice::from_ref(&acl)).unwrap();
            let mut cur = &delr[..];
            assert_eq!(decode_delete_acls_response(&mut cur, version).unwrap(), 0);
            assert!(
                !cur.has_remaining(),
                "DeleteAcls v{version} response must be leftover-empty"
            );
        }
        assert!(
            encode_create_acls_request(&mut BytesMut::new(), 4, std::slice::from_ref(&acl))
                .is_err(),
            "CreateAcls v4+ is not spoken"
        );
    }

    #[test]
    fn create_acls_v2_compact_layout_matches_independent_encode() {
        // Compact 1 creation type=2 name "t", LITERAL, principal "U",
        // host "*", ALL/ALLOW, empty tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x02, 0x74, 0x03, 0x02, 0x55, 0x02, 0x2a, 0x02, 0x03, 0x00, 0x00,
        ];
        let acl = AclBinding::allow_topic("t", "U");
        let mut buf = BytesMut::new();
        encode_create_acls_request(&mut buf, 2, std::slice::from_ref(&acl)).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v3 = BytesMut::new();
        encode_create_acls_request(&mut v3, 3, std::slice::from_ref(&acl)).unwrap();
        assert_eq!(
            &buf[..],
            &v3[..],
            "CreateAcls v3 must match v2 layout (user resource type is not a new field)"
        );
        let mut v0 = BytesMut::new();
        encode_create_acls_request(&mut v0, 0, std::slice::from_ref(&acl)).unwrap();
        assert_ne!(&buf[..], &v0[..], "CreateAcls v2 must not be classic v0");
        let mut v1 = BytesMut::new();
        encode_create_acls_request(&mut v1, 1, std::slice::from_ref(&acl)).unwrap();
        assert_ne!(&buf[..], &v1[..], "CreateAcls v2 must not be classic v1");
        assert_ne!(&v1[..], &v0[..], "CreateAcls v1 must include pattern type");

        // DescribeAcls v2: type=2, null name, ANY pattern, null principal/host,
        // ALL/ALLOW, empty tagged fields.
        const DESCRIBE: &[u8] = &[0x02, 0x00, 0x01, 0x00, 0x00, 0x02, 0x03, 0x00];
        buf.clear();
        encode_describe_acls_request(&mut buf, 2, ACL_RESOURCE_TOPIC).unwrap();
        assert_eq!(&buf[..], DESCRIBE);
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_acls_request(&mut cur, 2).unwrap(),
            ACL_RESOURCE_TOPIC
        );
        assert!(
            !cur.has_remaining(),
            "DescribeAcls v2 request must be leftover-empty"
        );

        // DeleteAcls v2: compact 1 filter, same fields as Describe plus
        // per-filter and top-level tagged fields.
        const DELETE: &[u8] = &[0x02, 0x02, 0x00, 0x01, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00];
        buf.clear();
        encode_delete_acls_request(&mut buf, 2, ACL_RESOURCE_TOPIC).unwrap();
        assert_eq!(&buf[..], DELETE);
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_acls_request(&mut cur, 2).unwrap(),
            ACL_RESOURCE_TOPIC
        );
        assert!(
            !cur.has_remaining(),
            "DeleteAcls v2 request must be leftover-empty"
        );
    }

    #[test]
    fn allow_topic_is_all_allow_on_topic() {
        let acl = AclBinding::allow_topic("events", "User:alice");
        assert_eq!(acl.resource_type, i8::from(AclResourceType::Topic));
        assert_eq!(acl.resource_type, ACL_RESOURCE_TOPIC);
        assert_eq!(acl.resource_name, "events");
        assert_eq!(acl.pattern_type, ACL_PATTERN_LITERAL);
        assert_eq!(acl.pattern_type, i8::from(AclPatternType::Literal));
        assert_eq!(acl.principal, "User:alice");
        assert_eq!(acl.host, "*");
        assert_eq!(acl.operation, ACL_OPERATION_ALL);
        assert_eq!(acl.permission, ACL_PERMISSION_ALLOW);
        assert_eq!(acl.operation, i8::from(AclOperation::All));
        assert_eq!(acl.permission, i8::from(AclPermission::Allow));
        assert_eq!(i8::from(AclResourceType::User), 7);
    }
}
