//! CreateAcls, DescribeAcls, and DeleteAcls (api keys 29–31).
//!
//! v0 is classic with no pattern type. v1 adds ResourcePatternType /
//! PatternTypeFilter (KIP-73; default LITERAL). v2–v3 are flexible
//! (compact arrays/strings plus tagged fields). v3 is the same layout
//! (user resource type). Kafka 4.0 `validVersions` is `1-3` (v0 removed).
//! This crate speaks 0–3. v4+ is not spoken.

use std::collections::HashMap;
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// ACL resource type: any (DescribeAcls / DeleteAcls filters).
pub const ACL_RESOURCE_ANY: i8 = 1;
/// ACL resource type: topic.
pub const ACL_RESOURCE_TOPIC: i8 = 2;
/// ACL operation: any (DescribeAcls / DeleteAcls filters).
pub const ACL_OPERATION_ANY: i8 = 1;
/// ACL operation: all.
pub const ACL_OPERATION_ALL: i8 = 2;
/// ACL operation: create delegation tokens (Java `CREATE_TOKENS`).
pub const ACL_OPERATION_CREATE_TOKENS: i8 = 13;
/// ACL operation: describe delegation tokens (Java `DESCRIBE_TOKENS`).
pub const ACL_OPERATION_DESCRIBE_TOKENS: i8 = 14;
/// ACL permission: any (DescribeAcls / DeleteAcls filters).
pub const ACL_PERMISSION_ANY: i8 = 1;
/// ACL permission: allow.
pub const ACL_PERMISSION_ALLOW: i8 = 3;
/// Java `ResourcePattern.WILDCARD_RESOURCE` (literal name for every resource).
pub const WILDCARD_RESOURCE: &str = "*";
/// Resource pattern type: any (DescribeAcls / DeleteAcls filters).
pub const ACL_PATTERN_ANY: i8 = 1;
/// Resource pattern type: match (DescribeAcls / DeleteAcls filters).
pub const ACL_PATTERN_MATCH: i8 = 2;
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

impl AclResourceType {
    /// Java `ResourceType.fromCode` (out of range is [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::Topic,
            3 => Self::Group,
            4 => Self::Cluster,
            5 => Self::TransactionalId,
            6 => Self::DelegationToken,
            7 => Self::User,
            _ => Self::Unknown,
        }
    }

    /// Java `ResourceType.fromString` (`toUpperCase`; unknown is [`Self::Unknown`]).
    #[must_use]
    pub fn from_string(name: &str) -> Self {
        if name.eq_ignore_ascii_case("UNKNOWN") {
            Self::Unknown
        } else if name.eq_ignore_ascii_case("ANY") {
            Self::Any
        } else if name.eq_ignore_ascii_case("TOPIC") {
            Self::Topic
        } else if name.eq_ignore_ascii_case("GROUP") {
            Self::Group
        } else if name.eq_ignore_ascii_case("CLUSTER") {
            Self::Cluster
        } else if name.eq_ignore_ascii_case("TRANSACTIONAL_ID") {
            Self::TransactionalId
        } else if name.eq_ignore_ascii_case("DELEGATION_TOKEN") {
            Self::DelegationToken
        } else if name.eq_ignore_ascii_case("USER") {
            Self::User
        } else {
            Self::Unknown
        }
    }

    /// Java `ResourceType.code`.
    #[must_use]
    pub const fn code(self) -> i8 {
        self as i8
    }

    /// Java `ResourceType.isUnknown`.
    #[must_use]
    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

impl fmt::Display for AclResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "UNKNOWN",
            Self::Any => "ANY",
            Self::Topic => "TOPIC",
            Self::Group => "GROUP",
            Self::Cluster => "CLUSTER",
            Self::TransactionalId => "TRANSACTIONAL_ID",
            Self::DelegationToken => "DELEGATION_TOKEN",
            Self::User => "USER",
        })
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

impl AclPatternType {
    /// Java `PatternType.fromCode` (out of range is [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::Match,
            3 => Self::Literal,
            4 => Self::Prefixed,
            _ => Self::Unknown,
        }
    }

    /// Java `PatternType.fromString` (exact enum name; unknown is [`Self::Unknown`]).
    ///
    /// Unlike [`AclResourceType::from_string`], this is **case-sensitive**
    /// (`LITERAL` matches; `literal` is Unknown).
    #[must_use]
    pub fn from_string(name: &str) -> Self {
        match name {
            "UNKNOWN" => Self::Unknown,
            "ANY" => Self::Any,
            "MATCH" => Self::Match,
            "LITERAL" => Self::Literal,
            "PREFIXED" => Self::Prefixed,
            _ => Self::Unknown,
        }
    }

    /// Java `PatternType.code`.
    #[must_use]
    pub const fn code(self) -> i8 {
        self as i8
    }

    /// Java `PatternType.isUnknown`.
    #[must_use]
    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Java `PatternType.isSpecific` (not UNKNOWN / ANY / MATCH).
    #[must_use]
    pub const fn is_specific(self) -> bool {
        matches!(self, Self::Literal | Self::Prefixed)
    }
}

impl fmt::Display for AclPatternType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "UNKNOWN",
            Self::Any => "ANY",
            Self::Match => "MATCH",
            Self::Literal => "LITERAL",
            Self::Prefixed => "PREFIXED",
        })
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
    /// Create delegation tokens (Java `CREATE_TOKENS`).
    CreateTokens = 13,
    /// Describe delegation tokens (Java `DESCRIBE_TOKENS`).
    DescribeTokens = 14,
}

impl From<AclOperation> for i8 {
    fn from(op: AclOperation) -> Self {
        op as i8
    }
}

impl AclOperation {
    /// Java `AclOperation.fromCode` (out of range is [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::All,
            3 => Self::Read,
            4 => Self::Write,
            5 => Self::Create,
            6 => Self::Delete,
            7 => Self::Alter,
            8 => Self::Describe,
            9 => Self::ClusterAction,
            10 => Self::DescribeConfigs,
            11 => Self::AlterConfigs,
            12 => Self::IdempotentWrite,
            13 => Self::CreateTokens,
            14 => Self::DescribeTokens,
            _ => Self::Unknown,
        }
    }

    /// Java `AclOperation.fromString` (`toUpperCase`; unknown is [`Self::Unknown`]).
    #[must_use]
    pub fn from_string(name: &str) -> Self {
        if name.eq_ignore_ascii_case("UNKNOWN") {
            Self::Unknown
        } else if name.eq_ignore_ascii_case("ANY") {
            Self::Any
        } else if name.eq_ignore_ascii_case("ALL") {
            Self::All
        } else if name.eq_ignore_ascii_case("READ") {
            Self::Read
        } else if name.eq_ignore_ascii_case("WRITE") {
            Self::Write
        } else if name.eq_ignore_ascii_case("CREATE") {
            Self::Create
        } else if name.eq_ignore_ascii_case("DELETE") {
            Self::Delete
        } else if name.eq_ignore_ascii_case("ALTER") {
            Self::Alter
        } else if name.eq_ignore_ascii_case("DESCRIBE") {
            Self::Describe
        } else if name.eq_ignore_ascii_case("CLUSTER_ACTION") {
            Self::ClusterAction
        } else if name.eq_ignore_ascii_case("DESCRIBE_CONFIGS") {
            Self::DescribeConfigs
        } else if name.eq_ignore_ascii_case("ALTER_CONFIGS") {
            Self::AlterConfigs
        } else if name.eq_ignore_ascii_case("IDEMPOTENT_WRITE") {
            Self::IdempotentWrite
        } else if name.eq_ignore_ascii_case("CREATE_TOKENS") {
            Self::CreateTokens
        } else if name.eq_ignore_ascii_case("DESCRIBE_TOKENS") {
            Self::DescribeTokens
        } else {
            Self::Unknown
        }
    }

    /// Java `AclOperation.code`.
    #[must_use]
    pub const fn code(self) -> i8 {
        self as i8
    }

    /// Java `AclOperation.isUnknown`.
    #[must_use]
    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

impl fmt::Display for AclOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "UNKNOWN",
            Self::Any => "ANY",
            Self::All => "ALL",
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Create => "CREATE",
            Self::Delete => "DELETE",
            Self::Alter => "ALTER",
            Self::Describe => "DESCRIBE",
            Self::ClusterAction => "CLUSTER_ACTION",
            Self::DescribeConfigs => "DESCRIBE_CONFIGS",
            Self::AlterConfigs => "ALTER_CONFIGS",
            Self::IdempotentWrite => "IDEMPOTENT_WRITE",
            Self::CreateTokens => "CREATE_TOKENS",
            Self::DescribeTokens => "DESCRIBE_TOKENS",
        })
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

impl AclPermission {
    /// Java `AclPermissionType.fromCode` (out of range is [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::Deny,
            3 => Self::Allow,
            _ => Self::Unknown,
        }
    }

    /// Java `AclPermissionType.fromString` (`toUpperCase`; unknown is [`Self::Unknown`]).
    #[must_use]
    pub fn from_string(name: &str) -> Self {
        if name.eq_ignore_ascii_case("UNKNOWN") {
            Self::Unknown
        } else if name.eq_ignore_ascii_case("ANY") {
            Self::Any
        } else if name.eq_ignore_ascii_case("DENY") {
            Self::Deny
        } else if name.eq_ignore_ascii_case("ALLOW") {
            Self::Allow
        } else {
            Self::Unknown
        }
    }

    /// Java `AclPermissionType.code`.
    #[must_use]
    pub const fn code(self) -> i8 {
        self as i8
    }

    /// Java `AclPermissionType.isUnknown`.
    #[must_use]
    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

impl fmt::Display for AclPermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "UNKNOWN",
            Self::Any => "ANY",
            Self::Deny => "DENY",
            Self::Allow => "ALLOW",
        })
    }
}

/// Java `ResourcePattern` (the resource half of an [`AclBinding`]).
///
/// Java's constructor rejects [`AclResourceType::Any`] and
/// [`AclPatternType::Any`] / [`AclPatternType::Match`]. This crate stores
/// the wire `i8` and does not panic; [`encode_create_acls_request`] checks
/// those rules. [`Self::is_unknown`] still reports UNKNOWN components.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourcePattern {
    /// Kafka resource type (`ACL_RESOURCE_TOPIC`, [`AclResourceType::Topic`], …).
    pub resource_type: i8,
    /// Resource name (topic name, …). [`WILDCARD_RESOURCE`] is every name.
    pub name: String,
    /// Pattern type (`ACL_PATTERN_LITERAL`, [`AclPatternType::Literal`], …).
    pub pattern_type: i8,
}

impl ResourcePattern {
    /// Java `ResourcePattern(ResourceType, String, PatternType)`.
    #[must_use]
    pub fn new(
        resource_type: impl Into<i8>,
        name: impl Into<String>,
        pattern_type: impl Into<i8>,
    ) -> Self {
        Self {
            resource_type: resource_type.into(),
            name: name.into(),
            pattern_type: pattern_type.into(),
        }
    }

    /// Java `ResourcePattern.resourceType`.
    #[must_use]
    pub fn resource_type(&self) -> AclResourceType {
        AclResourceType::from_id(self.resource_type)
    }

    /// Java `ResourcePattern.name`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `ResourcePattern.patternType`.
    #[must_use]
    pub fn pattern_type(&self) -> AclPatternType {
        AclPatternType::from_id(self.pattern_type)
    }

    /// Java `ResourcePattern.isUnknown`.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        self.resource_type().is_unknown() || self.pattern_type().is_unknown()
    }

    /// Java `ResourcePattern.toFilter`.
    #[must_use]
    pub fn to_filter(&self) -> ResourcePatternFilter {
        ResourcePatternFilter {
            resource_type: self.resource_type,
            name: Some(self.name.clone()),
            pattern_type: self.pattern_type,
        }
    }
}

impl fmt::Display for ResourcePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResourcePattern(resourceType={}, name={}, patternType={})",
            self.resource_type(),
            self.name,
            self.pattern_type()
        )
    }
}

/// Java `AccessControlEntry` (the ACE half of an [`AclBinding`]).
///
/// Java's constructor rejects [`AclOperation::Any`] and
/// [`AclPermission::Any`]. This crate stores the wire `i8` and does not
/// panic; [`encode_create_acls_request`] checks those rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccessControlEntry {
    /// Principal, for example `User:alice`.
    pub principal: String,
    /// Host filter (`*` is any).
    pub host: String,
    /// Operation (`ACL_OPERATION_ALL`, [`AclOperation::All`], …).
    pub operation: i8,
    /// Permission (`ACL_PERMISSION_ALLOW`, [`AclPermission::Allow`], …).
    pub permission: i8,
}

impl AccessControlEntry {
    /// Java `AccessControlEntry(principal, host, operation, permissionType)`.
    #[must_use]
    pub fn new(
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: impl Into<i8>,
        permission: impl Into<i8>,
    ) -> Self {
        Self {
            principal: principal.into(),
            host: host.into(),
            operation: operation.into(),
            permission: permission.into(),
        }
    }

    /// Java `AccessControlEntry.principal`.
    #[must_use]
    pub fn principal(&self) -> &str {
        self.principal.as_str()
    }

    /// Java `AccessControlEntry.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    /// Java `AccessControlEntry.operation`.
    #[must_use]
    pub fn operation(&self) -> AclOperation {
        AclOperation::from_id(self.operation)
    }

    /// Java `AccessControlEntry.permissionType`.
    #[must_use]
    pub fn permission_type(&self) -> AclPermission {
        AclPermission::from_id(self.permission)
    }

    /// Java `AccessControlEntry.isUnknown`.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        self.operation().is_unknown() || self.permission_type().is_unknown()
    }

    /// Java `AccessControlEntry.toFilter`.
    #[must_use]
    pub fn to_filter(&self) -> AccessControlEntryFilter {
        AccessControlEntryFilter {
            principal: Some(self.principal.clone()),
            host: Some(self.host.clone()),
            operation: self.operation,
            permission: self.permission,
        }
    }
}

impl fmt::Display for AccessControlEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_java_access_control_entry(
            f,
            Some(self.principal.as_str()),
            Some(self.host.as_str()),
            self.operation(),
            self.permission_type(),
        )
    }
}

fn write_java_acl_space_or(f: &mut fmt::Formatter<'_>, s: Option<&str>) -> fmt::Result {
    match s {
        Some(s) => f.write_str(s),
        None => f.write_str(" "),
    }
}

fn write_java_access_control_entry(
    f: &mut fmt::Formatter<'_>,
    principal: Option<&str>,
    host: Option<&str>,
    operation: AclOperation,
    permission: AclPermission,
) -> fmt::Result {
    f.write_str("(principal=")?;
    write_java_acl_space_or(f, principal)?;
    f.write_str(", host=")?;
    write_java_acl_space_or(f, host)?;
    write!(f, ", operation={operation}, permissionType={permission})")
}

/// Java `ResourcePatternFilter`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourcePatternFilter {
    /// Resource type filter (`AclResourceType::Topic`, [`ACL_RESOURCE_ANY`], …).
    pub resource_type: i8,
    /// Resource name, or `None` for any.
    pub name: Option<String>,
    /// Pattern type (`AclPatternType::Any`, …).
    pub pattern_type: i8,
}

impl ResourcePatternFilter {
    /// Java `ResourcePatternFilter(ResourceType, String, PatternType)`.
    #[must_use]
    pub fn new(
        resource_type: impl Into<i8>,
        name: Option<String>,
        pattern_type: impl Into<i8>,
    ) -> Self {
        Self {
            resource_type: resource_type.into(),
            name,
            pattern_type: pattern_type.into(),
        }
    }

    /// Java `ResourcePatternFilter.ANY`.
    #[must_use]
    pub fn any() -> Self {
        Self::new(ACL_RESOURCE_ANY, None, ACL_PATTERN_ANY)
    }

    /// Java `ResourcePatternFilter.resourceType`.
    #[must_use]
    pub fn resource_type(&self) -> AclResourceType {
        AclResourceType::from_id(self.resource_type)
    }

    /// Java `ResourcePatternFilter.name` (`None` is any).
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Java `ResourcePatternFilter.patternType`.
    #[must_use]
    pub fn pattern_type(&self) -> AclPatternType {
        AclPatternType::from_id(self.pattern_type)
    }

    /// Java `ResourcePatternFilter.isUnknown`.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        self.resource_type().is_unknown() || self.pattern_type().is_unknown()
    }

    /// Java `ResourcePatternFilter.matches`.
    ///
    /// Unsupported pattern types (Java throws) are a non-match here.
    #[must_use]
    pub fn matches(&self, pattern: &ResourcePattern) -> bool {
        if self.resource_type != ACL_RESOURCE_ANY && self.resource_type != pattern.resource_type {
            return false;
        }
        if self.pattern_type != ACL_PATTERN_ANY
            && self.pattern_type != ACL_PATTERN_MATCH
            && self.pattern_type != pattern.pattern_type
        {
            return false;
        }
        let Some(ref name) = self.name else {
            return true;
        };
        if self.pattern_type == ACL_PATTERN_ANY || self.pattern_type == pattern.pattern_type {
            return name == &pattern.name;
        }
        match AclPatternType::from_id(pattern.pattern_type) {
            AclPatternType::Literal => name == &pattern.name || pattern.name == WILDCARD_RESOURCE,
            AclPatternType::Prefixed => name.starts_with(pattern.name.as_str()),
            _ => false,
        }
    }

    /// Java `ResourcePatternFilter.matchesAtMostOne`.
    #[must_use]
    pub fn matches_at_most_one(&self) -> bool {
        self.find_indefinite_field().is_none()
    }

    /// Java `ResourcePatternFilter.findIndefiniteField`.
    #[must_use]
    pub fn find_indefinite_field(&self) -> Option<&'static str> {
        match AclResourceType::from_id(self.resource_type) {
            AclResourceType::Any => return Some("Resource type is ANY."),
            AclResourceType::Unknown => return Some("Resource type is UNKNOWN."),
            _ => {}
        }
        if self.name.is_none() {
            return Some("Resource name is NULL.");
        }
        match AclPatternType::from_id(self.pattern_type) {
            AclPatternType::Match => Some("Resource pattern type is MATCH."),
            AclPatternType::Unknown => Some("Resource pattern type is UNKNOWN."),
            _ => None,
        }
    }
}

impl fmt::Display for ResourcePatternFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java `ResourcePatternFilter.toString` uses the `ResourcePattern(` prefix.
        f.write_str("ResourcePattern(resourceType=")?;
        write!(f, "{}", self.resource_type())?;
        f.write_str(", name=")?;
        write_java_acl_space_or(f, self.name.as_deref())?;
        write!(f, ", patternType={})", self.pattern_type())
    }
}

/// Java `AccessControlEntryFilter`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccessControlEntryFilter {
    /// Principal, or `None` for any.
    pub principal: Option<String>,
    /// Host, or `None` for any.
    pub host: Option<String>,
    /// Operation (`AclOperation::Any`, …).
    pub operation: i8,
    /// Permission (`AclPermission::Any`, …).
    pub permission: i8,
}

impl AccessControlEntryFilter {
    /// Java `AccessControlEntryFilter(principal, host, operation, permissionType)`.
    #[must_use]
    pub fn new(
        principal: Option<String>,
        host: Option<String>,
        operation: impl Into<i8>,
        permission: impl Into<i8>,
    ) -> Self {
        Self {
            principal,
            host,
            operation: operation.into(),
            permission: permission.into(),
        }
    }

    /// Java `AccessControlEntryFilter.ANY`.
    #[must_use]
    pub fn any() -> Self {
        Self::new(None, None, ACL_OPERATION_ANY, ACL_PERMISSION_ANY)
    }

    /// Java `AccessControlEntryFilter.principal` (`None` is any).
    #[must_use]
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// Java `AccessControlEntryFilter.host` (`None` is any).
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// Java `AccessControlEntryFilter.operation`.
    #[must_use]
    pub fn operation(&self) -> AclOperation {
        AclOperation::from_id(self.operation)
    }

    /// Java `AccessControlEntryFilter.permissionType`.
    #[must_use]
    pub fn permission_type(&self) -> AclPermission {
        AclPermission::from_id(self.permission)
    }

    /// Java `AccessControlEntryFilter.isUnknown`.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        self.operation().is_unknown() || self.permission_type().is_unknown()
    }

    /// Java `AccessControlEntryFilter.matches`.
    #[must_use]
    pub fn matches(&self, other: &AccessControlEntry) -> bool {
        if let Some(ref principal) = self.principal {
            if principal != &other.principal {
                return false;
            }
        }
        if let Some(ref host) = self.host {
            if host != &other.host {
                return false;
            }
        }
        if self.operation != ACL_OPERATION_ANY && self.operation != other.operation {
            return false;
        }
        self.permission == ACL_PERMISSION_ANY || self.permission == other.permission
    }

    /// Java `AccessControlEntryFilter.matchesAtMostOne`.
    #[must_use]
    pub fn matches_at_most_one(&self) -> bool {
        self.find_indefinite_field().is_none()
    }

    /// Java `AccessControlEntryFilter.findIndefiniteField`.
    #[must_use]
    pub fn find_indefinite_field(&self) -> Option<&'static str> {
        if self.principal.is_none() {
            return Some("Principal is NULL");
        }
        if self.host.is_none() {
            return Some("Host is NULL");
        }
        match AclOperation::from_id(self.operation) {
            AclOperation::Any => return Some("Operation is ANY"),
            AclOperation::Unknown => return Some("Operation is UNKNOWN"),
            _ => {}
        }
        match AclPermission::from_id(self.permission) {
            AclPermission::Any => Some("Permission type is ANY"),
            AclPermission::Unknown => Some("Permission type is UNKNOWN"),
            _ => None,
        }
    }
}

impl fmt::Display for AccessControlEntryFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_java_access_control_entry(
            f,
            self.principal.as_deref(),
            self.host.as_deref(),
            self.operation(),
            self.permission_type(),
        )
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

    /// Java `AclBinding(ResourcePattern, AccessControlEntry)`.
    #[must_use]
    pub fn new(pattern: ResourcePattern, entry: AccessControlEntry) -> Self {
        Self {
            resource_type: pattern.resource_type,
            resource_name: pattern.name,
            pattern_type: pattern.pattern_type,
            principal: entry.principal,
            host: entry.host,
            operation: entry.operation,
            permission: entry.permission,
        }
    }

    /// Java `AclBinding.pattern`.
    #[must_use]
    pub fn pattern(&self) -> ResourcePattern {
        ResourcePattern {
            resource_type: self.resource_type,
            name: self.resource_name.clone(),
            pattern_type: self.pattern_type,
        }
    }

    /// Java `AclBinding.entry`.
    #[must_use]
    pub fn entry(&self) -> AccessControlEntry {
        AccessControlEntry {
            principal: self.principal.clone(),
            host: self.host.clone(),
            operation: self.operation,
            permission: self.permission,
        }
    }

    /// Java `AclBinding.isUnknown`.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        AclResourceType::from_id(self.resource_type).is_unknown()
            || AclPatternType::from_id(self.pattern_type).is_unknown()
            || AclOperation::from_id(self.operation).is_unknown()
            || AclPermission::from_id(self.permission).is_unknown()
    }

    /// Java `AclBinding.toFilter`.
    #[must_use]
    pub fn to_filter(&self) -> AclBindingFilter {
        AclBindingFilter::new(self.pattern().to_filter(), self.entry().to_filter())
    }
}

impl fmt::Display for AclBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(pattern={}, entry={})", self.pattern(), self.entry())
    }
}

/// Java `AclBindingFilter` for DescribeAcls / DeleteAcls.
///
/// Null name / principal / host match any. [`ACL_RESOURCE_ANY`] /
/// [`ACL_PATTERN_ANY`] / [`ACL_OPERATION_ANY`] / [`ACL_PERMISSION_ANY`]
/// match any on those fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBindingFilter {
    /// Resource type filter (`AclResourceType::Topic`, [`ACL_RESOURCE_ANY`], …).
    pub resource_type: i8,
    /// Resource name, or `None` for any.
    pub resource_name: Option<String>,
    /// Pattern type (`AclPatternType::Any`, …). Omitted on the wire at v0.
    pub pattern_type: i8,
    /// Principal, or `None` for any.
    pub principal: Option<String>,
    /// Host, or `None` for any.
    pub host: Option<String>,
    /// Operation (`AclOperation::Any`, …).
    pub operation: i8,
    /// Permission (`AclPermission::Any`, …).
    pub permission: i8,
}

impl AclBindingFilter {
    /// Java `AclBindingFilter(ResourcePatternFilter, AccessControlEntryFilter)`.
    #[must_use]
    pub fn new(
        pattern_filter: ResourcePatternFilter,
        entry_filter: AccessControlEntryFilter,
    ) -> Self {
        Self {
            resource_type: pattern_filter.resource_type,
            resource_name: pattern_filter.name,
            pattern_type: pattern_filter.pattern_type,
            principal: entry_filter.principal,
            host: entry_filter.host,
            operation: entry_filter.operation,
            permission: entry_filter.permission,
        }
    }

    /// Match every binding of this resource type (Java
    /// `AclBindingFilter` with `ResourcePatternFilter(type, null, ANY)`
    /// and `AccessControlEntryFilter.ANY`).
    #[must_use]
    pub fn resource_type(resource_type: impl Into<i8>) -> Self {
        Self {
            resource_type: resource_type.into(),
            resource_name: None,
            pattern_type: ACL_PATTERN_ANY,
            principal: None,
            host: None,
            operation: ACL_OPERATION_ANY,
            permission: ACL_PERMISSION_ANY,
        }
    }

    /// Match every binding (Java `AclBindingFilter.ANY`).
    #[must_use]
    pub fn any() -> Self {
        Self::resource_type(ACL_RESOURCE_ANY)
    }

    /// Resource name filter (`None` is any).
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.resource_name = Some(name.into());
        self
    }

    /// Principal filter (`None` is any).
    #[must_use]
    pub fn principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Host filter (`None` is any).
    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Pattern type (`AclPatternType::Literal`, …).
    #[must_use]
    pub fn pattern_type(mut self, pattern_type: impl Into<i8>) -> Self {
        self.pattern_type = pattern_type.into();
        self
    }

    /// Operation (`AclOperation::Read`, …).
    #[must_use]
    pub fn operation(mut self, operation: impl Into<i8>) -> Self {
        self.operation = operation.into();
        self
    }

    /// Permission (`AclPermission::Deny`, …).
    #[must_use]
    pub fn permission(mut self, permission: impl Into<i8>) -> Self {
        self.permission = permission.into();
        self
    }

    /// Whether `acl` matches this filter (Java `AclBindingFilter.matches`).
    #[must_use]
    pub fn matches(&self, acl: &AclBinding) -> bool {
        if self.resource_type != ACL_RESOURCE_ANY && self.resource_type != acl.resource_type {
            return false;
        }
        if self.pattern_type != ACL_PATTERN_ANY
            && self.pattern_type != ACL_PATTERN_MATCH
            && self.pattern_type != acl.pattern_type
        {
            return false;
        }
        if let Some(ref name) = self.resource_name {
            if self.pattern_type == ACL_PATTERN_PREFIXED {
                if !acl.resource_name.starts_with(name) {
                    return false;
                }
            } else if &acl.resource_name != name {
                return false;
            }
        }
        if let Some(ref principal) = self.principal {
            if &acl.principal != principal {
                return false;
            }
        }
        if let Some(ref host) = self.host {
            if &acl.host != host {
                return false;
            }
        }
        if self.operation != ACL_OPERATION_ANY && self.operation != acl.operation {
            return false;
        }
        if self.permission != ACL_PERMISSION_ANY && self.permission != acl.permission {
            return false;
        }
        true
    }

    /// Java `AclBindingFilter.patternFilter`.
    #[must_use]
    pub fn pattern_filter(&self) -> ResourcePatternFilter {
        ResourcePatternFilter {
            resource_type: self.resource_type,
            name: self.resource_name.clone(),
            pattern_type: self.pattern_type,
        }
    }

    /// Java `AclBindingFilter.entryFilter`.
    #[must_use]
    pub fn entry_filter(&self) -> AccessControlEntryFilter {
        AccessControlEntryFilter {
            principal: self.principal.clone(),
            host: self.host.clone(),
            operation: self.operation,
            permission: self.permission,
        }
    }

    /// Java `AclBindingFilter.isUnknown`.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        AclResourceType::from_id(self.resource_type).is_unknown()
            || AclPatternType::from_id(self.pattern_type).is_unknown()
            || AclOperation::from_id(self.operation).is_unknown()
            || AclPermission::from_id(self.permission).is_unknown()
    }

    /// Java `AclBindingFilter.matchesAtMostOne`.
    #[must_use]
    pub fn matches_at_most_one(&self) -> bool {
        self.find_indefinite_field().is_none()
    }

    /// Java `AclBindingFilter.findIndefiniteField`.
    #[must_use]
    pub fn find_indefinite_field(&self) -> Option<&'static str> {
        self.pattern_filter()
            .find_indefinite_field()
            .or_else(|| self.entry_filter().find_indefinite_field())
    }
}

impl fmt::Display for AclBindingFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(patternFilter={}, entryFilter={})",
            self.pattern_filter(),
            self.entry_filter()
        )
    }
}

/// Per-filter DeleteAcls result (Java `DeleteAclsResult.FilterResults`).
///
/// [`Self::error`] is Java `DeleteAclsRequest.getErrorResponse` one
/// FilterResult (`ErrorCode`; MatchingAcls stay the JSON default, empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedAclsFilterResult {
    /// Filter-level error, or `0`.
    pub error_code: i16,
    /// Filter-level error message.
    pub error_message: Option<String>,
    /// Bindings that matched this filter.
    pub matching: Vec<AclBinding>,
}

impl DeletedAclsFilterResult {
    /// Filter-level error, or `0`.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Filter-level error message.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Bindings that matched this filter (Java `FilterResults.values` bindings).
    #[must_use]
    pub fn matching(&self) -> &[AclBinding] {
        &self.matching
    }

    /// Java `DeleteAclsRequest.getErrorResponse` one FilterResult.
    ///
    /// Sets `ErrorCode`. `ErrorMessage` is the JSON default (null);
    /// official Java also sets the English `Errors.message` string.
    /// MatchingAcls stay the JSON default (empty); request filter
    /// fields are not copied. Throttle on the response is the JSON
    /// default (`0`).
    #[must_use]
    pub fn error(error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            matching: Vec::new(),
        }
    }

    /// Java `DeleteAclsRequest.getErrorResponse` FilterResults
    /// (`Collections.nCopies`).
    #[must_use]
    pub fn error_results(n: usize, error_code: i16) -> Vec<Self> {
        vec![Self::error(error_code); n]
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

/// Java `CreateAclsResponse` helpers.
pub struct CreateAclsResponse;

impl CreateAclsResponse {
    /// Java `CreateAclsResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }

    /// Java `CreateAclsResponse.errorCounts`.
    ///
    /// Counts per-creation error codes (including `NONE`).
    #[must_use]
    pub fn error_counts(results: &[AclCreationResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        for result in results {
            let count = counts.entry(result.error_code).or_insert(0);
            *count += 1;
        }
        counts
    }
}

/// Per-creation CreateAcls result (Java `AclCreationResult`).
///
/// [`Self::error`] / [`Self::error_results`] are Java
/// `CreateAclsRequest.getErrorResponse` one result / `nCopies`. Request
/// bindings are not copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclCreationResult {
    /// Per-creation error, or `0`.
    pub error_code: i16,
    /// Per-creation error message.
    pub error_message: Option<String>,
}

impl AclCreationResult {
    /// Per-creation error, or `0`.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Per-creation error message.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Java `CreateAclsRequest.getErrorResponse` one result.
    ///
    /// Sets `ErrorCode`. `ErrorMessage` is the JSON default (null);
    /// official Java also sets the English `Errors.message` string.
    /// Request ACL bindings are not copied. Throttle on the response is
    /// the JSON default (`0`).
    #[must_use]
    pub fn error(error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
        }
    }

    /// Java `CreateAclsRequest.getErrorResponse` Results
    /// (`Collections.nCopies`).
    #[must_use]
    pub fn error_results(n: usize, error_code: i16) -> Vec<Self> {
        vec![Self::error(error_code); n]
    }
}

/// One resource in a DescribeAcls response (Java `DescribeAclsResource`).
///
/// [`DescribeAclsResponse::acls_resources`] groups [`AclBinding`]s that
/// share a [`ResourcePattern`]. Encode writes this grouped layout;
/// decode flattens with [`DescribeAclsResponse::acl_bindings`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResource {
    /// Kafka resource type (`ACL_RESOURCE_TOPIC`, [`AclResourceType::Topic`], …).
    pub resource_type: i8,
    /// Resource name (topic name, …).
    pub resource_name: String,
    /// Pattern type (`ACL_PATTERN_LITERAL`, [`AclPatternType::Literal`], …).
    pub pattern_type: i8,
    /// ACEs on this resource (Java `AclDescription` list).
    pub acls: Vec<AccessControlEntry>,
}

impl DescribeAclsResource {
    /// Java `DescribeAclsResource.resourceType`.
    #[must_use]
    pub fn resource_type(&self) -> AclResourceType {
        AclResourceType::from_id(self.resource_type)
    }

    /// Java `DescribeAclsResource.resourceName`.
    #[must_use]
    pub fn resource_name(&self) -> &str {
        self.resource_name.as_str()
    }

    /// Java `DescribeAclsResource.patternType`.
    #[must_use]
    pub fn pattern_type(&self) -> AclPatternType {
        AclPatternType::from_id(self.pattern_type)
    }

    /// Java `DescribeAclsResource.acls`.
    #[must_use]
    pub fn acls(&self) -> &[AccessControlEntry] {
        &self.acls
    }

    /// Java `ResourcePattern` for this resource.
    #[must_use]
    pub fn pattern(&self) -> ResourcePattern {
        ResourcePattern {
            resource_type: self.resource_type,
            name: self.resource_name.clone(),
            pattern_type: self.pattern_type,
        }
    }
}

/// Java `DescribeAclsResponse` helpers.
pub struct DescribeAclsResponse;

impl DescribeAclsResponse {
    /// Java `DescribeAclsResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }

    /// Java `DescribeAclsResponse.aclsResources`.
    ///
    /// Groups bindings that share a [`ResourcePattern`]. Duplicate ACEs
    /// on the same pattern are dropped (Java `HashSet`). Resource and
    /// ACE order is first-seen (Java `HashMap` / `HashSet` order is not
    /// stable).
    #[must_use]
    pub fn acls_resources(acls: &[AclBinding]) -> Vec<DescribeAclsResource> {
        let mut resources: Vec<DescribeAclsResource> = Vec::new();
        for acl in acls {
            let entry = acl.entry();
            if let Some(resource) = resources.iter_mut().find(|r| {
                r.resource_type == acl.resource_type
                    && r.resource_name == acl.resource_name
                    && r.pattern_type == acl.pattern_type
            }) {
                if !resource.acls.contains(&entry) {
                    resource.acls.push(entry);
                }
            } else {
                resources.push(DescribeAclsResource {
                    resource_type: acl.resource_type,
                    resource_name: acl.resource_name.clone(),
                    pattern_type: acl.pattern_type,
                    acls: vec![entry],
                });
            }
        }
        resources
    }

    /// Java `DescribeAclsResponse.aclBindings`.
    #[must_use]
    pub fn acl_bindings(resources: &[DescribeAclsResource]) -> Vec<AclBinding> {
        resources
            .iter()
            .flat_map(|resource| {
                resource
                    .acls
                    .iter()
                    .cloned()
                    .map(|entry| AclBinding::new(resource.pattern(), entry))
            })
            .collect()
    }
}

/// Java `DeleteAclsResponse` helpers.
pub struct DeleteAclsResponse;

impl DeleteAclsResponse {
    /// Java `DeleteAclsResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }

    /// Java `DeleteAclsResponse.errorCounts`.
    ///
    /// Counts filter-level error codes (including `NONE`). Matching-ACL
    /// codes are not included (Java `filterResults` only).
    #[must_use]
    pub fn error_counts(results: &[DeletedAclsFilterResult]) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        for result in results {
            let count = counts.entry(result.error_code).or_insert(0);
            *count += 1;
        }
        counts
    }
}

/// Java `DescribeAclsResponse.validate` / `DeleteAclsResponse.validate` /
/// `CreateAclsRequest.validate`: v0 only supports LITERAL patterns.
fn reject_v0_non_literal_acl_patterns<'a>(
    version: i16,
    acls: impl IntoIterator<Item = &'a AclBinding>,
) -> Result<()> {
    if version == 0
        && acls
            .into_iter()
            .any(|a| a.pattern_type != ACL_PATTERN_LITERAL)
    {
        return Err(Error::Unsupported(
            "Version 0 only supports literal resource pattern types".into(),
        ));
    }
    Ok(())
}

/// Java `CreateAclsRequest.validate` unknown-element check.
///
/// Official Java appends generated `AclCreation.toString` after the
/// colon. This crate omits that generated list (crate [`AclBinding`]
/// `Display` is `AclBinding.toString`, not `AclCreation`).
fn reject_create_acls_unknown_elements(acls: &[AclBinding]) -> Result<()> {
    if acls.iter().any(AclBinding::is_unknown) {
        return Err(Error::protocol("CreatableAcls contain unknown elements"));
    }
    Ok(())
}

/// Java `DescribeAclsRequest.normalizeAndValidate` unknown-element check.
///
/// Official Java appends generated `DescribeAclsRequestData.toString`
/// after the colon. This crate omits that generated body.
fn reject_describe_acls_unknown_elements(filter: &AclBindingFilter) -> Result<()> {
    if filter.is_unknown() {
        return Err(Error::protocol(
            "DescribeAclsRequest contains UNKNOWN elements",
        ));
    }
    Ok(())
}

/// Java `DeleteAclsRequest.normalizeAndValidate` unknown-element check.
///
/// Official Java appends generated `DeleteAclsFilter.toString` after
/// `filters: `. This crate omits that generated list.
fn reject_delete_acls_unknown_elements(filters: &[AclBindingFilter]) -> Result<()> {
    if filters.iter().any(AclBindingFilter::is_unknown) {
        return Err(Error::protocol("Filters contain UNKNOWN elements"));
    }
    Ok(())
}

/// Java `DescribeAclsResponse.validate` unknown-element check.
fn reject_describe_acls_response_unknown_elements(acls: &[AclBinding]) -> Result<()> {
    if acls.iter().any(AclBinding::is_unknown) {
        return Err(Error::protocol("Contain UNKNOWN elements"));
    }
    Ok(())
}

/// Java `DeleteAclsResponse.validate` unknown-element check on MatchingAcls.
fn reject_delete_acls_matching_unknown_elements(results: &[DeletedAclsFilterResult]) -> Result<()> {
    if results
        .iter()
        .flat_map(|r| r.matching.iter())
        .any(AclBinding::is_unknown)
    {
        return Err(Error::protocol(
            "DeleteAclsMatchingAcls contain UNKNOWN elements",
        ));
    }
    Ok(())
}

/// Java `ResourcePattern` / `AccessControlEntry` constructors: CreateAcls
/// bindings must not use ANY resource type, ANY/MATCH pattern type, or
/// ANY operation / permission (filters still use those on Describe/Delete).
fn reject_create_acls_java_constructors(acls: &[AclBinding]) -> Result<()> {
    for a in acls {
        if a.resource_type == ACL_RESOURCE_ANY {
            return Err(Error::protocol("resourceType must not be ANY"));
        }
        if a.pattern_type == ACL_PATTERN_ANY || a.pattern_type == ACL_PATTERN_MATCH {
            return Err(Error::protocol(format!(
                "patternType must not be {}",
                AclPatternType::from_id(a.pattern_type)
            )));
        }
        if a.operation == ACL_OPERATION_ANY {
            return Err(Error::protocol("operation must not be ANY"));
        }
        if a.permission == ACL_PERMISSION_ANY {
            return Err(Error::protocol("permissionType must not be ANY"));
        }
    }
    Ok(())
}

/// Encode CreateAcls v0–3 (classic through v1; flexible from v2).
///
/// Java `ResourcePattern` / `AccessControlEntry` constructors reject ANY
/// resource type, ANY/MATCH pattern type, and ANY operation / permission.
/// Java `CreateAclsRequest.validate` rejects non-LITERAL pattern types
/// on v0 (`UnsupportedVersionException`) and UNKNOWN resource / pattern /
/// operation / permission (`IllegalArgumentException`).
pub fn encode_create_acls_request(
    buf: &mut BytesMut,
    version: i16,
    acls: &[AclBinding],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    reject_create_acls_java_constructors(acls)?;
    reject_v0_non_literal_acl_patterns(version, acls.iter())?;
    reject_create_acls_unknown_elements(acls)?;
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

/// Encode CreateAcls: throttle `0` plus per-binding results.
///
/// Each result is [`AclCreationResult`] (ErrorCode; ErrorMessage is the
/// JSON default, null).
pub fn encode_create_acls_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[AclCreationResult],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode CreateAcls: per-binding [`AclCreationResult`]s.
pub fn decode_create_acls_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<AclCreationResult>> {
    let flexible = acl_api_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(AclCreationResult {
            error_code,
            error_message,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// Encode DescribeAcls with a Java `AclBindingFilter`.
///
/// v1+ sends [`AclBindingFilter::pattern_type`]. v0 omits it. Java
/// `DescribeAclsRequest.normalizeAndValidate` rejects MATCH / PREFIXED /
/// UNKNOWN on v0 (`UnsupportedVersionException`); ANY is allowed (Java
/// rewrites it to LITERAL in memory; the v0 field is omitted either way).
/// UNKNOWN resource / pattern / operation / permission is Java
/// `IllegalArgumentException` on every version (after the v0 pattern
/// check).
pub fn encode_describe_acls_request(
    buf: &mut BytesMut,
    version: i16,
    filter: &AclBindingFilter,
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    if version == 0
        && filter.pattern_type != ACL_PATTERN_LITERAL
        && filter.pattern_type != ACL_PATTERN_ANY
    {
        return Err(Error::Unsupported(
            "Version 0 only supports literal resource pattern types".into(),
        ));
    }
    reject_describe_acls_unknown_elements(filter)?;
    put_acl_filter_fields(buf, version, flexible, filter)?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DescribeAcls: the filter. v0 fills [`ACL_PATTERN_ANY`].
pub fn decode_describe_acls_request<B: Buf>(buf: &mut B, version: i16) -> Result<AclBindingFilter> {
    let flexible = acl_api_flexible(version)?;
    let filter = get_acl_filter_fields(buf, version, flexible)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(filter)
}

/// Encode DescribeAcls with matching bindings.
///
/// Java `DescribeAclsResponse.aclsResources` groups bindings that share
/// a [`ResourcePattern`] (one resource, several ACEs). Java
/// `DescribeAclsResponse.validate` rejects non-LITERAL pattern types
/// on v0 (`UnsupportedVersionException`) and UNKNOWN resource / pattern /
/// operation / permission (`Contain UNKNOWN elements`).
pub fn encode_describe_acls_response(
    buf: &mut BytesMut,
    version: i16,
    acls: &[AclBinding],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    reject_v0_non_literal_acl_patterns(version, acls.iter())?;
    reject_describe_acls_response_unknown_elements(acls)?;
    let resources = DescribeAclsResponse::acls_resources(acls);
    buf.put_i32(0);
    buf.put_i16(0);
    buf::put_string(buf, flexible, None)?;
    buf::put_array_len(buf, flexible, Some(resources.len()))?;
    for resource in &resources {
        buf.put_i8(resource.resource_type);
        buf::put_string(buf, flexible, Some(&resource.resource_name))?;
        if version >= 1 {
            buf.put_i8(resource.pattern_type);
        }
        buf::put_array_len(buf, flexible, Some(resource.acls.len()))?;
        for ace in &resource.acls {
            buf::put_string(buf, flexible, Some(&ace.principal))?;
            buf::put_string(buf, flexible, Some(&ace.host))?;
            buf.put_i8(ace.operation);
            buf.put_i8(ace.permission);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DescribeAcls bindings. Top-level error returns an empty list.
///
/// Flattens grouped resources with [`DescribeAclsResponse::acl_bindings`]
/// (Java `DescribeAclsResponse.aclBindings`).
pub fn decode_describe_acls_response<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<AclBinding>> {
    let flexible = acl_api_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let err = buf::get_i16(buf)?;
    let _msg = buf::get_string(buf, flexible)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut resources = Vec::new();
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let resource_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pattern_type = if version >= 1 {
            buf::get_i8(buf)?
        } else {
            ACL_PATTERN_LITERAL
        };
        let an = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut acls = Vec::with_capacity(an);
        for _ in 0..an {
            let principal = buf::get_string(buf, flexible)?.unwrap_or_default();
            let host = buf::get_string(buf, flexible)?.unwrap_or_default();
            let operation = buf::get_i8(buf)?;
            let permission = buf::get_i8(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            acls.push(AccessControlEntry {
                principal,
                host,
                operation,
                permission,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        resources.push(DescribeAclsResource {
            resource_type,
            resource_name,
            pattern_type,
            acls,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    if err != 0 {
        return Ok(Vec::new());
    }
    Ok(DescribeAclsResponse::acl_bindings(&resources))
}

fn put_acl_filter_fields(
    buf: &mut BytesMut,
    version: i16,
    flexible: bool,
    filter: &AclBindingFilter,
) -> Result<()> {
    buf.put_i8(filter.resource_type);
    buf::put_string(buf, flexible, filter.resource_name.as_deref())?;
    if version >= 1 {
        buf.put_i8(filter.pattern_type);
    }
    buf::put_string(buf, flexible, filter.principal.as_deref())?;
    buf::put_string(buf, flexible, filter.host.as_deref())?;
    buf.put_i8(filter.operation);
    buf.put_i8(filter.permission);
    Ok(())
}

fn get_acl_filter_fields<B: Buf>(
    buf: &mut B,
    version: i16,
    flexible: bool,
) -> Result<AclBindingFilter> {
    let resource_type = buf::get_i8(buf)?;
    let resource_name = buf::get_string(buf, flexible)?;
    let pattern_type = if version >= 1 {
        buf::get_i8(buf)?
    } else {
        ACL_PATTERN_ANY
    };
    let principal = buf::get_string(buf, flexible)?;
    let host = buf::get_string(buf, flexible)?;
    let operation = buf::get_i8(buf)?;
    let permission = buf::get_i8(buf)?;
    Ok(AclBindingFilter {
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission,
    })
}

/// Encode DeleteAcls Filters of N (Java `deleteAcls(Collection)`).
///
/// v1+ sends [`AclBindingFilter::pattern_type`] on each filter. v0 omits it.
/// Java `DeleteAclsRequest.normalizeAndValidate` rejects MATCH / PREFIXED /
/// UNKNOWN on v0 (`UnsupportedVersionException`); ANY is allowed (Java
/// rewrites it to LITERAL in memory; the v0 field is omitted either way).
/// UNKNOWN resource / pattern / operation / permission is Java
/// `IllegalArgumentException` on every version (after the v0 pattern
/// check).
pub fn encode_delete_acls_request(
    buf: &mut BytesMut,
    version: i16,
    filters: &[AclBindingFilter],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    if version == 0 {
        for filter in filters {
            if filter.pattern_type != ACL_PATTERN_LITERAL && filter.pattern_type != ACL_PATTERN_ANY
            {
                return Err(Error::Unsupported(format!(
                    "Version 0 does not support pattern type {} (only LITERAL and ANY are supported)",
                    AclPatternType::from_id(filter.pattern_type)
                )));
            }
        }
    }
    reject_delete_acls_unknown_elements(filters)?;
    buf::put_array_len(buf, flexible, Some(filters.len()))?;
    for filter in filters {
        put_acl_filter_fields(buf, version, flexible, filter)?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DeleteAcls: every filter.
pub fn decode_delete_acls_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<AclBindingFilter>> {
    let flexible = acl_api_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let filter = get_acl_filter_fields(buf, version, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(filter);
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

fn put_delete_matching_acl(
    buf: &mut BytesMut,
    version: i16,
    flexible: bool,
    acl: &AclBinding,
) -> Result<()> {
    buf.put_i16(0);
    buf::put_string(buf, flexible, None)?;
    buf.put_i8(acl.resource_type);
    buf::put_string(buf, flexible, Some(&acl.resource_name))?;
    if version >= 1 {
        buf.put_i8(acl.pattern_type);
    }
    buf::put_string(buf, flexible, Some(&acl.principal))?;
    buf::put_string(buf, flexible, Some(&acl.host))?;
    buf.put_i8(acl.operation);
    buf.put_i8(acl.permission);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn get_delete_matching_acl<B: Buf>(
    buf: &mut B,
    version: i16,
    flexible: bool,
) -> Result<AclBinding> {
    let _err = buf::get_i16(buf)?;
    let _msg = buf::get_string(buf, flexible)?;
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
    Ok(AclBinding {
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission,
    })
}

/// Encode DeleteAcls: first-filter error plus matching bindings.
pub fn encode_delete_acls_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    matching: &[AclBinding],
) -> Result<()> {
    encode_delete_acls_filter_results(
        buf,
        version,
        &[DeletedAclsFilterResult {
            error_code,
            error_message: None,
            matching: matching.to_vec(),
        }],
    )
}

/// Encode DeleteAcls FilterResults of N.
///
/// Java `DeleteAclsResponse.validate` rejects non-LITERAL matching ACL
/// pattern types on v0 (`UnsupportedVersionException`) and UNKNOWN
/// resource / pattern / operation / permission on MatchingAcls
/// (`DeleteAclsMatchingAcls contain UNKNOWN elements`).
pub fn encode_delete_acls_filter_results(
    buf: &mut BytesMut,
    version: i16,
    results: &[DeletedAclsFilterResult],
) -> Result<()> {
    let flexible = acl_api_flexible(version)?;
    reject_v0_non_literal_acl_patterns(version, results.iter().flat_map(|r| r.matching.iter()))?;
    reject_delete_acls_matching_unknown_elements(results)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        buf::put_array_len(buf, flexible, Some(r.matching.len()))?;
        for a in &r.matching {
            put_delete_matching_acl(buf, version, flexible, a)?;
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode DeleteAcls: first filter error code.
pub fn decode_delete_acls_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let results = decode_delete_acls_filter_results(buf, version)?;
    Ok(results.first().map(|r| r.error_code).unwrap_or(0))
}

/// Decode DeleteAcls: every FilterResult.
pub fn decode_delete_acls_filter_results<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DeletedAclsFilterResult>> {
    let flexible = acl_api_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        let mn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut matching = Vec::with_capacity(mn);
        for _ in 0..mn {
            matching.push(get_delete_matching_acl(buf, version, flexible)?);
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(DeletedAclsFilterResult {
            error_code,
            error_message,
            matching,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn create_acls_not_controller_is_not_at_byte_four() {
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_create_acls_response(
                &mut buf,
                version,
                &[AclCreationResult::error(crate::error::NOT_CONTROLLER)],
            )
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
                vec![AclCreationResult::error(crate::error::NOT_CONTROLLER)]
            );
            assert!(
                !cur.has_remaining(),
                "CreateAcls v{version} NOT_CONTROLLER must be leftover-empty"
            );
        }
    }

    #[test]
    fn create_acls_response_error_counts_matches_java() {
        assert!(CreateAclsResponse::error_counts(&[]).is_empty());
        let counts = CreateAclsResponse::error_counts(&[
            AclCreationResult::error(0),
            AclCreationResult::error(crate::error::SECURITY_DISABLED),
            AclCreationResult::error(0),
            AclCreationResult::error(crate::error::CLUSTER_AUTHORIZATION_FAILED),
        ]);
        assert_eq!(
            counts,
            HashMap::from([
                (0, 2),
                (crate::error::SECURITY_DISABLED, 1),
                (crate::error::CLUSTER_AUTHORIZATION_FAILED, 1),
            ])
        );
    }

    #[test]
    fn delete_acls_response_error_counts_matches_java() {
        assert!(DeleteAclsResponse::error_counts(&[]).is_empty());
        let counts = DeleteAclsResponse::error_counts(&[
            DeletedAclsFilterResult::error(0),
            DeletedAclsFilterResult::error(crate::error::SECURITY_DISABLED),
            DeletedAclsFilterResult::error(0),
        ]);
        assert_eq!(
            counts,
            HashMap::from([(0, 2), (crate::error::SECURITY_DISABLED, 1),])
        );
    }

    #[test]
    fn create_acls_get_error_response_does_not_copy_bindings() {
        let err = AclCreationResult::error_results(2, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(err.len(), 2);
        let first = err.first().expect("first creation");
        assert_eq!(
            first.error_code(),
            crate::error::CLUSTER_AUTHORIZATION_FAILED
        );
        assert!(first.error_message().is_none());
        assert_eq!(
            err,
            vec![
                AclCreationResult::error(crate::error::CLUSTER_AUTHORIZATION_FAILED),
                AclCreationResult::error(crate::error::CLUSTER_AUTHORIZATION_FAILED),
            ]
        );
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_create_acls_response(&mut buf, version, &err).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(decode_create_acls_response(&mut cur, version).unwrap(), err);
            assert!(
                !cur.has_remaining(),
                "CreateAcls v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }
        let empty = AclCreationResult::error_results(0, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert!(empty.is_empty());
        for version in [0i16, 1, 2, 3] {
            let mut buf = BytesMut::new();
            encode_create_acls_response(&mut buf, version, &empty).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_create_acls_response(&mut cur, version).unwrap(),
                empty
            );
            assert!(
                !cur.has_remaining(),
                "CreateAcls v{version} empty getErrorResponse leftover-empty; leftover {} bytes",
                cur.remaining()
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
            encode_delete_acls_request(
                &mut del,
                version,
                &[AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC)],
            )
            .unwrap();
            let mut cur = &del[..];
            let got = decode_delete_acls_request(&mut cur, version).unwrap();
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].resource_type, ACL_RESOURCE_TOPIC);
            assert_eq!(got[0].operation, ACL_OPERATION_ANY);
            assert_eq!(got[0].permission, ACL_PERMISSION_ANY);
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
    fn describe_acls_response_groups_bindings_like_java() {
        let alice = AclBinding::allow_topic("t", "User:alice");
        let bob = AclBinding::allow_topic("t", "User:bob");
        let other = AclBinding::allow_topic("u", "User:alice");
        let grouped = DescribeAclsResponse::acls_resources(&[
            alice.clone(),
            bob.clone(),
            alice.clone(),
            other.clone(),
        ]);
        assert_eq!(grouped.len(), 2, "two ResourcePatterns");
        let first = grouped.first().expect("topic t");
        assert_eq!(first.resource_name(), "t");
        assert_eq!(first.resource_type(), AclResourceType::Topic);
        assert_eq!(first.pattern_type(), AclPatternType::Literal);
        assert_eq!(first.pattern(), alice.pattern());
        assert_eq!(first.acls(), &[alice.entry(), bob.entry()]);
        let second = grouped.get(1).expect("topic u");
        assert_eq!(second.resource_name(), "u");
        assert_eq!(second.acls(), &[other.entry()]);
        assert_eq!(
            DescribeAclsResponse::acl_bindings(&grouped),
            vec![alice.clone(), bob.clone(), other.clone()],
            "aclBindings flattens grouped resources"
        );

        for version in [0i16, 1, 2, 3] {
            let acls = [alice.clone(), bob.clone()];
            let mut buf = BytesMut::new();
            encode_describe_acls_response(&mut buf, version, &acls).unwrap();
            let mut cur = buf.as_ref();
            assert_eq!(
                decode_describe_acls_response(&mut cur, version).unwrap(),
                acls.to_vec()
            );
            assert!(
                !cur.has_remaining(),
                "DescribeAcls v{version} grouped leftover-empty; leftover {} bytes",
                cur.remaining()
            );
        }

        // v0 classic: throttle i32 + error i16 + null STRING i16, then
        // Resources INT32 length. Grouped same-pattern ACLs are one resource.
        let mut v0 = BytesMut::new();
        encode_describe_acls_response(&mut v0, 0, &[alice.clone(), bob.clone()]).unwrap();
        assert_eq!(
            v0.get(8..12),
            Some([0, 0, 0, 1].as_slice()),
            "v0 grouped Resources length is 1"
        );
        // v2 flexible: throttle i32 + error i16 + compact-null STRING,
        // then compact array length n+1 (1 resource is 0x02).
        let mut v2 = BytesMut::new();
        encode_describe_acls_response(&mut v2, 2, &[alice, bob]).unwrap();
        assert_eq!(
            v2.get(7).copied(),
            Some(0x02),
            "v2 grouped Resources compact length is 1"
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
        // ANY/ANY (Java AccessControlEntryFilter.ANY), empty tagged fields.
        const DESCRIBE: &[u8] = &[0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x01, 0x00];
        buf.clear();
        encode_describe_acls_request(
            &mut buf,
            2,
            &AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC),
        )
        .unwrap();
        assert_eq!(&buf[..], DESCRIBE);
        let mut cur = &buf[..];
        let got = decode_describe_acls_request(&mut cur, 2).unwrap();
        assert_eq!(got.resource_type, ACL_RESOURCE_TOPIC);
        assert_eq!(got.operation, ACL_OPERATION_ANY);
        assert_eq!(got.permission, ACL_PERMISSION_ANY);
        assert!(
            !cur.has_remaining(),
            "DescribeAcls v2 request must be leftover-empty"
        );

        // DeleteAcls v2: compact 1 filter, same fields as Describe plus
        // per-filter and top-level tagged fields.
        const DELETE: &[u8] = &[0x02, 0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00];
        buf.clear();
        encode_delete_acls_request(
            &mut buf,
            2,
            &[AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC)],
        )
        .unwrap();
        assert_eq!(&buf[..], DELETE);
        let mut cur = &buf[..];
        let got = decode_delete_acls_request(&mut cur, 2).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].resource_type, ACL_RESOURCE_TOPIC);
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
        assert_eq!(acl.pattern().name(), "events");
        assert_eq!(acl.pattern().resource_type(), AclResourceType::Topic);
        assert_eq!(acl.pattern().pattern_type(), AclPatternType::Literal);
        assert_eq!(acl.entry().principal(), "User:alice");
        assert_eq!(acl.entry().host(), "*");
        assert_eq!(acl.entry().operation(), AclOperation::All);
        assert_eq!(acl.entry().permission_type(), AclPermission::Allow);
        assert!(!acl.is_unknown());
        assert_eq!(
            acl.to_string(),
            "(pattern=ResourcePattern(resourceType=TOPIC, name=events, patternType=LITERAL), entry=(principal=User:alice, host=*, operation=ALL, permissionType=ALLOW))"
        );
        assert_eq!(
            acl.pattern().to_string(),
            "ResourcePattern(resourceType=TOPIC, name=events, patternType=LITERAL)"
        );
        assert_eq!(
            acl.entry().to_string(),
            "(principal=User:alice, host=*, operation=ALL, permissionType=ALLOW)"
        );
        let filter = acl.to_filter();
        assert!(filter.matches(&acl));
        assert!(filter.matches_at_most_one());
        assert!(filter.find_indefinite_field().is_none());
        assert_eq!(AclBinding::new(acl.pattern(), acl.entry()), acl);
    }

    #[test]
    fn acl_java_types_match_kafka() {
        assert_eq!(AclResourceType::Topic.code(), 2);
        assert_eq!(AclResourceType::User.code(), 7);
        assert_eq!(AclPatternType::Literal.code(), 3);
        assert_eq!(AclPatternType::Prefixed.code(), 4);
        assert_eq!(AclOperation::Read.code(), 3);
        assert_eq!(
            AclOperation::CreateTokens.code(),
            ACL_OPERATION_CREATE_TOKENS
        );
        assert_eq!(AclPermission::Allow.code(), ACL_PERMISSION_ALLOW);
        assert_eq!(AclPermission::Deny.code(), 2);
        assert_eq!(AclResourceType::from_id(2), AclResourceType::Topic);
        assert_eq!(AclResourceType::from_id(99), AclResourceType::Unknown);
        assert_eq!(
            AclResourceType::from_string("topic"),
            AclResourceType::Topic
        );
        assert_eq!(
            AclResourceType::from_string("TRANSACTIONAL_ID"),
            AclResourceType::TransactionalId
        );
        assert!(AclResourceType::Unknown.is_unknown());
        assert_eq!(AclPatternType::from_id(3), AclPatternType::Literal);
        assert_eq!(
            AclPatternType::from_string("LITERAL"),
            AclPatternType::Literal
        );
        assert_eq!(
            AclPatternType::from_string("literal"),
            AclPatternType::Unknown,
            "Java PatternType.fromString is case-sensitive"
        );
        assert!(AclPatternType::Literal.is_specific());
        assert!(!AclPatternType::Match.is_specific());
        assert_eq!(
            AclOperation::from_id(ACL_OPERATION_CREATE_TOKENS),
            AclOperation::CreateTokens
        );
        assert_eq!(
            AclOperation::from_string("CREATE_TOKENS"),
            AclOperation::CreateTokens
        );
        assert_eq!(
            AclOperation::from_string("idempotent_write"),
            AclOperation::IdempotentWrite
        );
        assert_eq!(AclOperation::from_id(99), AclOperation::Unknown);
        assert_eq!(AclPermission::from_string("deny"), AclPermission::Deny);
        assert_eq!(AclPermission::from_id(99), AclPermission::Unknown);
        assert_eq!(AclResourceType::Topic.to_string(), "TOPIC");
        assert_eq!(
            AclResourceType::TransactionalId.to_string(),
            "TRANSACTIONAL_ID"
        );
        assert_eq!(AclPatternType::Literal.to_string(), "LITERAL");
        assert_eq!(AclOperation::ClusterAction.to_string(), "CLUSTER_ACTION");
        assert_eq!(AclOperation::CreateTokens.to_string(), "CREATE_TOKENS");
        assert_eq!(AclPermission::Allow.to_string(), "ALLOW");
        assert_eq!(
            ResourcePatternFilter::any().to_string(),
            "ResourcePattern(resourceType=ANY, name= , patternType=ANY)"
        );
        assert_eq!(
            AccessControlEntryFilter::any().to_string(),
            "(principal= , host= , operation=ANY, permissionType=ANY)"
        );
        assert_eq!(
            AclBindingFilter::any().to_string(),
            "(patternFilter=ResourcePattern(resourceType=ANY, name= , patternType=ANY), entryFilter=(principal= , host= , operation=ANY, permissionType=ANY))"
        );

        let pattern =
            ResourcePattern::new(AclResourceType::Topic, "events", AclPatternType::Literal);
        assert_eq!(pattern.name(), "events");
        assert!(!pattern.is_unknown());
        let wildcard = ResourcePattern::new(
            AclResourceType::Topic,
            WILDCARD_RESOURCE,
            AclPatternType::Literal,
        );
        let match_filter = ResourcePatternFilter::new(
            AclResourceType::Topic,
            Some("payments.received".into()),
            AclPatternType::Match,
        );
        assert!(match_filter.matches(&ResourcePattern::new(
            AclResourceType::Topic,
            "payments.received",
            AclPatternType::Literal,
        )));
        assert!(match_filter.matches(&wildcard));
        let pay_prefix = ResourcePattern::new(
            AclResourceType::Topic,
            "payments.",
            AclPatternType::Prefixed,
        );
        assert!(match_filter.matches(&pay_prefix));
        assert!(!match_filter.matches(&pattern));
        assert_eq!(
            ResourcePatternFilter::any().find_indefinite_field(),
            Some("Resource type is ANY.")
        );
        let specific = ResourcePatternFilter::new(
            AclResourceType::Topic,
            Some("t".into()),
            AclPatternType::Literal,
        );
        assert!(specific.matches_at_most_one());
        assert!(specific.matches(&ResourcePattern::new(
            AclResourceType::Topic,
            "t",
            AclPatternType::Literal,
        )));
        assert!(!specific.matches(&wildcard));

        let entry =
            AccessControlEntry::new("User:alice", "*", AclOperation::Read, AclPermission::Allow);
        assert_eq!(entry.principal(), "User:alice");
        assert_eq!(entry.operation(), AclOperation::Read);
        assert!(!entry.is_unknown());
        assert!(entry.to_filter().matches(&entry));
        assert!(entry.to_filter().matches_at_most_one());
        assert!(AccessControlEntryFilter::any().matches(&entry));
        assert_eq!(
            AccessControlEntryFilter::any().find_indefinite_field(),
            Some("Principal is NULL")
        );

        let binding = AclBinding::new(pattern.clone(), entry.clone());
        assert_eq!(binding.pattern(), pattern);
        assert_eq!(binding.entry(), entry);
        let any = AclBindingFilter::new(
            ResourcePatternFilter::any(),
            AccessControlEntryFilter::any(),
        );
        assert_eq!(any, AclBindingFilter::any());
        assert!(!any.matches_at_most_one());
        assert!(!any.is_unknown());
    }

    #[test]
    fn delete_acls_v2_filters_of_two_matches_independent_encode() {
        // Compact array of 2: topic-any + topic name "t" principal "U".
        const REQ: &[u8] = &[
            0x03, 0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x01, 0x00, 0x02, 0x02, 0x74, 0x01, 0x02,
            0x55, 0x00, 0x01, 0x01, 0x00, 0x00,
        ];
        let filters = [
            AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC),
            AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC)
                .name("t")
                .principal("U"),
        ];
        let mut buf = BytesMut::new();
        encode_delete_acls_request(&mut buf, 2, &filters).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let got = decode_delete_acls_request(&mut cur, 2).unwrap();
        assert_eq!(got, filters);
        assert!(
            !cur.has_remaining(),
            "DeleteAcls v2 Filters of 2 must be leftover-empty"
        );

        let err =
            DeletedAclsFilterResult::error_results(2, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(err.len(), 2);
        let first = err.first().expect("first filter");
        assert_eq!(
            first.error_code(),
            crate::error::CLUSTER_AUTHORIZATION_FAILED
        );
        assert!(first.error_message().is_none());
        assert!(first.matching().is_empty());
        assert_eq!(
            err,
            vec![
                DeletedAclsFilterResult::error(crate::error::CLUSTER_AUTHORIZATION_FAILED),
                DeletedAclsFilterResult::error(crate::error::CLUSTER_AUTHORIZATION_FAILED),
            ]
        );
        buf.clear();
        encode_delete_acls_filter_results(&mut buf, 2, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(decode_delete_acls_filter_results(&mut cur, 2).unwrap(), err);
        assert!(
            !cur.has_remaining(),
            "DeleteAcls v2 getErrorResponse leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_delete_acls_filter_results(&mut buf, 3, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(decode_delete_acls_filter_results(&mut cur, 3).unwrap(), err);
        assert!(
            !cur.has_remaining(),
            "DeleteAcls v3 getErrorResponse leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_delete_acls_filter_results(&mut buf, 1, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(decode_delete_acls_filter_results(&mut cur, 1).unwrap(), err);
        assert!(
            !cur.has_remaining(),
            "DeleteAcls v1 getErrorResponse leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        buf.clear();
        encode_delete_acls_filter_results(&mut buf, 0, &err).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(decode_delete_acls_filter_results(&mut cur, 0).unwrap(), err);
        assert!(
            !cur.has_remaining(),
            "DeleteAcls v0 getErrorResponse leftover-empty; leftover {} bytes",
            cur.remaining()
        );
        let empty =
            DeletedAclsFilterResult::error_results(0, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert!(empty.is_empty());
        buf.clear();
        encode_delete_acls_filter_results(&mut buf, 2, &empty).unwrap();
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_delete_acls_filter_results(&mut cur, 2).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "DeleteAcls empty getErrorResponse leftover-empty; leftover {} bytes",
            cur.remaining()
        );
    }

    #[test]
    fn acl_binding_filter_matches_principal_and_any() {
        let alice = AclBinding::allow_topic("t", "User:alice");
        let bob = AclBinding::allow_topic("t", "User:bob");
        assert!(AclBindingFilter::resource_type(AclResourceType::Topic).matches(&alice));
        assert!(AclBindingFilter::any().matches(&bob));
        let alice_only =
            AclBindingFilter::resource_type(AclResourceType::Topic).principal("User:alice");
        assert!(alice_only.matches(&alice));
        assert!(!alice_only.matches(&bob));
        let deny = AclBinding::allow_topic("t", "User:alice").permission(AclPermission::Deny);
        assert!(AclBindingFilter::resource_type(AclResourceType::Topic).matches(&deny));
        assert!(!AclBindingFilter::resource_type(AclResourceType::Topic)
            .permission(AclPermission::Allow)
            .matches(&deny));
    }

    #[test]
    fn acl_v0_pattern_type_matches_java() {
        assert!(!CreateAclsResponse::should_client_throttle(0));
        assert!(CreateAclsResponse::should_client_throttle(1));
        assert!(!DescribeAclsResponse::should_client_throttle(0));
        assert!(DescribeAclsResponse::should_client_throttle(1));
        assert!(!DeleteAclsResponse::should_client_throttle(0));
        assert!(DeleteAclsResponse::should_client_throttle(1));
        let prefixed =
            AclBinding::allow_topic("t", "User:alice").pattern_type(AclPatternType::Prefixed);
        let err =
            encode_create_acls_request(&mut BytesMut::new(), 0, std::slice::from_ref(&prefixed))
                .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "CreateAcls v0 PREFIXED is Java UnsupportedVersionException, got {err}"
        );
        assert!(
            err.to_string().contains("literal resource pattern types"),
            "got {err}"
        );
        encode_create_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&prefixed))
            .unwrap();

        let match_filter =
            AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC).pattern_type(AclPatternType::Match);
        let err = encode_describe_acls_request(&mut BytesMut::new(), 0, &match_filter).unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "DescribeAcls v0 MATCH is Java UnsupportedVersionException, got {err}"
        );
        encode_describe_acls_request(
            &mut BytesMut::new(),
            0,
            &AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC),
        )
        .unwrap();
        encode_describe_acls_request(&mut BytesMut::new(), 1, &match_filter).unwrap();

        let err = encode_delete_acls_request(
            &mut BytesMut::new(),
            0,
            std::slice::from_ref(&match_filter),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "DeleteAcls v0 MATCH is Java UnsupportedVersionException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("pattern type MATCH (only LITERAL and ANY are supported)"),
            "got {err}"
        );
        encode_delete_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&match_filter))
            .unwrap();

        let err =
            encode_describe_acls_response(&mut BytesMut::new(), 0, std::slice::from_ref(&prefixed))
                .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "DescribeAcls response v0 PREFIXED is Java UnsupportedVersionException, got {err}"
        );
        encode_describe_acls_response(&mut BytesMut::new(), 1, std::slice::from_ref(&prefixed))
            .unwrap();

        let err = encode_delete_acls_response(
            &mut BytesMut::new(),
            0,
            0,
            std::slice::from_ref(&prefixed),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "DeleteAcls response v0 PREFIXED is Java UnsupportedVersionException, got {err}"
        );
        encode_delete_acls_response(&mut BytesMut::new(), 1, 0, std::slice::from_ref(&prefixed))
            .unwrap();
    }

    #[test]
    fn create_acls_constructor_matches_java() {
        let any_resource = AclBinding::allow(AclResourceType::Any, "t", "User:alice");
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&any_resource),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "ANY resource type is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("resourceType must not be ANY"),
            "got {err}"
        );

        let any_pattern =
            AclBinding::allow_topic("t", "User:alice").pattern_type(AclPatternType::Any);
        let err =
            encode_create_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&any_pattern))
                .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "ANY pattern type is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("patternType must not be ANY"),
            "got {err}"
        );

        let match_pattern =
            AclBinding::allow_topic("t", "User:alice").pattern_type(AclPatternType::Match);
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&match_pattern),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "MATCH pattern type is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("patternType must not be MATCH"),
            "got {err}"
        );

        let any_op = AclBinding::allow_topic("t", "User:alice").operation(AclOperation::Any);
        let err =
            encode_create_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&any_op))
                .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "ANY operation is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("operation must not be ANY"),
            "got {err}"
        );

        let any_perm = AclBinding::allow_topic("t", "User:alice").permission(AclPermission::Any);
        let err =
            encode_create_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&any_perm))
                .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "ANY permission is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("permissionType must not be ANY"),
            "got {err}"
        );

        encode_create_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&AclBinding::allow_topic("t", "User:alice")),
        )
        .unwrap();
    }

    #[test]
    fn acl_unknown_elements_match_java() {
        let unknown_type = AclBinding::allow(AclResourceType::Unknown, "t", "User:alice");
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_type),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "CreateAcls UNKNOWN resource is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("CreatableAcls contain unknown elements"),
            "got {err}"
        );
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            0,
            std::slice::from_ref(&unknown_type),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "CreateAcls v0 UNKNOWN resource is still IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("CreatableAcls contain unknown elements"),
            "got {err}"
        );

        let unknown_pattern =
            AclBinding::allow_topic("t", "User:alice").pattern_type(AclPatternType::Unknown);
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_pattern),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "CreateAcls UNKNOWN pattern is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("CreatableAcls contain unknown elements"),
            "got {err}"
        );
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            0,
            std::slice::from_ref(&unknown_pattern),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "CreateAcls v0 UNKNOWN pattern is Java UnsupportedVersionException first, got {err}"
        );

        let unknown_op =
            AclBinding::allow_topic("t", "User:alice").operation(AclOperation::Unknown);
        let err =
            encode_create_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&unknown_op))
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("CreatableAcls contain unknown elements"),
            "got {err}"
        );

        let unknown_perm =
            AclBinding::allow_topic("t", "User:alice").permission(AclPermission::Unknown);
        let err = encode_create_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_perm),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("CreatableAcls contain unknown elements"),
            "got {err}"
        );

        let describe = AclBindingFilter::resource_type(AclResourceType::Unknown);
        let err = encode_describe_acls_request(&mut BytesMut::new(), 1, &describe).unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DescribeAcls UNKNOWN resource is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("DescribeAclsRequest contains UNKNOWN elements"),
            "got {err}"
        );
        let err = encode_describe_acls_request(&mut BytesMut::new(), 0, &describe).unwrap_err();
        assert!(
            err.to_string()
                .contains("DescribeAclsRequest contains UNKNOWN elements"),
            "got {err}"
        );

        let delete =
            AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC).operation(AclOperation::Unknown);
        let err =
            encode_delete_acls_request(&mut BytesMut::new(), 1, std::slice::from_ref(&delete))
                .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DeleteAcls UNKNOWN operation is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("Filters contain UNKNOWN elements"),
            "got {err}"
        );
        encode_delete_acls_request(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC)),
        )
        .unwrap();
        encode_describe_acls_request(
            &mut BytesMut::new(),
            1,
            &AclBindingFilter::resource_type(ACL_RESOURCE_TOPIC),
        )
        .unwrap();
    }

    #[test]
    fn acl_response_unknown_elements_match_java() {
        let unknown_type = AclBinding::allow(AclResourceType::Unknown, "t", "User:alice");
        let err = encode_describe_acls_response(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_type),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DescribeAcls response UNKNOWN resource is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("Contain UNKNOWN elements"),
            "got {err}"
        );
        let err = encode_describe_acls_response(
            &mut BytesMut::new(),
            0,
            std::slice::from_ref(&unknown_type),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DescribeAcls response v0 UNKNOWN resource is still IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("Contain UNKNOWN elements"),
            "got {err}"
        );

        let unknown_pattern =
            AclBinding::allow_topic("t", "User:alice").pattern_type(AclPatternType::Unknown);
        let err = encode_describe_acls_response(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_pattern),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DescribeAcls response UNKNOWN pattern is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string().contains("Contain UNKNOWN elements"),
            "got {err}"
        );
        let err = encode_describe_acls_response(
            &mut BytesMut::new(),
            0,
            std::slice::from_ref(&unknown_pattern),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "DescribeAcls response v0 UNKNOWN pattern is Java UnsupportedVersionException first, got {err}"
        );

        let unknown_op =
            AclBinding::allow_topic("t", "User:alice").operation(AclOperation::Unknown);
        let err = encode_describe_acls_response(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_op),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Contain UNKNOWN elements"),
            "got {err}"
        );
        let unknown_perm =
            AclBinding::allow_topic("t", "User:alice").permission(AclPermission::Unknown);
        let err = encode_describe_acls_response(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&unknown_perm),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Contain UNKNOWN elements"),
            "got {err}"
        );

        encode_describe_acls_response(
            &mut BytesMut::new(),
            1,
            std::slice::from_ref(&AclBinding::allow_topic("t", "User:alice")),
        )
        .unwrap();

        let err = encode_delete_acls_response(
            &mut BytesMut::new(),
            1,
            0,
            std::slice::from_ref(&unknown_type),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DeleteAcls response UNKNOWN resource is Java IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("DeleteAclsMatchingAcls contain UNKNOWN elements"),
            "got {err}"
        );
        let err = encode_delete_acls_response(
            &mut BytesMut::new(),
            0,
            0,
            std::slice::from_ref(&unknown_type),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Protocol(_)),
            "DeleteAcls response v0 UNKNOWN resource is still IllegalArgumentException, got {err}"
        );
        assert!(
            err.to_string()
                .contains("DeleteAclsMatchingAcls contain UNKNOWN elements"),
            "got {err}"
        );
        let err = encode_delete_acls_response(
            &mut BytesMut::new(),
            0,
            0,
            std::slice::from_ref(&unknown_pattern),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "DeleteAcls response v0 UNKNOWN pattern is Java UnsupportedVersionException first, got {err}"
        );
        let err = encode_delete_acls_response(
            &mut BytesMut::new(),
            1,
            0,
            std::slice::from_ref(&unknown_op),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("DeleteAclsMatchingAcls contain UNKNOWN elements"),
            "got {err}"
        );
        encode_delete_acls_response(
            &mut BytesMut::new(),
            1,
            0,
            std::slice::from_ref(&AclBinding::allow_topic("t", "User:alice")),
        )
        .unwrap();
    }
}
