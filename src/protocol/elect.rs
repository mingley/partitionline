//! ElectLeaders (KIP-183 / KIP-460, api key 43). Classic v0–v1, flexible v2.

use std::collections::HashMap;
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Java `org.apache.kafka.common.ElectionType`.
///
/// `PREFERRED` elects the preferred replica. `UNCLEAN` elects the first
/// live replica when there is no in-sync replica (KIP-460; request v1+).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum ElectionType {
    /// Java `ElectionType.PREFERRED` (`0`).
    Preferred = 0,
    /// Java `ElectionType.UNCLEAN` (`1`).
    Unclean = 1,
}

impl ElectionType {
    /// Java `ElectionType.PREFERRED.value`.
    pub const PREFERRED_VALUE: i8 = 0;
    /// Java `ElectionType.UNCLEAN.value`.
    pub const UNCLEAN_VALUE: i8 = 1;

    /// Java `ElectionType.value` (the `byte` field).
    #[must_use]
    pub const fn value(self) -> i8 {
        self as i8
    }

    /// Java `ElectionType.valueOf(byte)`.
    ///
    /// Unknown values are [`Error::protocol`]
    /// `Value {value} must be one of [PREFERRED, UNCLEAN]`.
    pub fn value_of(value: i8) -> Result<Self> {
        match value {
            Self::PREFERRED_VALUE => Ok(Self::Preferred),
            Self::UNCLEAN_VALUE => Ok(Self::Unclean),
            other => Err(Error::protocol(format!(
                "Value {other} must be one of [PREFERRED, UNCLEAN]"
            ))),
        }
    }
}

impl fmt::Display for ElectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preferred => f.write_str("PREFERRED"),
            Self::Unclean => f.write_str("UNCLEAN"),
        }
    }
}

/// One topic in ElectLeaders `TopicPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes whose leader should be elected.
    pub partitions: Vec<i32>,
}

impl ElectLeadersTopic {
    /// Topic `topic` plus partition indexes.
    #[must_use]
    pub fn new(topic: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic: topic.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Partition indexes.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// ElectLeaders request (preferred or unclean election).
///
/// [`Self::build`] is Java `ElectLeadersRequest.Builder.build`.
/// Unclean election on v0 is `UnsupportedVersionException`
/// ([`Self::UNCLEAN_NOT_SUPPORTED_ON_V0_MSG`]). Null
/// [`Self::topic_partitions`] means every partition (Java `Set` null).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersRequest {
    /// Election type. v0 is always preferred on the wire.
    pub election_type: ElectionType,
    /// Topics to elect, or `None` for every partition.
    pub topic_partitions: Option<Vec<ElectLeadersTopic>>,
    /// ElectLeaders `TimeoutMs` (JSON default `60000`).
    pub timeout_ms: i32,
}

impl ElectLeadersRequest {
    /// JSON default for [`Self::timeout_ms`].
    pub const DEFAULT_TIMEOUT_MS: i32 = 60_000;
    /// Java `ElectLeadersRequest.Builder.build` on v0 with unclean
    /// election.
    pub const UNCLEAN_NOT_SUPPORTED_ON_V0_MSG: &'static str =
        "API Version 0 only supports PREFERRED election type";

    /// Construct [`Self`]. `timeout_ms` is JSON default `60000` when
    /// using [`Self::new`].
    #[must_use]
    pub fn new(
        election_type: ElectionType,
        topic_partitions: Option<Vec<ElectLeadersTopic>>,
        timeout_ms: i32,
    ) -> Self {
        Self {
            election_type,
            topic_partitions,
            timeout_ms,
        }
    }

    /// Java `ElectLeadersRequest.Builder.build`.
    ///
    /// Unclean election on v0 is [`Error::Unsupported`]
    /// ([`Self::UNCLEAN_NOT_SUPPORTED_ON_V0_MSG`]). Encode still writes
    /// independently after this helper. This crate speaks 0–2.
    pub fn build(version: i16, election_type: ElectionType) -> Result<()> {
        if version == 0 && election_type != ElectionType::Preferred {
            return Err(Error::Unsupported(
                Self::UNCLEAN_NOT_SUPPORTED_ON_V0_MSG.into(),
            ));
        }
        Ok(())
    }

    /// Java `ElectLeadersRequest.electionType` (the request field).
    #[must_use]
    pub const fn election_type(&self) -> ElectionType {
        self.election_type
    }

    /// Java `ElectLeadersRequest.electionType` at a spoken version.
    ///
    /// v0 is always [`ElectionType::Preferred`]. v1+ is
    /// [`Self::election_type`].
    #[must_use]
    pub fn election_type_at(&self, version: i16) -> ElectionType {
        if version == 0 {
            ElectionType::Preferred
        } else {
            self.election_type
        }
    }

    /// Topics to elect, or `None` for every partition.
    #[must_use]
    pub fn topic_partitions(&self) -> Option<&[ElectLeadersTopic]> {
        self.topic_partitions.as_deref()
    }

    /// ElectLeaders `TimeoutMs`.
    #[must_use]
    pub const fn timeout_ms(&self) -> i32 {
        self.timeout_ms
    }

    /// Null `TopicPartitions` means every topic-partition.
    #[must_use]
    pub fn is_all_partitions(&self) -> bool {
        self.topic_partitions.is_none()
    }

    /// Java `ElectLeadersRequest.getErrorResponse`.
    ///
    /// Copies each request topic-partition into
    /// [`ElectLeadersResponse::replica_election_results`]. Top-level
    /// and per-partition `ErrorMessage` stay the JSON default (null);
    /// official Java also sets `ApiError.message`. ThrottleTimeMs is
    /// `throttle_time_ms`. Encode omits ErrorCode below v1 (decode
    /// fills `0`).
    #[must_use]
    pub fn error_response(&self, error_code: i16, throttle_time_ms: i32) -> ElectLeadersResponse {
        let replica_election_results = match &self.topic_partitions {
            None => Vec::new(),
            Some(topics) => topics
                .iter()
                .map(|t| ReplicaElectionResult {
                    topic: t.topic.clone(),
                    partition_result: t
                        .partitions
                        .iter()
                        .map(|p| PartitionElectionResult {
                            partition_id: *p,
                            error_code,
                            error_message: None,
                        })
                        .collect(),
                })
                .collect(),
        };
        ElectLeadersResponse {
            throttle_time_ms,
            error_code,
            replica_election_results,
        }
    }
}

/// Per-partition ElectLeaders result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionElectionResult {
    /// Partition index.
    pub partition_id: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl PartitionElectionResult {
    /// Partition `partition_id` with `error_code`.
    #[must_use]
    pub fn new(partition_id: i32, error_code: i16, error_message: Option<String>) -> Self {
        Self {
            partition_id,
            error_code,
            error_message,
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// Per-topic ElectLeaders result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaElectionResult {
    /// Topic name.
    pub topic: String,
    /// Per-partition results.
    pub partition_result: Vec<PartitionElectionResult>,
}

impl ReplicaElectionResult {
    /// Topic `topic` plus partition results.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition_result: Vec<PartitionElectionResult>) -> Self {
        Self {
            topic: topic.into(),
            partition_result,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    /// Per-partition results.
    #[must_use]
    pub fn partition_result(&self) -> &[PartitionElectionResult] {
        &self.partition_result
    }
}

/// ElectLeaders response.
///
/// [`Self::error_counts`] is Java `ElectLeadersResponse.errorCounts`.
/// [`Self::should_client_throttle`] is Java
/// `ElectLeadersResponse.shouldClientThrottle`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResponse {
    /// ElectLeaders `ThrottleTimeMs` (JSON `0+`). JSON default is `0`.
    pub throttle_time_ms: i32,
    /// Top-level error code. v0 omits this on the wire (decode fills `0`).
    pub error_code: i16,
    /// Per-topic election results.
    pub replica_election_results: Vec<ReplicaElectionResult>,
}

impl ElectLeadersResponse {
    /// Construct [`Self`]. ThrottleTimeMs is JSON default `0`.
    #[must_use]
    pub fn new(error_code: i16, replica_election_results: Vec<ReplicaElectionResult>) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            replica_election_results,
        }
    }

    /// ElectLeaders `ThrottleTimeMs` (JSON `0+`).
    #[must_use]
    pub fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms
    }

    /// Top-level error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Per-topic results.
    #[must_use]
    pub fn replica_election_results(&self) -> &[ReplicaElectionResult] {
        &self.replica_election_results
    }

    /// Java `ElectLeadersResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(_version: i16) -> bool {
        true
    }

    /// Java `ElectLeadersResponse.errorCounts`.
    ///
    /// Counts the top-level `errorCode` (including `NONE`; v0 decode
    /// fills `0`) plus each partition-level code (including `NONE`).
    #[must_use]
    pub fn error_counts(&self) -> HashMap<i16, i32> {
        let mut counts = HashMap::new();
        let count = counts.entry(self.error_code).or_insert(0);
        *count += 1;
        for topic in &self.replica_election_results {
            for partition in &topic.partition_result {
                let count = counts.entry(partition.error_code).or_insert(0);
                *count += 1;
            }
        }
        counts
    }
}

/// Check that ElectLeaders `version` is spoken (0–2).
///
/// Classic at v0–v1. Flexible from v2. Kafka 4.0 `validVersions` is
/// `0-2`. This crate speaks 0–2. v0 omits ElectionType (preferred
/// only). v1 ElectionType (KIP-460). v1 response top-level ErrorCode.
/// v3+ is not spoken.
fn elect_leaders_spoken(version: i16) -> Result<i16> {
    match version {
        0..=2 => Ok(version),
        other => Err(Error::protocol(format!(
            "ElectLeaders version {other} is not implemented"
        ))),
    }
}

fn elect_leaders_flexible(version: i16) -> Result<bool> {
    Ok(elect_leaders_spoken(version)? >= 2)
}

/// Encode ElectLeaders v0–v2.
///
/// Java `ElectLeadersRequest.Builder.build` rejects unclean election on
/// v0 ([`ElectLeadersRequest::UNCLEAN_NOT_SUPPORTED_ON_V0_MSG`]).
pub fn encode_elect_leaders_request(
    buf: &mut BytesMut,
    version: i16,
    req: &ElectLeadersRequest,
) -> Result<()> {
    let flexible = elect_leaders_flexible(version)?;
    ElectLeadersRequest::build(version, req.election_type)?;
    if version >= 1 {
        buf.put_i8(req.election_type.value());
    }
    match &req.topic_partitions {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(topics) => {
            buf::put_array_len(buf, flexible, Some(topics.len()))?;
            for t in topics {
                buf::put_string(buf, flexible, Some(&t.topic))?;
                buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
        }
    }
    buf.put_i32(req.timeout_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode ElectLeaders v0–v2.
///
/// v0 does not send ElectionType; decode fills
/// [`ElectionType::Preferred`].
pub fn decode_elect_leaders_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ElectLeadersRequest> {
    let flexible = elect_leaders_flexible(version)?;
    let election_type = if version >= 1 {
        ElectionType::value_of(buf::get_i8(buf)?)?
    } else {
        ElectionType::Preferred
    };
    let n = buf::get_array_len(buf, flexible)?;
    let topic_partitions = match n {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
                let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
                let mut partitions = Vec::with_capacity(pn);
                for _ in 0..pn {
                    partitions.push(buf::get_i32(buf)?);
                }
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                topics.push(ElectLeadersTopic { topic, partitions });
            }
            Some(topics)
        }
    };
    let timeout_ms = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ElectLeadersRequest {
        election_type,
        topic_partitions,
        timeout_ms,
    })
}

/// Encode ElectLeaders v0–v2 response.
///
/// v0 omits top-level ErrorCode. ThrottleTimeMs is JSON `0+`.
pub fn encode_elect_leaders_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ElectLeadersResponse,
) -> Result<()> {
    let flexible = elect_leaders_flexible(version)?;
    buf.put_i32(resp.throttle_time_ms);
    if version >= 1 {
        buf.put_i16(resp.error_code);
    }
    buf::put_array_len(buf, flexible, Some(resp.replica_election_results.len()))?;
    for t in &resp.replica_election_results {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partition_result.len()))?;
        for p in &t.partition_result {
            buf.put_i32(p.partition_id);
            buf.put_i16(p.error_code);
            buf::put_string(buf, flexible, p.error_message.as_deref())?;
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

/// Decode ElectLeaders v0–v2 response.
///
/// v0 omits top-level ErrorCode; decode fills `0`.
pub fn decode_elect_leaders_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ElectLeadersResponse> {
    let flexible = elect_leaders_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = if version >= 1 { buf::get_i16(buf)? } else { 0 };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut replica_election_results = Vec::with_capacity(n);
    for _ in 0..n {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partition_result = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_id = buf::get_i32(buf)?;
            let part_error = buf::get_i16(buf)?;
            let error_message = buf::get_string(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partition_result.push(PartitionElectionResult {
                partition_id,
                error_code: part_error,
                error_message,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        replica_election_results.push(ReplicaElectionResult {
            topic,
            partition_result,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ElectLeadersResponse {
        throttle_time_ms,
        error_code,
        replica_election_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_req() -> ElectLeadersRequest {
        ElectLeadersRequest::new(
            ElectionType::Preferred,
            Some(vec![ElectLeadersTopic::new("orders", vec![0, 1])]),
            ElectLeadersRequest::DEFAULT_TIMEOUT_MS,
        )
    }

    fn sample_resp() -> ElectLeadersResponse {
        ElectLeadersResponse::new(
            0,
            vec![ReplicaElectionResult::new(
                "orders",
                vec![
                    PartitionElectionResult::new(0, 0, None),
                    PartitionElectionResult::new(1, 0, None),
                ],
            )],
        )
    }

    #[test]
    fn election_type_value_of_matches_java() {
        assert_eq!(ElectionType::value_of(0).unwrap(), ElectionType::Preferred);
        assert_eq!(ElectionType::value_of(1).unwrap(), ElectionType::Unclean);
        assert_eq!(ElectionType::Preferred.value(), 0);
        assert_eq!(ElectionType::Unclean.value(), 1);
        assert_eq!(ElectionType::Preferred.to_string(), "PREFERRED");
        assert_eq!(ElectionType::Unclean.to_string(), "UNCLEAN");
        let err = ElectionType::value_of(2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "protocol: Value 2 must be one of [PREFERRED, UNCLEAN]"
        );
    }

    #[test]
    fn elect_leaders_v0_unclean_is_unsupported() {
        let err = ElectLeadersRequest::build(0, ElectionType::Unclean).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "unsupported: {}",
                ElectLeadersRequest::UNCLEAN_NOT_SUPPORTED_ON_V0_MSG
            )
        );
        ElectLeadersRequest::build(1, ElectionType::Unclean).unwrap();
        let mut buf = BytesMut::new();
        let req = ElectLeadersRequest::new(ElectionType::Unclean, None, 1_000);
        let enc = encode_elect_leaders_request(&mut buf, 0, &req).unwrap_err();
        assert_eq!(
            enc.to_string(),
            format!(
                "unsupported: {}",
                ElectLeadersRequest::UNCLEAN_NOT_SUPPORTED_ON_V0_MSG
            )
        );
    }

    #[test]
    fn elect_leaders_v0_preferred_roundtrip_is_leftover_empty() {
        let req = sample_req();
        let mut buf = BytesMut::new();
        encode_elect_leaders_request(&mut buf, 0, &req).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_elect_leaders_request(&mut cur, 0).unwrap();
        assert_eq!(decoded, req);
        assert!(!cur.has_remaining(), "v0 request leftover-empty");

        let resp = sample_resp();
        buf.clear();
        encode_elect_leaders_response(&mut buf, 0, &resp).unwrap();
        let mut cur = &buf[..];
        let got = decode_elect_leaders_response(&mut cur, 0).unwrap();
        assert_eq!(got, resp);
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
    }

    #[test]
    fn elect_leaders_v1_unclean_roundtrip_is_leftover_empty() {
        let req = ElectLeadersRequest::new(
            ElectionType::Unclean,
            Some(vec![ElectLeadersTopic::new("t", vec![2])]),
            5_000,
        );
        let mut buf = BytesMut::new();
        encode_elect_leaders_request(&mut buf, 1, &req).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_elect_leaders_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, req);
        assert!(!cur.has_remaining(), "v1 request leftover-empty");

        let resp = ElectLeadersResponse::new(
            41,
            vec![ReplicaElectionResult::new(
                "t",
                vec![PartitionElectionResult::new(
                    2,
                    41,
                    Some("Not controller".into()),
                )],
            )],
        );
        buf.clear();
        encode_elect_leaders_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        let got = decode_elect_leaders_response(&mut cur, 1).unwrap();
        assert_eq!(got, resp);
        assert!(!cur.has_remaining(), "v1 response leftover-empty");
    }

    #[test]
    fn elect_leaders_v2_flexible_roundtrip_is_leftover_empty() {
        let req = sample_req();
        let mut buf = BytesMut::new();
        encode_elect_leaders_request(&mut buf, 2, &req).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_elect_leaders_request(&mut cur, 2).unwrap();
        assert_eq!(decoded, req);
        assert!(!cur.has_remaining(), "v2 request leftover-empty");

        let resp = sample_resp();
        buf.clear();
        encode_elect_leaders_response(&mut buf, 2, &resp).unwrap();
        let mut cur = &buf[..];
        let got = decode_elect_leaders_response(&mut cur, 2).unwrap();
        assert_eq!(got, resp);
        assert!(!cur.has_remaining(), "v2 response leftover-empty");
    }

    #[test]
    fn elect_leaders_v0_decode_election_type_is_preferred() {
        let req = sample_req();
        let mut buf = BytesMut::new();
        encode_elect_leaders_request(&mut buf, 0, &req).unwrap();
        let decoded = decode_elect_leaders_request(&mut &buf[..], 0).unwrap();
        assert_eq!(decoded.election_type_at(0), ElectionType::Preferred);
        assert_eq!(req.election_type_at(0), ElectionType::Preferred);
        let unclean = ElectLeadersRequest::new(ElectionType::Unclean, None, 1);
        assert_eq!(unclean.election_type_at(0), ElectionType::Preferred);
        assert_eq!(unclean.election_type_at(1), ElectionType::Unclean);
    }

    #[test]
    fn elect_leaders_request_getters_match_fields() {
        let req = sample_req();
        assert_eq!(req.election_type(), ElectionType::Preferred);
        assert_eq!(req.timeout_ms(), ElectLeadersRequest::DEFAULT_TIMEOUT_MS);
        let topics = req.topic_partitions().expect("sample has topics");
        assert_eq!(topics[0].topic(), "orders");
        assert_eq!(topics[0].partitions(), &[0, 1]);
        assert!(!req.is_all_partitions());
    }

    #[test]
    fn elect_leaders_null_topic_partitions_means_all() {
        let req = ElectLeadersRequest::new(ElectionType::Preferred, None, 60_000);
        assert!(req.is_all_partitions());
        let mut buf = BytesMut::new();
        encode_elect_leaders_request(&mut buf, 2, &req).unwrap();
        let decoded = decode_elect_leaders_request(&mut &buf[..], 2).unwrap();
        assert!(decoded.is_all_partitions());
        assert!(decoded.topic_partitions.is_none());
        assert!(!sample_req().is_all_partitions());
    }

    #[test]
    fn elect_leaders_error_response_copies_partitions() {
        let req = sample_req();
        let resp = req.error_response(41, 7);
        assert_eq!(resp.throttle_time_ms, 7);
        assert_eq!(resp.error_code, 41);
        assert_eq!(resp.replica_election_results.len(), 1);
        assert_eq!(resp.replica_election_results[0].topic, "orders");
        assert_eq!(resp.replica_election_results[0].partition_result.len(), 2);
        assert_eq!(
            resp.replica_election_results[0].partition_result[0].partition_id,
            0
        );
        assert_eq!(
            resp.replica_election_results[0].partition_result[0].error_code,
            41
        );
        let all = ElectLeadersRequest::new(ElectionType::Preferred, None, 1);
        assert!(all.error_response(1, 0).replica_election_results.is_empty());
    }

    #[test]
    fn elect_leaders_error_counts_includes_none() {
        let resp = sample_resp();
        let counts = resp.error_counts();
        assert_eq!(counts.get(&0), Some(&3));
        assert!(ElectLeadersResponse::should_client_throttle(0));
        assert!(ElectLeadersResponse::should_client_throttle(2));
    }

    #[test]
    fn elect_leaders_version_3_is_not_spoken() {
        let err = encode_elect_leaders_request(&mut BytesMut::new(), 3, &sample_req()).unwrap_err();
        assert!(
            err.to_string()
                .contains("ElectLeaders version 3 is not implemented"),
            "{err}"
        );
    }
}
