//! Bounded Kafka Streams group heartbeat (88) and describe (89), version0.
//!
//! These are Streams broker protocols, distinct from consumer/share groups.
//! The reachable v0 wire schemas agree in Apache4.1.2/4.2.1/4.3.1. The first
//! release marks v0 unstable; the wire version alone cannot identify release
//! stability. A caller must negotiate the actual advertised API independently.
//! Nullable arrays retain null versus empty; signed epochs/offsets are retained.
//! Unknown tags are bounded and discarded. Strict UTF8 and ascending unique tags
//! are local admission policies; no upstream rejection is inferred from them.
//! Booleans accept every nonzero byte and nullable structs accept every negative
//! marker as null, matching Apache generated readers; encoders write canonical
//! 0/1 booleans and -1/1 struct markers. Defaults follow the4.3.1 schema.
//!
//! Decode limits cover requested Vec slots and copied string bytes, not allocator
//! rounding/RSS. Encoding validates before allocating one bounded output vector.
//! Neither these codecs nor their API identifiers implement a Streams coordinator.

use std::mem::size_of;

use crate::error::{Error, Result};

/// Positive admission limits for a complete Streams request/response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum complete encoded body size.
    pub wire_bytes: usize,
    /// Maximum bytes in any individual UTF8 string.
    pub string_bytes: usize,
    /// Aggregate bytes copied for strings across the message.
    pub total_string_bytes: usize,
    /// Maximum elements in any single array.
    pub array_elements: usize,
    /// Aggregate array elements across every nested structure.
    pub total_elements: usize,
    /// Maximum unknown tagged fields in any single structure.
    pub tagged_fields: usize,
    /// Aggregate unknown tagged fields across the whole message.
    pub total_tagged_fields: usize,
    /// Aggregate unknown tagged-field payload bytes.
    pub tag_bytes: usize,
    /// Requested Vec slot bytes plus copied string bytes during decoding.
    pub decoded_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            wire_bytes: 1024 * 1024,
            string_bytes: 64 * 1024,
            total_string_bytes: 1024 * 1024,
            array_elements: 8192,
            total_elements: 32768,
            tagged_fields: 64,
            total_tagged_fields: 1024,
            tag_bytes: 64 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
        }
    }
}

impl Limits {
    /// Validate before any body traversal/allocation.
    pub fn validate(self) -> Result<()> {
        if [
            self.wire_bytes,
            self.string_bytes,
            self.total_string_bytes,
            self.array_elements,
            self.total_elements,
            self.tagged_fields,
            self.total_tagged_fields,
            self.tag_bytes,
            self.decoded_bytes,
        ]
        .contains(&0)
        {
            return Err(Error::protocol("Streams codec limits must be positive"));
        }
        Ok(())
    }
}

fn charge(used: &mut usize, amount: usize, limit: usize, message: &'static str) -> Result<()> {
    let next = used
        .checked_add(amount)
        .ok_or_else(|| Error::protocol(message))?;
    if next > limit {
        return Err(Error::protocol(message));
    }
    *used = next;
    Ok(())
}

struct Budget {
    limits: Limits,
    strings: usize,
    elements: usize,
    tags: usize,
    tag_bytes: usize,
    decoded: usize,
}

impl Budget {
    fn new(limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            strings: 0,
            elements: 0,
            tags: 0,
            tag_bytes: 0,
            decoded: 0,
        })
    }

    fn string(&mut self, length: usize) -> Result<()> {
        if length > self.limits.string_bytes {
            return Err(Error::protocol("Streams string exceeds limit"));
        }
        charge(
            &mut self.strings,
            length,
            self.limits.total_string_bytes,
            "Streams aggregate string bytes exceed limit",
        )?;
        charge(
            &mut self.decoded,
            length,
            self.limits.decoded_bytes,
            "Streams decoded reservation exceeds limit",
        )
    }

    fn array<T>(&mut self, count: usize) -> Result<()> {
        if count > self.limits.array_elements {
            return Err(Error::protocol("Streams array count exceeds limit"));
        }
        charge(
            &mut self.elements,
            count,
            self.limits.total_elements,
            "Streams aggregate elements exceed limit",
        )?;
        let bytes = count
            .checked_mul(size_of::<T>())
            .ok_or_else(|| Error::protocol("Streams array reservation overflow"))?;
        charge(
            &mut self.decoded,
            bytes,
            self.limits.decoded_bytes,
            "Streams decoded reservation exceeds limit",
        )
    }
}

trait Wire: Sized {
    const MIN_BYTES: usize;
    fn read(reader: &mut Reader<'_>) -> Result<Self>;
    fn write(&self, writer: &mut Writer) -> Result<()>;
}

struct Reader<'a> {
    input: &'a [u8],
    budget: Budget,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8], limits: Limits) -> Result<Self> {
        let budget = Budget::new(limits)?;
        if input.len() > limits.wire_bytes {
            return Err(Error::protocol("Streams input exceeds wire limit"));
        }
        Ok(Self { input, budget })
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (head, tail) = self
            .input
            .split_at_checked(length)
            .ok_or_else(|| Error::protocol("Truncated Streams body"))?;
        self.input = tail;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8> {
        self.take(1)?
            .first()
            .copied()
            .ok_or_else(|| Error::protocol("Truncated Streams byte"))
    }

    fn varint(&mut self) -> Result<u32> {
        let mut value = 0u32;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.byte()?;
            if shift == 28 && byte > 15 {
                return Err(Error::protocol("Streams unsigned varint overflow"));
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(Error::protocol("Streams unsigned varint too long"))
    }

    fn boolean(&mut self) -> Result<bool> {
        Ok(self.byte()? != 0)
    }

    fn nullable_string(&mut self) -> Result<Option<String>> {
        let encoded = self.varint()?;
        if encoded == 0 {
            return Ok(None);
        }
        let length = usize::try_from(encoded - 1)
            .map_err(|_| Error::protocol("Streams string length overflow"))?;
        let raw = self.take(length)?;
        self.budget.string(length)?;
        let text =
            std::str::from_utf8(raw).map_err(|_| Error::protocol("Streams string is not UTF8"))?;
        Ok(Some(text.to_owned()))
    }

    fn string(&mut self) -> Result<String> {
        self.nullable_string()?
            .ok_or_else(|| Error::protocol("Null nonnullable Streams string"))
    }

    fn nullable_array<T: Wire>(&mut self) -> Result<Option<Vec<T>>> {
        let encoded = self.varint()?;
        if encoded == 0 {
            return Ok(None);
        }
        let count = usize::try_from(encoded - 1)
            .map_err(|_| Error::protocol("Streams array count overflow"))?;
        let minimum = count
            .checked_mul(T::MIN_BYTES)
            .ok_or_else(|| Error::protocol("Streams minimum array size overflow"))?;
        if minimum > self.input.len() {
            return Err(Error::protocol("Streams array exceeds remaining input"));
        }
        self.budget.array::<T>(count)?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(T::read(self)?);
        }
        Ok(Some(values))
    }

    fn array<T: Wire>(&mut self) -> Result<Vec<T>> {
        self.nullable_array()?
            .ok_or_else(|| Error::protocol("Null nonnullable Streams array"))
    }

    fn structure<T: Wire>(&mut self) -> Result<T> {
        T::read(self)
    }

    fn nullable_structure<T: Wire>(&mut self) -> Result<Option<T>> {
        if self.i8()? < 0 {
            Ok(None)
        } else {
            T::read(self).map(Some)
        }
    }

    fn tags(&mut self) -> Result<()> {
        let count = usize::try_from(self.varint()?)
            .map_err(|_| Error::protocol("Streams tag count overflow"))?;
        if count > self.budget.limits.tagged_fields
            || count
                .checked_mul(2)
                .is_none_or(|minimum| minimum > self.input.len())
        {
            return Err(Error::protocol(
                "Streams tagged-field count exceeds limit/input",
            ));
        }
        charge(
            &mut self.budget.tags,
            count,
            self.budget.limits.total_tagged_fields,
            "Streams aggregate tag count exceeds limit",
        )?;
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|value| tag <= value) {
                return Err(Error::protocol("Streams tags must be ascending and unique"));
            }
            previous = Some(tag);
            let length = usize::try_from(self.varint()?)
                .map_err(|_| Error::protocol("Streams tag size overflow"))?;
            charge(
                &mut self.budget.tag_bytes,
                length,
                self.budget.limits.tag_bytes,
                "Streams aggregate tag bytes exceed limit",
            )?;
            let _payload = self.take(length)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(Error::protocol("Trailing Streams body bytes"))
        }
    }
}

struct Writer {
    output: Option<Vec<u8>>,
    length: usize,
    budget: Budget,
}

impl Writer {
    fn new(limits: Limits, capacity: Option<usize>) -> Result<Self> {
        let budget = Budget::new(limits)?;
        Ok(Self {
            output: capacity.map(Vec::with_capacity),
            length: 0,
            budget,
        })
    }

    fn put(&mut self, raw: &[u8]) -> Result<()> {
        charge(
            &mut self.length,
            raw.len(),
            self.budget.limits.wire_bytes,
            "Streams output exceeds wire limit",
        )?;
        if let Some(output) = &mut self.output {
            output.extend_from_slice(raw);
        }
        Ok(())
    }

    fn byte(&mut self, value: u8) -> Result<()> {
        self.put(&[value])
    }

    fn varint(&mut self, mut value: u32) -> Result<()> {
        while value >= 128 {
            let byte = u8::try_from(value & 127)
                .map_err(|_| Error::protocol("Streams varint byte overflow"))?;
            self.byte(byte | 128)?;
            value >>= 7;
        }
        self.byte(u8::try_from(value).map_err(|_| Error::protocol("Streams varint byte overflow"))?)
    }

    fn boolean(&mut self, value: &bool) -> Result<()> {
        self.byte(u8::from(*value))
    }

    fn string(&mut self, value: &str) -> Result<()> {
        self.budget.string(value.len())?;
        let length = u32::try_from(value.len())
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| Error::protocol("Streams compact string length overflow"))?;
        self.varint(length)?;
        self.put(value.as_bytes())
    }

    fn nullable_string(&mut self, value: &Option<String>) -> Result<()> {
        match value {
            Some(text) => self.string(text),
            None => self.byte(0),
        }
    }

    fn array<T: Wire>(&mut self, values: &[T]) -> Result<()> {
        self.budget.array::<T>(values.len())?;
        let count = u32::try_from(values.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| Error::protocol("Streams compact array count overflow"))?;
        self.varint(count)?;
        for value in values {
            value.write(self)?;
        }
        Ok(())
    }

    fn nullable_array<T: Wire>(&mut self, values: &Option<Vec<T>>) -> Result<()> {
        match values {
            Some(values) => self.array(values),
            None => self.byte(0),
        }
    }

    fn structure<T: Wire>(&mut self, value: &T) -> Result<()> {
        value.write(self)
    }

    fn nullable_structure<T: Wire>(&mut self, value: &Option<T>) -> Result<()> {
        match value {
            Some(value) => {
                self.byte(1)?;
                value.write(self)
            }
            None => self.byte(255),
        }
    }

    fn tags(&mut self) -> Result<()> {
        self.byte(0)
    }
}

macro_rules! integer_codec {
    ($type:ty, $method:ident, $bytes:expr) => {
        impl Reader<'_> {
            fn $method(&mut self) -> Result<$type> {
                let value = self
                    .take($bytes)?
                    .try_into()
                    .map_err(|_| Error::protocol("Truncated Streams integer"))?;
                Ok(<$type>::from_be_bytes(value))
            }
        }
        impl Writer {
            fn $method(&mut self, value: &$type) -> Result<()> {
                self.put(&value.to_be_bytes())
            }
        }
        impl Wire for $type {
            const MIN_BYTES: usize = $bytes;
            fn read(reader: &mut Reader<'_>) -> Result<Self> {
                reader.$method()
            }
            fn write(&self, writer: &mut Writer) -> Result<()> {
                writer.$method(self)
            }
        }
    };
}
integer_codec!(i8, i8, 1);
integer_codec!(i16, i16, 2);
integer_codec!(u16, u16, 2);
integer_codec!(i32, i32, 4);
integer_codec!(i64, i64, 8);

impl Wire for String {
    const MIN_BYTES: usize = 1;
    fn read(reader: &mut Reader<'_>) -> Result<Self> {
        reader.string()
    }
    fn write(&self, writer: &mut Writer) -> Result<()> {
        writer.string(self)
    }
}

macro_rules! wire_struct {
    ($(#[$doc:meta])* $name:ident {
        $($(#[$field_doc:meta])* $field:ident: $type:ty = $default:expr =>
          $method:ident $(<$inner:ty>)?, $minimum:expr;)*
    }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name { $($(#[$field_doc])* pub $field: $type,)* }
        impl Default for $name {
            fn default() -> Self { Self { $($field: $default,)* } }
        }
        impl Wire for $name {
            const MIN_BYTES: usize = 1 $(+ $minimum)*;
            fn read(reader: &mut Reader<'_>) -> Result<Self> {
                let value = Self { $($field: reader.$method$(::<$inner>)?()?,)* };
                reader.tags()?;
                Ok(value)
            }
            fn write(&self, writer: &mut Writer) -> Result<()> {
                $(writer.$method$(::<$inner>)?(&self.$field)?;)*
                writer.tags()
            }
        }
    };
}

fn version0(version: i16) -> Result<()> {
    if version != 0 {
        return Err(Error::protocol("Streams group APIs support version0 only"));
    }
    Ok(())
}

fn decode<T: Wire>(input: &[u8], version: i16, limits: Limits) -> Result<T> {
    version0(version)?;
    let mut reader = Reader::new(input, limits)?;
    let value = T::read(&mut reader)?;
    reader.finish()?;
    Ok(value)
}

fn encode<T: Wire>(value: &T, version: i16, limits: Limits) -> Result<Vec<u8>> {
    version0(version)?;
    let mut measure = Writer::new(limits, None)?;
    value.write(&mut measure)?;
    let mut writer = Writer::new(limits, Some(measure.length))?;
    value.write(&mut writer)?;
    if writer.length != measure.length {
        return Err(Error::protocol("Streams encoded size mismatch"));
    }
    writer
        .output
        .ok_or_else(|| Error::protocol("Streams output missing"))
}

wire_struct! {
    /// Kafka Streams `KeyValue` version0 wire fields.
    KeyValue {
        /// key of the config
        key: String = String::new() => string, 1;
        /// value of the config
        value: String = String::new() => string, 1;
    }
}

wire_struct! {
    /// Kafka Streams `TopicInfo` version0 wire fields.
    TopicInfo {
        /// The name of the topic.
        name: String = String::new() => string, 1;
        /// The number of partitions in the topic. Can be 0 if no specific number of
        /// partitions is enforced. Always 0 for changelog topics.
        partitions: i32 = 0 => i32, 4;
        /// The replication factor of the topic. Can be 0 if the default replication factor
        /// should be used.
        replication_factor: i16 = 0 => i16, 2;
        /// Topic-level configurations as key-value pairs.
        topic_configs: Vec<KeyValue> = Vec::new() => array<KeyValue>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `Endpoint` version0 wire fields.
    Endpoint {
        /// host of the endpoint
        host: String = String::new() => string, 1;
        /// port of the endpoint
        port: u16 = 0 => u16, 2;
    }
}

wire_struct! {
    /// Kafka Streams `TaskOffset` version0 wire fields.
    TaskOffset {
        /// The subtopology identifier.
        subtopology_id: String = String::new() => string, 1;
        /// The partition.
        partition: i32 = 0 => i32, 4;
        /// The offset.
        offset: i64 = 0 => i64, 8;
    }
}

wire_struct! {
    /// Kafka Streams `TaskIds` version0 wire fields.
    TaskIds {
        /// The subtopology identifier.
        subtopology_id: String = String::new() => string, 1;
        /// The partitions of the input topics processed by this member.
        partitions: Vec<i32> = Vec::new() => array<i32>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `CopartitionGroup` version0 wire fields.
    CopartitionGroup {
        /// The topics the topology reads from. Index into the array on the subtopology level.
        source_topics: Vec<i16> = Vec::new() => array<i16>, 1;
        /// Regular expressions identifying topics the subtopology reads from. Index into the
        /// array on the subtopology level.
        source_topic_regex: Vec<i16> = Vec::new() => array<i16>, 1;
        /// The set of source topics that are internally created repartition topics. Index
        /// into the array on the subtopology level.
        repartition_source_topics: Vec<i16> = Vec::new() => array<i16>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `HeartbeatSubtopology` version0 wire fields.
    HeartbeatSubtopology {
        /// String to uniquely identify the subtopology. Deterministically generated from the
        /// topology
        subtopology_id: String = String::new() => string, 1;
        /// The topics the topology reads from.
        source_topics: Vec<String> = Vec::new() => array<String>, 1;
        /// The regular expressions identifying topics the subtopology reads from.
        source_topic_regex: Vec<String> = Vec::new() => array<String>, 1;
        /// The set of state changelog topics associated with this subtopology. Created
        /// automatically.
        state_changelog_topics: Vec<TopicInfo> = Vec::new() => array<TopicInfo>, 1;
        /// The repartition topics the subtopology writes to.
        repartition_sink_topics: Vec<String> = Vec::new() => array<String>, 1;
        /// The set of source topics that are internally created repartition topics. Created
        /// automatically.
        repartition_source_topics: Vec<TopicInfo> = Vec::new() => array<TopicInfo>, 1;
        /// A subset of source topics that must be copartitioned.
        copartition_groups: Vec<CopartitionGroup> = Vec::new() => array<CopartitionGroup>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `HeartbeatTopology` version0 wire fields.
    HeartbeatTopology {
        /// The epoch of the topology. Used to check if the topology corresponds to the
        /// topology initialized on the brokers.
        epoch: i32 = 0 => i32, 4;
        /// The sub-topologies of the streams application.
        subtopologies: Vec<HeartbeatSubtopology> = Vec::new() => array<HeartbeatSubtopology>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `StreamsGroupHeartbeatRequest` version0 wire fields.
    StreamsGroupHeartbeatRequest {
        /// The group identifier.
        group_id: String = String::new() => string, 1;
        /// The member ID generated by the streams consumer. The member ID must be kept during
        /// the entire lifetime of the streams consumer process.
        member_id: String = String::new() => string, 1;
        /// The current member epoch; 0 to join the group; -1 to leave the group; -2 to
        /// indicate that the static member will rejoin.
        member_epoch: i32 = 0 => i32, 4;
        /// The current endpoint epoch of this client, represents the latest endpoint epoch
        /// this client received
        endpoint_information_epoch: i32 = 0 => i32, 4;
        /// null if not provided or if it didn't change since the last heartbeat; the instance
        /// ID for static membership otherwise.
        instance_id: Option<String> = None => nullable_string, 1;
        /// null if not provided or if it didn't change since the last heartbeat; the rack ID
        /// of the member otherwise.
        rack_id: Option<String> = None => nullable_string, 1;
        /// -1 if it didn't change since the last heartbeat; the maximum time in milliseconds
        /// that the coordinator will wait on the member to revoke its tasks otherwise.
        rebalance_timeout_ms: i32 = -1 => i32, 4;
        /// The topology metadata of the streams application. Used to initialize the topology
        /// of the group and to check if the topology corresponds to the topology initialized
        /// for the group. Only sent when memberEpoch = 0, must be non-empty. Null otherwise.
        topology: Option<HeartbeatTopology> = None => nullable_structure<HeartbeatTopology>, 1;
        /// Currently owned active tasks for this client. Null if unchanged since last
        /// heartbeat.
        active_tasks: Option<Vec<TaskIds>> = None => nullable_array<TaskIds>, 1;
        /// Currently owned standby tasks for this client. Null if unchanged since last
        /// heartbeat.
        standby_tasks: Option<Vec<TaskIds>> = None => nullable_array<TaskIds>, 1;
        /// Currently owned warm-up tasks for this client. Null if unchanged since last
        /// heartbeat.
        warmup_tasks: Option<Vec<TaskIds>> = None => nullable_array<TaskIds>, 1;
        /// Identity of the streams instance that may have multiple consumers. Null if
        /// unchanged since last heartbeat.
        process_id: Option<String> = None => nullable_string, 1;
        /// User-defined endpoint for Interactive Queries. Null if unchanged since last
        /// heartbeat, or if not defined on the client.
        user_endpoint: Option<Endpoint> = None => nullable_structure<Endpoint>, 1;
        /// Used for rack-aware assignment algorithm. Null if unchanged since last heartbeat.
        client_tags: Option<Vec<KeyValue>> = None => nullable_array<KeyValue>, 1;
        /// Cumulative changelog offsets for tasks. Only updated when a warm-up task has
        /// caught up, and according to the task offset interval. Null if unchanged since last
        /// heartbeat.
        task_offsets: Option<Vec<TaskOffset>> = None => nullable_array<TaskOffset>, 1;
        /// Cumulative changelog end-offsets for tasks. Only updated when a warm-up task has
        /// caught up, and according to the task offset interval. Null if unchanged since last
        /// heartbeat.
        task_end_offsets: Option<Vec<TaskOffset>> = None => nullable_array<TaskOffset>, 1;
        /// Whether all Streams clients in the group should shut down.
        shutdown_application: bool = false => boolean, 1;
    }
}

wire_struct! {
    /// Kafka Streams `Status` version0 wire fields.
    Status {
        /// A code to indicate that a particular status is active for the group membership
        status_code: i8 = 0 => i8, 1;
        /// A string representation of the status.
        status_detail: String = String::new() => string, 1;
    }
}

wire_struct! {
    /// Kafka Streams `TopicPartition` version0 wire fields.
    TopicPartition {
        /// topic name
        topic: String = String::new() => string, 1;
        /// partitions
        partitions: Vec<i32> = Vec::new() => array<i32>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `EndpointToPartitions` version0 wire fields.
    EndpointToPartitions {
        /// User-defined endpoint to connect to the node
        user_endpoint: Endpoint = Endpoint::default() => structure<Endpoint>, Endpoint::MIN_BYTES;
        /// All topic partitions materialized by active tasks on the node
        active_partitions: Vec<TopicPartition> = Vec::new() => array<TopicPartition>, 1;
        /// All topic partitions materialized by standby tasks on the node
        standby_partitions: Vec<TopicPartition> = Vec::new() => array<TopicPartition>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `StreamsGroupHeartbeatResponse` version0 wire fields.
    StreamsGroupHeartbeatResponse {
        /// The duration in milliseconds for which the request was throttled due to a quota
        /// violation, or zero if the request did not violate any quota.
        throttle_time_ms: i32 = 0 => i32, 4;
        /// The top-level error code, or 0 if there was no error
        error_code: i16 = 0 => i16, 2;
        /// The top-level error message, or null if there was no error.
        error_message: Option<String> = None => nullable_string, 1;
        /// The member id is always generated by the streams consumer.
        member_id: String = String::new() => string, 1;
        /// The member epoch.
        member_epoch: i32 = 0 => i32, 4;
        /// The heartbeat interval in milliseconds.
        heartbeat_interval_ms: i32 = 0 => i32, 4;
        /// The maximal lag a warm-up task can have to be considered caught-up.
        acceptable_recovery_lag: i32 = 0 => i32, 4;
        /// The interval in which the task changelog offsets on a client are updated on the
        /// broker. The offsets are sent with the next heartbeat after this time has passed.
        task_offset_interval_ms: i32 = 0 => i32, 4;
        /// Indicate zero or more status for the group.
        status: Option<Vec<Status>> = Some(Vec::new()) => nullable_array<Status>, 1;
        /// Assigned active tasks for this client. Null if unchanged since last heartbeat.
        active_tasks: Option<Vec<TaskIds>> = None => nullable_array<TaskIds>, 1;
        /// Assigned standby tasks for this client. Null if unchanged since last heartbeat.
        standby_tasks: Option<Vec<TaskIds>> = None => nullable_array<TaskIds>, 1;
        /// Assigned warm-up tasks for this client. Null if unchanged since last heartbeat.
        warmup_tasks: Option<Vec<TaskIds>> = None => nullable_array<TaskIds>, 1;
        /// The endpoint epoch set in the response
        endpoint_information_epoch: i32 = 0 => i32, 4;
        /// Global assignment information used for IQ. Null if unchanged since last heartbeat.
        partitions_by_user_endpoint: Option<Vec<EndpointToPartitions>> = None => nullable_array<EndpointToPartitions>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `StreamsGroupDescribeRequest` version0 wire fields.
    StreamsGroupDescribeRequest {
        /// The ids of the groups to describe
        group_ids: Vec<String> = Vec::new() => array<String>, 1;
        /// Whether to include authorized operations.
        include_authorized_operations: bool = false => boolean, 1;
    }
}

wire_struct! {
    /// Kafka Streams `Assignment` version0 wire fields.
    Assignment {
        /// Active tasks for this client.
        active_tasks: Vec<TaskIds> = Vec::new() => array<TaskIds>, 1;
        /// Standby tasks for this client.
        standby_tasks: Vec<TaskIds> = Vec::new() => array<TaskIds>, 1;
        /// Warm-up tasks for this client.
        warmup_tasks: Vec<TaskIds> = Vec::new() => array<TaskIds>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `DescribedSubtopology` version0 wire fields.
    DescribedSubtopology {
        /// String to uniquely identify the subtopology.
        subtopology_id: String = String::new() => string, 1;
        /// The topics the subtopology reads from.
        source_topics: Vec<String> = Vec::new() => array<String>, 1;
        /// The repartition topics the subtopology writes to.
        repartition_sink_topics: Vec<String> = Vec::new() => array<String>, 1;
        /// The set of state changelog topics associated with this subtopology. Created
        /// automatically.
        state_changelog_topics: Vec<TopicInfo> = Vec::new() => array<TopicInfo>, 1;
        /// The set of source topics that are internally created repartition topics. Created
        /// automatically.
        repartition_source_topics: Vec<TopicInfo> = Vec::new() => array<TopicInfo>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `DescribedTopology` version0 wire fields.
    DescribedTopology {
        /// The epoch of the currently initialized topology for this group.
        epoch: i32 = 0 => i32, 4;
        /// The subtopologies of the streams application. This contains the configured
        /// subtopologies, where the number of partitions are set and any regular expressions
        /// are resolved to actual topics. Null if the group is uninitialized, source topics
        /// are missing or incorrectly partitioned.
        subtopologies: Option<Vec<DescribedSubtopology>> = None => nullable_array<DescribedSubtopology>, 1;
    }
}

wire_struct! {
    /// Kafka Streams `StreamsMember` version0 wire fields.
    StreamsMember {
        /// The member ID.
        member_id: String = String::new() => string, 1;
        /// The member epoch.
        member_epoch: i32 = 0 => i32, 4;
        /// The member instance ID for static membership.
        instance_id: Option<String> = None => nullable_string, 1;
        /// The rack ID.
        rack_id: Option<String> = None => nullable_string, 1;
        /// The client ID.
        client_id: String = String::new() => string, 1;
        /// The client host.
        client_host: String = String::new() => string, 1;
        /// The epoch of the topology on the client.
        topology_epoch: i32 = 0 => i32, 4;
        /// Identity of the streams instance that may have multiple clients.
        process_id: String = String::new() => string, 1;
        /// User-defined endpoint for Interactive Queries. Null if not defined for this
        /// client.
        user_endpoint: Option<Endpoint> = None => nullable_structure<Endpoint>, 1;
        /// Used for rack-aware assignment algorithm.
        client_tags: Vec<KeyValue> = Vec::new() => array<KeyValue>, 1;
        /// Cumulative changelog offsets for tasks.
        task_offsets: Vec<TaskOffset> = Vec::new() => array<TaskOffset>, 1;
        /// Cumulative changelog end offsets for tasks.
        task_end_offsets: Vec<TaskOffset> = Vec::new() => array<TaskOffset>, 1;
        /// The current assignment.
        assignment: Assignment = Assignment::default() => structure<Assignment>, Assignment::MIN_BYTES;
        /// The target assignment.
        target_assignment: Assignment = Assignment::default() => structure<Assignment>, Assignment::MIN_BYTES;
        /// True for classic members that have not been upgraded yet.
        is_classic: bool = false => boolean, 1;
    }
}

wire_struct! {
    /// Kafka Streams `DescribedStreamsGroup` version0 wire fields.
    DescribedStreamsGroup {
        /// The describe error, or 0 if there was no error.
        error_code: i16 = 0 => i16, 2;
        /// The top-level error message, or null if there was no error.
        error_message: Option<String> = None => nullable_string, 1;
        /// The group ID string.
        group_id: String = String::new() => string, 1;
        /// The group state string, or the empty string.
        group_state: String = String::new() => string, 1;
        /// The group epoch.
        group_epoch: i32 = 0 => i32, 4;
        /// The assignment epoch.
        assignment_epoch: i32 = 0 => i32, 4;
        /// The topology metadata currently initialized for the streams application. Can be
        /// null in case of a describe error.
        topology: Option<DescribedTopology> = None => nullable_structure<DescribedTopology>, 1;
        /// The members.
        members: Vec<StreamsMember> = Vec::new() => array<StreamsMember>, 1;
        /// 32-bit bitfield to represent authorized operations for this group.
        authorized_operations: i32 = i32::MIN => i32, 4;
    }
}

wire_struct! {
    /// Kafka Streams `StreamsGroupDescribeResponse` version0 wire fields.
    StreamsGroupDescribeResponse {
        /// The duration in milliseconds for which the request was throttled due to a quota
        /// violation, or zero if the request did not violate any quota.
        throttle_time_ms: i32 = 0 => i32, 4;
        /// Each described group.
        groups: Vec<DescribedStreamsGroup> = Vec::new() => array<DescribedStreamsGroup>, 1;
    }
}

/// Decode a complete bounded API88v0 request; unknown tags are discarded.
pub fn decode_streams_group_heartbeat_request(
    input: &[u8],
    version: i16,
    limits: Limits,
) -> Result<StreamsGroupHeartbeatRequest> {
    decode(input, version, limits)
}

/// Encode a bounded canonical API88v0 request; validate before output allocation.
pub fn encode_streams_group_heartbeat_request(
    value: &StreamsGroupHeartbeatRequest,
    version: i16,
    limits: Limits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}

/// Decode a complete bounded API88v0 response; unknown tags are discarded.
pub fn decode_streams_group_heartbeat_response(
    input: &[u8],
    version: i16,
    limits: Limits,
) -> Result<StreamsGroupHeartbeatResponse> {
    decode(input, version, limits)
}

/// Encode a bounded canonical API88v0 response; validate before output allocation.
pub fn encode_streams_group_heartbeat_response(
    value: &StreamsGroupHeartbeatResponse,
    version: i16,
    limits: Limits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}

/// Decode a complete bounded API89v0 request; unknown tags are discarded.
pub fn decode_streams_group_describe_request(
    input: &[u8],
    version: i16,
    limits: Limits,
) -> Result<StreamsGroupDescribeRequest> {
    decode(input, version, limits)
}

/// Encode a bounded canonical API89v0 request; validate before output allocation.
pub fn encode_streams_group_describe_request(
    value: &StreamsGroupDescribeRequest,
    version: i16,
    limits: Limits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}

/// Decode a complete bounded API89v0 response; unknown tags are discarded.
pub fn decode_streams_group_describe_response(
    input: &[u8],
    version: i16,
    limits: Limits,
) -> Result<StreamsGroupDescribeResponse> {
    decode(input, version, limits)
}

/// Encode a bounded canonical API89v0 response; validate before output allocation.
pub fn encode_streams_group_describe_response(
    value: &StreamsGroupDescribeResponse,
    version: i16,
    limits: Limits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}
