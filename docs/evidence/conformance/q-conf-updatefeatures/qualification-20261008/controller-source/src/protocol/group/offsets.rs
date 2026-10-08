//! Bounded typed OffsetCommit/OffsetFetch8..10, including v10 topic UUIDs.
//! Legacy name-only codecs remain separate; they must not project UUID bodies
//! into invented names. Nullable metadata is preserved and unknown tags discarded.

use std::mem::size_of;

use crate::error::{Error, Result};

/// Positive admission limits for a complete Offset request/response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetLimits {
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

impl Default for OffsetLimits {
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

impl OffsetLimits {
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
            return Err(Error::protocol("Offset codec limits must be positive"));
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
    limits: OffsetLimits,
    strings: usize,
    elements: usize,
    tags: usize,
    tag_bytes: usize,
    decoded: usize,
}

impl Budget {
    fn new(limits: OffsetLimits) -> Result<Self> {
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
            return Err(Error::protocol("Offset string exceeds limit"));
        }
        charge(
            &mut self.strings,
            length,
            self.limits.total_string_bytes,
            "Offset aggregate string bytes exceed limit",
        )?;
        charge(
            &mut self.decoded,
            length,
            self.limits.decoded_bytes,
            "Offset decoded reservation exceeds limit",
        )
    }

    fn array<T>(&mut self, count: usize) -> Result<()> {
        if count > self.limits.array_elements {
            return Err(Error::protocol("Offset array count exceeds limit"));
        }
        charge(
            &mut self.elements,
            count,
            self.limits.total_elements,
            "Offset aggregate elements exceed limit",
        )?;
        let bytes = count
            .checked_mul(size_of::<T>())
            .ok_or_else(|| Error::protocol("Offset array reservation overflow"))?;
        charge(
            &mut self.decoded,
            bytes,
            self.limits.decoded_bytes,
            "Offset decoded reservation exceeds limit",
        )
    }
}

trait Wire: Sized {
    const MIN_BYTES: usize;
    fn read(reader: &mut Reader<'_>) -> Result<Self>;
    fn write(&self, writer: &mut Writer) -> Result<()>;
}

struct Reader<'a> {
    version: i16,
    input: &'a [u8],
    budget: Budget,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8], limits: OffsetLimits, version: i16) -> Result<Self> {
        let budget = Budget::new(limits)?;
        if input.len() > limits.wire_bytes {
            return Err(Error::protocol("Offset input exceeds wire limit"));
        }
        Ok(Self {
            input,
            budget,
            version,
        })
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (head, tail) = self
            .input
            .split_at_checked(length)
            .ok_or_else(|| Error::protocol("Truncated Offset body"))?;
        self.input = tail;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8> {
        self.take(1)?
            .first()
            .copied()
            .ok_or_else(|| Error::protocol("Truncated Offset byte"))
    }

    fn varint(&mut self) -> Result<u32> {
        let mut value = 0u32;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.byte()?;
            if shift == 28 && byte > 15 {
                return Err(Error::protocol("Offset unsigned varint overflow"));
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(Error::protocol("Offset unsigned varint too long"))
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
            .map_err(|_| Error::protocol("Offset string length overflow"))?;
        let raw = self.take(length)?;
        self.budget.string(length)?;
        let text =
            std::str::from_utf8(raw).map_err(|_| Error::protocol("Offset string is not UTF8"))?;
        Ok(Some(text.to_owned()))
    }

    fn string(&mut self) -> Result<String> {
        self.nullable_string()?
            .ok_or_else(|| Error::protocol("Null nonnullable Offset string"))
    }

    fn nullable_array<T: Wire>(&mut self) -> Result<Option<Vec<T>>> {
        let encoded = self.varint()?;
        if encoded == 0 {
            return Ok(None);
        }
        let count = usize::try_from(encoded - 1)
            .map_err(|_| Error::protocol("Offset array count overflow"))?;
        let minimum = count
            .checked_mul(T::MIN_BYTES)
            .ok_or_else(|| Error::protocol("Offset minimum array size overflow"))?;
        if minimum > self.input.len() {
            return Err(Error::protocol("Offset array exceeds remaining input"));
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
            .ok_or_else(|| Error::protocol("Null nonnullable Offset array"))
    }

    fn structure<T: Wire>(&mut self) -> Result<T> {
        T::read(self)
    }

    fn tags(&mut self) -> Result<()> {
        let count = usize::try_from(self.varint()?)
            .map_err(|_| Error::protocol("Offset tag count overflow"))?;
        if count > self.budget.limits.tagged_fields
            || count
                .checked_mul(2)
                .is_none_or(|minimum| minimum > self.input.len())
        {
            return Err(Error::protocol(
                "Offset tagged-field count exceeds limit/input",
            ));
        }
        charge(
            &mut self.budget.tags,
            count,
            self.budget.limits.total_tagged_fields,
            "Offset aggregate tag count exceeds limit",
        )?;
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|value| tag <= value) {
                return Err(Error::protocol("Offset tags must be ascending and unique"));
            }
            previous = Some(tag);
            let length = usize::try_from(self.varint()?)
                .map_err(|_| Error::protocol("Offset tag size overflow"))?;
            charge(
                &mut self.budget.tag_bytes,
                length,
                self.budget.limits.tag_bytes,
                "Offset aggregate tag bytes exceed limit",
            )?;
            let _payload = self.take(length)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(Error::protocol("Trailing Offset body bytes"))
        }
    }
}

struct Writer {
    version: i16,
    output: Option<Vec<u8>>,
    length: usize,
    budget: Budget,
}

impl Writer {
    fn new(limits: OffsetLimits, capacity: Option<usize>, version: i16) -> Result<Self> {
        let budget = Budget::new(limits)?;
        Ok(Self {
            version,
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
            "Offset output exceeds wire limit",
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
                .map_err(|_| Error::protocol("Offset varint byte overflow"))?;
            self.byte(byte | 128)?;
            value >>= 7;
        }
        self.byte(u8::try_from(value).map_err(|_| Error::protocol("Offset varint byte overflow"))?)
    }

    fn boolean(&mut self, value: &bool) -> Result<()> {
        self.byte(u8::from(*value))
    }

    fn string(&mut self, value: &str) -> Result<()> {
        self.budget.string(value.len())?;
        let length = u32::try_from(value.len())
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| Error::protocol("Offset compact string length overflow"))?;
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
            .ok_or_else(|| Error::protocol("Offset compact array count overflow"))?;
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
                    .map_err(|_| Error::protocol("Truncated Offset integer"))?;
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
integer_codec!(i16, i16, 2);
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

fn version_range(version: i16) -> Result<()> {
    if !(8..=10).contains(&version) {
        return Err(Error::protocol("typed offset codecs support versions8..10"));
    }
    Ok(())
}

fn decode<T: Wire>(input: &[u8], version: i16, limits: OffsetLimits) -> Result<T> {
    version_range(version)?;
    let mut reader = Reader::new(input, limits, version)?;
    let value = T::read(&mut reader)?;
    reader.finish()?;
    Ok(value)
}

fn encode<T: Wire>(value: &T, version: i16, limits: OffsetLimits) -> Result<Vec<u8>> {
    version_range(version)?;
    let mut measure = Writer::new(limits, None, version)?;
    value.write(&mut measure)?;
    let mut writer = Writer::new(limits, Some(measure.length), version)?;
    value.write(&mut writer)?;
    if writer.length != measure.length {
        return Err(Error::protocol("Offset encoded size mismatch"));
    }
    writer
        .output
        .ok_or_else(|| Error::protocol("Offset output missing"))
}

fn validate<T: Wire>(value: &T, version: i16, limits: OffsetLimits) -> Result<()> {
    version_range(version)?;
    value.write(&mut Writer::new(limits, None, version)?)
}

pub(crate) fn validate_offset_commit_request_data(
    value: &OffsetCommitRequestData,
    version: i16,
    limits: OffsetLimits,
) -> Result<()> {
    validate(value, version, limits)
}

pub(crate) fn validate_offset_fetch_request_data(
    value: &OffsetFetchRequestData,
    version: i16,
    limits: OffsetLimits,
) -> Result<()> {
    validate(value, version, limits)
}

pub(crate) fn validate_offset_fetch_response_data(
    value: &OffsetFetchResponseData,
    version: i16,
    limits: OffsetLimits,
) -> Result<()> {
    validate(value, version, limits)
}

/// Metadata names used only to capture offset UUID bindings. No automatic
/// topic creation or authorization-operation queries are requested.
pub(crate) fn encode_offset_metadata_names(
    names: Option<&[String]>,
    version: i16,
    limits: OffsetLimits,
) -> Result<Vec<u8>> {
    if !(10..=13).contains(&version) {
        return Err(Error::Unsupported(
            "offset UUID resolution requires Metadata v10-13".into(),
        ));
    }
    fn write(writer: &mut Writer, names: Option<&[String]>, version: i16) -> Result<()> {
        match names {
            None => writer.byte(0)?,
            Some(names) => {
                writer.budget.array::<String>(names.len())?;
                writer.varint(
                    u32::try_from(names.len())
                        .ok()
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| Error::protocol("offset metadata count overflow"))?,
                )?;
                for name in names {
                    writer.put(&[0; 16])?;
                    writer.string(name)?;
                    writer.tags()?;
                }
            }
        }
        writer.byte(0)?;
        if version == 10 {
            writer.byte(0)?;
        }
        writer.byte(0)?;
        writer.tags()
    }
    let mut measure = Writer::new(limits, None, version)?;
    write(&mut measure, names, version)?;
    let mut writer = Writer::new(limits, Some(measure.length), version)?;
    write(&mut writer, names, version)?;
    writer
        .output
        .ok_or_else(|| Error::protocol("missing offset metadata output"))
}

/// Decode just bounded name/UUID bindings, while validating all Metadata
/// fields and the whole input. Partition arrays are traversed without storage.
pub(crate) fn decode_offset_metadata_bindings(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
) -> Result<std::collections::HashMap<String, [u8; 16]>> {
    metadata_bindings(input, version, limits, true)
}

pub(crate) fn validate_offset_metadata_bounds(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
) -> Result<()> {
    metadata_bindings(input, version, limits, false).map(|_| ())
}

fn metadata_bindings(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
    require_identity: bool,
) -> Result<std::collections::HashMap<String, [u8; 16]>> {
    use crate::protocol::api::{Broker, PartitionMetadata, TopicMetadata};
    use std::collections::{HashMap, HashSet};
    if !(10..=13).contains(&version) {
        return Err(Error::Unsupported(
            "offset UUID resolution requires Metadata v10-13".into(),
        ));
    }
    fn count<T>(reader: &mut Reader<'_>, minimum: usize) -> Result<usize> {
        let count = reader
            .varint()?
            .checked_sub(1)
            .ok_or_else(|| Error::protocol("null offset metadata array"))?;
        let count = usize::try_from(count)
            .map_err(|_| Error::protocol("offset metadata count overflow"))?;
        if count
            .checked_mul(minimum)
            .is_none_or(|bytes| bytes > reader.input.len())
        {
            return Err(Error::protocol("offset metadata count exceeds input"));
        }
        reader.budget.array::<T>(count)?;
        Ok(count)
    }
    let mut reader = Reader::new(input, limits, version)?;
    let _throttle = reader.i32()?;
    let brokers = count::<Broker>(&mut reader, 11)?;
    if brokers > 256 {
        return Err(Error::protocol("too many offset metadata brokers"));
    }
    let mut broker_ids = HashSet::new();
    for _ in 0..brokers {
        if !broker_ids.insert(reader.i32()?) {
            return Err(Error::protocol("duplicate offset metadata broker"));
        }
        let host = reader.string()?;
        let port = reader.i32()?;
        if host.is_empty() || !(1..=65535).contains(&port) {
            return Err(Error::protocol("invalid offset metadata broker address"));
        }
        let _rack = reader.nullable_string()?;
        reader.tags()?;
    }
    let _cluster = reader.nullable_string()?;
    let _controller = reader.i32()?;
    let topics = count::<TopicMetadata>(&mut reader, 26)?;
    let mut bindings = HashMap::new();
    let mut ids = HashSet::new();
    let mut terminal = 0;
    for _ in 0..topics {
        let code = reader.i16()?;
        let name = reader.nullable_string()?;
        let id: [u8; 16] = reader
            .take(16)?
            .try_into()
            .map_err(|_| Error::protocol("truncated metadata UUID"))?;
        let _internal = reader.boolean()?;
        let partitions = count::<PartitionMetadata>(&mut reader, 18)?;
        let mut indexes = HashSet::new();
        for _ in 0..partitions {
            let _code = reader.i16()?;
            let partition = reader.i32()?;
            if !indexes.insert(partition) {
                return Err(Error::protocol("duplicate offset metadata partition"));
            }
            let _leader = reader.i32()?;
            let _epoch = reader.i32()?;
            for _ in 0..3 {
                let count = count::<i32>(&mut reader, 4)?;
                let bytes = count
                    .checked_mul(4)
                    .ok_or_else(|| Error::protocol("offset metadata replica bytes overflow"))?;
                let _replicas = reader.take(bytes)?;
            }
            reader.tags()?;
        }
        let _authorized = reader.i32()?;
        reader.tags()?;
        if !require_identity {
            continue;
        }
        if code != 0 {
            terminal = code;
            continue;
        }
        let name = name
            .ok_or_else(|| Error::Unsupported("offset UUID has no metadata topic name".into()))?;
        if id == [0; 16] {
            return Err(Error::Unsupported(
                "offset metadata returned a zero topic UUID".into(),
            ));
        }
        if bindings.insert(name, id).is_some() || !ids.insert(id) {
            return Err(Error::protocol(
                "ambiguous offset metadata name/UUID mapping",
            ));
        }
    }
    if version == 10 {
        let _authorized = reader.i32()?;
    }
    if version == 13 {
        let code = reader.i16()?;
        if code != 0 {
            terminal = code;
        }
    }
    reader.tags()?;
    reader.finish()?;
    if require_identity && terminal != 0 {
        return Err(Error::broker(terminal, "offset Metadata"));
    }
    Ok(bindings)
}

/// Topic names are sent at8/9; UUIDs are sent at10. A wire codec does not
/// resolve identities or silently drop one representation in a fallback.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OffsetTopicIdentity {
    /// A name for a negotiated8/9 body.
    Name(String),
    /// An exact UUID for a negotiated10 body, including the zero sentinel.
    Id([u8; 16]),
}
impl Default for OffsetTopicIdentity {
    fn default() -> Self {
        Self::Name(String::new())
    }
}
impl Wire for OffsetTopicIdentity {
    const MIN_BYTES: usize = 1;
    fn read(reader: &mut Reader<'_>) -> Result<Self> {
        if reader.version == 10 {
            Ok(Self::Id(
                reader
                    .take(16)?
                    .try_into()
                    .map_err(|_| Error::protocol("truncated offset UUID"))?,
            ))
        } else {
            Ok(Self::Name(reader.string()?))
        }
    }
    fn write(&self, writer: &mut Writer) -> Result<()> {
        match (writer.version, self) {
            (10, Self::Id(id)) => writer.put(id),
            (8 | 9, Self::Name(name)) => writer.string(name),
            _ => Err(Error::Unsupported(
                "offset topic identity differs from negotiated wire version".into(),
            )),
        }
    }
}
wire_struct! {
    /// A committed partition, preserving nullable user metadata.
    OffsetCommitPartitionData {
        /// Partition index.
        partition_index: i32 = 0 => i32,4;
        /// Next offset to consume.
        committed_offset: i64 = 0 => i64,8;
        /// Leader epoch, or-1.
        committed_leader_epoch: i32 = -1 => i32,4;
        /// Null, empty, or user metadata.
        committed_metadata: Option<String> = None => nullable_string,1;
    }
}
wire_struct! {
    /// Topic identity and committed partitions.
    OffsetCommitTopicData {
        /// Name at8/9, UUID at10.
        identity: OffsetTopicIdentity = OffsetTopicIdentity::default() => structure<OffsetTopicIdentity>,1;
        /// Committed partitions, including duplicates in wire order.
        partitions: Vec<OffsetCommitPartitionData> = Vec::new() => array<OffsetCommitPartitionData>,1;
    }
}
wire_struct! {
    /// Complete flexible OffsetCommit request8..10.
    OffsetCommitRequestData {
        /// Group ID.
        group_id: String = String::new() => string,1;
        /// Classic generation or consumer member epoch; default-1.
        generation_id_or_member_epoch: i32 = -1 => i32,4;
        /// Member ID.
        member_id: String = String::new() => string,1;
        /// Static member identity; null and empty are distinct.
        group_instance_id: Option<String> = None => nullable_string,1;
        /// Topic identities and partition commits.
        topics: Vec<OffsetCommitTopicData> = Vec::new() => array<OffsetCommitTopicData>,1;
    }
}
wire_struct! {
    /// One partition's commit outcome.
    OffsetCommitPartitionResponseData {
        /// Partition index.
        partition_index: i32 = 0 => i32,4;
        /// Kafka error code, or0.
        error_code: i16 = 0 => i16,2;
    }
}
wire_struct! {
    /// A topic's commit outcomes, retaining its wire identity.
    OffsetCommitTopicResponseData {
        /// Name at8/9, UUID at10.
        identity: OffsetTopicIdentity = OffsetTopicIdentity::default() => structure<OffsetTopicIdentity>,1;
        /// Partition outcomes.
        partitions: Vec<OffsetCommitPartitionResponseData> = Vec::new() => array<OffsetCommitPartitionResponseData>,1;
    }
}
wire_struct! {
    /// Complete flexible OffsetCommit response8..10.
    OffsetCommitResponseData {
        /// Broker throttle in milliseconds.
        throttle_time_ms: i32 = 0 => i32,4;
        /// Topic identities and partition outcomes.
        topics: Vec<OffsetCommitTopicResponseData> = Vec::new() => array<OffsetCommitTopicResponseData>,1;
    }
}
wire_struct! {
    /// A topic identity and requested offset partitions.
    OffsetFetchTopicData {
        /// Name at8/9, UUID at10.
        identity: OffsetTopicIdentity = OffsetTopicIdentity::default() => structure<OffsetTopicIdentity>,1;
        /// Partition indexes, in wire order.
        partition_indexes: Vec<i32> = Vec::new() => array<i32>,1;
    }
}
/// One group in an OffsetFetch request. At8, member fields must be defaults;
/// a codec cannot silently remove membership validation requested by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroupData {
    /// Group ID.
    pub group_id: String,
    /// Nullable member ID, available at9/10.
    pub member_id: Option<String>,
    /// Consumer member epoch, or-1; available at9/10.
    pub member_epoch: i32,
    /// Null selects all committed topics; empty selects none.
    pub topics: Option<Vec<OffsetFetchTopicData>>,
}
impl Default for OffsetFetchGroupData {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            member_id: None,
            member_epoch: -1,
            topics: Some(Vec::new()),
        }
    }
}
impl Wire for OffsetFetchGroupData {
    const MIN_BYTES: usize = 3;
    fn read(reader: &mut Reader<'_>) -> Result<Self> {
        let group_id = reader.string()?;
        let (member_id, member_epoch) = if reader.version >= 9 {
            (reader.nullable_string()?, reader.i32()?)
        } else {
            (None, -1)
        };
        let topics = reader.nullable_array::<OffsetFetchTopicData>()?;
        reader.tags()?;
        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            topics,
        })
    }
    fn write(&self, writer: &mut Writer) -> Result<()> {
        if writer.version == 8 && (self.member_id.is_some() || self.member_epoch != -1) {
            return Err(Error::Unsupported(
                "OffsetFetch8 cannot represent requested member identity/epoch".into(),
            ));
        }
        writer.string(&self.group_id)?;
        if writer.version >= 9 {
            writer.nullable_string(&self.member_id)?;
            writer.i32(&self.member_epoch)?;
        }
        writer.nullable_array(&self.topics)?;
        writer.tags()
    }
}
wire_struct! {
    /// Complete batched flexible OffsetFetch request8..10.
    OffsetFetchRequestData {
        /// Groups in wire order.
        groups: Vec<OffsetFetchGroupData> = Vec::new() => array<OffsetFetchGroupData>,1;
        /// Require committed offsets to be stable.
        require_stable: bool = false => boolean,1;
    }
}
wire_struct! {
    /// Full fetched partition outcome with nullable metadata.
    OffsetFetchPartitionData {
        /// Partition index.
        partition_index: i32 = 0 => i32,4;
        /// Committed next offset; often-1 when no commit exists.
        committed_offset: i64 = -1 => i64,8;
        /// Committed leader epoch, or-1.
        committed_leader_epoch: i32 = -1 => i32,4;
        /// Null, empty, or committed user metadata.
        metadata: Option<String> = None => nullable_string,1;
        /// Kafka error code, or0.
        error_code: i16 = 0 => i16,2;
    }
}
wire_struct! {
    /// Fetched outcomes retaining the topic's exact wire identity.
    OffsetFetchTopicResponseData {
        /// Name at8/9, UUID at10.
        identity: OffsetTopicIdentity = OffsetTopicIdentity::default() => structure<OffsetTopicIdentity>,1;
        /// Partition outcomes.
        partitions: Vec<OffsetFetchPartitionData> = Vec::new() => array<OffsetFetchPartitionData>,1;
    }
}
wire_struct! {
    /// Fetched outcomes for one group, including its group-level error.
    OffsetFetchGroupResponseData {
        /// Group ID.
        group_id: String = String::new() => string,1;
        /// Topic outcomes, including errors and duplicates in wire order.
        topics: Vec<OffsetFetchTopicResponseData> = Vec::new() => array<OffsetFetchTopicResponseData>,1;
        /// Group error code, or0.
        error_code: i16 = 0 => i16,2;
    }
}
wire_struct! {
    /// Complete batched flexible OffsetFetch response8..10.
    OffsetFetchResponseData {
        /// Broker throttle in milliseconds.
        throttle_time_ms: i32 = 0 => i32,4;
        /// Group outcomes.
        groups: Vec<OffsetFetchGroupResponseData> = Vec::new() => array<OffsetFetchGroupResponseData>,1;
    }
}
/// Decode one whole bounded OffsetCommit8..10 request.
pub fn decode_offset_commit_request_data(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
) -> Result<OffsetCommitRequestData> {
    decode(input, version, limits)
}
/// Validate before allocating one OffsetCommit8..10 request body.
pub fn encode_offset_commit_request_data(
    value: &OffsetCommitRequestData,
    version: i16,
    limits: OffsetLimits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}
/// Decode one whole bounded OffsetCommit8..10 response.
pub fn decode_offset_commit_response_data(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
) -> Result<OffsetCommitResponseData> {
    decode(input, version, limits)
}
/// Validate before allocating one OffsetCommit8..10 response body.
pub fn encode_offset_commit_response_data(
    value: &OffsetCommitResponseData,
    version: i16,
    limits: OffsetLimits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}
/// Decode one whole bounded OffsetFetch8..10 request.
pub fn decode_offset_fetch_request_data(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
) -> Result<OffsetFetchRequestData> {
    decode(input, version, limits)
}
/// Validate before allocating one OffsetFetch8..10 request body.
pub fn encode_offset_fetch_request_data(
    value: &OffsetFetchRequestData,
    version: i16,
    limits: OffsetLimits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}
/// Decode one whole bounded OffsetFetch8..10 response.
pub fn decode_offset_fetch_response_data(
    input: &[u8],
    version: i16,
    limits: OffsetLimits,
) -> Result<OffsetFetchResponseData> {
    decode(input, version, limits)
}
/// Validate before allocating one OffsetFetch8..10 response body.
pub fn encode_offset_fetch_response_data(
    value: &OffsetFetchResponseData,
    version: i16,
    limits: OffsetLimits,
) -> Result<Vec<u8>> {
    encode(value, version, limits)
}
