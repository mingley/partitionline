//! Bounded Metadata v0 bodies and the fields absent from that version.
use super::{Broker, MetadataRequestTopic, MetadataResponse, PartitionMetadata, TopicMetadata};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

const WIRE: usize = 1024 * 1024;
const ARRAY: usize = 8192;
const ELEMENTS: usize = 32768;
const DECODED: usize = 4 * 1024 * 1024;

pub(super) fn take_body<B: Buf>(input: &mut B) -> Result<Bytes> {
    if input.remaining() > WIRE {
        return Err(Error::protocol("Metadata v0 body exceeds 1 MiB"));
    }
    Ok(input.copy_to_bytes(input.remaining()))
}

struct Reader<'a> {
    input: &'a [u8],
    elements: usize,
    owned: usize,
    strings: usize,
}
impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            elements: 0,
            owned: 0,
            strings: 0,
        }
    }
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let value = self
            .input
            .get(..size)
            .ok_or_else(|| Error::protocol("truncated Metadata v0"))?;
        self.input = self
            .input
            .get(size..)
            .ok_or_else(|| Error::protocol("truncated Metadata v0"))?;
        Ok(value)
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| Error::protocol("Metadata v0 int16"))?,
        ))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| Error::protocol("Metadata v0 int32"))?,
        ))
    }
    fn charge(&mut self, size: usize) -> Result<()> {
        self.owned = self
            .owned
            .checked_add(size)
            .filter(|n| *n <= DECODED)
            .ok_or_else(|| Error::protocol("Metadata v0 decoded storage exceeds limit"))?;
        Ok(())
    }
    fn count<T>(&mut self, min: usize, max: usize) -> Result<usize> {
        let count = usize::try_from(self.i32()?)
            .map_err(|_| Error::protocol("null/negative Metadata v0 array"))?;
        if count > max || count > self.input.len() / min {
            return Err(Error::protocol(
                "Metadata v0 array exceeds count/input limit",
            ));
        }
        self.elements = self
            .elements
            .checked_add(count)
            .filter(|n| *n <= ELEMENTS)
            .ok_or_else(|| Error::protocol("Metadata v0 aggregate elements exceed limit"))?;
        self.charge(
            count
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| Error::protocol("Metadata v0 storage overflow"))?,
        )?;
        Ok(count)
    }
    fn string(&mut self) -> Result<String> {
        let size =
            usize::try_from(self.i16()?).map_err(|_| Error::protocol("null Metadata v0 string"))?;
        self.strings = self
            .strings
            .checked_add(size)
            .filter(|n| *n <= WIRE)
            .ok_or_else(|| Error::protocol("Metadata v0 strings exceed limit"))?;
        self.charge(size)?;
        Ok(std::str::from_utf8(self.take(size)?)
            .map_err(|_| Error::protocol("Metadata v0 invalid UTF-8"))?
            .to_owned())
    }
    fn ints(&mut self) -> Result<Vec<i32>> {
        let count = self.count::<i32>(4, ARRAY)?;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            result.push(self.i32()?);
        }
        Ok(result)
    }
    fn finish(&self) -> Result<()> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(Error::protocol("trailing Metadata v0 bytes"))
        }
    }
}

pub(super) fn decode_request(input: &[u8]) -> Result<Vec<MetadataRequestTopic>> {
    let mut reader = Reader::new(input);
    let count = reader.count::<MetadataRequestTopic>(2, ARRAY)?;
    let mut topics = Vec::with_capacity(count);
    for _ in 0..count {
        topics.push(MetadataRequestTopic {
            name: Some(reader.string()?),
            topic_id: [0; 16],
        });
    }
    reader.finish()?;
    Ok(topics)
}

pub(super) fn decode_response(input: &[u8]) -> Result<MetadataResponse> {
    let mut reader = Reader::new(input);
    reader.charge(std::mem::size_of::<MetadataResponse>())?;
    let count = reader.count::<Broker>(10, 256)?;
    let mut brokers = Vec::with_capacity(count);
    for _ in 0..count {
        brokers.push(Broker {
            node_id: reader.i32()?,
            host: reader.string()?,
            port: reader.i32()?,
            rack: None,
        });
    }
    let count = reader.count::<TopicMetadata>(8, ARRAY)?;
    let mut topics = Vec::with_capacity(count);
    for _ in 0..count {
        let error_code = reader.i16()?;
        let name = Some(reader.string()?);
        let count = reader.count::<PartitionMetadata>(18, ARRAY)?;
        let mut partitions = Vec::with_capacity(count);
        let mut leader_slots = 0usize;
        for _ in 0..count {
            let error_code = reader.i16()?;
            let partition_index = reader.i32()?;
            if let Ok(index) = usize::try_from(partition_index) {
                if index >= ARRAY {
                    return Err(Error::protocol(
                        "Metadata v0 partition index exceeds route limit",
                    ));
                }
                leader_slots = leader_slots.max(index + 1);
            }
            partitions.push(PartitionMetadata {
                error_code,
                partition_index,
                leader_id: reader.i32()?,
                leader_epoch: -1,
                replica_nodes: reader.ints()?,
                isr_nodes: reader.ints()?,
                offline_replicas: Vec::new(),
            });
        }
        reader.charge(leader_slots * 2 * std::mem::size_of::<i32>())?;
        topics.push(TopicMetadata {
            error_code,
            name,
            topic_id: [0; 16],
            is_internal: false,
            partitions,
            topic_authorized_operations: i32::MIN,
        });
    }
    reader.finish()?;
    Ok(MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: None,
        controller_id: -1,
        topics,
        cluster_authorized_operations: i32::MIN,
        error_code: 0,
    })
}

pub(super) fn validate_response(response: &MetadataResponse) -> Result<()> {
    struct Measure {
        wire: usize,
        owned: usize,
        elements: usize,
    }
    impl Measure {
        fn wire(&mut self, bytes: usize) -> Result<()> {
            self.wire = self
                .wire
                .checked_add(bytes)
                .filter(|n| *n <= WIRE)
                .ok_or_else(|| Error::protocol("Metadata v0 response exceeds 1 MiB"))?;
            Ok(())
        }
        fn own(&mut self, bytes: usize) -> Result<()> {
            self.owned = self
                .owned
                .checked_add(bytes)
                .filter(|n| *n <= DECODED)
                .ok_or_else(|| Error::protocol("Metadata v0 response storage exceeds limit"))?;
            Ok(())
        }
        fn array<T>(&mut self, count: usize, max: usize) -> Result<()> {
            if count > max {
                return Err(Error::protocol("Metadata v0 response array exceeds limit"));
            }
            self.elements = self
                .elements
                .checked_add(count)
                .filter(|n| *n <= ELEMENTS)
                .ok_or_else(|| {
                    Error::protocol("Metadata v0 response aggregate elements exceed limit")
                })?;
            self.own(
                count
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or_else(|| Error::protocol("Metadata v0 storage overflow"))?,
            )?;
            self.wire(4)
        }
        fn string(&mut self, name: &str) -> Result<()> {
            if name.len() > i16::MAX as usize {
                return Err(Error::protocol("Metadata v0 response string exceeds limit"));
            }
            self.own(name.len())?;
            self.wire(2 + name.len())
        }
    }
    let mut measure = Measure {
        wire: 0,
        owned: std::mem::size_of::<MetadataResponse>(),
        elements: 0,
    };
    measure.array::<Broker>(response.brokers.len(), 256)?;
    for broker in &response.brokers {
        measure.wire(8)?;
        measure.string(&broker.host)?;
    }
    measure.array::<TopicMetadata>(response.topics.len(), ARRAY)?;
    for topic in &response.topics {
        measure.wire(2)?;
        measure.string(
            topic
                .name
                .as_deref()
                .ok_or_else(|| Error::protocol("null Metadata v0 response topic name"))?,
        )?;
        measure.array::<PartitionMetadata>(topic.partitions.len(), ARRAY)?;
        for partition in &topic.partitions {
            measure.wire(10)?;
            for nodes in [&partition.replica_nodes, &partition.isr_nodes] {
                measure.array::<i32>(nodes.len(), ARRAY)?;
                measure.wire(
                    nodes
                        .len()
                        .checked_mul(4)
                        .ok_or_else(|| Error::protocol("Metadata v0 node bytes overflow"))?,
                )?;
            }
        }
    }
    Ok(())
}

fn request_policy(allow: bool, topic_auth: bool, cluster_auth: bool) -> Result<()> {
    if !allow || topic_auth || cluster_auth {
        return Err(Error::Unsupported(
            "Metadata v0 cannot represent topic-creation or authorization flags".into(),
        ));
    }
    Ok(())
}
fn names_size<'a>(names: impl Iterator<Item = &'a str>) -> Result<(i32, usize)> {
    let mut count = 0i32;
    let mut bytes = 4usize;
    for name in names {
        if usize::try_from(count).map_err(|_| Error::protocol("Metadata count"))? >= ARRAY
            || name.len() > i16::MAX as usize
        {
            return Err(Error::protocol(
                "Metadata v0 topic count/string exceeds limit",
            ));
        }
        count += 1;
        bytes = bytes
            .checked_add(2 + name.len())
            .filter(|n| *n <= WIRE)
            .ok_or_else(|| Error::protocol("Metadata v0 request exceeds 1 MiB"))?;
    }
    Ok((count, bytes))
}
fn put_name(output: &mut BytesMut, name: &str) -> Result<()> {
    output.put_i16(
        i16::try_from(name.len())
            .map_err(|_| Error::protocol("Metadata v0 string exceeds limit"))?,
    );
    output.extend_from_slice(name.as_bytes());
    Ok(())
}
pub(super) fn encode_names(
    output: &mut BytesMut,
    names: Option<&[String]>,
    allow: bool,
    topic_auth: bool,
) -> Result<()> {
    request_policy(allow, topic_auth, false)?;
    let names = names.unwrap_or(&[]);
    let (count, bytes) = names_size(names.iter().map(String::as_str))?;
    output.reserve(bytes);
    output.put_i32(count);
    for name in names {
        put_name(output, name)?;
    }
    Ok(())
}
pub(super) fn encode_topics(
    output: &mut BytesMut,
    topics: Option<&[MetadataRequestTopic]>,
    allow: bool,
    topic_auth: bool,
    cluster_auth: bool,
) -> Result<()> {
    request_policy(allow, topic_auth, cluster_auth)?;
    let topics = topics.unwrap_or(&[]);
    for topic in topics {
        if topic.topic_id != [0; 16] || topic.name.is_none() {
            return Err(Error::Unsupported(
                "Metadata v0 requires topic names and cannot carry UUIDs".into(),
            ));
        }
    }
    let (count, bytes) = names_size(topics.iter().filter_map(|topic| topic.name.as_deref()))?;
    output.reserve(bytes);
    output.put_i32(count);
    for topic in topics {
        put_name(
            output,
            topic
                .name
                .as_deref()
                .ok_or_else(|| Error::protocol("missing Metadata v0 name"))?,
        )?;
    }
    Ok(())
}
