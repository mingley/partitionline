//! Finite, owned Kafka-shaped peer for resource driver rehearsals.
use bytes::{Bytes, BytesMut};
use partitionline::protocol::{
    api::{
        decode_produce_request, encode_api_versions_response, encode_metadata_response,
        encode_produce_response, ApiVersion, ApiVersionsResponse, Broker, MetadataResponse,
        PartitionMetadata, ProducePartitionResponse, TopicMetadata,
    },
    api_keys::{API_VERSIONS, FETCH, METADATA, PRODUCE},
    fetch::{decode_fetch_request, encode_fetch_response, FetchedPartition, FetchedTopic},
    header::{decode_request_header, encode_response_header},
    records::{Record, RecordBatch},
};
use partitionline::{Error, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};

pub(super) struct Peer {
    pub(super) address: String,
    pub(super) active: Arc<AtomicUsize>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}

struct Connection(Arc<AtomicUsize>);
impl Drop for Connection {
    fn drop(&mut self) {
        let _previous = self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Peer {
    pub(super) async fn start(topic: String, delay: Duration) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let active = Arc::new(AtomicUsize::new(0));
        let (stop, mut stopped) = watch::channel(false);
        let connections = Arc::clone(&active);
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let result = loop {
                tokio::select! {
                    _ = stopped.changed() => break Ok(()),
                    accepted = listener.accept(), if tasks.len() < 16 => {
                        let (socket, _) = match accepted { Ok(v) => v, Err(e) => break Err(e.into()) };
                        let _previous = connections.fetch_add(1, Ordering::SeqCst);
                        let guard = Connection(Arc::clone(&connections));
                        let topic = topic.clone();
                        let _task = tasks.spawn(async move {
                            let _connection = guard;
                            serve(socket, &topic, address.port(), delay).await
                        });
                    }
                    joined = tasks.join_next(), if !tasks.is_empty() => {
                        match joined {
                            Some(Ok(Ok(()))) => {},
                            Some(Ok(Err(e))) => break Err(e),
                            Some(Err(e)) => break Err(Error::protocol(format!("peer task: {e}"))),
                            None => {},
                        }
                    }
                }
            };
            // Abort blocked I/O, then await every connection task before returning.
            tasks.shutdown().await;
            result
        });
        Ok(Self {
            address: address.to_string(),
            active,
            stop,
            task: Some(task),
        })
    }

    pub(super) async fn close(mut self) -> Result<()> {
        let _sent = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|e| Error::protocol(format!("peer join: {e}")))??;
        }
        if self.active.load(Ordering::SeqCst) != 0 {
            return Err(Error::protocol("peer connection leak"));
        }
        let listener = TcpListener::bind(&self.address).await?;
        drop(listener);
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _sent = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve(mut socket: TcpStream, topic: &str, port: u16, delay: Duration) -> Result<()> {
    socket.set_nodelay(true)?;
    let mut offset = 0_i64;
    loop {
        let size = match socket.read_i32().await {
            Ok(size) => usize::try_from(size).map_err(|_| Error::protocol("negative frame"))?,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(())
            }
            Err(e) => return Err(e.into()),
        };
        if !(1..=1024 * 1024).contains(&size) {
            return Err(Error::protocol("peer frame limit"));
        }
        let mut frame = vec![0; size];
        let _read = socket.read_exact(&mut frame).await?;
        let mut input = frame.as_slice();
        let header = decode_request_header(&mut input)?;
        let mut out = BytesMut::new();
        encode_response_header(
            &mut out,
            header.api_key,
            header.api_version,
            header.correlation_id,
        )?;
        match header.api_key {
            API_VERSIONS => encode_api_versions_response(
                &mut out,
                header.api_version,
                &ApiVersionsResponse {
                    api_keys: [
                        (API_VERSIONS, 0, 4),
                        (METADATA, 1, 4),
                        (PRODUCE, 3, 3),
                        (FETCH, 4, 4),
                    ]
                    .into_iter()
                    .map(|(api_key, min_version, max_version)| ApiVersion {
                        api_key,
                        min_version,
                        max_version,
                    })
                    .collect(),
                    ..Default::default()
                },
            )?,
            METADATA => encode_metadata_response(
                &mut out,
                header.api_version,
                &MetadataResponse {
                    throttle_time_ms: 0,
                    brokers: vec![Broker::new(0, "127.0.0.1", i32::from(port), None)],
                    cluster_id: Some("resource-rehearsal".into()),
                    controller_id: 0,
                    topics: vec![TopicMetadata::new(
                        0,
                        topic,
                        false,
                        vec![PartitionMetadata::new(
                            0,
                            0,
                            Some(0),
                            Some(0),
                            vec![0],
                            vec![0],
                            Vec::new(),
                        )],
                    )],
                    cluster_authorized_operations: i32::MIN,
                    error_code: 0,
                },
            )?,
            PRODUCE => {
                let (_, _, _, topics) = decode_produce_request(&mut input, header.api_version)?;
                let mut parts = Vec::new();
                for t in topics {
                    for p in t.partitions {
                        let count = p.records.records.len();
                        let base = offset;
                        offset = offset
                            .checked_add(
                                i64::try_from(count)
                                    .map_err(|_| Error::protocol("peer record count"))?,
                            )
                            .ok_or_else(|| Error::protocol("peer offset overflow"))?;
                        parts.push(ProducePartitionResponse {
                            topic: t.topic.clone(),
                            partition: p.index,
                            error_code: 0,
                            base_offset: base,
                            log_append_time_ms: -1,
                            log_start_offset: 0,
                            current_leader_id: -1,
                            current_leader_epoch: -1,
                            record_errors: Vec::new(),
                            error_message: None,
                        });
                    }
                }
                tokio::time::sleep(delay).await;
                encode_produce_response(&mut out, header.api_version, &parts)?;
            }
            FETCH => {
                let (_, _, topics, ..) = decode_fetch_request(&mut input, header.api_version)?;
                let mut fetched = Vec::new();
                for t in topics {
                    let mut partitions = Vec::new();
                    for p in t.partitions {
                        let records = (0..8)
                            .map(|delta| {
                                let mut value = vec![b'x'; 256];
                                let id = p.fetch_offset + delta;
                                for (target, byte) in value.iter_mut().zip(id.to_be_bytes()) {
                                    *target = byte;
                                }
                                Record {
                                    offset: id,
                                    timestamp: 0,
                                    key: None,
                                    value: Some(Bytes::from(value)),
                                    headers: Vec::new(),
                                }
                            })
                            .collect();
                        let mut batch = RecordBatch::from_records(records);
                        batch.base_offset = p.fetch_offset;
                        let mut part = FetchedPartition::partition_response(p.partition, 0);
                        part.high_watermark = p.fetch_offset + 8;
                        part.last_stable_offset = part.high_watermark;
                        part.records = vec![batch];
                        partitions.push(part);
                    }
                    fetched.push(FetchedTopic {
                        topic: t.topic,
                        topic_id: [0; 16],
                        partitions,
                    });
                }
                encode_fetch_response(&mut out, header.api_version, &fetched)?;
            }
            _ => return Err(Error::protocol("unexpected resource peer API")),
        }
        socket
            .write_i32(i32::try_from(out.len()).map_err(|_| Error::protocol("peer response size"))?)
            .await?;
        socket.write_all(&out).await?;
    }
}
