use bytes::{BufMut, BytesMut};
use partitionline::protocol::api::{
    decode_produce_request, encode_api_versions_response, encode_metadata_response,
    encode_produce_response, ApiVersion, ApiVersionsResponse, Broker, MetadataResponse,
    PartitionMetadata, ProducePartitionResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, METADATA, PRODUCE};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn read_frame(
    stream: &mut tokio::net::TcpStream,
    buf: &mut BytesMut,
) -> std::io::Result<BytesMut> {
    loop {
        if buf.len() >= 4 {
            let size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            let total = 4 + size as usize;
            if buf.len() >= total {
                let mut frame = buf.split_to(total);
                let _ = frame.split_to(4);
                return Ok(frame);
            }
        }
        let n = stream.read_buf(buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof",
            ));
        }
    }
}

async fn write_frame(stream: &mut tokio::net::TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let mut out = BytesMut::with_capacity(4 + payload.len());
    out.put_i32(payload.len() as i32);
    out.extend_from_slice(payload);
    stream.write_all(&out).await
}

async fn handle_conn(mut stream: tokio::net::TcpStream, host: String, port: i32) {
    stream.set_nodelay(true).ok();
    let mut buf = BytesMut::new();
    let mut produce_offset = 0i64;
    loop {
        let mut frame = match read_frame(&mut stream, &mut buf).await {
            Ok(f) => f,
            Err(_) => break,
        };
        let header = match decode_request_header(&mut frame) {
            Ok(h) => h,
            Err(_) => break,
        };
        let mut body = BytesMut::new();
        encode_response_header(
            &mut body,
            header.api_key,
            header.api_version,
            header.correlation_id,
        );
        match header.api_key {
            API_VERSIONS => {
                let resp = ApiVersionsResponse {
                    error_code: 0,
                    api_keys: vec![
                        ApiVersion {
                            api_key: PRODUCE,
                            min_version: 3,
                            max_version: 9,
                        },
                        ApiVersion {
                            api_key: METADATA,
                            min_version: 1,
                            max_version: 12,
                        },
                        ApiVersion {
                            api_key: API_VERSIONS,
                            min_version: 0,
                            max_version: 4,
                        },
                    ],
                    throttle_time_ms: 0,
                };
                encode_api_versions_response(&mut body, header.api_version, &resp);
            }
            METADATA => {
                let resp = MetadataResponse {
                    throttle_time_ms: 0,
                    brokers: vec![Broker {
                        node_id: 1,
                        host: host.clone(),
                        port,
                        rack: None,
                    }],
                    cluster_id: Some("mock".into()),
                    controller_id: 1,
                    topics: vec![TopicMetadata {
                        error_code: 0,
                        name: Some("t".into()),
                        topic_id: [0u8; 16],
                        is_internal: false,
                        partitions: vec![PartitionMetadata {
                            error_code: 0,
                            partition_index: 0,
                            leader_id: 1,
                            leader_epoch: 0,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                        }],
                    }],
                };
                encode_metadata_response(&mut body, header.api_version, &resp);
            }
            PRODUCE => {
                let decoded = decode_produce_request(&mut frame, header.api_version).unwrap();
                let mut parts = Vec::new();
                for topic in decoded.2 {
                    for p in topic.partitions {
                        let n = p.records.records.len() as i64;
                        parts.push(ProducePartitionResponse {
                            topic: topic.topic.clone(),
                            partition: p.index,
                            error_code: 0,
                            base_offset: produce_offset,
                            log_append_time_ms: -1,
                            log_start_offset: 0,
                        });
                        produce_offset += n;
                    }
                }
                encode_produce_response(&mut body, header.api_version, &parts);
            }
            _ => break,
        }
        if write_frame(&mut stream, &body).await.is_err() {
            break;
        }
    }
}

#[tokio::test]
async fn produce_one_record_against_mock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let host = addr.ip().to_string();
    let port = addr.port() as i32;
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let host = host.clone();
            tokio::spawn(handle_conn(stream, host, port));
        }
    });

    let mut cfg = ProducerConfig::bootstrap([format!("127.0.0.1:{}", addr.port())]);
    cfg.linger = Duration::ZERO;
    cfg.client_id = "test".into();
    let producer = Producer::new(cfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"hello"[..]))
        .await
        .unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(md.partition, 0);
    assert_eq!(md.offset, 0);
    let md2 = producer
        .send(ProduceRecord::to("t").key(&b"k"[..]).value(&b"v"[..]))
        .await
        .unwrap();
    assert_eq!(md2.offset, 1);
    producer.close().await.unwrap();
}
