//! Read-only cost visibility probe: all arms execute the pinned baseline.
//! The original micro-request/v9 is body-only. Framed cases additionally
//! encode the actual borrowed request header with a default client id.

use std::hint::black_box;
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use codec::{build_records, census, CountingAlloc};
use partitionline::protocol::api::{
    encode_produce_request, ProducePartitionData, ProduceTopicData,
};
use partitionline::protocol::header::{decode_request_header, encode_request_header_fields};
use partitionline::protocol::records::RecordBatch;
use sha2::{Digest, Sha256};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[derive(Clone, Copy)]
enum Case {
    HeaderClassic,
    HeaderFlexible,
    BodyFlexible,
    FrameClassic,
    FrameFlexible,
}

impl Case {
    fn parse(name: &str) -> Self {
        match name {
            "header-classic" => Self::HeaderClassic,
            "header-flexible" => Self::HeaderFlexible,
            "micro-request-v9-body" => Self::BodyFlexible,
            "frame-classic" => Self::FrameClassic,
            "frame-flexible" => Self::FrameFlexible,
            _ => panic!("unknown probe case"),
        }
    }

    fn version(self) -> i16 {
        match self {
            Self::HeaderClassic | Self::FrameClassic => 8,
            Self::HeaderFlexible | Self::BodyFlexible | Self::FrameFlexible => 9,
        }
    }

    fn framed(self) -> bool {
        matches!(self, Self::FrameClassic | Self::FrameFlexible)
    }
}

#[inline(never)]
fn encode_case(
    case: Case,
    output: &mut BytesMut,
    topics: &[ProduceTopicData],
    correlation: i32,
    client_id: &str,
) {
    output.clear();
    if case.framed() {
        output.put_i32(0);
    }
    if !matches!(case, Case::BodyFlexible) {
        encode_request_header_fields(
            output,
            black_box(0),
            black_box(case.version()),
            black_box(correlation),
            Some(black_box(client_id)),
        )
        .expect("valid request header");
    }
    if matches!(
        case,
        Case::BodyFlexible | Case::FrameClassic | Case::FrameFlexible
    ) {
        encode_produce_request(
            black_box(output),
            case.version(),
            None,
            1,
            30_000,
            black_box(topics),
        )
        .expect("valid produce body");
    }
    if case.framed() {
        let size = i32::try_from(output.len() - 4).expect("small frame");
        output[..4].copy_from_slice(&size.to_be_bytes());
    }
    black_box(&output[..]);
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    assert_eq!(argv.len(), 3, "usage: header-cost-probe CASE ITERATIONS");
    let name = &argv[1];
    let case = Case::parse(name);
    let iterations: u64 = argv[2].parse().expect("integer iterations");
    assert!(iterations > 0);
    let topics = [ProduceTopicData {
        topic: "t".to_owned(),
        partitions: vec![ProducePartitionData {
            index: 0,
            records: RecordBatch::from_records(build_records(0xC0DEC, 100, 16, 100, "random", 0)),
        }],
    }];
    let client_id = "partitionline";
    let mut output = BytesMut::with_capacity(16 * 1024);
    // Fixture/setup and twenty warmups precede the steady-state loop.
    for correlation in 0..20 {
        encode_case(case, &mut output, &topics, correlation, client_id);
    }
    let (_, allocations, allocated_bytes) = census(|| {
        encode_case(case, &mut output, &topics, -1234567, client_id);
    });
    let encoded_bytes = output.len();
    let digest = Sha256::digest(&output);
    let digest_hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    let body_only = matches!(case, Case::BodyFlexible);
    if !body_only {
        let mut input = if case.framed() {
            &output[4..]
        } else {
            &output[..]
        };
        let decoded = decode_request_header(&mut input).expect("encoded header decodes");
        assert_eq!(decoded.api_key, 0);
        assert_eq!(decoded.api_version, case.version());
        assert_eq!(decoded.correlation_id, -1234567);
        assert_eq!(decoded.client_id.as_deref(), Some(client_id));
        if !case.framed() {
            assert!(input.is_empty());
        }
    }
    // The original named body case starts with BytesMut::new, unlike the
    // retained BrokerConn-style buffers used above. Save its exact census.
    let (original_allocations, original_allocated_bytes) = if body_only {
        let (_, allocs, bytes) = census(|| {
            let mut fresh = BytesMut::new();
            encode_produce_request(&mut fresh, 9, None, 1, 30_000, &topics)
                .expect("valid original named body");
            assert_eq!(&fresh[..], &output[..]);
            black_box(fresh);
        });
        (Some(allocs), Some(bytes))
    } else {
        (None, None)
    };
    let started = Instant::now();
    for iteration in 0..iterations {
        encode_case(
            black_box(case),
            &mut output,
            &topics,
            black_box(iteration as i32),
            client_id,
        );
    }
    let wall_ns = started.elapsed().as_nanos();
    println!(
        "{}",
        serde_json::json!({
            "case":name,"iterations":iterations,"client_id":client_id,
            "client_id_bytes":client_id.len(),"api_key":0,"api_version":case.version(),
            "records_per_body":100,"key_bytes":16,"value_bytes":100,
            "payload_seed":0xC0DECu64,"entropy":"random","record_headers":0,
            "buffer_policy":"retained 16 KiB capacity; warmed before loop",
            "warmup_iterations":20,"steady_allocations_per_operation":allocations,
            "steady_allocated_bytes_per_operation":allocated_bytes,
            "original_named_body_allocations":original_allocations,
            "original_named_body_allocated_bytes":original_allocated_bytes,
            "encoded_bytes":encoded_bytes,"encoded_sample_sha256":digest_hex,
            "sample_correlation_id":-1234567,"header_decode_checks_passed":!body_only,
            "loop_wall_ns":wall_ns,"loop_wall_ns_per_operation":wall_ns as f64 / iterations as f64,
            "candidate":false
        })
    );
}
