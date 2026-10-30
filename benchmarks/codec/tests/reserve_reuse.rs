//! Count growth allocations for encoders using partially occupied buffers.
//! One test owns the process-wide census; fixture construction is not counted.

use bytes::BytesMut;
use codec::{census, CountingAlloc};
use partitionline::protocol::{
    api::{encode_api_versions_response, ApiVersion, ApiVersionsResponse},
    txn::{encode_add_partitions_to_txn_request, encode_end_txn_request, TxnPartitionsTopic},
};
use sha2::{Digest, Sha256};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn check_encoder(
    name: &str,
    version: i16,
    encode: impl Fn(&mut BytesMut) -> partitionline::Result<()>,
) -> bool {
    let mut expected = BytesMut::new();
    encode(&mut expected).unwrap();
    let digest = format!("{:x}", Sha256::digest(&expected));
    let mut within_budget = true;
    for capacity in [64, 512] {
        for prefix in [0, 17, capacity - 1, capacity] {
            let mut output = BytesMut::with_capacity(capacity);
            output.resize(prefix, 0xa5);
            let (result, allocations, allocated_bytes) = census(|| encode(&mut output));
            result.unwrap();
            assert_eq!(&output[..prefix], vec![0xa5; prefix].as_slice());
            assert_eq!(&output[prefix..], expected.as_ref());
            println!(
                "{}",
                serde_json::json!({
                    "encoder": name, "version": version,
                    "initial_capacity": capacity, "prefix_bytes": prefix,
                    "body_bytes": expected.len(), "body_sha256": digest,
                    "growth_allocations": allocations, "allocated_bytes": allocated_bytes,
                    "final_capacity": output.capacity(),
                })
            );
            within_budget &= allocations <= 1;

            output.truncate(prefix);
            let (result, allocations, _) = census(|| encode(&mut output));
            result.unwrap();
            assert_eq!(&output[prefix..], expected.as_ref());
            assert_eq!(allocations, 0, "{name} v{version}: warmed buffer grew");
        }
    }
    within_budget
}

pub fn run() {
    let versions = ApiVersionsResponse {
        api_keys: (0..1024)
            .map(|api_key| ApiVersion {
                api_key,
                min_version: 0,
                max_version: 1,
            })
            .collect(),
        ..ApiVersionsResponse::default()
    };
    let topics = [TxnPartitionsTopic {
        topic: "events".repeat(32),
        partitions: (0..1024).collect(),
    }];
    let transaction = "txn".repeat(1024);
    let mut within_budget = true;
    for version in [0, 4] {
        within_budget &= check_encoder("ApiVersions", version, |out| {
            encode_api_versions_response(out, version, &versions)
        });
    }
    for version in [0, 3] {
        within_budget &= check_encoder("AddPartitionsToTxn", version, |out| {
            encode_add_partitions_to_txn_request(out, version, &transaction, 1, 0, &topics)
        });
    }
    for version in [0, 5] {
        within_budget &= check_encoder("EndTxn", version, |out| {
            encode_end_txn_request(out, version, &transaction, 1, 0, true)
        });
    }
    assert!(
        within_budget,
        "encoder outgrew its initial reservation; see census rows"
    );
}

#[test]
fn planned_encoder_growth_uses_at_most_one_allocation() {
    run();
}
