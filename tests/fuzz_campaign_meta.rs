//! Campaign metadata proof (KL-01). Distinct from 15s CI smoke.
//!
//! Opens the committed example and the retained-artifacts directory. A smoke
//! or zero-duration stamp must not parse as a campaign. An example metadata
//! file is never execution evidence. Validates corpus seeds across compressed,
//! tagged, transactional, and boundary-length frames.

#![expect(
    clippy::too_many_lines,
    reason = "seed corpus setup and decoders testing"
)]

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use partitionline::protocol::api::{
    decode_metadata_response, decode_produce_response, encode_metadata_response,
    encode_produce_response, MetadataRequest,
};
use partitionline::protocol::cgheartbeat::{
    decode_consumer_group_heartbeat_request, decode_consumer_group_heartbeat_response,
    encode_consumer_group_heartbeat_request, encode_consumer_group_heartbeat_response,
    ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse,
};
use partitionline::protocol::fetch::{decode_fetch_response, encode_fetch_response};
use partitionline::protocol::group::{
    decode_heartbeat_response, decode_join_group_response, decode_offset_commit_response,
    decode_sync_group_response, encode_heartbeat_response, encode_join_group_response,
    encode_offset_commit_response, encode_sync_group_response,
};
use partitionline::protocol::records::{
    decode_record_batches, encode_record_batch, Compression, ControlRecordType,
    EndTransactionMarker, Record, RecordBatch,
};
use partitionline::protocol::share::{decode_share_fetch_response, encode_share_fetch_response};

fn write_file(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(bytes)?;
    Ok(())
}

fn read_file(path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    let _ = f.read_to_end(&mut buf)?;
    Ok(buf)
}

fn after_key<'a>(raw: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{key}\"");
    let i = raw.find(&pat)?;
    let rest = raw.get(i.saturating_add(pat.len())..)?;
    rest.trim_start().strip_prefix(':').map(str::trim_start)
}

fn json_string(raw: &str, key: &str) -> Option<String> {
    let rest = after_key(raw, key)?;
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    rest.get(..end).map(str::to_string)
}

fn json_bool(raw: &str, key: &str) -> Option<bool> {
    let rest = after_key(raw, key)?;
    if rest.starts_with("true") {
        Some(true)
    } else if rest.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn json_u64(raw: &str, key: &str) -> Option<u64> {
    let rest = after_key(raw, key)?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn json_has_key(raw: &str, key: &str) -> bool {
    after_key(raw, key).is_some()
}

fn is_campaign(raw: &str) -> bool {
    json_string(raw, "kind").as_deref() == Some("campaign")
        && json_u64(raw, "duration_seconds").is_some_and(|d| d > 15)
}

fn is_execution_evidence(raw: &str) -> bool {
    if !is_campaign(raw) {
        return false;
    }
    if json_bool(raw, "is_example") == Some(true) {
        return false;
    }
    if let Some(id) = json_string(raw, "campaign_id") {
        if id.contains("example") || id.contains("fixture") {
            return false;
        }
    }
    true
}

/// Generates seed corpus files for all 7 fuzz targets if they are not already present.
fn generate_seeds_if_missing(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let seeds_dir = root.join("fuzz/seeds");

    // 1. decode_record_batches
    let rec_dir = seeds_dir.join("decode_record_batches");
    std::fs::create_dir_all(&rec_dir)?;

    let rec = Record {
        offset: 0,
        timestamp: 1000,
        key: Some(bytes::Bytes::from_static(b"k1")),
        value: Some(bytes::Bytes::from_static(b"v1")),
        headers: vec![],
    };

    // Compressed: gzip, snappy, lz4
    let gz_path = rec_dir.join("compressed_gzip.bin");
    if !gz_path.is_file() {
        let batch =
            RecordBatch::from_records(vec![rec.clone()]).with_compression(Compression::Gzip);
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&gz_path, &buf)?;
    }

    let snap_path = rec_dir.join("compressed_snappy.bin");
    if !snap_path.is_file() {
        let batch =
            RecordBatch::from_records(vec![rec.clone()]).with_compression(Compression::Snappy);
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&snap_path, &buf)?;
    }

    let lz4_path = rec_dir.join("compressed_lz4.bin");
    if !lz4_path.is_file() {
        let batch = RecordBatch::from_records(vec![rec.clone()]).with_compression(Compression::Lz4);
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&lz4_path, &buf)?;
    }

    // Transactional: data, commit marker, abort marker
    let txn_data_path = rec_dir.join("transactional_data.bin");
    if !txn_data_path.is_file() {
        let mut batch = RecordBatch::from_records(vec![rec.clone()]).with_transactional(true);
        batch.producer_id = 9999;
        batch.producer_epoch = 1;
        batch.base_sequence = 0;
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&txn_data_path, &buf)?;
    }

    let txn_commit_path = rec_dir.join("transactional_control_commit.bin");
    if !txn_commit_path.is_file() {
        let marker = EndTransactionMarker::new(ControlRecordType::Commit, 1)?;
        let batch = RecordBatch::with_end_transaction_marker(0, 1000, 0, 9999, 1, &marker)?;
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&txn_commit_path, &buf)?;
    }

    let txn_abort_path = rec_dir.join("transactional_control_abort.bin");
    if !txn_abort_path.is_file() {
        let marker = EndTransactionMarker::new(ControlRecordType::Abort, 1)?;
        let batch = RecordBatch::with_end_transaction_marker(0, 1000, 0, 9999, 1, &marker)?;
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&txn_abort_path, &buf)?;
    }

    // Boundary-length: empty, single-record, header-only
    let b_empty_path = rec_dir.join("boundary_empty.bin");
    if !b_empty_path.is_file() {
        write_file(&b_empty_path, b"")?;
    }

    let b_single_path = rec_dir.join("boundary_single_record.bin");
    if !b_single_path.is_file() {
        let batch = RecordBatch::from_records(vec![Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(bytes::Bytes::from_static(b"x")),
            headers: vec![],
        }]);
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&b_single_path, &buf)?;
    }

    let b_hdr_path = rec_dir.join("boundary_header_only.bin");
    if !b_hdr_path.is_file() {
        let batch = RecordBatch::from_records(vec![]);
        let mut buf = bytes::BytesMut::new();
        encode_record_batch(&mut buf, &batch)?;
        write_file(&b_hdr_path, &buf)?;
    }

    // 2. decode_fetch_response
    let fetch_dir = seeds_dir.join("decode_fetch_response");
    std::fs::create_dir_all(&fetch_dir)?;
    let fetch_v12 = fetch_dir.join("tagged_fetch_v12.bin");
    if !fetch_v12.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_fetch_response(&mut buf, 12, &[])?;
        write_file(&fetch_v12, &buf)?;
    }
    let fetch_v16 = fetch_dir.join("tagged_fetch_v16.bin");
    if !fetch_v16.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_fetch_response(&mut buf, 16, &[])?;
        write_file(&fetch_v16, &buf)?;
    }
    let fetch_b = fetch_dir.join("boundary_fetch_empty.bin");
    if !fetch_b.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_fetch_response(&mut buf, 4, &[])?;
        write_file(&fetch_b, &buf)?;
    }

    // 3. decode_produce_response
    let prod_dir = seeds_dir.join("decode_produce_response");
    std::fs::create_dir_all(&prod_dir)?;
    let prod_v9 = prod_dir.join("tagged_produce_v9.bin");
    if !prod_v9.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_produce_response(&mut buf, 9, &[])?;
        write_file(&prod_v9, &buf)?;
    }
    let prod_b = prod_dir.join("boundary_produce_empty.bin");
    if !prod_b.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_produce_response(&mut buf, 3, &[])?;
        write_file(&prod_b, &buf)?;
    }

    // 4. decode_metadata_response
    let meta_dir = seeds_dir.join("decode_metadata_response");
    std::fs::create_dir_all(&meta_dir)?;
    let meta_v9 = meta_dir.join("tagged_metadata_v9.bin");
    if !meta_v9.is_file() {
        let resp = MetadataRequest::error_response(None, 0);
        let mut buf = bytes::BytesMut::new();
        encode_metadata_response(&mut buf, 9, &resp)?;
        write_file(&meta_v9, &buf)?;
    }
    let meta_b = meta_dir.join("boundary_metadata_empty.bin");
    if !meta_b.is_file() {
        let resp = MetadataRequest::error_response(None, 0);
        let mut buf = bytes::BytesMut::new();
        encode_metadata_response(&mut buf, 1, &resp)?;
        write_file(&meta_b, &buf)?;
    }

    // 5. decode_cgheartbeat_responses
    let cghb_dir = seeds_dir.join("decode_cgheartbeat_responses");
    std::fs::create_dir_all(&cghb_dir)?;
    let cghb_req_v0 = cghb_dir.join("tagged_cgheartbeat_req_v0.bin");
    if !cghb_req_v0.is_file() {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "seed-group".into(),
            member_id: "seed-member".into(),
            member_epoch: 1,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 30000,
            subscribed_topic_names: Some(vec!["seed-topic".into()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        };
        let mut buf = bytes::BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, 0, &req)?;
        write_file(&cghb_req_v0, &buf)?;
    }
    let cghb_resp_v0 = cghb_dir.join("tagged_cgheartbeat_resp_v0.bin");
    if !cghb_resp_v0.is_file() {
        let resp = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("seed-member".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        let mut buf = bytes::BytesMut::new();
        encode_consumer_group_heartbeat_response(&mut buf, 0, &resp)?;
        write_file(&cghb_resp_v0, &buf)?;
    }
    let cghb_resp_v1 = cghb_dir.join("tagged_cgheartbeat_resp_v1.bin");
    if !cghb_resp_v1.is_file() {
        let resp = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("seed-member".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        let mut buf = bytes::BytesMut::new();
        encode_consumer_group_heartbeat_response(&mut buf, 1, &resp)?;
        write_file(&cghb_resp_v1, &buf)?;
    }

    // 6. decode_share_fetch_response
    let share_dir = seeds_dir.join("decode_share_fetch_response");
    std::fs::create_dir_all(&share_dir)?;
    let share_v0 = share_dir.join("tagged_share_fetch_v0.bin");
    if !share_v0.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_share_fetch_response(&mut buf, 0, &[])?;
        write_file(&share_v0, &buf)?;
    }
    let share_b = share_dir.join("boundary_share_fetch_empty.bin");
    if !share_b.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_share_fetch_response(&mut buf, 1, &[])?;
        write_file(&share_b, &buf)?;
    }

    // 7. decode_group_responses
    let grp_dir = seeds_dir.join("decode_group_responses");
    std::fs::create_dir_all(&grp_dir)?;
    let jg_v9 = grp_dir.join("join_group_v9.bin");
    if !jg_v9.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_join_group_response(&mut buf, 9, 0, 1, "roundrobin", "leader", "member", &[])?;
        write_file(&jg_v9, &buf)?;
    }
    let sg_v5 = grp_dir.join("sync_group_v5.bin");
    if !sg_v5.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_sync_group_response(&mut buf, 5, 0, &[])?;
        write_file(&sg_v5, &buf)?;
    }
    let hb_v4 = grp_dir.join("heartbeat_v4.bin");
    if !hb_v4.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_heartbeat_response(&mut buf, 4, 0)?;
        write_file(&hb_v4, &buf)?;
    }
    let oc_v9 = grp_dir.join("offset_commit_v9.bin");
    if !oc_v9.is_file() {
        let mut buf = bytes::BytesMut::new();
        encode_offset_commit_response(&mut buf, 9, &[], 0)?;
        write_file(&oc_v9, &buf)?;
    }
    Ok(())
}

#[test]
fn committed_campaign_metadata_is_not_smoke() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let meta_path = root.join("fuzz/campaign/metadata.example.json");
    assert!(
        meta_path.is_file(),
        "committed campaign metadata missing: {}",
        meta_path.display()
    );
    let mut f = std::fs::File::open(&meta_path).expect("open campaign metadata");
    let mut raw = String::new();
    let n = f.read_to_string(&mut raw).expect("read campaign metadata");
    assert_eq!(n, raw.len());

    let kind = json_string(&raw, "kind").expect("kind");
    assert_ne!(kind, "smoke", "campaign metadata must not be kind=smoke");
    assert_eq!(kind, "campaign");

    let duration = json_u64(&raw, "duration_seconds").expect("duration_seconds");
    assert!(
        duration > 15,
        "campaign duration_seconds must be > 15, got {duration}"
    );

    assert!(json_has_key(&raw, "targets"));
    assert!(json_has_key(&raw, "started_at"));
    assert!(json_has_key(&raw, "finished_at"));
    assert!(json_has_key(&raw, "toolchain"));
    assert!(json_has_key(&raw, "source"));
    assert!(json_has_key(&raw, "corpus"));
    assert!(json_has_key(&raw, "coverage"));
    assert!(json_has_key(&raw, "campaign_id"));
    assert!(json_string(&raw, "campaign_id").is_some_and(|id| !id.is_empty()));

    let artifacts = json_string(&raw, "artifacts_dir").expect("artifacts_dir");
    let art = if Path::new(&artifacts).is_absolute() {
        PathBuf::from(&artifacts)
    } else {
        root.join(&artifacts)
    };
    assert!(
        art.is_dir(),
        "artifacts_dir must exist relative to CARGO_MANIFEST_DIR: {}",
        art.display()
    );
    assert!(is_campaign(&raw));

    // Example fixture must NOT be treated as execution evidence
    assert!(
        !is_execution_evidence(&raw),
        "example metadata fixture must not pass as execution evidence"
    );

    // Schema file must also exist
    let schema_path = root.join("fuzz/campaign/metadata.schema.json");
    assert!(
        schema_path.is_file(),
        "campaign metadata schema missing: {}",
        schema_path.display()
    );
}

#[test]
fn smoke_and_zero_campaign_are_rejected() {
    assert!(!is_campaign(
        r#"{"kind":"smoke","duration_seconds":15,"coverage":"unavailable"}"#
    ));
    assert!(!is_campaign(
        r#"{"kind":"campaign","duration_seconds":15,"coverage":"unavailable"}"#
    ));
    assert!(!is_campaign(
        r#"{"kind":"campaign","duration_seconds":0,"coverage":"unavailable"}"#
    ));
    assert!(is_campaign(
        r#"{"kind":"campaign","duration_seconds":3600,"coverage":"unavailable"}"#
    ));
}

#[test]
fn example_metadata_is_never_execution_evidence() {
    assert!(!is_execution_evidence(
        r#"{"kind":"campaign","campaign_id":"kl-01-example-fixture","is_example":true,"duration_seconds":3600}"#
    ));
    assert!(!is_execution_evidence(
        r#"{"kind":"campaign","campaign_id":"example-run","duration_seconds":3600}"#
    ));
    assert!(is_execution_evidence(
        r#"{"kind":"campaign","campaign_id":"campaign-20260921-prod","is_example":false,"duration_seconds":120}"#
    ));
}

#[test]
fn seeds_inventory_and_decoders_test() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    generate_seeds_if_missing(&root).expect("generate seeds");

    let seeds_dir = root.join("fuzz/seeds");
    assert!(seeds_dir.is_dir(), "fuzz/seeds must be a directory");

    let targets = [
        "decode_record_batches",
        "decode_fetch_response",
        "decode_produce_response",
        "decode_metadata_response",
        "decode_cgheartbeat_responses",
        "decode_share_fetch_response",
        "decode_group_responses",
    ];

    let mut total_seeds = 0;
    let mut has_compressed = false;
    let mut has_tagged = false;
    let mut has_transactional = false;
    let mut has_boundary = false;

    for target in &targets {
        let target_dir = seeds_dir.join(target);
        assert!(
            target_dir.is_dir(),
            "target seed directory missing: {}",
            target_dir.display()
        );
        let entries = std::fs::read_dir(&target_dir).expect("read target dir");
        let mut count = 0;
        for entry in entries {
            let entry = entry.expect("dir entry");
            let p = entry.path();
            if p.is_file() {
                count += 1;
                total_seeds += 1;
                let name = p.file_name().unwrap().to_string_lossy();
                if name.contains("compressed") {
                    has_compressed = true;
                }
                if name.contains("tagged") {
                    has_tagged = true;
                }
                if name.contains("transactional") {
                    has_transactional = true;
                }
                if name.contains("boundary") {
                    has_boundary = true;
                }

                // Verify every seed file decodes without panicking
                let data = read_file(&p).expect("read seed file");
                match *target {
                    "decode_record_batches" => {
                        let mut cur = data.as_slice();
                        drop(decode_record_batches(&mut cur));
                    }
                    "decode_fetch_response" => {
                        for v in [4_i16, 7, 11, 12, 13, 16, 17] {
                            let mut cur = data.as_slice();
                            drop(decode_fetch_response(&mut cur, v));
                        }
                    }
                    "decode_produce_response" => {
                        for v in [3_i16, 8, 9, 10, 11, 12] {
                            let mut cur = data.as_slice();
                            drop(decode_produce_response(&mut cur, v));
                        }
                    }
                    "decode_metadata_response" => {
                        for v in [1_i16, 8, 9, 10, 12, 13] {
                            let mut cur = data.as_slice();
                            drop(decode_metadata_response(&mut cur, v));
                        }
                    }
                    "decode_cgheartbeat_responses" => {
                        for v in [0_i16, 1] {
                            let mut cur = data.as_slice();
                            drop(decode_consumer_group_heartbeat_request(&mut cur, v));
                            let mut cur = data.as_slice();
                            drop(decode_consumer_group_heartbeat_response(&mut cur, v));
                        }
                    }
                    "decode_share_fetch_response" => {
                        for v in [0_i16, 1] {
                            let mut cur = data.as_slice();
                            drop(decode_share_fetch_response(&mut cur, v));
                        }
                    }
                    "decode_group_responses" => {
                        for v in [2_i16, 3, 4, 5, 6, 7, 8, 9] {
                            let mut cur = data.as_slice();
                            drop(decode_join_group_response(&mut cur, v));
                            let mut cur = data.as_slice();
                            drop(decode_offset_commit_response(&mut cur, v));
                        }
                        for v in [0_i16, 1, 2, 3, 4, 5] {
                            let mut cur = data.as_slice();
                            drop(decode_sync_group_response(&mut cur, v));
                        }
                        for v in [0_i16, 1, 2, 3, 4] {
                            let mut cur = data.as_slice();
                            drop(decode_heartbeat_response(&mut cur, v));
                        }
                    }
                    _ => panic!("unknown fuzz target: {target}"),
                }
            }
        }
        assert!(count > 0, "target {target} has 0 seeds");
    }

    assert!(
        total_seeds >= 20,
        "expected at least 20 seeds across targets, got {total_seeds}"
    );
    assert!(has_compressed, "must seed compressed frames");
    assert!(has_tagged, "must seed tagged frames");
    assert!(has_transactional, "must seed transactional frames");
    assert!(has_boundary, "must seed boundary-length frames");
}
