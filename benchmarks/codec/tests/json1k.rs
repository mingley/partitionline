//! One serial census test: process-wide allocator counters cannot run in parallel.
use codec::json1k::{census_report, value, COUNTS, SEED, VALUE_BYTES};
#[global_allocator]
static ALLOC: codec::CountingAlloc = codec::CountingAlloc;

#[test]
fn seeded_json1k_payloads_and_allocation_baselines() {
    for count in COUNTS {
        for i in 0..count {
            let bytes = value(SEED, i);
            assert_eq!(bytes.len(), VALUE_BYTES);
            assert_eq!(bytes, value(SEED, i));
            assert_ne!(bytes, value(SEED + 1, i));
            let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(document["sequence"].as_u64(), Some(i as u64));
            assert!(document["message"].as_str().unwrap().len() > 700);
        }
    }
    let mut baseline: serde_json::Value =
        serde_json::from_str(include_str!("../json1k-alloc-baseline.json")).unwrap();
    let mut measured = census_report();
    assert_eq!(measured["cells"].as_array().unwrap().len(), 24);
    // Decimal JSON round-tripping can change a ratio by one ULP. All input,
    // output and allocation integer fields stay exact; ratio tolerance is tiny.
    for (actual, expected) in measured["cells"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(baseline["cells"].as_array_mut().unwrap().iter_mut())
    {
        let actual_ratio = actual
            .as_object_mut()
            .unwrap()
            .remove("compressed_to_section_ratio")
            .unwrap()
            .as_f64()
            .unwrap();
        let expected_ratio = expected
            .as_object_mut()
            .unwrap()
            .remove("compressed_to_section_ratio")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!(
            (actual_ratio - expected_ratio).abs() <= 1e-12,
            "ratio drift in {}",
            actual["cell"]
        );
        if expected["cell"].as_str().unwrap().contains(":gzip:") {
            let bytes = expected["allocated_bytes"].as_u64().unwrap();
            expected["allocated_bytes"] = (bytes - codec::ZLIB_RS_STATE_BYTES_BELOW_X86_64).into();
        }
        assert_eq!(actual, expected, "new-cell input/output/allocation drift");
    }
    assert_eq!(measured, baseline);
}
