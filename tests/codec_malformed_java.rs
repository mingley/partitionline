//! Error ordering for malformed batches checked against Kafka Java 4.3.1.
use bytes::Buf;
use partitionline::protocol::records::decode_record_batch;

#[test]
fn java_malformed_batches_report_the_same_first_error() {
    let cases: [(&[u8], &str); 4] = [
        (
            include_bytes!("fixtures/codec-malformed/premature-eof.batch"),
            "Incorrect declared batch size, premature EOF reached",
        ),
        (
            include_bytes!("fixtures/codec-malformed/negative-header-key.batch"),
            "Invalid negative header key size -1",
        ),
        (
            include_bytes!("fixtures/codec-malformed/first-malformed-before-eof.batch"),
            "Invalid negative header key size -1",
        ),
        (
            include_bytes!("fixtures/codec-malformed/excess-headers.batch"),
            "Found invalid number of record headers. 10 is larger than the remaining size of the buffer",
        ),
    ];
    for (wire, expected) in cases {
        let mut contiguous = wire;
        let error = decode_record_batch(&mut contiguous).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        let midpoint = wire.len() / 2;
        let mut segmented = (&wire[..midpoint]).chain(&wire[midpoint..]);
        let error = decode_record_batch(&mut segmented).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn extreme_count_is_rejected_by_the_metadata_limit() {
    let mut wire = &include_bytes!("fixtures/codec-malformed/huge-count.batch")[..];
    let error = decode_record_batch(&mut wire).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("decoded record metadata exceeds maximum allowable size"),
        "{error}"
    );
}
