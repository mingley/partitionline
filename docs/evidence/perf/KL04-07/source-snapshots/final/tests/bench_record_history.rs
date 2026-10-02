//! Regression checks for the independently hashed benchmark record envelope.
#![expect(
    clippy::disallowed_methods,
    reason = "synchronous journal tests inspect persisted bytes without an async runtime"
)]

#[path = "../examples/common/bench_history.rs"]
#[expect(
    dead_code,
    reason = "test target includes settings helpers used only by benchmark mains"
)]
mod history;

#[test]
fn seeded_record_matches_independent_python_sha256_fixture() {
    let value = history::payload(0x5eed0001, 17, 100).unwrap();
    assert_eq!(value.len(), 100);
    assert_eq!(history::identity(&value), Some((0x5eed0001, 17)));
    assert_eq!(
        history::hash(&value),
        "8f6fe2334395dee27d62e4c6123c0f9149f886cb49467965a72dc3cb60439a8c"
    );
    assert_eq!(
        history::key(0x5eed0001, 2).as_ref(),
        b"plbench-000000005eed0001-2"
    );
}

#[test]
fn corruptions_and_truncations_do_not_pass_record_identity_or_hash() {
    let good = history::payload(7, 9, 100).unwrap();
    let mut corrupt = good.to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    assert_ne!(history::hash(&good), history::hash(&corrupt));
    assert_eq!(history::identity(&corrupt), Some((7, 9)));
    assert!(history::identity(b"not a benchmark envelope").is_none());
    for length in 0..24 {
        assert!(history::identity(good.get(..length).unwrap()).is_none());
    }
    assert!(history::payload(7, 9, 23).is_err());
}

#[test]
fn journals_checkpoint_records_and_never_replace_existing_attempts() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "partitionline-bench-history-{}-{unique}.jsonl",
        std::process::id()
    ));
    let name = path.to_str().unwrap();
    let mut journal = history::Journal::create(name).unwrap();
    journal.line("{\"kind\":\"config\"}").unwrap();
    journal
        .record(
            "7:9",
            "topic",
            0,
            Some(11),
            Some(b"key"),
            b"value",
            "measure",
            "accepted",
        )
        .unwrap();
    journal.checkpoint().unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(String::from_utf8_lossy(&before).contains("\"offset\":11"));
    assert!(history::Journal::create(name).is_err());
    assert_eq!(before, std::fs::read(&path).unwrap());
    drop(journal);
    std::fs::remove_file(path).unwrap();
}
