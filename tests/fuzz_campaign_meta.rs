//! Campaign metadata proof (KL-01). Distinct from 15s CI smoke.
//!
//! Opens the committed example and the retained-artifacts directory. A smoke
//! or zero-duration stamp must not parse as a campaign.

use std::io::Read;
use std::path::{Path, PathBuf};

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
