#![allow(clippy::panic)]
//! Mixed-version feature updates and an explicit downgrade rollback.

use partitionline_broker::admin::{self, Admin, Assignment};
use partitionline_broker::journal;
use std::time::{SystemTime, UNIX_EPOCH};

fn limits() -> admin::Limits {
    let journal =
        journal::Limits::new(8192, 1 << 20, 64, 1 << 20).unwrap_or_else(|_| panic!("journal"));
    admin::Limits::new(8, 4, 4, journal).unwrap_or_else(|_| panic!("admin limits"))
}

#[test]
fn unsupported_downgrade_restores_the_proven_feature_level() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("pl-upgrade-{nanos}"));
    let mut admin =
        Admin::open(&path, limits(), "cluster-1", 1).unwrap_or_else(|error| panic!("{error:?}"));
    admin
        .register_log_dir(0, 0, "data-a")
        .unwrap_or_else(|error| panic!("{error:?}"));
    admin
        .alter_assignment(
            0,
            0,
            Assignment {
                topic: "orders",
                partition: 0,
                replicas: &[1],
                leader: 1,
                log_dir: "data-a",
            },
        )
        .unwrap_or_else(|error| panic!("{error:?}"));
    admin
        .update_feature(0, 0, "metadata.version", 2)
        .unwrap_or_else(|error| panic!("{error:?}"));
    admin
        .update_feature(1, 0, "metadata.version", 3)
        .unwrap_or_else(|error| panic!("{error:?}"));
    let proven = admin
        .feature_level(1, "metadata.version")
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(proven, 3);
    let downgrade = admin.update_feature(0, 0, "metadata.version", 1);
    assert!(matches!(downgrade, Err(admin::Error::FeatureDowngrade)));
    let unsupported = admin.update_feature(2, 0, "metadata.version", 4);
    assert!(matches!(unsupported, Err(admin::Error::Unsupported)));
    assert_eq!(
        admin
            .feature_level(0, "metadata.version")
            .unwrap_or_else(|error| panic!("{error:?}")),
        proven
    );
    assert_eq!(
        admin
            .describe_log_dir(1, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}")),
        "data-a"
    );
    drop(admin);
    let restored =
        Admin::open(&path, limits(), "cluster-1", 1).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        restored
            .feature_level(1, "metadata.version")
            .unwrap_or_else(|error| panic!("{error:?}")),
        proven
    );
    assert_eq!(
        restored
            .describe_log_dir(0, "orders", 0)
            .unwrap_or_else(|error| panic!("{error:?}")),
        "data-a"
    );
    let body = format!(
        "{{\"proven_feature\":\"metadata.version\",\"proven_level\":{proven},\"downgrade_rejected\":true,\"unsupported_version_rejected\":true,\"log_dir\":\"data-a\",\"rollback_level\":{proven}}}\n"
    );
    let out = std::env::var("PL_UPGRADE_CAMPAIGN_OUT").unwrap_or_default();
    if !out.is_empty() {
        let mut file = std::fs::File::create(&out).unwrap_or_else(|error| panic!("{error:?}"));
        std::io::Write::write_all(&mut file, body.as_bytes())
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
}
