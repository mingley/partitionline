//! Read-only metadata-quorum inspection through a broker bootstrap endpoint.
//! Run: KAFKA_BOOTSTRAP=127.0.0.1:9092 cargo run --example describe_quorum
use partitionline::Admin;
use std::time::Duration;

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let mut admin = Admin::connect(bootstrap).await?;
    let result = admin.describe_quorum_timeout(Duration::from_secs(10)).await;
    admin.close().await?;
    let quorum = result?;
    println!(
        "leader={} epoch={} high_watermark={} voters={} observers={} nodes={}",
        quorum.leader_id,
        quorum.leader_epoch,
        quorum.high_watermark,
        quorum.voters.len(),
        quorum.observers.len(),
        quorum.nodes.len()
    );
    for voter in quorum.voters {
        println!(
            "voter={} log_end_offset={} last_fetch_timestamp={}",
            voter.replica_id, voter.log_end_offset, voter.last_fetch_timestamp
        );
    }
    Ok(())
}
