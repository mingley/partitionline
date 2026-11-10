//! Isolated wire probe server for static client-label Produce quotas.
use partitionline_broker::{
    catalog::{Catalog, TopicId},
    fetch,
    metadata::{self, ClientProduceQuotas, ProduceQuota, QuotaRouter},
    produce,
    transport::{self, Transport},
};
use std::{error::Error, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "partitionline-client-quota-wire-{}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    let common = metadata::Config::new(0, "127.0.0.1".into(), 9092, "quota-wire".into());
    let (mut catalog, _) = Catalog::open(root.join("catalog"), common.catalog_limits)?;
    let mut raw = [0u8; 16];
    raw[15] = 2;
    catalog.create("alpha", TopicId::new(raw)?, 1)?;
    drop(catalog);
    // Burst is selected to make the first fixture fit and the second exceed it.
    // It is not Kafka's sampled quota algorithm.
    let quota = ProduceQuota::new(20, 200, 8_000)?;
    let labels = ["quota-alpha-v3", "quota-bravo-v3", "quota-alpha-v6", "quota-bravo-v6"];
    let named = labels.into_iter().map(|name| (name.to_owned(), quota)).collect();
    let policies = ClientProduceQuotas::new(quota, named)?;
    let (router, _) = QuotaRouter::open_with_client_quotas(
        root.join("catalog"), common, produce::Config::new(root.join("parts")),
        fetch::Limits::default(), policies,
    ).await?;
    let router = Arc::new(router);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 9092).into(), transport::Config::default(), Arc::clone(&router),
    ).await?;
    let ready = std::env::var_os("QUOTA_READY_FILE").ok_or("missing ready path")?;
    std::fs::write(ready, b"ready\n")?;
    println!("quota wire server ready");
    tokio::signal::ctrl_c().await?;
    transport.shutdown().await?;
    router.shutdown().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
