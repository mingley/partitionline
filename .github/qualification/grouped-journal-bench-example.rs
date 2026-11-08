//! Temporary grouped journal qualification workload.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let width = std::env::args().nth(1).ok_or("missing width")?.parse::<usize>()?;
    let dir = std::env::temp_dir().join(format!(
        "pl-grouped-bench-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
    ));
    std::fs::create_dir(&dir)?;
    let elapsed = partitionline_broker::journal::grouped_reservation_qualification(&dir.join("log"), width)?;
    println!("PL_NS={elapsed}");
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
