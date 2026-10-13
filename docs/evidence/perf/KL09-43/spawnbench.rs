use std::time::Instant;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    for _ in 0..10 {
        let mut set = tokio::task::JoinSet::new();
        for i in 0..3 {
            set.spawn(async move { i });
        }
        while set.join_next().await.is_some() {}
    }
    let rounds = 200;
    let t = Instant::now();
    for _ in 0..rounds {
        let mut set = tokio::task::JoinSet::new();
        for i in 0..3 {
            set.spawn(async move { i });
        }
        let mut n = 0;
        while let Some(v) = set.join_next().await {
            n += v.unwrap();
        }
        std::hint::black_box(n);
    }
    let ns = t.elapsed().as_nanos() / rounds;
    println!("joinset_3_ns={ns}");
    let cell = 131_929_000u128;
    println!(
        "pct_of_multinode_round={:.4}",
        ns as f64 / cell as f64 * 100.0
    );
}
