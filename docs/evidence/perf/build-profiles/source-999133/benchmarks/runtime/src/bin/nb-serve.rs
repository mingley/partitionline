//! Null-broker subprocess for the runtime harness (KL09-09/10).
//!
//! Binds loopback port(s), prints `READY ports=P0[,P1..]`, then serves
//! until the harness writes `quit` on stdin (or `--serve-for-secs`
//! elapses as a backstop). Always writes the broker artifact before
//! exiting so the harness can reconcile counts and validation failures.

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::Duration;

use nullbroker::{write_artifact, Config, NullBroker};

fn usage() -> ! {
    eprintln!(
        "usage: nb-serve --bind <ip> --partitions <n> --serve-for-secs <s> --artifact <path> \
         [--nodes <n>] [--dead-nodes <n>] [--slow-node <id>] [--slow-delay-ms <ms>] \
         [--fault-seed <u64>] [--fault-rate-per-million <n>] \
         [--synth-seed <u64>] [--synth-records-per-partition <n>] \
         [--synth-records-per-batch <n>] [--synth-payload-bytes <n>] \
         [--synth-header-count <n>] [--synth-codec <0..3>] [--synth-abort-every <n>]"
    );
    std::process::exit(2);
}

fn next_value(args: &mut std::iter::Skip<std::env::Args>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("nb-serve: {flag} needs a value");
        usage()
    })
}

fn main() {
    let mut config = Config::default();
    let mut serve_for = Duration::from_secs(900);
    let mut artifact: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => {
                let ip = next_value(&mut args, "--bind");
                config.bind = format!("{ip}:0");
            }
            "--partitions" => {
                config.partitions = next_value(&mut args, "--partitions")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--serve-for-secs" => {
                let secs: u64 = next_value(&mut args, "--serve-for-secs")
                    .parse()
                    .unwrap_or_else(|_| usage());
                serve_for = Duration::from_secs(secs);
            }
            "--artifact" => artifact = Some(PathBuf::from(next_value(&mut args, "--artifact"))),
            "--nodes" => {
                config.nodes = next_value(&mut args, "--nodes")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--dead-nodes" => {
                config.dead_nodes = next_value(&mut args, "--dead-nodes")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--slow-node" => {
                let id: i32 = next_value(&mut args, "--slow-node")
                    .parse()
                    .unwrap_or_else(|_| usage());
                config.slow_node = Some(id);
            }
            "--slow-delay-ms" => {
                let ms: u64 = next_value(&mut args, "--slow-delay-ms")
                    .parse()
                    .unwrap_or_else(|_| usage());
                config.slow_delay = Duration::from_millis(ms);
            }
            "--fault-seed" => {
                config.fault_seed = next_value(&mut args, "--fault-seed")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--fault-rate-per-million" => {
                config.fault_rate_per_million = next_value(&mut args, "--fault-rate-per-million")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--synth-seed" => {
                config.synth.seed = next_value(&mut args, "--synth-seed")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--synth-records-per-partition" => {
                config.synth.records_per_partition =
                    next_value(&mut args, "--synth-records-per-partition")
                        .parse()
                        .unwrap_or_else(|_| usage());
            }
            "--synth-records-per-batch" => {
                config.synth.records_per_batch = next_value(&mut args, "--synth-records-per-batch")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--synth-payload-bytes" => {
                config.synth.payload_bytes = next_value(&mut args, "--synth-payload-bytes")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--synth-header-count" => {
                config.synth.header_count = next_value(&mut args, "--synth-header-count")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--synth-codec" => {
                config.synth.codec = next_value(&mut args, "--synth-codec")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--synth-abort-every" => {
                config.synth.abort_every = next_value(&mut args, "--synth-abort-every")
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            _ => usage(),
        }
    }
    let artifact = artifact.unwrap_or_else(|| usage());
    config.serve_for = serve_for;
    // `bind_all` anchors node 0 to an ephemeral port when the base
    // port is 0 and continues sequentially from there.
    if !config.bind.contains(':') {
        config.bind.push_str(":0");
    }

    let bound = NullBroker::bind_all(&config).unwrap_or_else(|e| {
        eprintln!("nb-serve: bind failed: {e}");
        std::process::exit(1);
    });
    let ports: Vec<String> = bound
        .iter()
        .map(|(_, l)| {
            l.local_addr()
                .map(|a| a.port().to_string())
                .unwrap_or_else(|_| "?".to_owned())
        })
        .collect();
    let broker = std::sync::Arc::new(NullBroker::with_bound(&bound, &config).unwrap_or_else(|e| {
        eprintln!("nb-serve: broker init failed: {e}");
        std::process::exit(1);
    }));
    println!("READY ports={}", ports.join(","));
    let _ = std::io::stdout().flush();

    // Watch stdin on a thread; each node serves on its own thread so
    // that either path (quit received or backstop elapsed) reaches
    // the artifact write.
    let quitter = std::sync::Arc::clone(&broker);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(text) if text.trim() == "quit" => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        quitter.shutdown();
    });
    let mut handles = Vec::with_capacity(bound.len());
    for (node_id, listener) in &bound {
        let broker = std::sync::Arc::clone(&broker);
        let listener = listener.try_clone().unwrap_or_else(|e| {
            eprintln!("nb-serve: listener clone failed: {e}");
            std::process::exit(1);
        });
        let node_id = *node_id;
        handles.push(std::thread::spawn(move || {
            broker.serve(node_id, &listener, serve_for)
        }));
    }
    let mut failed = false;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            _ => failed = true,
        }
    }
    if failed {
        eprintln!("nb-serve: serve failed on at least one node");
        std::process::exit(1);
    }
    let report = broker.report();
    if let Err(e) = write_artifact(&artifact, &report) {
        eprintln!("nb-serve: artifact write failed: {e}");
        std::process::exit(1);
    }
}
