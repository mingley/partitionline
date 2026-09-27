//! Null-broker subprocess for the runtime harness (KL09-09).
//!
//! Binds an ephemeral loopback port, prints `READY port=N`, then serves
//! until the harness writes `quit` on stdin (or `--serve-for-secs`
//! elapses as a backstop). Always writes the broker artifact before
//! exiting so the harness can reconcile counts and validation failures.

use std::io::{BufRead, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;

use nullbroker::synth::SynthConfig;
use nullbroker::{write_artifact, NullBroker};

fn usage() -> ! {
    eprintln!(
        "usage: nb-serve --bind <ip> --partitions <n> --serve-for-secs <s> --artifact <path>"
    );
    std::process::exit(2);
}

fn main() {
    let mut bind = String::from("127.0.0.1");
    let mut partitions: i32 = 6;
    let mut serve_for = Duration::from_secs(900);
    let mut artifact: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => bind = args.next().unwrap_or_else(|| usage()),
            "--partitions" => {
                partitions = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--serve-for-secs" => {
                let secs: u64 = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
                serve_for = Duration::from_secs(secs);
            }
            "--artifact" => artifact = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            _ => usage(),
        }
    }
    let artifact = artifact.unwrap_or_else(|| usage());

    let listener = TcpListener::bind(format!("{bind}:0")).unwrap_or_else(|e| {
        eprintln!("nb-serve: bind failed: {e}");
        std::process::exit(1);
    });
    let port = listener.local_addr().map(|a| a.port()).unwrap_or_else(|e| {
        eprintln!("nb-serve: local_addr failed: {e}");
        std::process::exit(1);
    });
    let broker = std::sync::Arc::new(
        NullBroker::with_listener(&listener, partitions, SynthConfig::default()).unwrap_or_else(
            |e| {
                eprintln!("nb-serve: broker init failed: {e}");
                std::process::exit(1);
            },
        ),
    );
    println!("READY port={port}");
    let _ = std::io::stdout().flush();

    // Watch stdin on a thread: `serve` runs here so that either path
    // (quit received or backstop elapsed) reaches the artifact write.
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
    if let Err(e) = broker.serve(0, &listener, serve_for) {
        eprintln!("nb-serve: serve failed: {e}");
        std::process::exit(1);
    }
    let report = broker.report();
    if let Err(e) = write_artifact(&artifact, &report) {
        eprintln!("nb-serve: artifact write failed: {e}");
        std::process::exit(1);
    }
}
