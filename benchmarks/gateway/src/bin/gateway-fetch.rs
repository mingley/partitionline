//! Drive or register the gateway fetch gzip cell without linking librdkafka.

use std::path::PathBuf;

use gateway::cells::{find_fetch, FETCH_CELLS};
use gateway::settings::TlsMode;

fn main() {
    if let Err(error) = run() {
        eprintln!("gateway-fetch: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(usage)?;
    match command.as_str() {
        "emit-config" => {
            let cell = find_fetch(&flag(&mut args, "--cell")?)?;
            let tls = tls_from_env();
            println!(
                "{}",
                serde_json::to_string_pretty(&gateway::fetch::partitionline_fetch_emit_config(
                    cell, tls
                ))
                .map_err(|e| e.to_string())?
            );
        }
        "compare" => {
            let left = read_json(&flag(&mut args, "--left")?)?;
            let right = read_json(&flag(&mut args, "--right")?)?;
            gateway::fetch::compare_fetch_effective(&left, &right)?;
            println!("{{\"comparable\":true}}");
        }
        "campaign" => {
            let out = PathBuf::from(flag(&mut args, "--out")?);
            let repetitions = std::env::var("TOTAL_REPETITIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5)
                .max(5);
            let paths = gateway::fetch::run_fetch_campaign(&out, repetitions)?;
            println!("{{\"results\":{}}}", paths.len());
        }
        "cells" => {
            for cell in FETCH_CELLS {
                println!("{}\t{}\t{}", cell.id, cell.codec, cell.ack_mode.label());
            }
        }
        _ => return Err(usage()),
    }
    Ok(())
}

fn usage() -> String {
    "usage: gateway-fetch emit-config --cell <id> | compare --left f --right f | campaign --out dir | cells".into()
}

fn flag(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    let got = args.next().ok_or_else(|| format!("missing {name}"))?;
    if got != name {
        return Err(format!("expected {name}, got {got}"));
    }
    args.next()
        .ok_or_else(|| format!("{name} requires a value"))
}

fn read_json(path: &str) -> Result<serde_json::Value, String> {
    serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}

fn tls_from_env() -> TlsMode {
    TlsMode::from_protocol(
        std::env::var("SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "PLAINTEXT".into())
            .as_str(),
        std::env::var("TLS_CLIENT_CERT_PEM").is_ok(),
    )
}
