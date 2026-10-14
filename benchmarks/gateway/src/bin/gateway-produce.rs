//! Drive or register gateway produce cells without linking librdkafka.

use std::path::PathBuf;

use gateway::cells::{find, AckMode, Load, CELLS};
use gateway::settings::TlsMode;

fn main() {
    if let Err(error) = run() {
        eprintln!("gateway-produce: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(usage)?;
    match command.as_str() {
        "emit-config" => {
            let cell = find(&flag(&mut args, "--cell")?)?;
            let tls = tls_from_env();
            println!(
                "{}",
                serde_json::to_string_pretty(&gateway::partitionline_emit_config(cell, tls))
                    .map_err(|e| e.to_string())?
            );
        }
        "compare" => {
            let left = read_json(&flag(&mut args, "--left")?)?;
            let right = read_json(&flag(&mut args, "--right")?)?;
            let ack = flag(&mut args, "--ack-mode").ok();
            let mode = match ack.as_deref() {
                Some("per-record") => AckMode::PerRecordCompletion,
                _ => AckMode::FireAndForgetTimedFlush,
            };
            gateway::compare_effective(&left, &right, mode, mode)?;
            println!(
                "{{\"comparable\":true,\"acknowledgment_mode\":{}}}",
                serde_json::to_string(mode.label()).map_err(|e| e.to_string())?
            );
        }
        "campaign" => {
            let out = PathBuf::from(flag(&mut args, "--out")?);
            let repetitions = std::env::var("TOTAL_REPETITIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5)
                .max(5);
            let paths = gateway::run_campaign(&out, repetitions)?;
            println!("{{\"results\":{}}}", paths.len());
        }
        "cells" => {
            for cell in CELLS {
                println!(
                    "{}\t{}\t{}\t{}",
                    cell.id,
                    cell.codec,
                    cell.ack_mode.label(),
                    Load::OpenLoop25k.id()
                );
            }
        }
        _ => return Err(usage()),
    }
    Ok(())
}

fn usage() -> String {
    "usage: gateway-produce emit-config --cell <id> | compare --left f --right f | campaign --out dir | cells".into()
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
