//! Local operator tool for the experimental broker admin journal.
//!
//! It does not open a listen socket and it does not deploy a cluster.

use partitionline_broker::admin::{self, Admin, Assignment};
use partitionline_broker::journal;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

fn main() -> ExitCode {
    match run(env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("operator: {error}");
            ExitCode::from(1)
        }
    }
}

fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let args: Vec<String> = args.collect();
    let command = args.first().map(String::as_str).unwrap_or("");
    match command {
        "init" => init(&args),
        "register-dir" => register_dir(&args),
        "assign" => assign(&args),
        "feature" => feature(&args),
        "status" => status(&args),
        "backup" => backup(&args),
        "restore" => restore(&args),
        _ => {
            Err("usage: operator init|register-dir|assign|feature|status|backup|restore".to_owned())
        }
    }
}

fn limits() -> Result<admin::Limits, String> {
    let journal =
        journal::Limits::new(8192, 1 << 20, 64, 1 << 20).map_err(|error| format!("{error:?}"))?;
    admin::Limits::new(16, 8, 8, journal).map_err(|error| format!("{error:?}"))
}

fn journal_path(data: &Path) -> PathBuf {
    data.join("admin.journal")
}

fn flag<'a>(args: &'a [String], name: &str) -> Result<&'a str, String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
        .ok_or_else(|| format!("missing {name}"))
}

fn open(data: &Path) -> Result<Admin, String> {
    fs::create_dir_all(data).map_err(|error| error.to_string())?;
    Admin::open(journal_path(data), limits()?, "cluster-1", 1).map_err(|error| format!("{error:?}"))
}

fn init(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(flag(args, "--data")?);
    if flag(args, "--cluster")? != "cluster-1" || flag(args, "--voter")? != "1" {
        return Err("this package example uses cluster-1 and voter 1".to_owned());
    }
    let admin = open(&data)?;
    let (cluster, epoch) = admin
        .describe_cluster(0)
        .map_err(|error| format!("{error:?}"))?;
    println!("cluster {cluster} epoch {epoch}");
    Ok(())
}

fn register_dir(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(flag(args, "--data")?);
    let directory = flag(args, "--log-dir")?;
    let mut admin = open(&data)?;
    admin
        .register_log_dir(0, 0, directory)
        .map_err(|error| format!("{error:?}"))?;
    println!("log_dir {directory}");
    Ok(())
}

fn assign(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(flag(args, "--data")?);
    let topic = flag(args, "--topic")?;
    let partition: i32 = flag(args, "--partition")?
        .parse()
        .map_err(|_| "bad partition")?;
    let replicas = parse_replicas(flag(args, "--replicas")?)?;
    let leader: i32 = flag(args, "--leader")?.parse().map_err(|_| "bad leader")?;
    let log_dir = flag(args, "--log-dir")?;
    let mut admin = open(&data)?;
    admin
        .alter_assignment(
            0,
            0,
            Assignment {
                topic,
                partition,
                replicas: &replicas,
                leader,
                log_dir,
            },
        )
        .map_err(|error| format!("{error:?}"))?;
    println!("assigned {topic} {partition}");
    Ok(())
}

fn feature(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(flag(args, "--data")?);
    let name = flag(args, "--name")?;
    let level: i16 = flag(args, "--level")?.parse().map_err(|_| "bad level")?;
    let version: i16 = flag(args, "--version")
        .unwrap_or("0")
        .parse()
        .map_err(|_| "bad version")?;
    let mut admin = open(&data)?;
    admin
        .update_feature(version, 0, name, level)
        .map_err(|error| format!("{error:?}"))?;
    println!("feature {name} {level}");
    Ok(())
}

fn status(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(flag(args, "--data")?);
    let admin = open(&data)?;
    let (cluster, epoch) = admin
        .describe_cluster(0)
        .map_err(|error| format!("{error:?}"))?;
    println!("cluster {cluster} epoch {epoch}");
    if let Ok(level) = admin.feature_level(0, "metadata.version") {
        println!("feature metadata.version {level}");
    }
    if let Ok(directory) = admin.describe_log_dir(0, "orders", 0) {
        println!("log_dir orders 0 {directory}");
    }
    let (voters, _) = admin
        .describe_quorum(0)
        .map_err(|error| format!("{error:?}"))?;
    println!(
        "voters {}",
        voters
            .iter()
            .map(|voter| voter.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    Ok(())
}

fn backup(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(flag(args, "--data")?);
    let to = PathBuf::from(flag(args, "--to")?);
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::copy(journal_path(&data), &to).map_err(|error| error.to_string())?;
    println!("backup {}", to.display());
    Ok(())
}

fn restore(args: &[String]) -> Result<(), String> {
    let from = PathBuf::from(flag(args, "--from")?);
    let data = PathBuf::from(flag(args, "--data")?);
    fs::create_dir_all(&data).map_err(|error| error.to_string())?;
    fs::copy(&from, journal_path(&data)).map_err(|error| error.to_string())?;
    println!("restored {}", data.display());
    Ok(())
}

fn parse_replicas(text: &str) -> Result<Vec<i32>, String> {
    let mut replicas = Vec::new();
    for part in text.split(',') {
        if part.is_empty() {
            return Err("empty replica".to_owned());
        }
        replicas.push(part.parse().map_err(|_| "bad replica")?);
    }
    Ok(replicas)
}
