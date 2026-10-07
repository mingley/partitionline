//! Explicit Tokio configurations for benchmark execution.

use std::path::Path;

/// Benchmark runtime flavor and explicit multi-thread worker count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeConfig {
    /// Run tasks on the calling thread; no background runtime workers.
    CurrentThread,
    /// Run tasks on between one and 64 background workers.
    MultiThread(usize),
}

impl RuntimeConfig {
    /// Validate an explicit flavor. Current-thread execution accepts no workers
    /// (or zero); multi-thread execution requires between one and 64.
    pub fn parse(flavor: &str, workers: Option<usize>) -> Result<Self, String> {
        match (flavor, workers) {
            ("current_thread", None | Some(0)) => Ok(Self::CurrentThread),
            ("multi_thread", Some(n @ 1..=64)) => Ok(Self::MultiThread(n)),
            _ => {
                Err("current_thread requires workers 0; multi_thread requires workers 1..64".into())
            }
        }
    }

    /// Read the isolated native-driver benchmark settings. These variables
    /// affect only this harness, never the client library or bench examples.
    pub fn from_env() -> Result<Self, String> {
        let flavor =
            std::env::var("PL_BENCH_RUNTIME_FLAVOR").unwrap_or_else(|_| "current_thread".into());
        let workers = std::env::var("PL_BENCH_RUNTIME_WORKERS")
            .ok()
            .map(|s| {
                s.parse()
                    .map_err(|_| "invalid PL_BENCH_RUNTIME_WORKERS".to_owned())
            })
            .transpose()?;
        Self::parse(&flavor, workers)
    }

    /// Build the selected runtime with I/O and time enabled.
    pub fn build(self) -> Result<tokio::runtime::Runtime, std::io::Error> {
        match self {
            Self::CurrentThread => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
            Self::MultiThread(n) if (1..=64).contains(&n) => {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(n)
                    .enable_all()
                    .build()
            }
            Self::MultiThread(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "runtime workers must be 1..64",
            )),
        }
    }

    /// Observe the built runtime, including Tokio's scheduler worker count.
    /// Tokio reports one scheduler worker for a current-thread runtime, which
    /// still has zero background worker threads.
    pub fn observation(self, runtime: &tokio::runtime::Runtime) -> serde_json::Value {
        let (requested_flavor, background_workers) = match self {
            Self::CurrentThread => ("current_thread", 0),
            Self::MultiThread(n) => ("multi_thread", n),
        };
        let observed_flavor = match runtime.handle().runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => "current_thread",
            tokio::runtime::RuntimeFlavor::MultiThread => "multi_thread",
            _ => "unknown",
        };
        serde_json::json!({
            "requested_flavor": requested_flavor,
            "requested_background_workers": background_workers,
            "observed_flavor": observed_flavor,
            "observed_scheduler_workers": runtime.metrics().num_workers(),
            "observed_alive_tasks": runtime.metrics().num_alive_tasks(),
        })
    }

    /// Write the observed runtime configuration to a new bounded sidecar.
    pub fn record(self, runtime: &tokio::runtime::Runtime, path: &Path) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        serde_json::to_writer_pretty(&mut file, &self.observation(runtime))?;
        std::io::Write::write_all(&mut file, b"\n")?;
        file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_workers_and_runtime_observations_match() {
        for workers in [1, 2, 4, 5] {
            let config = RuntimeConfig::parse("multi_thread", Some(workers)).unwrap();
            let runtime = config.build().unwrap();
            let observed = config.observation(&runtime);
            assert_eq!(observed["observed_flavor"], "multi_thread");
            assert_eq!(observed["observed_scheduler_workers"], workers);
            assert_eq!(runtime.block_on(async { 1 + 2 }), 3);
        }
        let config = RuntimeConfig::parse("current_thread", None).unwrap();
        let runtime = config.build().unwrap();
        let observed = config.observation(&runtime);
        assert_eq!(observed["observed_flavor"], "current_thread");
        assert_eq!(observed["requested_background_workers"], 0);
        assert_eq!(observed["observed_scheduler_workers"], 1);
    }

    #[test]
    fn inconsistent_unbounded_and_missing_workers_are_rejected() {
        for (flavor, workers) in [
            ("current_thread", Some(1)),
            ("multi_thread", None),
            ("multi_thread", Some(0)),
            ("multi_thread", Some(65)),
            ("multi_thread", Some(usize::MAX)),
            ("other", Some(1)),
        ] {
            assert!(RuntimeConfig::parse(flavor, workers).is_err());
        }
        assert!(RuntimeConfig::MultiThread(0).build().is_err());
        assert!(RuntimeConfig::MultiThread(65).build().is_err());
    }
}
