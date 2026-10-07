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
        Self::write_observation(path, &self.observation(runtime))
    }

    fn write_observation(path: &Path, observation: &serde_json::Value) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        serde_json::to_writer_pretty(&mut file, observation)?;
        std::io::Write::write_all(&mut file, b"\n")?;
        file.sync_all()
    }

    /// Let already-cancelled client tasks release their runtime registration
    /// before another measured phase. This barrier is outside the measurement;
    /// it does not change or qualify the library's close contract (KL02-12).
    pub fn quiesce(self, runtime: &tokio::runtime::Runtime) -> Result<serde_json::Value, String> {
        let before = runtime.metrics().num_alive_tasks();
        let start = std::time::Instant::now();
        let deadline = start + std::time::Duration::from_secs(2);
        while runtime.metrics().num_alive_tasks() != 0 {
            if std::time::Instant::now() >= deadline {
                return Err("runtime client cancellation did not finish within two seconds".into());
            }
            runtime.block_on(async {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            });
        }
        let mut observed = self.observation(runtime);
        let row = observed
            .as_object_mut()
            .ok_or("invalid runtime observation")?;
        row.insert("post_close_tasks_before_barrier".into(), before.into());
        row.insert(
            "post_close_barrier_ns".into(),
            u64::try_from(start.elapsed().as_nanos())
                .map_err(|_| "barrier clock overflow")?
                .into(),
        );
        row.insert("post_close_barrier_outside_measurement".into(), true.into());
        Ok(observed)
    }

    /// Execute an isolated native benchmark workload on this runtime. Record
    /// startup and bounded post-workload cancellation separately from timing.
    pub fn run_native(
        self,
        workload: impl std::future::Future<Output = partitionline::Result<()>>,
    ) -> partitionline::Result<()> {
        let rt = self
            .build()
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        let observation = std::env::var("PL_BENCH_RUNTIME_OBSERVATION")
            .map_err(|_| partitionline::Error::protocol("PL_BENCH_RUNTIME_OBSERVATION required"))?;
        let path = Path::new(&observation);
        self.record(&rt, path)
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        let sampler =
            crate::measure::RssSampler::start(std::time::Duration::from_millis(10), 60_000);
        let thread_snapshot = std::fs::read_to_string("/proc/self/status")
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        let start_utc = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        let cpu_start = crate::measure::cpu_now();
        let start = std::time::Instant::now();
        let ((result, completion), allocations, allocated_bytes) = codec::census(|| {
            let result = rt.block_on(workload);
            (result, self.quiesce(&rt))
        });
        let wall_seconds = start.elapsed().as_secs_f64();
        let cpu = crate::measure::cpu_now().saturating_sub(cpu_start);
        let end_utc = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        let rss = sampler.stop();
        let host = crate::host::probe().map_err(partitionline::Error::protocol)?;
        let resources = serde_json::json!({
            "scope": "whole native workload including setup, warmup, timed work, output and close cancellation barrier; runtime construction excluded",
            "allocation_scope": "process-wide allocation census over all client threads in the same workload interval; diagnostic sampling/output included",
            "allocation_count": allocations,
            "total_allocated_bytes": allocated_bytes,
            "user_cpu_seconds": cpu.user_us as f64 / 1e6,
            "system_cpu_seconds": cpu.sys_us as f64 / 1e6,
            "wall_seconds": wall_seconds,
            "start_time_utc": start_utc,
            "end_time_utc": end_utc,
            "rss_sample_peak_bytes": rss.0,
            "rss_sample_mean_bytes": rss.1,
            "rss_scope": "10ms samples across workload interval; sampler created before census and joined after census",
            "process_peak_rss_bytes": crate::measure::peak_rss_bytes(),
            "thread_snapshot_before_workload": thread_snapshot,
            "host": {
                "hostname": host.hostname, "os": host.os, "os_family": host.os_family,
                "kernel_version": host.kernel_version, "arch": host.arch,
                "cpu": {"model": host.cpu_model, "physical_cores": host.physical_cores,
                        "logical_cores": host.logical_cores, "frequency_mhz": host.frequency_mhz},
                "memory": {"unit": "bytes", "total_bytes": host.memory_total_bytes}
            }
        });
        Self::write_observation(&path.with_extension("resources.json"), &resources)
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        let completion = completion.map_err(partitionline::Error::protocol)?;
        Self::write_observation(&path.with_extension("closure.json"), &completion)
            .map_err(|e| partitionline::Error::protocol(e.to_string()))?;
        result
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

    #[test]
    fn quiescence_rejects_a_live_task_and_accepts_completed_cancellation() {
        let config = RuntimeConfig::CurrentThread;
        let rt = config.build().unwrap();
        let task = rt.spawn(std::future::pending::<()>());
        let start = std::time::Instant::now();
        assert!(config.quiesce(&rt).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(3));
        task.abort();
        let observed = config.quiesce(&rt).unwrap();
        assert_eq!(observed["observed_alive_tasks"], 0);
        assert_eq!(observed["post_close_barrier_outside_measurement"], true);
    }
}
