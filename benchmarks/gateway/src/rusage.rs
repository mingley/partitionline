//! Process rusage used as the only CPU source for gateway cells.

use serde::{Deserialize, Serialize};

/// User/system CPU and peak RSS from `getrusage`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcessRusage {
    /// User CPU seconds.
    pub user_cpu_seconds: f64,
    /// System CPU seconds.
    pub system_cpu_seconds: f64,
    /// Peak RSS in bytes (Linux `ru_maxrss` is KiB).
    pub peak_rss_bytes: u64,
    /// Thread count from `/proc/self/status`, or 1.
    pub threads_count: u64,
}

impl ProcessRusage {
    /// `RUSAGE_SELF` snapshot.
    #[must_use]
    pub fn current() -> Self {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return Self {
                threads_count: 1,
                ..Self::default()
            };
        }
        let usage = unsafe { usage.assume_init() };
        Self {
            user_cpu_seconds: timeval(&usage.ru_utime),
            system_cpu_seconds: timeval(&usage.ru_stime),
            peak_rss_bytes: rss_bytes(usage.ru_maxrss),
            threads_count: thread_count().max(1),
        }
    }

    /// `wait4` for a child pid; does not also `wait` the `std::process::Child`.
    pub fn wait4(pid: u32) -> Result<(i32, Self), String> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let mut status = 0;
        let rc = unsafe { libc::wait4(pid as libc::pid_t, &mut status, 0, usage.as_mut_ptr()) };
        if rc < 0 {
            return Err(format!(
                "wait4({pid}) failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let usage = unsafe { usage.assume_init() };
        let code = libc::WEXITSTATUS(status);
        Ok((
            code,
            Self {
                user_cpu_seconds: timeval(&usage.ru_utime),
                system_cpu_seconds: timeval(&usage.ru_stime),
                peak_rss_bytes: rss_bytes(usage.ru_maxrss),
                threads_count: 1,
            },
        ))
    }

    /// CPU used since `earlier`. Peak RSS stays the later snapshot (not a delta).
    #[must_use]
    pub fn since(self, earlier: Self) -> Self {
        Self {
            user_cpu_seconds: (self.user_cpu_seconds - earlier.user_cpu_seconds).max(0.0),
            system_cpu_seconds: (self.system_cpu_seconds - earlier.system_cpu_seconds).max(0.0),
            peak_rss_bytes: self.peak_rss_bytes.max(earlier.peak_rss_bytes),
            threads_count: self.threads_count,
        }
    }

    /// CPU seconds per acknowledged record. Never uses enqueue counts.
    #[must_use]
    pub fn cpu_seconds_per_record(self, acknowledged: u64) -> Option<f64> {
        if acknowledged == 0 {
            return None;
        }
        Some((self.user_cpu_seconds + self.system_cpu_seconds) / acknowledged as f64)
    }
}

fn timeval(tv: &libc::timeval) -> f64 {
    tv.tv_sec as f64 + (tv.tv_usec as f64 / 1_000_000.0)
}

fn rss_bytes(maxrss: i64) -> u64 {
    u64::try_from(maxrss.max(0))
        .unwrap_or(0)
        .saturating_mul(1024)
}

fn thread_count() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 1;
    };
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("Threads:")
                .and_then(|s| s.trim().parse().ok())
        })
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::ProcessRusage;

    #[test]
    fn since_subtracts_cpu_and_keeps_peak_rss() {
        let earlier = ProcessRusage {
            user_cpu_seconds: 1.5,
            system_cpu_seconds: 0.5,
            peak_rss_bytes: 4_000,
            threads_count: 2,
        };
        let later = ProcessRusage {
            user_cpu_seconds: 2.0,
            system_cpu_seconds: 0.75,
            peak_rss_bytes: 8_000,
            threads_count: 5,
        };
        let delta = later.since(earlier);
        assert!((delta.user_cpu_seconds - 0.5).abs() < 1e-12);
        assert!((delta.system_cpu_seconds - 0.25).abs() < 1e-12);
        assert_eq!(delta.peak_rss_bytes, 8_000);
        assert_eq!(delta.threads_count, 5);
        assert!((delta.cpu_seconds_per_record(100).unwrap() - 0.0075).abs() < 1e-12);
    }
}
