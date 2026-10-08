//! Client-side resource measurement (KL09-09): CPU time, RSS and the
//! census allocation counter, all from manifest-declared tooling
//! (`libc`, `codec::census`) inside this harness crate. The core crate
//! is untouched.
//!
//! The measured phase runs on an explicitly selected Tokio runtime with the
//! broker in a separate process, so `RUSAGE_SELF` CPU and the
//! process-wide census count only client work (plus the RSS sampler,
//! which is allocation-free by construction: fixed stack buffers, a
//! pre-sized sample ring, no heap traffic while sampling).

#[cfg(target_os = "linux")]
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Process CPU time in microseconds (user + system).
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuSample {
    /// User CPU microseconds.
    pub user_us: u64,
    /// System CPU microseconds.
    pub sys_us: u64,
}

impl CpuSample {
    /// Total CPU microseconds.
    #[must_use]
    pub fn total_us(self) -> u64 {
        self.user_us.saturating_add(self.sys_us)
    }

    /// Saturating `self - earlier`, per component.
    #[must_use]
    pub fn saturating_sub(self, earlier: CpuSample) -> CpuSample {
        CpuSample {
            user_us: self.user_us.saturating_sub(earlier.user_us),
            sys_us: self.sys_us.saturating_sub(earlier.sys_us),
        }
    }
}

fn timeval_to_us(tv: libc::timeval) -> u64 {
    (tv.tv_sec.max(0) as u64)
        .saturating_mul(1_000_000)
        .saturating_add((tv.tv_usec.max(0) as u64).min(999_999))
}

/// Current process CPU time via `getrusage(RUSAGE_SELF)`.
#[must_use]
pub fn cpu_now() -> CpuSample {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // getrusage with a valid pointer cannot fail on Linux/macOS; a
    // zero sample on error keeps the artifact honest (0 CPU observed).
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return CpuSample::default();
    }
    CpuSample {
        user_us: timeval_to_us(ru.ru_utime),
        sys_us: timeval_to_us(ru.ru_stime),
    }
}

/// Process peak RSS in bytes via `ru_maxrss`.
///
/// Linux reports kilobytes, macOS bytes; other platforms report 0
/// (documented gap, never a fabricated number).
#[must_use]
pub fn peak_rss_bytes() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return 0;
    }
    let maxrss = ru.ru_maxrss.max(0) as u64;
    #[cfg(target_os = "linux")]
    {
        maxrss.saturating_mul(1024)
    }
    #[cfg(target_os = "macos")]
    {
        maxrss
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = maxrss;
        0
    }
}

/// Current RSS in bytes. Allocation-free on Linux (raw `read` into a
/// stack buffer) and macOS (`task_info` into a stack struct) so the
/// sampler does not pollute the allocation census.
#[must_use]
pub fn rss_now() -> u64 {
    #[cfg(target_os = "linux")]
    {
        linux_rss_now()
    }
    #[cfg(target_os = "macos")]
    {
        macos_rss_now()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

#[cfg(target_os = "linux")]
fn linux_rss_now() -> u64 {
    use std::os::unix::io::FromRawFd;
    // Second field of /proc/self/statm is resident pages.
    let fd = unsafe { libc::open(c"/proc/self/statm".as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return 0;
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    loop {
        if len >= buf.len() {
            break;
        }
        match file.read(&mut buf[len..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => len += n,
        }
    }
    // Skip the first field, parse the second as u64, no allocation.
    let mut field = 0;
    let mut value: u64 = 0;
    let mut in_number = false;
    for &b in &buf[..len] {
        if b.is_ascii_whitespace() {
            if in_number {
                field += 1;
                if field == 2 {
                    break;
                }
                in_number = false;
            }
        } else if field == 1 && b.is_ascii_digit() {
            in_number = true;
            value = value.saturating_mul(10).saturating_add(u64::from(b - b'0'));
        } else if field == 0 {
            in_number = true;
        } else {
            break;
        }
    }
    if field == 0 && !in_number {
        return 0;
    }
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(0) as u64;
    value.saturating_mul(page)
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
// `mach_task_self_` is deprecated in favor of the `mach2` crate; a
// whole new dependency for one port constant is not worth it.
fn macos_rss_now() -> u64 {
    let mut info: libc::mach_task_basic_info = unsafe { std::mem::zeroed() };
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    let rc = unsafe {
        libc::task_info(
            libc::mach_task_self_,
            libc::MACH_TASK_BASIC_INFO,
            (&mut info as *mut libc::mach_task_basic_info).cast(),
            &mut count,
        )
    };
    if rc != libc::KERN_SUCCESS {
        return 0;
    }
    info.resident_size
}

/// Periodic RSS sampler. Pre-allocates its sample buffer before the
/// measured phase; `stop` returns `(peak_bytes, mean_bytes)`.
pub struct RssSampler {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Vec<u64>>>,
}

impl RssSampler {
    /// Start sampling every `interval`. `cap` bounds the buffer.
    pub fn start(interval: Duration, cap: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        // Allocate on the caller before the measured interval begins.
        let mut samples = Vec::with_capacity(cap);
        let handle = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                if samples.len() < cap {
                    samples.push(rss_now());
                }
                std::thread::sleep(interval);
            }
            samples
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop sampling and summarize as `(sample_peak, sample_mean)`.
    /// Deliberately NOT mixed with `ru_maxrss`: that counter is
    /// monotonic per process, so in a multi-cell run it would
    /// attribute an earlier cell's peak to every later cell. Callers
    /// report `ru_maxrss` separately as the process-wide reference.
    /// With no samples, both fall back to an instantaneous read.
    pub fn stop(mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        let samples = self
            .handle
            .take()
            .map(|h| h.join().unwrap_or_default())
            .unwrap_or_default();
        if samples.is_empty() {
            let now = rss_now();
            return (now, now);
        }
        let peak = samples.iter().copied().max().unwrap_or(0);
        let sum: u128 = samples.iter().map(|&s| u128::from(s)).sum();
        let mean = (sum / samples.len() as u128).min(u128::from(u64::MAX)) as u64;
        (peak, mean)
    }
}
