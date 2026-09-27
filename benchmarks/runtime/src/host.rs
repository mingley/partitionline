//! Host provenance probe (KL09-09): real machine facts for result
//! artifacts, from libc and the filesystem on Linux and macOS.
//!
//! Every value is measured, never hardcoded. Approximate fallbacks
//! (documented below) apply only where the platform exposes no exact
//! source; fallbacks always err toward the observed value.

use std::ffi::CStr;

/// Machine facts for `provenance.host`.
#[derive(Debug, Clone)]
pub struct HostInfo {
    /// Short hostname.
    pub hostname: String,
    /// OS name and release, e.g. `Linux 6.8.0` / `Darwin 24.6.0`.
    pub os: String,
    /// `std::env::consts::OS`.
    pub os_family: String,
    /// Kernel release plus version string.
    pub kernel_version: String,
    /// `std::env::consts::ARCH`.
    pub arch: String,
    /// CPU model string.
    pub cpu_model: String,
    /// Physical cores (unique core ids where topology exists, else logical).
    pub physical_cores: u64,
    /// Logical cores.
    pub logical_cores: u64,
    /// Nominal MHz.
    pub frequency_mhz: f64,
    /// Total RAM bytes.
    pub memory_total_bytes: u64,
}

fn cstr_to_string(buf: &[libc::c_char]) -> String {
    let bytes: &[u8] = unsafe { std::mem::transmute(buf) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Collect host facts. Fails closed only when nothing at all can be
/// observed; individual fields fall back to documented approximations.
pub fn probe() -> Result<HostInfo, String> {
    let hostname = {
        let mut buf = [0 as libc::c_char; 256];
        if unsafe { libc::gethostname(buf.as_mut_ptr(), buf.len()) } != 0 {
            return Err("gethostname failed".to_owned());
        }
        cstr_to_string(&buf)
    };
    let (sysname, release, version) = unsafe {
        let mut u: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut u) != 0 {
            return Err("uname failed".to_owned());
        }
        (
            cstr_to_string(&u.sysname),
            cstr_to_string(&u.release),
            cstr_to_string(&u.version),
        )
    };
    let logical_cores = std::thread::available_parallelism().map_or(1, |n| n.get() as u64);
    #[cfg(target_os = "macos")]
    let (cpu_model, physical_cores, frequency_mhz, memory_total_bytes) = macos_cpu_mem()?;
    #[cfg(target_os = "linux")]
    let (cpu_model, physical_cores, frequency_mhz, memory_total_bytes) =
        linux_cpu_mem(logical_cores)?;
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let (cpu_model, physical_cores, frequency_mhz, memory_total_bytes) =
        ("unknown".to_owned(), logical_cores, 0.0, 0);
    Ok(HostInfo {
        hostname,
        os: format!("{sysname} {release}"),
        os_family: std::env::consts::OS.to_owned(),
        kernel_version: format!("{release} {version}"),
        arch: std::env::consts::ARCH.to_owned(),
        cpu_model,
        physical_cores,
        logical_cores,
        frequency_mhz,
        memory_total_bytes,
    })
}

#[cfg(target_os = "macos")]
fn sysctl_string(name: &CStr) -> Result<String, String> {
    let mut len: libc::size_t = 0;
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(format!("sysctl size for {name:?} failed"));
    }
    let mut buf = vec![0u8; len.max(1)];
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(format!("sysctl read for {name:?} failed"));
    }
    buf.truncate(len);
    while buf.last() == Some(&0) {
        buf.pop();
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

#[cfg(target_os = "macos")]
fn sysctl_u64(name: &CStr) -> Result<u64, String> {
    let mut out: u64 = 0;
    let mut len = std::mem::size_of::<u64>() as libc::size_t;
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&mut out as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(format!("sysctl read for {name:?} failed"));
    }
    // Narrow values (hw.physicalcpu is int32) still land correctly:
    // sysctl writes `len` bytes into `out`.
    Ok(out)
}

#[cfg(target_os = "macos")]
fn macos_cpu_mem() -> Result<(String, u64, f64, u64), String> {
    let model = sysctl_string(c"machdep.cpu.brand_string")
        .or_else(|_| sysctl_string(c"machdep.cpu.brand"))?;
    let physical = sysctl_u64(c"hw.physicalcpu")?;
    // Apple Silicon exposes no frequency sysctl; 0.0 marks the gap
    // (documented approximation: unknown, not measured).
    let freq_hz = sysctl_u64(c"hw.cpufrequency").unwrap_or(0);
    let mem = sysctl_u64(c"hw.memsize")?;
    Ok((model, physical, freq_hz as f64 / 1e6, mem))
}

#[cfg(target_os = "linux")]
fn linux_cpu_mem(logical: u64) -> Result<(String, u64, f64, u64), String> {
    let cpuinfo =
        std::fs::read_to_string("/proc/cpuinfo").map_err(|e| format!("read /proc/cpuinfo: {e}"))?;
    let mut model = String::from("unknown");
    let mut mhz = 0.0;
    let mut cores = std::collections::BTreeSet::new();
    let mut pkg: &str = "";
    let mut core: &str = "";
    for line in cpuinfo.lines() {
        if let Some(v) = line.strip_prefix("model name") {
            if let Some(v) = v.split(':').nth(1) {
                model = v.trim().to_owned();
            }
        } else if let Some(v) = line.strip_prefix("cpu MHz") {
            if let Some(v) = v.split(':').nth(1) {
                if let Ok(f) = v.trim().parse::<f64>() {
                    if f > 0.0 {
                        mhz = f;
                    }
                }
            }
        } else if let Some(v) = line.strip_prefix("physical id") {
            pkg = v.split(':').nth(1).map_or("", str::trim);
        } else if let Some(v) = line.strip_prefix("core id") {
            core = v.split(':').nth(1).map_or("", str::trim);
        } else if line.is_empty() {
            if !pkg.is_empty() || !core.is_empty() {
                cores.insert((pkg.to_owned(), core.to_owned()));
            }
            pkg = "";
            core = "";
        }
    }
    if !pkg.is_empty() || !core.is_empty() {
        cores.insert((pkg.to_owned(), core.to_owned()));
    }
    // VMs often lack topology: fall back to the logical count rather
    // than inventing a smaller number.
    let physical = if cores.is_empty() {
        logical
    } else {
        cores.len() as u64
    };
    let meminfo =
        std::fs::read_to_string("/proc/meminfo").map_err(|e| format!("read /proc/meminfo: {e}"))?;
    let mut total_kb = 0u64;
    for line in meminfo.lines() {
        if let Some(v) = line.strip_prefix("MemTotal:") {
            let num: String = v.chars().filter(|c| c.is_ascii_digit()).collect();
            total_kb = num.parse().unwrap_or(0);
            break;
        }
    }
    if total_kb == 0 {
        return Err("MemTotal missing from /proc/meminfo".to_owned());
    }
    Ok((model, physical, mhz, total_kb * 1024))
}
