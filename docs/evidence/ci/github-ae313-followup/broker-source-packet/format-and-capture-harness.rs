#![forbid(unsafe_code)]
use std::{fs::{self,File},io::Write,path::PathBuf,sync::{atomic::{AtomicBool,AtomicU64,Ordering},Arc,Barrier},thread};
type TestResult<T=()> = Result<T,Box<dyn std::error::Error+Send+Sync>>;
struct Gate { capture:Option<PathBuf>,captured_bytes:AtomicU64,capture_failed:AtomicBool }
impl Gate {
    fn capture_file(&self, name: &str, bytes: &[u8]) -> TestResult {
        let result = (|| -> TestResult {
            let Some(root) = &self.capture else {
                return Ok(());
            };
            let size = u64::try_from(bytes.len())?;
            // Preserve fetch_update's capped, checked-add reservation on MSRV.
            // fetch_max would not charge cumulative bytes or enforce the cap.
            let mut current = self.captured_bytes.load(Ordering::Acquire);
            loop {
                let next = current
                    .checked_add(size)
                    .filter(|next| *next <= 16 * 1024 * 1024)
                    .ok_or("finite per-case proxy capture bytes")?;
                match self.captured_bytes.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join(name))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.capture_failed.store(true, Ordering::Release);
        }
        result
    }
}
struct ForwardReceipt {
    source: usize,
    target: usize,
    connection: u64,
    reply: bool,
    ordinal: u64,
    received_ms: u128,
    delay_ms: u64,
    forward_started_ms: Option<u128>,
    forward_finished_ms: Option<u128>,
    write_ok: Option<bool>,
    disposition: &'static str,
}
impl Gate {
    fn forward_receipt(&self, name: &str, bytes: &[u8], r: ForwardReceipt) -> TestResult {
        let rpc = u64::from_be_bytes(bytes[16..24].try_into()?);
        let time = |v: Option<u128>| v.map_or_else(|| "null".into(), |n| n.to_string());
        let ok = r.write_ok.map_or_else(|| "null".into(), |b| b.to_string());
        let metadata = format!("{{\"schema_version\":2,\"packet_file\":\"{name}\",\"proxy_pid\":{},\"clock_basis\":\"parent proxy process monotonic Instant epoch; not owner process clock\",\"source\":{},\"target\":{},\"connection\":{},\"reply\":{},\"ordinal\":{},\"kind\":{},\"rpc\":{rpc},\"received_ms\":{},\"selected_delay_ms\":{},\"forward_started_ms\":{},\"forward_finished_ms\":{},\"forward_write_ok\":{ok},\"disposition\":\"{}\"}}\n", std::process::id(), r.source, r.target, r.connection, r.reply, r.ordinal, bytes[14], r.received_ms, r.delay_ms, time(r.forward_started_ms), time(r.forward_finished_ms), r.disposition);
        self.capture_file(&format!("{name}.forward.json"), metadata.as_bytes())
    }
}

fn gate(path:PathBuf)->TestResult<Arc<Gate>> { fs::create_dir(&path)?;Ok(Arc::new(Gate{capture:Some(path),captured_bytes:AtomicU64::new(0),capture_failed:AtomicBool::new(false)})) }
fn main()->TestResult {
    let out=PathBuf::from(std::env::args_os().nth(1).ok_or("output directory")?);
    fs::create_dir(&out)?;
    let json=gate(out.join("json"))?;
    let mut packet=[0u8;28];packet[14]=20;packet[16..24].copy_from_slice(&42u64.to_be_bytes());
    json.forward_receipt("known.packet",&packet,ForwardReceipt{source:1,target:2,connection:3,reply:true,ordinal:4,received_ms:5,delay_ms:6,forward_started_ms:Some(7),forward_finished_ms:Some(8),write_ok:Some(true),disposition:"forwarded"})?;
    let bounds=gate(out.join("bounds"))?;
    bounds.captured_bytes.store(16*1024*1024-1,Ordering::Release);
    bounds.capture_file("exact",&[1])?;
    assert_eq!(bounds.captured_bytes.load(Ordering::Acquire),16*1024*1024);
    assert!(bounds.capture_file("over-limit",&[2]).is_err());
    assert!(!out.join("bounds/over-limit").exists());
    bounds.captured_bytes.store(u64::MAX-2,Ordering::Release);
    assert!(bounds.capture_file("overflow",&[1,2,3]).is_err());
    assert_eq!(bounds.captured_bytes.load(Ordering::Acquire),u64::MAX-2);
    assert!(!out.join("bounds/overflow").exists());
    let concurrent=gate(out.join("concurrent"))?;
    let start=Arc::new(Barrier::new(8));let success=Arc::new(AtomicU64::new(0));let mut tasks=Vec::new();
    for worker in 0..8 {
        let g=Arc::clone(&concurrent);let start=Arc::clone(&start);let success=Arc::clone(&success);
        tasks.push(thread::spawn(move || ->TestResult {
            let payload=vec![worker as u8;1024*1024];start.wait();
            for ordinal in 0..20 {
                let result=g.capture_file(&format!("worker-{worker}-{ordinal}"),&payload);
                if result.is_ok(){success.fetch_add(1,Ordering::AcqRel);}
                assert!(g.captured_bytes.load(Ordering::Acquire)<=16*1024*1024);
            }
            Ok(())
        }));
    }
    for task in tasks { task.join().map_err(|_|"thread panicked")??; }
    assert_eq!(success.load(Ordering::Acquire),16);
    assert_eq!(concurrent.captured_bytes.load(Ordering::Acquire),16*1024*1024);
    assert_eq!(fs::read_dir(out.join("concurrent"))?.count(),16);
    assert!(concurrent.capture_failed.load(Ordering::Acquire));
    let summary=format!("{{\"scope\":\"standalone std source harness; not broker/TCP qualification\",\"concurrent_attempts\":160,\"accepted\":16,\"reserved_bytes\":{},\"boundary_checks\":3,\"threads_joined\":8}}\n",concurrent.captured_bytes.load(Ordering::Acquire));
    let mut file=File::create(out.join("summary.json"))?;file.write_all(summary.as_bytes())?;file.sync_all()?;
    Ok(())
}
