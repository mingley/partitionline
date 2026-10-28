//! Process-level ordinary server startup, durable restart and shutdown.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn args(data: &Path, port: u16, cluster: &str) -> Vec<String> {
    vec![
        "serve".into(),
        "--data".into(),
        data.display().to_string(),
        "--listen".into(),
        format!("127.0.0.1:{port}"),
        "--advertise-host".into(),
        "127.0.0.1".into(),
        "--cluster".into(),
        cluster.into(),
        "--broker-id".into(),
        "0".into(),
    ]
}

fn start(data: &Path, port: u16) -> Process {
    let mut child = Command::new(env!("CARGO_BIN_EXE_operator"))
        .args(args(data, port, "operator-test"))
        .args(["--sync-concurrency", "2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let result = reader.read_line(&mut line).map(|_| line);
        let _ = tx.send(result);
        // Keep the pipe open while the process runs, then drain its final line.
        let mut rest = Vec::new();
        let _ = reader.read_to_end(&mut rest);
    });
    let guard = Process(child);
    let ready = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("server ready timeout")
        .unwrap();
    assert!(
        ready.starts_with(&format!("ready 127.0.0.1:{port} ")),
        "unexpected readiness: {ready:?}"
    );
    guard
}

#[allow(
    clippy::disallowed_methods,
    reason = "This synchronous process test owns no async executor"
)]
fn stop(mut process: Process) {
    let status = Command::new("kill")
        .args(["-TERM", &process.0.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "server shutdown failed: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "server shutdown timeout");
        thread::sleep(Duration::from_millis(20));
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "This synchronous process test owns no async executor"
)]
fn exchange(port: u16, payload: &[u8]) -> Vec<u8> {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
        .write_all(&(payload.len() as i32).to_be_bytes())
        .unwrap();
    socket.write_all(payload).unwrap();
    let mut length = [0; 4];
    socket.read_exact(&mut length).unwrap();
    let length = i32::from_be_bytes(length);
    assert!((0..=1024 * 1024).contains(&length));
    let mut response = vec![0; length as usize];
    socket.read_exact(&mut response).unwrap();
    response
}

fn create_alpha() -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&19i16.to_be_bytes()); // CreateTopics v4
    b.extend_from_slice(&4i16.to_be_bytes());
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&5i16.to_be_bytes());
    b.extend_from_slice(b"alpha");
    b.extend_from_slice(&2i32.to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes()); // explicit assignments
    b.extend_from_slice(&0i32.to_be_bytes()); // topic configs
    b.extend_from_slice(&60_000i32.to_be_bytes());
    b.push(0); // validate_only
    b
}

fn header(api: i16, version: i16) -> Vec<u8> {
    let mut request = Vec::new();
    request.extend_from_slice(&api.to_be_bytes());
    request.extend_from_slice(&version.to_be_bytes());
    request.extend_from_slice(&77i32.to_be_bytes());
    request.extend_from_slice(&(-1i16).to_be_bytes());
    request
}

fn assert_advertised_profile(port: u16) {
    let versions = exchange(port, &header(18, 0));
    assert_eq!(&versions[..4], &77i32.to_be_bytes());
    assert_eq!(&versions[4..6], &0i16.to_be_bytes());
    let count = i32::from_be_bytes(versions[6..10].try_into().unwrap());
    assert_eq!(count, 7);
    assert_eq!(versions.len(), 10 + count as usize * 6);
    let entries: Vec<_> = versions[10..]
        .as_chunks::<6>()
        .0
        .iter()
        .map(|bytes| {
            (
                i16::from_be_bytes(bytes[0..2].try_into().unwrap()),
                i16::from_be_bytes(bytes[2..4].try_into().unwrap()),
                i16::from_be_bytes(bytes[4..6].try_into().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        entries,
        vec![
            (0, 3, 13),
            (1, 4, 6),
            (2, 1, 3),
            (3, 0, 13),
            (18, 0, 4),
            (19, 2, 4),
            (20, 1, 6)
        ]
    );
    let mut metadata_request = header(3, 1);
    metadata_request.extend_from_slice(&(-1i32).to_be_bytes());
    let response = exchange(port, &metadata_request);
    assert_eq!(i32::from_be_bytes(response[4..8].try_into().unwrap()), 1);
    assert_eq!(i32::from_be_bytes(response[8..12].try_into().unwrap()), 0);
    let host_len = i16::from_be_bytes(response[12..14].try_into().unwrap()) as usize;
    assert_eq!(&response[14..14 + host_len], b"127.0.0.1");
    let end = 14 + host_len;
    assert_eq!(
        i32::from_be_bytes(response[end..end + 4].try_into().unwrap()),
        i32::from(port)
    );
}

#[test]
fn durable_rf1_server_restarts_and_rejects_shared_or_wrong_identity() {
    let name = format!(
        "partitionline-operator-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let data: PathBuf = std::env::temp_dir().join(name);
    let held = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = held.local_addr().unwrap().port();
    drop(held);
    let process = start(&data, port);
    assert_advertised_profile(port);
    for invalid in ["0", "65"] {
        let result = Command::new(env!("CARGO_BIN_EXE_operator"))
            .args(args(&data, port, "operator-test"))
            .args(["--sync-concurrency", invalid])
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("sync concurrency"));
    }
    let conflicting = Command::new(env!("CARGO_BIN_EXE_operator"))
        .args(args(&data, port, "operator-test"))
        .output()
        .unwrap();
    assert!(!conflicting.status.success());
    assert!(String::from_utf8_lossy(&conflicting.stderr).contains("locked"));

    let create = exchange(port, &create_alpha());
    assert_eq!(&create[..4], &77i32.to_be_bytes());
    let name_len = u16::from_be_bytes(create[12..14].try_into().unwrap()) as usize;
    assert_eq!(
        i16::from_be_bytes(create[14 + name_len..16 + name_len].try_into().unwrap()),
        0
    );
    let produce_request = include_bytes!("fixtures/produce/4.1.2/produce-v3-acks1.request.bin");
    let produce = exchange(port, produce_request);
    assert_eq!(i16::from_be_bytes(produce[23..25].try_into().unwrap()), 0);
    let fetch_request = include_bytes!("fixtures/fetch/4.1.2/fetch-v4-offset0.request.bin");
    let before = exchange(port, fetch_request);
    assert!(
        before.len() > 100,
        "fetch did not return the produced batch"
    );
    assert_eq!(i16::from_be_bytes(before[27..29].try_into().unwrap()), 0);
    assert_eq!(i64::from_be_bytes(before[29..37].try_into().unwrap()), 1);
    // The broker assigns the base offset and leader epoch outside the batch's
    // CRC-protected payload. Verify every byte from magic onward survives.
    let protected_batch = &produce_request[71..];
    assert!(before
        .windows(protected_batch.len())
        .any(|part| part == protected_batch));
    stop(process);

    let wrong = Command::new(env!("CARGO_BIN_EXE_operator"))
        .args(args(&data, port, "wrong-cluster"))
        .output()
        .unwrap();
    assert!(!wrong.status.success());
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("identity does not match"));
    let restarted = start(&data, port);
    assert_advertised_profile(port);
    assert_eq!(exchange(port, fetch_request), before);
    stop(restarted);
    fs::remove_dir_all(data).unwrap();
}
