//! Real processes and Kafka-framed sockets for the metadata-only quorum profile.
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
fn start(root: &Path, id: usize, members: &str) -> Process {
    let mut child = Command::new(env!("CARGO_BIN_EXE_operator"))
        .args([
            "serve-quorum",
            "--data",
            root.to_str().unwrap(),
            "--cluster",
            "quorum-process-test",
            "--id",
            &id.to_string(),
            "--members",
            members,
        ])
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
        let mut rest = Vec::new();
        let _ = reader.read_to_end(&mut rest);
    });
    let guard = Process(child);
    let ready = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("quorum startup timeout")
        .unwrap();
    assert!(
        ready.contains("profile=metadata-only-quorum"),
        "unexpected startup: {ready:?}"
    );
    guard
}
#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn stop(mut process: Process) {
    assert!(Command::new("kill")
        .args(["-TERM", &process.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "shutdown: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "shutdown timeout");
        thread::sleep(Duration::from_millis(20));
    }
}
#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn exchange(port: u16, payload: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut socket = TcpStream::connect(("127.0.0.1", port))?;
    socket.set_read_timeout(Some(Duration::from_secs(6)))?;
    socket.set_write_timeout(Some(Duration::from_secs(6)))?;
    socket.write_all(&(payload.len() as i32).to_be_bytes())?;
    socket.write_all(payload)?;
    let mut length = [0; 4];
    socket.read_exact(&mut length)?;
    let length = i32::from_be_bytes(length);
    if !(0..=1024 * 1024).contains(&length) {
        return Err(std::io::Error::other("response length"));
    }
    let mut body = vec![0; length as usize];
    socket.read_exact(&mut body)?;
    Ok(body)
}
fn header(api: i16, version: i16) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&api.to_be_bytes());
    b.extend_from_slice(&version.to_be_bytes());
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b
}
fn string(b: &mut Vec<u8>, name: &str) {
    b.extend_from_slice(&(name.len() as i16).to_be_bytes());
    b.extend_from_slice(name.as_bytes());
}
fn create(name: &str) -> Vec<u8> {
    let mut b = header(19, 3);
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, name);
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&3000i32.to_be_bytes());
    b.push(0);
    b
}
fn delete(name: &str) -> Vec<u8> {
    let mut b = header(20, 1);
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, name);
    b.extend_from_slice(&3000i32.to_be_bytes());
    b
}

fn metadata(name: &str) -> Vec<u8> {
    let mut b = header(3, 1);
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, name);
    b
}
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let end = self.pos.checked_add(n).unwrap();
        let value = self.bytes.get(self.pos..end).unwrap();
        self.pos = end;
        value
    }
    fn i16(&mut self) -> i16 {
        i16::from_be_bytes(self.take(2).try_into().unwrap())
    }
    fn i32(&mut self) -> i32 {
        i32::from_be_bytes(self.take(4).try_into().unwrap())
    }
    fn string(&mut self) -> Option<&'a str> {
        let n = self.i16();
        if n == -1 {
            None
        } else {
            Some(std::str::from_utf8(self.take(n as usize)).unwrap())
        }
    }
}
fn assert_api_versions(port: u16) {
    let bytes = exchange(port, &header(18, 0)).unwrap();
    let mut r = Cursor {
        bytes: &bytes,
        pos: 0,
    };
    assert_eq!(r.i32(), 77);
    assert_eq!(r.i16(), 0);
    assert_eq!(r.i32(), 4);
    for (key, min, max) in [(3, 0, 13), (18, 0, 4), (19, 2, 4), (20, 1, 6)] {
        assert_eq!((r.i16(), r.i16(), r.i16()), (key, min, max));
    }
    assert_eq!(r.pos, bytes.len());
}

fn create_error(bytes: &[u8], name: &str) -> i16 {
    let mut r = Cursor { bytes, pos: 0 };
    assert_eq!(r.i32(), 77);
    r.i32();
    assert_eq!(r.i32(), 1);
    assert_eq!(r.string(), Some(name));
    r.i16()
}
fn delete_error(bytes: &[u8], name: &str) -> i16 {
    let mut r = Cursor { bytes, pos: 0 };
    assert_eq!(r.i32(), 77);
    r.i32();
    assert_eq!(r.i32(), 1);
    assert_eq!(r.string(), Some(name));
    r.i16()
}

fn read_metadata(bytes: &[u8], ports: &[u16], name: &str) -> (i32, i16) {
    let mut r = Cursor { bytes, pos: 0 };
    assert_eq!(r.i32(), 77);
    assert_eq!(r.i32(), 3);
    for (id, port) in ports.iter().enumerate() {
        assert_eq!(r.i32(), id as i32);
        assert_eq!(r.string(), Some("127.0.0.1"));
        assert_eq!(r.i32(), i32::from(*port));
        assert_eq!(r.string(), None);
    }
    let controller = r.i32();
    assert!((-1..=2).contains(&controller));
    assert_eq!(r.i32(), 1);
    let error = r.i16();
    assert_eq!(r.string(), Some(name));
    assert_eq!(r.take(1), &[0]);
    let partitions = r.i32();
    if error == 0 {
        assert_eq!(partitions, 1);
        assert_eq!(r.i16(), 5);
        assert_eq!(r.i32(), 0);
        assert_eq!(r.i32(), -1);
        assert_eq!(r.i32(), 0);
        assert_eq!(r.i32(), 0);
    } else {
        assert_eq!(partitions, 0);
    }
    assert_eq!(r.pos, bytes.len());
    (controller, error)
}
#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn leader(ports: &[u16], alive: &[bool], wanted: Option<usize>) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        for (id, port) in ports.iter().enumerate() {
            if !alive[id] {
                continue;
            }
            if let Ok(bytes) = exchange(*port, &metadata("probe")) {
                let (controller, _) = read_metadata(&bytes, ports, "probe");
                if controller >= 0
                    && alive[controller as usize]
                    && wanted != Some(controller as usize)
                {
                    return controller as usize;
                }
            }
        }
        assert!(Instant::now() < deadline, "leader election timeout");
        thread::sleep(Duration::from_millis(50));
    }
}
#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn create_eventually(ports: &[u16], alive: &[bool], name: &str, old: Option<usize>) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let current = leader(ports, alive, old);
        match exchange(ports[current], &create(name)) {
            Ok(bytes) => match create_error(&bytes, name) {
                0 => return current,
                41 => {} // Leader discovery can precede its active term barrier.
                error => panic!("ambiguous or unexpected create error {error} for {name}"),
            },
            Err(error) => panic!("ambiguous create transport error for {name}: {error}"),
        }
        assert!(Instant::now() < deadline, "create deadline");
        thread::sleep(Duration::from_millis(50));
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn delete_eventually(ports: &[u16], alive: &[bool], name: &str) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let current = leader(ports, alive, None);
        match exchange(ports[current], &delete(name)) {
            Ok(bytes) => match delete_error(&bytes, name) {
                0 => return current,
                41 => {}
                error => panic!("ambiguous or unexpected delete error {error} for {name}"),
            },
            Err(error) => panic!("ambiguous delete transport error for {name}: {error}"),
        }
        assert!(Instant::now() < deadline, "delete deadline");
        thread::sleep(Duration::from_millis(50));
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn wait_present(ports: &[u16], alive: &[bool], name: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    for (id, port) in ports.iter().enumerate() {
        if !alive[id] {
            continue;
        }
        loop {
            if let Ok(bytes) = exchange(*port, &metadata(name)) {
                if read_metadata(&bytes, ports, name).1 == 0 {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "topic not applied on {id}");
            thread::sleep(Duration::from_millis(50));
        }
    }
}
#[expect(
    clippy::disallowed_methods,
    reason = "Synchronous process test runs outside a Tokio executor."
)]
fn wait_absent(ports: &[u16], alive: &[bool], name: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    for (id, port) in ports.iter().enumerate() {
        if !alive[id] {
            continue;
        }
        loop {
            if let Ok(bytes) = exchange(*port, &metadata(name)) {
                if read_metadata(&bytes, ports, name).1 == 3 {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "topic still visible on {id}");
            thread::sleep(Duration::from_millis(50));
        }
    }
}

#[test]
fn three_process_metadata_quorum_fails_over_and_replays() {
    let root = std::env::temp_dir().join(format!(
        "partitionline-quorum-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    let reservations: Vec<_> = (0..6)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let all: Vec<_> = reservations
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    drop(reservations);
    let clients = [all[1], all[3], all[5]];
    let members = (0..3)
        .map(|id| {
            format!(
                "{id}@{:032x}@127.0.0.1:{}@127.0.0.1:{}",
                id + 1,
                all[id * 2],
                clients[id]
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let paths: Vec<PathBuf> = (0..3).map(|id| root.join(id.to_string())).collect();
    let mut nodes: Vec<Option<Process>> = (0..3)
        .map(|id| Some(start(&paths[id], id, &members)))
        .collect();
    let mut alive = [true; 3];
    for port in clients {
        assert_api_versions(port);
    }
    let conflict = Command::new(env!("CARGO_BIN_EXE_operator"))
        .args([
            "serve-quorum",
            "--data",
            paths[0].to_str().unwrap(),
            "--cluster",
            "quorum-process-test",
            "--id",
            "0",
            "--members",
            &members,
        ])
        .output()
        .unwrap();
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("locked"));
    let first = create_eventually(&clients, &alive, "before-failover", None);
    wait_present(&clients, &alive, "before-failover");
    stop(nodes[first].take().unwrap());
    alive[first] = false;
    let wrong_identity = Command::new(env!("CARGO_BIN_EXE_operator"))
        .args([
            "serve-quorum",
            "--data",
            paths[first].to_str().unwrap(),
            "--cluster",
            "different-cluster",
            "--id",
            &first.to_string(),
            "--members",
            &members,
        ])
        .output()
        .unwrap();
    assert!(!wrong_identity.status.success());
    assert!(String::from_utf8_lossy(&wrong_identity.stderr).contains("identity"));
    let _next = create_eventually(&clients, &alive, "after-failover", Some(first));
    wait_present(&clients, &alive, "after-failover");
    nodes[first] = Some(start(&paths[first], first, &members));
    alive[first] = true;
    wait_present(&clients, &alive, "before-failover");
    wait_present(&clients, &alive, "after-failover");
    let live_leader = leader(&clients, &alive, None);
    for id in 0..3 {
        if id != live_leader {
            stop(nodes[id].take().unwrap());
            alive[id] = false;
        }
    }
    let refused = exchange(clients[live_leader], &create("without-quorum"));
    if let Ok(bytes) = refused {
        assert_ne!(
            create_error(&bytes, "without-quorum"),
            0,
            "minority acknowledged a create"
        );
    }
    for id in 0..3 {
        if id != live_leader {
            nodes[id] = Some(start(&paths[id], id, &members));
            alive[id] = true;
        }
    }
    wait_present(&clients, &alive, "before-failover");
    wait_present(&clients, &alive, "after-failover");
    let current = delete_eventually(&clients, &alive, "before-failover");
    wait_absent(&clients, &alive, "before-failover");
    stop(nodes[current].take().unwrap());
    alive[current] = false;
    nodes[current] = Some(start(&paths[current], current, &members));
    alive[current] = true;
    wait_absent(&clients, &alive, "before-failover");
    wait_present(&clients, &alive, "after-failover");
    for node in nodes.into_iter().flatten() {
        stop(node);
    }
    // Every private and client listener has been joined and released.
    for port in all {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        drop(listener);
    }
    fs::remove_dir_all(root).unwrap();
}
