from pathlib import Path
import sys

chunk = sys.argv[1]
assert chunk in ("8", "8192")
path = Path("src/net.rs")
source = path.read_text()
start = source.index('    #[tokio::test(flavor = "current_thread")]\n    async fn write_timeout_shuts_the_broker_down()')
end = source.index('    #[tokio::test(flavor = "current_thread")]\n    async fn an_expired_pipeline_lets_the_broker_leave()', start)
new_test = r'''    #[tokio::test(flavor = "current_thread")]
    async fn write_timeout_shuts_the_broker_down() {
        use std::io::Read;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (gate_tx, gate_rx) = std::sync::mpsc::channel::<std::time::Instant>();
        let (done_tx, done_rx) =
            std::sync::mpsc::channel::<(usize, usize, u128, u128, u128, String)>();
        let progress_bytes = Arc::new(AtomicUsize::new(0));
        let progress_reads = Arc::new(AtomicUsize::new(0));
        let peer_bytes = Arc::clone(&progress_bytes);
        let peer_reads = Arc::clone(&progress_reads);
        let accepted = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = socket.into_std().unwrap();
            peer.set_nonblocking(false).unwrap();
            let _thread = std::thread::spawn(move || {
                let _unused_result = ready_tx.send(());
                let gate = gate_rx.recv().unwrap();
                let gate_to_thread = gate.elapsed().as_micros();
                let mut first_read = None;
                let mut bytes = 0usize;
                let mut reads = 0usize;
                let mut buf = [0u8; BUFFER_SIZE];
                let end = loop {
                    match peer.read(&mut buf) {
                        Ok(0) => break "eof".to_owned(),
                        Err(error) => break format!("read_error:{:?}", error.kind()),
                        Ok(n) => {
                            if first_read.is_none() {
                                first_read = Some(gate.elapsed().as_micros());
                            }
                            bytes += n;
                            reads += 1;
                            if reads % 4096 == 0 {
                                peer_bytes.store(bytes, Ordering::Relaxed);
                                peer_reads.store(reads, Ordering::Relaxed);
                            }
                        }
                    }
                };
                peer_bytes.store(bytes, Ordering::Relaxed);
                peer_reads.store(reads, Ordering::Relaxed);
                let _unused_result = done_tx.send((
                    bytes,
                    reads,
                    gate_to_thread,
                    first_read.unwrap_or(u128::MAX),
                    gate.elapsed().as_micros(),
                    end,
                ));
            });
        });
        let mut conn = BrokerConn::connect(&addr, "write-timeout", Duration::from_secs(2))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        loop {
            match ready_rx.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    if started.elapsed() >= Duration::from_secs(1) {
                        panic!("broker thread did not start");
                    }
                    tokio::task::yield_now().await;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("broker thread dropped the start signal");
                }
            }
        }
        let payload = vec![1u8; 8 * 1024 * 1024];
        let write_started = std::time::Instant::now();
        let result = conn
            .write_all_timeout(&payload, Duration::from_millis(200))
            .await;
        let write_us = write_started.elapsed().as_micros();
        assert!(
            matches!(result, Err(Error::Timeout)),
            "the write finished instead of timing out"
        );
        let gate = std::time::Instant::now();
        let _unused_result = gate_tx.send(gate);
        let within_deadline = done_rx.recv_timeout(Duration::from_millis(300));
        let completed_on_time = within_deadline.is_ok();
        let observed = match within_deadline {
            Ok(details) => Some(details),
            Err(error) => {
                eprintln!(
                    "write_timeout_diag phase=deadline elapsed_us={} bytes_at_last_sample={} reads_at_last_sample={} channel_error={error:?}",
                    gate.elapsed().as_micros(),
                    progress_bytes.load(Ordering::Relaxed),
                    progress_reads.load(Ordering::Relaxed),
                );
                done_rx.recv_timeout(Duration::from_secs(10)).ok()
            }
        };
        if let Some((bytes, reads, gate_to_thread_us, first_read_us, terminal_us, end)) = observed {
            eprintln!(
                "write_timeout_diag phase=terminal chunk=BUFFER_SIZE completed_on_time={completed_on_time} write_us={write_us} gate_to_thread_us={gate_to_thread_us} first_read_us={first_read_us} terminal_us={terminal_us} bytes={bytes} reads={reads} end={end}"
            );
        } else {
            eprintln!(
                "write_timeout_diag phase=no_terminal chunk=BUFFER_SIZE write_us={write_us} elapsed_us={} bytes_at_last_sample={} reads_at_last_sample={}",
                gate.elapsed().as_micros(),
                progress_bytes.load(Ordering::Relaxed),
                progress_reads.load(Ordering::Relaxed),
            );
        }
        assert!(
            completed_on_time,
            "a write timeout left the broker blocked in a read"
        );
        accepted.abort();
    }

'''.replace("BUFFER_SIZE", chunk)
path.write_text(source[:start] + new_test + source[end:])
