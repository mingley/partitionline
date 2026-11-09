//! Loopback controls for the current Produce-quota transport boundary.

use partitionline_broker::transport::{Config, Transport};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::timeout,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn frame(body: &[u8]) -> Result<Vec<u8>> {
    let mut bytes = i32::try_from(body.len())?.to_be_bytes().to_vec();
    bytes.extend_from_slice(body);
    Ok(bytes)
}

async fn read_response(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let size = timeout(Duration::from_secs(2), socket.read_i32()).await??;
    let mut body = vec![0; usize::try_from(size)?];
    timeout(Duration::from_secs(2), socket.read_exact(&mut body)).await??;
    Ok(body)
}

async fn server(
    started: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
) -> Result<Transport> {
    let config = Config::new(
        2, 1, 16, 16,
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )?;
    Ok(Transport::bind(
        "127.0.0.1:0".parse()?,
        config,
        Arc::new(move |body: Vec<u8>| {
            let (started, release, calls, dropped) = (
                started.clone(),
                release.clone(),
                calls.clone(),
                dropped.clone(),
            );
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if body == b"hold" {
                    let _guard = Dropped(dropped);
                    started.notify_one();
                    release.notified().await;
                }
                Ok::<_, io::Error>(Some(body))
            }
        }),
    )
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn two_legal_pipelined_frames_can_cancel_the_active_handler() -> Result {
    let started = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut server = server(
        started.clone(),
        Arc::new(Notify::new()),
        calls.clone(),
        dropped.clone(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    socket.write_all(&frame(b"hold")?).await?;
    timeout(Duration::from_secs(2), started.notified()).await?;

    // Each subsequent body is exactly the configured per-frame limit. The
    // combined pipeline exceeds the disconnect watcher's one-frame budget.
    socket.write_all(&[frame(&[1; 16])?, frame(&[2; 16])?].concat()).await?;
    let mut byte = [0];
    match timeout(Duration::from_secs(2), socket.read(&mut byte)).await? {
        Ok(0) => {}
        Err(error) if matches!(error.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe) => {}
        other => return Err(format!("expected closed connection, got {other:?}").into()),
    }
    let report = server.shutdown().await?;
    assert_eq!(report.invalid_lengths, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn small_pipelined_frames_keep_their_order() -> Result {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut server = server(started.clone(), release.clone(), calls.clone(), dropped.clone()).await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    socket.write_all(&frame(b"hold")?).await?;
    timeout(Duration::from_secs(2), started.notified()).await?;
    socket.write_all(&[frame(b"a")?, frame(b"b")?].concat()).await?;
    release.notify_one();
    assert_eq!(read_response(&mut socket).await?, b"hold");
    assert_eq!(read_response(&mut socket).await?, b"a");
    assert_eq!(read_response(&mut socket).await?, b"b");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(server.shutdown().await?.invalid_lengths, 0);
    Ok(())
}
