//! Bounded backpressure for valid and invalid pipelined frames.

use partitionline_broker::transport::{Config, Transport};
use std::{
    future::{poll_fn, Future},
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
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

async fn no_response_yet(socket: &mut TcpStream) {
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(60), socket.read(&mut byte))
            .await
            .is_err(),
        "the active handler was canceled or a response was sent early"
    );
}

async fn expect_closed(socket: &mut TcpStream) -> Result {
    let mut byte = [0];
    match timeout(Duration::from_secs(2), socket.read(&mut byte)).await? {
        Ok(0) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        other => Err(format!("expected closed connection, got {other:?}").into()),
    }
}

async fn drain_to_close(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut chunk = [0; 32];
    loop {
        match timeout(Duration::from_secs(2), socket.read(&mut chunk)).await? {
            Ok(0) => return Ok(result),
            Ok(n) => {
                result.extend_from_slice(&chunk[..n]);
                if result.len() > 20 {
                    return Err("unexpected extra responses".into());
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) =>
            {
                return Ok(result);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn server(
    started: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
) -> Result<Transport> {
    let config = Config::new(
        2,
        1,
        16,
        16,
        Duration::from_secs(3),
        Duration::from_secs(3),
        Duration::from_secs(3),
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
                // Let the watcher find the next malformed prefix before this
                // particular valid request has a chance to produce a response.
                if body == [7; 16] {
                    return std::future::pending::<io::Result<Option<Vec<u8>>>>().await;
                }
                Ok::<_, io::Error>(Some(body))
            }
        }),
    )
    .await?)
}

async fn setup() -> Result<(
    Transport,
    TcpStream,
    Arc<Notify>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
)> {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let server = server(
        started.clone(),
        release.clone(),
        calls.clone(),
        dropped.clone(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    socket.set_nodelay(true)?;
    socket.write_all(&frame(b"hold")?).await?;
    timeout(Duration::from_secs(2), started.notified()).await?;
    Ok((server, socket, release, calls, dropped))
}

#[tokio::test(flavor = "current_thread")]
async fn fragmented_valid_pipeline_above_one_frame_keeps_order() -> Result {
    let (mut server, mut socket, release, calls, dropped) = setup().await?;
    let pipeline = [frame(&[1; 16])?, frame(&[2; 16])?].concat();
    for byte in pipeline {
        socket.write_all(&[byte]).await?;
    }
    no_response_yet(&mut socket).await;
    release.notify_one();
    assert_eq!(read_response(&mut socket).await?, b"hold");
    assert_eq!(read_response(&mut socket).await?, [1; 16]);
    assert_eq!(read_response(&mut socket).await?, [2; 16]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let report = server.shutdown().await?;
    assert_eq!(report.invalid_lengths, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn full_buffer_defers_half_close_until_active_handler_completes() -> Result {
    let (mut server, mut socket, release, _calls, dropped) = setup().await?;
    socket.write_all(&frame(&[1; 16])?).await?;
    socket.shutdown().await?;
    no_response_yet(&mut socket).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    release.notify_one();
    assert_eq!(read_response(&mut socket).await?, b"hold");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    // EOF can cancel the buffered request, or it may be answered first if
    // its handler completes before EOF is observed.
    let remaining = drain_to_close(&mut socket).await?;
    assert!(remaining.is_empty() || remaining == frame(&[1; 16])?);
    let report = timeout(Duration::from_secs(2), server.shutdown()).await??;
    assert_eq!(report.peer_closes, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_prefix_after_full_buffer_is_detected_after_active_response() -> Result {
    let (mut server, mut socket, release, _calls, _dropped) = setup().await?;
    socket
        .write_all(&[frame(&[7; 16])?, 17i32.to_be_bytes().to_vec()].concat())
        .await?;
    no_response_yet(&mut socket).await;
    release.notify_one();
    assert_eq!(read_response(&mut socket).await?, b"hold");
    expect_closed(&mut socket).await?;
    let report = server.shutdown().await?;
    assert_eq!(report.invalid_lengths, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_cancels_a_handler_with_a_full_buffer() -> Result {
    let (mut server, mut socket, _release, _calls, dropped) = setup().await?;
    socket
        .write_all(&[frame(&[1; 16])?, frame(&[2; 16])?].concat())
        .await?;
    no_response_yet(&mut socket).await;
    let report = timeout(Duration::from_secs(2), server.shutdown()).await??;
    assert_eq!(report.shutdown_connections, 1);
    assert_eq!(report.joined_connections, 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    expect_closed(&mut socket).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_shutdown_can_be_retried_with_a_full_buffer() -> Result {
    let (mut server, mut socket, _release, _calls, dropped) = setup().await?;
    socket
        .write_all(&[frame(&[1; 16])?, frame(&[2; 16])?].concat())
        .await?;
    no_response_yet(&mut socket).await;
    let mut shutdown = Box::pin(server.shutdown());
    poll_fn(|context| {
        assert!(shutdown.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(shutdown);
    let report = timeout(Duration::from_secs(2), server.shutdown()).await??;
    assert_eq!(report.shutdown_connections, 1);
    assert_eq!(report.joined_connections, 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    expect_closed(&mut socket).await?;
    Ok(())
}
