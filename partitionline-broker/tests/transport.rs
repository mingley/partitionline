//! Loopback framing, bounded admission, absolute deadlines and joined shutdown.

use partitionline_broker::transport::{Config, Handler, Transport};
use std::{
    future::{pending, poll_fn, Future},
    io,
    net::{Ipv4Addr, SocketAddr},
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
    time::{sleep, timeout},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn config(
    connections: usize,
    handlers: usize,
    request: usize,
    response: usize,
    read_ms: u64,
    handler_ms: u64,
    write_ms: u64,
) -> Result<Config> {
    Ok(Config::new(
        connections,
        handlers,
        request,
        response,
        Duration::from_millis(read_ms),
        Duration::from_millis(handler_ms),
        Duration::from_millis(write_ms),
    )?)
}

async fn start<H: Handler + 'static>(handler: H, config: Config) -> Result<Transport> {
    Ok(Transport::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config,
        Arc::new(handler),
    )
    .await?)
}

async fn connect(server: &Transport) -> Result<TcpStream> {
    let socket = TcpStream::connect(server.local_addr()).await?;
    socket.set_nodelay(true)?;
    Ok(socket)
}

fn framed(payload: &[u8]) -> Result<Vec<u8>> {
    let prefix = i32::try_from(payload.len())?.to_be_bytes();
    Ok([&prefix[..], payload].concat())
}

async fn send(socket: &mut TcpStream, payload: &[u8]) -> Result {
    socket.write_all(&framed(payload)?).await?;
    Ok(())
}

async fn response(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let mut prefix = [0; 4];
    let _ = timeout(Duration::from_secs(2), socket.read_exact(&mut prefix)).await??;
    let length = usize::try_from(i32::from_be_bytes(prefix))?;
    assert!(
        length <= 64 * 1024 * 1024,
        "test peer returned an unbounded length"
    );
    let mut body = vec![0; length];
    let _ = timeout(Duration::from_secs(2), socket.read_exact(&mut body)).await??;
    Ok(body)
}

async fn closed(socket: &mut TcpStream) -> Result<bool> {
    let mut byte = [0];
    match timeout(Duration::from_secs(2), socket.read(&mut byte)).await? {
        Ok(0) => Ok(true),
        Ok(_) => Ok(false),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(true)
        }
        Err(error) => Err(error.into()),
    }
}

async fn count_at_least(count: &AtomicUsize, minimum: usize) -> Result {
    timeout(Duration::from_secs(2), async {
        while count.load(Ordering::SeqCst) < minimum {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

fn echo(request: Vec<u8>) -> impl Future<Output = io::Result<Option<Vec<u8>>>> + Send {
    std::future::ready(Ok(Some(request)))
}

struct Active {
    active: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Active {
    fn new(active: Arc<AtomicUsize>, dropped: Arc<AtomicUsize>, peak: &AtomicUsize) -> Self {
        let value = active.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = peak.fetch_max(value, Ordering::SeqCst);
        Self { active, dropped }
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        let _ = self.active.fetch_sub(1, Ordering::SeqCst);
        let _ = self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn rejects_zero_and_out_of_range_configuration_without_io() {
    let second = Duration::from_secs(1);
    for (connections, handlers, request, response) in [
        (0, 1, 1, 1),
        (65_537, 1, 1, 1),
        (1, 0, 1, 1),
        (1, 2, 1, 1),
        (1, 1, 0, 1),
        (1, 1, 1, 0),
        (1, 1, 64 * 1024 * 1024 + 1, 1),
        (1, 1, 1, 64 * 1024 * 1024 + 1),
    ] {
        assert!(Config::new(
            connections,
            handlers,
            request,
            response,
            second,
            second,
            second
        )
        .is_err());
    }
    for duration in [
        Duration::ZERO,
        Duration::from_secs(24 * 60 * 60 + 1),
        Duration::MAX,
    ] {
        assert!(Config::new(1, 1, 1, 1, duration, second, second).is_err());
        assert!(Config::new(1, 1, 1, 1, second, duration, second).is_err());
        assert!(Config::new(1, 1, 1, 1, second, second, duration).is_err());
    }
    let defaults = Config::default();
    assert_eq!(defaults.max_connections(), 64);
    assert_eq!(defaults.max_handlers(), 32);
    assert_eq!(defaults.max_request_bytes(), 8 * 1024 * 1024);
    assert_eq!(defaults.max_response_bytes(), 8 * 1024 * 1024);
    assert_eq!(defaults.read_timeout(), Duration::from_secs(10));
    assert_eq!(defaults.handler_timeout(), Duration::from_secs(30));
    assert_eq!(defaults.write_timeout(), Duration::from_secs(10));
}

#[tokio::test]
async fn fragmented_prefix_body_empty_and_exact_limit_roundtrip() -> Result {
    let mut server = start(echo, config(2, 1, 8, 8, 2000, 2000, 2000)?).await?;
    let mut client = connect(&server).await?;
    for payload in [&b""[..], &b"abcdefg"[..], &b"12345678"[..]] {
        for byte in framed(payload)? {
            client.write_all(&[byte]).await?;
            tokio::task::yield_now().await;
        }
        assert_eq!(response(&mut client).await?, payload);
    }
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, 1);
    assert_eq!(report.joined_connections, 1);
    assert!(closed(&mut client).await?);
    Ok(())
}

#[tokio::test]
async fn pipelined_requests_keep_wire_and_handler_order() -> Result {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let mut server = start(
        move |request: Vec<u8>| {
            let calls = handler_calls.clone();
            async move {
                let ordinal = calls.fetch_add(1, Ordering::SeqCst);
                if request == b"first" {
                    sleep(Duration::from_millis(40)).await;
                }
                let mut response = request;
                response.push(u8::try_from(ordinal).map_err(io::Error::other)?);
                Ok::<_, io::Error>(Some(response))
            }
        },
        config(2, 2, 32, 32, 2000, 2000, 2000)?,
    )
    .await?;
    let mut client = connect(&server).await?;
    client
        .write_all(&[framed(b"first")?, framed(b"second")?].concat())
        .await?;
    assert_eq!(response(&mut client).await?, b"first\0");
    assert_eq!(response(&mut client).await?, b"second\x01");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let report = server.shutdown().await?;
    assert_eq!(report.joined_connections, 1);
    Ok(())
}

#[tokio::test]
async fn pipelined_no_reply_sends_no_frame_and_keeps_connection_usable() -> Result {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let mut server = start(
        move |request: Vec<u8>| {
            let calls = handler_calls.clone();
            async move {
                let _ = calls.fetch_add(1, Ordering::SeqCst);
                if request == b"no-reply" {
                    sleep(Duration::from_millis(10)).await;
                    Ok::<_, io::Error>(None)
                } else {
                    Ok(Some(request))
                }
            }
        },
        config(1, 1, 32, 32, 2000, 2000, 2000)?,
    )
    .await?;
    let mut client = connect(&server).await?;
    client
        .write_all(&[framed(b"no-reply")?, framed(b"reply")?].concat())
        .await?;
    // A zero-length response for None would be observed here and fail.
    assert_eq!(response(&mut client).await?, b"reply");
    send(&mut client, b"still-usable").await?;
    assert_eq!(response(&mut client).await?, b"still-usable");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(server.shutdown().await?.joined_connections, 1);
    Ok(())
}

#[tokio::test]
async fn adversarial_lengths_close_before_payload_or_handler_and_isolate_peers() -> Result {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let mut server = start(
        move |request| {
            let _ = handler_calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, io::Error>(Some(request)))
        },
        config(4, 2, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    for length in [-1i32, i32::MIN, 9, i32::MAX] {
        let mut client = connect(&server).await?;
        client.write_all(&length.to_be_bytes()).await?;
        assert!(closed(&mut client).await?, "prefix-only rejection {length}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut healthy = connect(&server).await?;
    send(&mut healthy, b"ok").await?;
    assert_eq!(response(&mut healthy).await?, b"ok");
    let report = server.shutdown().await?;
    assert_eq!(report.invalid_lengths, 4);
    assert_eq!(report.accepted_connections, report.joined_connections);
    Ok(())
}

#[tokio::test]
async fn truncated_prefix_and_body_never_invoke_handler() -> Result {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let mut server = start(
        move |request| {
            let _ = handler_calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, io::Error>(Some(request)))
        },
        config(2, 1, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    for bytes in [&[0, 0][..], &[0, 0, 0, 4, b'a'][..]] {
        let mut client = connect(&server).await?;
        client.write_all(bytes).await?;
        client.shutdown().await?;
        assert!(closed(&mut client).await?);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.shutdown().await?.peer_closes, 2);
    Ok(())
}

#[tokio::test]
async fn slowloris_progress_does_not_reset_prefix_plus_body_deadline() -> Result {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let mut server = start(
        move |request| {
            let _ = handler_calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, io::Error>(Some(request)))
        },
        config(2, 1, 8, 8, 100, 2000, 2000)?,
    )
    .await?;
    let mut client = connect(&server).await?;
    send(&mut client, b"ready").await?;
    assert_eq!(response(&mut client).await?, b"ready");
    // Each gap is under 100ms; their aggregate prefix+body duration exceeds it.
    client.write_all(&[0, 0]).await?;
    sleep(Duration::from_millis(60)).await;
    client.write_all(&[0, 2, b'a']).await?;
    sleep(Duration::from_millis(60)).await;
    let _ = client.write_all(b"b").await;
    assert!(closed(&mut client).await?);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.shutdown().await?.read_deadlines, 1);
    Ok(())
}

#[tokio::test]
async fn idle_connection_has_an_absolute_read_deadline() -> Result {
    let mut server = start(echo, config(1, 1, 8, 8, 40, 2000, 2000)?).await?;
    let mut client = connect(&server).await?;
    assert!(closed(&mut client).await?);
    assert_eq!(server.shutdown().await?.read_deadlines, 1);
    Ok(())
}

#[tokio::test]
async fn handler_error_and_oversized_response_close_only_their_connections() -> Result {
    let mut server = start(
        |request: Vec<u8>| async move {
            if request == b"fail" {
                Err(io::Error::other("test application failure"))
            } else if request == b"large" {
                Ok(Some(vec![0; 9]))
            } else {
                Ok(Some(request))
            }
        },
        config(3, 2, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    for payload in [&b"fail"[..], &b"large"[..]] {
        let mut client = connect(&server).await?;
        send(&mut client, payload).await?;
        assert!(closed(&mut client).await?);
    }
    let mut healthy = connect(&server).await?;
    send(&mut healthy, b"ok").await?;
    assert_eq!(response(&mut healthy).await?, b"ok");
    let report = server.shutdown().await?;
    assert_eq!(report.handler_errors, 1);
    assert_eq!(report.oversized_responses, 1);
    assert_eq!(report.joined_connections, 3);
    Ok(())
}

#[tokio::test]
async fn hanging_handler_times_out_and_its_future_is_dropped() -> Result {
    let active = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, d, p) = (active.clone(), dropped.clone(), peak.clone());
    let mut server = start(
        move |_| {
            let (a, d, p) = (a.clone(), d.clone(), p.clone());
            async move {
                let _guard = Active::new(a, d, &p);
                pending::<io::Result<Option<Vec<u8>>>>().await
            }
        },
        config(1, 1, 8, 8, 2000, 40, 2000)?,
    )
    .await?;
    let mut client = connect(&server).await?;
    send(&mut client, b"wait").await?;
    count_at_least(&active, 1).await?;
    assert!(closed(&mut client).await?);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(server.shutdown().await?.handler_deadlines, 1);
    Ok(())
}

#[tokio::test]
async fn handler_deadline_includes_waiting_for_concurrency_permit() -> Result {
    let started = Arc::new(AtomicUsize::new(0));
    let h = started.clone();
    let mut server = start(
        move |request: Vec<u8>| {
            let h = h.clone();
            async move {
                let _ = h.fetch_add(1, Ordering::SeqCst);
                if request == b"first" {
                    sleep(Duration::from_millis(250)).await;
                    Ok(Some(request))
                } else {
                    pending::<io::Result<Option<Vec<u8>>>>().await
                }
            }
        },
        config(2, 1, 8, 8, 2000, 300, 2000)?,
    )
    .await?;
    let mut first = connect(&server).await?;
    send(&mut first, b"first").await?;
    count_at_least(&started, 1).await?;
    let mut queued = connect(&server).await?;
    send(&mut queued, b"queued").await?;
    // A reset after the first handler releases its permit would take ~550ms.
    assert!(timeout(Duration::from_millis(450), closed(&mut queued)).await??);
    assert_eq!(response(&mut first).await?, b"first");
    assert_eq!(server.shutdown().await?.handler_deadlines, 1);
    Ok(())
}

#[tokio::test]
async fn connection_overload_rejects_without_spawning_handler() -> Result {
    let active = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, d, p) = (active.clone(), dropped.clone(), peak.clone());
    let mut server = start(
        move |_| {
            let (a, d, p) = (a.clone(), d.clone(), p.clone());
            async move {
                let _guard = Active::new(a, d, &p);
                pending::<io::Result<Option<Vec<u8>>>>().await
            }
        },
        config(1, 1, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    let mut first = connect(&server).await?;
    send(&mut first, b"hold").await?;
    count_at_least(&active, 1).await?;
    let mut rejected = connect(&server).await?;
    assert!(closed(&mut rejected).await?);
    assert_eq!(active.load(Ordering::SeqCst), 1);
    let report = server.shutdown().await?;
    assert_eq!(report.overload_rejections, 1);
    assert_eq!(report.accepted_connections, 1);
    assert_eq!(report.peak_connections, 1);
    assert_eq!(report.joined_connections, 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn handler_work_is_bounded_and_shutdown_joins_active_and_queued_requests() -> Result {
    let active = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, d, p) = (active.clone(), dropped.clone(), peak.clone());
    let mut server = start(
        move |_| {
            let (a, d, p) = (a.clone(), d.clone(), p.clone());
            async move {
                let _guard = Active::new(a, d, &p);
                pending::<io::Result<Option<Vec<u8>>>>().await
            }
        },
        config(4, 2, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    let mut clients = Vec::new();
    for _ in 0..4 {
        let mut socket = connect(&server).await?;
        send(&mut socket, b"hold").await?;
        clients.push(socket);
    }
    count_at_least(&active, 2).await?;
    // Give admission/read tasks a turn; pending handlers retain both permits.
    sleep(Duration::from_millis(20)).await;
    assert_eq!(active.load(Ordering::SeqCst), 2);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    let report = timeout(Duration::from_secs(1), server.shutdown()).await??;
    assert_eq!(report.accepted_connections, 4);
    assert_eq!(report.joined_connections, 4);
    assert_eq!(report.shutdown_connections, 4);
    assert_eq!(report.peak_connections, 4);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    for socket in &mut clients {
        assert!(closed(socket).await?);
    }
    assert_eq!(server.shutdown().await?, report);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_shutdown_await_can_be_retried_and_still_joins() -> Result {
    let active = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, d, p) = (active.clone(), dropped.clone(), peak.clone());
    let mut server = start(
        move |_| {
            let (a, d, p) = (a.clone(), d.clone(), p.clone());
            async move {
                let _guard = Active::new(a, d, &p);
                pending::<io::Result<Option<Vec<u8>>>>().await
            }
        },
        config(1, 1, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    let mut client = connect(&server).await?;
    send(&mut client, b"hold").await?;
    count_at_least(&active, 1).await?;
    let mut shutdown = Box::pin(server.shutdown());
    poll_fn(|context| {
        assert!(shutdown.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(shutdown);
    let report = timeout(Duration::from_secs(1), server.shutdown()).await??;
    assert_eq!(report.joined_connections, 1);
    assert_eq!(report.shutdown_connections, 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(closed(&mut client).await?);
    Ok(())
}

#[tokio::test]
async fn dropping_transport_requests_socket_and_handler_cleanup() -> Result {
    let active = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (a, d, p) = (active.clone(), dropped.clone(), peak.clone());
    let server = start(
        move |_| {
            let (a, d, p) = (a.clone(), d.clone(), p.clone());
            async move {
                let _guard = Active::new(a, d, &p);
                pending::<io::Result<Option<Vec<u8>>>>().await
            }
        },
        config(1, 1, 8, 8, 2000, 2000, 2000)?,
    )
    .await?;
    let addr = server.local_addr();
    let mut client = connect(&server).await?;
    send(&mut client, b"hold").await?;
    count_at_least(&active, 1).await?;
    drop(server);
    assert!(closed(&mut client).await?);
    count_at_least(&dropped, 1).await?;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(TcpStream::connect(addr).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unread_large_response_hits_absolute_write_deadline_and_joins() -> Result {
    let produced = Arc::new(AtomicUsize::new(0));
    let h = produced.clone();
    let mut server = start(
        move |_| {
            let h = h.clone();
            async move {
                let response = vec![0; 64 * 1024 * 1024];
                h.store(1, Ordering::SeqCst);
                Ok::<_, io::Error>(Some(response))
            }
        },
        config(1, 1, 8, 64 * 1024 * 1024, 2000, 2000, 50)?,
    )
    .await?;
    let mut client = connect(&server).await?;
    send(&mut client, b"large").await?;
    count_at_least(&produced, 1).await?;
    // The peer deliberately never drains its receive buffer.
    sleep(Duration::from_millis(200)).await;
    let report = timeout(Duration::from_secs(1), server.shutdown()).await??;
    assert_eq!(report.write_deadlines, 1);
    assert_eq!(report.joined_connections, 1);
    Ok(())
}
