//! Public controls for the opt-in router-wide Produce bandwidth policy.
#![allow(clippy::unwrap_used)]

use partitionline_broker::{
    catalog::{Catalog, TopicId},
    fetch,
    metadata::{self, ClientProduceQuotas, ProduceQuota, QuotaRouter},
    produce,
    transport::{self, ExchangeControl, Handler, Peer, Transport},
};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch() -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "partitionline-quota-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root)?;
    let config = common();
    let (mut catalog, _) = Catalog::open(root.join("catalog"), config.catalog_limits)?;
    let mut id = [0; 16];
    id[15] = 2;
    catalog.create("alpha", TopicId::new(id)?, 2)?;
    drop(catalog);
    Ok(root)
}
fn common() -> metadata::Config {
    metadata::Config::new(0, "127.0.0.1".into(), 19095, "quota-test".into())
}
async fn fixture(name: &str, suffix: &str) -> Result<Vec<u8>> {
    Ok(tokio::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/produce/4.3.1")
            .join(format!("{name}.{suffix}.bin")),
    )
    .await?)
}
async fn open(root: &Path, millis: u32) -> Result<QuotaRouter> {
    Ok(QuotaRouter::open_with_read_store(
        root.join("catalog"),
        common(),
        produce::Config::new(root.join("parts")),
        fetch::Limits::default(),
        ProduceQuota::new(1, 1, millis)?,
    )
    .await?
    .0)
}
fn with_client_id(request: &[u8], name: &str) -> Result<Vec<u8>> {
    let prefix = request.get(..8).ok_or("short request prefix")?;
    let old_len = i16::from_be_bytes(
        request
            .get(8..10)
            .ok_or("short client length")?
            .try_into()?,
    );
    let old_len = if old_len == -1 {
        0
    } else {
        usize::try_from(old_len)?
    };
    let tail = request.get(10 + old_len..).ok_or("short client ID")?;
    let mut result = Vec::with_capacity(prefix.len() + 2 + name.len() + tail.len());
    result.extend_from_slice(prefix);
    result.extend_from_slice(&i16::try_from(name.len())?.to_be_bytes());
    result.extend_from_slice(name.as_bytes());
    result.extend_from_slice(tail);
    Ok(result)
}

async fn send(socket: &mut TcpStream, bytes: &[u8]) -> Result {
    socket
        .write_all(&i32::try_from(bytes.len())?.to_be_bytes())
        .await?;
    socket.write_all(bytes).await?;
    Ok(())
}
async fn receive(socket: &mut TcpStream) -> Result<Vec<u8>> {
    let size = socket.read_i32().await?;
    let mut bytes = vec![0; usize::try_from(size)?];
    socket.read_exact(&mut bytes).await?;
    Ok(bytes)
}
#[derive(Debug, PartialEq, Eq)]
enum SocketObservation {
    NoData,
    Byte(u8),
    Eof,
}

async fn observe_socket(socket: &TcpStream, wait: Duration) -> Result<SocketObservation> {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(wait, socket.peek(&mut byte)).await {
        Err(_) => Ok(SocketObservation::NoData),
        Ok(Ok(0)) => Ok(SocketObservation::Eof),
        Ok(Ok(_)) => Ok(SocketObservation::Byte(byte[0])),
        Ok(Err(error)) => Err(error.into()),
    }
}

#[tokio::test]
async fn socket_probe_distinguishes_empty_data_and_eof() -> Result {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut client = TcpStream::connect(listener.local_addr()?).await?;
    let (mut server, _) = listener.accept().await?;
    server.write_all(&[7]).await?;
    client.readable().await?;
    let mut byte = [0u8; 1];
    assert_eq!(client.try_read(&mut byte)?, 1);
    assert_eq!(byte[0], 7);
    assert_eq!(
        observe_socket(&client, Duration::from_millis(100)).await?,
        SocketObservation::NoData
    );
    server.write_all(&[9]).await?;
    assert_eq!(
        observe_socket(&client, Duration::from_secs(1)).await?,
        SocketObservation::Byte(9)
    );
    client.read_exact(&mut byte).await?;
    server.shutdown().await?;
    assert_eq!(
        observe_socket(&client, Duration::from_secs(1)).await?,
        SocketObservation::Eof
    );
    Ok(())
}

fn metadata_request() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&3i16.to_be_bytes());
    bytes.extend_from_slice(&1i16.to_be_bytes());
    bytes.extend_from_slice(&77i32.to_be_bytes());
    bytes.extend_from_slice(&(-1i16).to_be_bytes());
    bytes.extend_from_slice(&(-1i32).to_be_bytes());
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_produce_goldens_get_only_the_writer_throttle_field() -> Result {
    for version in 3..=13 {
        for acks in [-1, 0, 1] {
            let root = scratch()?;
            let router = open(&root, 500).await?;
            let name = format!("produce-v{version}-acks{acks}");
            let request = fixture(&name, "request").await?;
            let start = Instant::now();
            let (outcome, deadline) = router.dispatch_controlled(request).await?.into_parts();
            if acks == 0 {
                assert!(outcome.response.is_none(), "version {version} acks=0");
            } else {
                let mut expected = fixture(&name, "response").await?;
                let response = outcome.response.ok_or("missing response")?;
                let field = expected
                    .len()
                    .checked_sub(if version >= 9 { 5 } else { 4 })
                    .ok_or("short response")?;
                expected[field..field + 4].copy_from_slice(&500i32.to_be_bytes());
                assert_eq!(response, expected, "version {version} acks={acks}");
            }
            assert!(deadline.is_some_and(|until| until >= start + Duration::from_millis(500)));
            router.shutdown().await?;
            fs::remove_dir_all(root)?;
        }
    }
    Ok(())
}

struct Observe {
    inner: Arc<QuotaRouter>,
    started: mpsc::UnboundedSender<(i16, Instant)>,
    deadlines: mpsc::UnboundedSender<Option<Instant>>,
    delay_delivery: bool,
}

impl Handler for Observe {
    type Error = metadata::QuotaRouterError;
    async fn handle(&self, _request: Vec<u8>) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
        Err(metadata::QuotaRouterError::ControlledExchangeRequired)
    }
    async fn exchange_with_control(
        &self,
        peer: &Peer,
        request: Vec<u8>,
        spare: Vec<u8>,
    ) -> std::result::Result<ExchangeControl, Self::Error> {
        let key = i16::from_be_bytes([request[0], request[1]]);
        let _ = self.started.send((key, Instant::now()));
        let control =
            Handler::exchange_with_control(self.inner.as_ref(), peer, request, spare).await?;
        let (outcome, resume_at) = control.into_parts();
        if key == 0 {
            let _ = self.deadlines.send(resume_at);
            if self.delay_delivery {
                if let Some(until) = resume_at {
                    tokio::time::sleep_until(
                        tokio::time::Instant::from_std(until) + Duration::from_millis(50),
                    )
                    .await;
                }
            }
        }
        match resume_at {
            Some(until) => ExchangeControl::with_resume_at(outcome, until)
                .map_err(|_| metadata::QuotaRouterError::AccountingFailed),
            None => Ok(ExchangeControl::new(outcome)),
        }
    }
}

struct Observations {
    handler: Arc<Observe>,
    started: mpsc::UnboundedReceiver<(i16, Instant)>,
    deadlines: mpsc::UnboundedReceiver<Option<Instant>>,
}

fn observing(router: &Arc<QuotaRouter>, delay_delivery: bool) -> Observations {
    let (started, started_rx) = mpsc::unbounded_channel();
    let (deadlines, deadlines_rx) = mpsc::unbounded_channel();
    Observations {
        handler: Arc::new(Observe {
            inner: Arc::clone(router),
            started,
            deadlines,
            delay_delivery,
        }),
        started: started_rx,
        deadlines: deadlines_rx,
    }
}

async fn started_request(
    events: &mut mpsc::UnboundedReceiver<(i16, Instant)>,
    expected_key: i16,
) -> Result<Instant> {
    let (key, at) = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await?
        .ok_or("handler observation channel closed")?;
    assert_eq!(key, expected_key);
    Ok(at)
}

async fn produce_deadline(
    events: &mut mpsc::UnboundedReceiver<Option<Instant>>,
) -> Result<Instant> {
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await?
        .flatten()
        .ok_or_else(|| "missing Produce deadline".into())
}

// A single-byte read is cancellation safe. If the response begins at the
// deadline, keep the consumed byte when reading the rest of the frame.
async fn receive_after_mute(socket: &mut TcpStream, deadline: Instant) -> Result<Vec<u8>> {
    if deadline.saturating_duration_since(Instant::now()) < Duration::from_millis(300) {
        return Err("too little mute time remains for the control".into());
    }
    let mut header = [0u8; 4];
    let mut byte = [0u8; 1];
    let prefix = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        socket.read(&mut byte),
    )
    .await
    {
        Err(_) => 0,
        Ok(Ok(0)) => return Err("connection closed during mute".into()),
        Ok(Ok(_)) if Instant::now() < deadline => {
            return Err("response byte arrived before resume".into())
        }
        Ok(Ok(_)) => {
            header[0] = byte[0];
            1
        }
        Ok(Err(error)) => return Err(error.into()),
    };
    socket.read_exact(&mut header[prefix..]).await?;
    let size = i32::from_be_bytes(header);
    let mut body = vec![0; usize::try_from(size)?];
    socket.read_exact(&mut body).await?;
    Ok(body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfigured_and_malformed_clients_charge_the_shared_fallback() -> Result {
    let root = scratch()?;
    let fixture = fixture("produce-v3-acks1", "request").await?;
    let unknown = with_client_id(&fixture, "unknown")?;
    let empty = with_client_id(&fixture, "")?;
    let mut null = with_client_id(&fixture, "")?;
    null[8..10].copy_from_slice(&(-1i16).to_be_bytes());
    let alpha = with_client_id(&fixture, "alpha")?;
    let slow = ProduceQuota::new(1, u64::try_from(unknown.len())?, 60_000)?;
    let fast = ProduceQuota::new(1_000_000, 1_000_000, 60_000)?;
    let policies = ClientProduceQuotas::new(slow, vec![("alpha".to_owned(), fast)])?;
    let (router, _) = QuotaRouter::open_with_client_quotas(
        root.join("catalog"),
        common(),
        produce::Config::new(root.join("parts")),
        fetch::Limits::default(),
        policies,
    )
    .await?;

    // A started Produce with a malformed UTF-8 client ID fails parsing but
    // still consumes fallback quota, just like other started work.
    let malformed = vec![0, 0, 0, 3, 0, 0, 0, 7, 0, 1, 0xff];
    assert!(router.dispatch_controlled(malformed).await.is_err());
    let mut reported = Vec::new();
    for request in [unknown, null, empty, alpha] {
        let (outcome, _) = router.dispatch_controlled(request).await?.into_parts();
        let response = outcome.response.ok_or("missing Produce response")?;
        let start = response.len().checked_sub(4).ok_or("short response")?;
        reported.push(i32::from_be_bytes(response[start..].try_into()?));
    }
    assert!(
        reported[0] > 0,
        "malformed work was not charged to fallback"
    );
    assert!(
        reported[1] > reported[0] + 1_000,
        "null ID did not share fallback"
    );
    assert!(reported[2] > 0, "empty ID did not share fallback");
    assert_eq!(reported[3], 0, "named alpha inherited fallback debt");
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_quota_separates_connections_and_sequences_acks_zero() -> Result {
    let root = scratch()?;
    let slow = ProduceQuota::new(25, 1, 15_000)?;
    let fast = ProduceQuota::new(1_000_000, 1_000_000, 15_000)?;
    let policies = ClientProduceQuotas::new(
        fast,
        vec![("alpha".to_owned(), slow), ("bravo".to_owned(), fast)],
    )?;
    let router = Arc::new(
        QuotaRouter::open_with_client_quotas(
            root.join("catalog"),
            common(),
            produce::Config::new(root.join("parts")),
            fetch::Limits::default(),
            policies,
        )
        .await?
        .0,
    );
    let Observations {
        handler,
        mut started,
        mut deadlines,
    } = observing(&router, false);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::default(),
        handler,
    )
    .await?;
    let mut first = TcpStream::connect(transport.local_addr()).await?;
    let mut second = TcpStream::connect(transport.local_addr()).await?;
    let mut third = TcpStream::connect(transport.local_addr()).await?;
    let a = with_client_id(&fixture("produce-v3-acks0", "request").await?, "alpha")?;
    let b = with_client_id(&fixture("produce-v9-acks1", "request").await?, "bravo")?;
    let a_again = with_client_id(&fixture("produce-v3-acks1", "request").await?, "alpha")?;

    send(&mut first, &a).await?;
    started_request(&mut started, 0).await?;
    let deadline = produce_deadline(&mut deadlines).await?;
    send(&mut first, &metadata_request()).await?;
    send(&mut second, &b).await?;
    let b_started = started_request(&mut started, 0).await?;
    assert!(
        b_started < deadline,
        "unrelated label waited for alpha's mute"
    );
    let b_deadline = tokio::time::timeout(Duration::from_secs(2), deadlines.recv())
        .await?
        .ok_or("deadline observation closed")?;
    assert!(b_deadline.is_none(), "bravo inherited alpha's debt");
    let response = tokio::time::timeout(Duration::from_secs(2), receive(&mut second)).await??;
    let mut expected = fixture("produce-v9-acks1", "response").await?;
    // Alpha's already completed acks=0 append advances the sole partition.
    assert_eq!(&expected[19..27], &0i64.to_be_bytes());
    expected[19..27].copy_from_slice(&1i64.to_be_bytes());
    assert_eq!(
        response, expected,
        "bravo response, offset and zero throttle"
    );
    assert!(Instant::now() < deadline);

    // A second connection with alpha's label must see the first connection's
    // debt. The first alpha request had acks=0, but it was already charged.
    send(&mut third, &a_again).await?;
    assert!(started_request(&mut started, 0).await? < deadline);
    let _second_alpha_deadline = produce_deadline(&mut deadlines).await?;
    let alpha_response =
        tokio::time::timeout(Duration::from_secs(2), receive(&mut third)).await??;
    let field = alpha_response
        .len()
        .checked_sub(4)
        .ok_or("short response")?;
    let alpha_throttle = i32::from_be_bytes(alpha_response[field..].try_into()?);
    let standalone_ms = i32::try_from((a_again.len() - 1) * 40)?;
    assert!(
        alpha_throttle > standalone_ms + 1_000,
        "second alpha connection did not retain first connection's debt"
    );
    let mut alpha_expected = fixture("produce-v3-acks1", "response").await?;
    assert_eq!(&alpha_expected[25..33], &0i64.to_be_bytes());
    alpha_expected[25..33].copy_from_slice(&2i64.to_be_bytes());
    let throttle_field = alpha_expected.len() - 4;
    alpha_expected[throttle_field..].copy_from_slice(&alpha_throttle.to_be_bytes());
    assert_eq!(
        alpha_response, alpha_expected,
        "second alpha response and offset"
    );
    assert!(Instant::now() < deadline);
    assert!(
        started.try_recv().is_err(),
        "alpha's metadata reached the handler during mute"
    );
    let reply = tokio::time::timeout(
        Duration::from_secs(6),
        receive_after_mute(&mut first, deadline),
    )
    .await??;
    assert_eq!(&reply[..4], &77i32.to_be_bytes());
    assert!(started_request(&mut started, 3).await? >= deadline);

    transport.shutdown().await?;
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_zero_mutes_a_pipelined_request_then_resumes() -> Result {
    for version in [3, 10, 13] {
        let root = scratch()?;
        let router = Arc::new(open(&root, 1_000).await?);
        let Observations {
            handler,
            mut started,
            mut deadlines,
        } = observing(&router, false);
        let mut transport = Transport::bind(
            ([127, 0, 0, 1], 0).into(),
            transport::Config::default(),
            handler,
        )
        .await?;
        let mut socket = TcpStream::connect(transport.local_addr()).await?;
        send(
            &mut socket,
            &fixture(&format!("produce-v{version}-acks0"), "request").await?,
        )
        .await?;
        started_request(&mut started, 0).await?;
        let deadline = produce_deadline(&mut deadlines).await?;
        send(&mut socket, &metadata_request()).await?;
        let response = tokio::time::timeout(
            Duration::from_secs(4),
            receive_after_mute(&mut socket, deadline),
        )
        .await??;
        assert_eq!(&response[..4], &77i32.to_be_bytes());
        assert!(
            started_request(&mut started, 3).await? >= deadline,
            "Metadata reached the handler before resume (version {version})"
        );
        transport.shutdown().await?;
        router.shutdown().await?;
        fs::remove_dir_all(root)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throttle_response_is_prompt_and_pipelined_response_follows_resume() -> Result {
    for version in [3, 5, 6] {
        let root = scratch()?;
        let router = Arc::new(open(&root, 1_000).await?);
        let Observations {
            handler,
            mut started,
            mut deadlines,
        } = observing(&router, false);
        let mut transport = Transport::bind(
            ([127, 0, 0, 1], 0).into(),
            transport::Config::default(),
            handler,
        )
        .await?;
        let mut socket = TcpStream::connect(transport.local_addr()).await?;
        send(
            &mut socket,
            &fixture(&format!("produce-v{version}-acks1"), "request").await?,
        )
        .await?;
        started_request(&mut started, 0).await?;
        let deadline = produce_deadline(&mut deadlines).await?;
        let response =
            tokio::time::timeout(Duration::from_millis(500), receive(&mut socket)).await??;
        assert!(
            Instant::now() < deadline,
            "v{version} response was delayed through the mute"
        );
        assert_eq!(
            i32::from_be_bytes(response[response.len() - 4..].try_into()?),
            1_000
        );
        send(&mut socket, &metadata_request()).await?;
        let response = tokio::time::timeout(
            Duration::from_secs(4),
            receive_after_mute(&mut socket, deadline),
        )
        .await??;
        assert_eq!(&response[..4], &77i32.to_be_bytes());
        assert!(
            started_request(&mut started, 3).await? >= deadline,
            "Metadata reached the handler before resume (version {version})"
        );
        transport.shutdown().await?;
        router.shutdown().await?;
        fs::remove_dir_all(root)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_interrupts_a_muted_connection() -> Result {
    let root = scratch()?;
    let router = Arc::new(open(&root, 3_000).await?);
    let Observations {
        handler,
        mut started,
        mut deadlines,
    } = observing(&router, false);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::default(),
        handler,
    )
    .await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, &fixture("produce-v6-acks1", "request").await?).await?;
    started_request(&mut started, 0).await?;
    let deadline = produce_deadline(&mut deadlines).await?;
    let response = tokio::time::timeout(Duration::from_millis(500), receive(&mut socket)).await??;
    assert_eq!(
        i32::from_be_bytes(response[response.len() - 4..].try_into()?),
        3_000
    );
    assert!(deadline.saturating_duration_since(Instant::now()) >= Duration::from_secs(1));
    send(&mut socket, &metadata_request()).await?;
    let mut byte = [0u8; 1];
    match tokio::time::timeout(Duration::from_millis(100), socket.read(&mut byte)).await {
        Err(_) => {}
        Ok(Ok(0)) => return Err("connection closed before shutdown".into()),
        Ok(Ok(_)) => return Err("unexpected response byte during mute".into()),
        Ok(Err(error)) => return Err(error.into()),
    }
    tokio::time::timeout(Duration::from_millis(750), transport.shutdown()).await??;
    assert!(Instant::now() < deadline, "shutdown waited for the pause");
    assert!(
        started.try_recv().is_err(),
        "pipelined Metadata reached the handler"
    );
    match tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte)).await? {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        Ok(_) => return Err("unexpected response after shutdown".into()),
        Err(error) => return Err(error.into()),
    }
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_reply_delivery_consumes_the_absolute_pause() -> Result {
    let root = scratch()?;
    let router = Arc::new(open(&root, 1_500).await?);
    let Observations {
        handler,
        mut started,
        mut deadlines,
    } = observing(&router, true);
    let mut transport = Transport::bind(
        ([127, 0, 0, 1], 0).into(),
        transport::Config::default(),
        handler,
    )
    .await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, &fixture("produce-v6-acks1", "request").await?).await?;
    started_request(&mut started, 0).await?;
    let deadline = produce_deadline(&mut deadlines).await?;
    assert!(deadline.saturating_duration_since(Instant::now()) >= Duration::from_millis(500));
    let response = tokio::time::timeout(Duration::from_secs(3), receive(&mut socket)).await??;
    assert!(
        Instant::now() >= deadline,
        "the reply was not delayed through its deadline"
    );
    assert_eq!(
        i32::from_be_bytes(response[response.len() - 4..].try_into()?),
        1_500
    );
    send(&mut socket, &metadata_request()).await?;
    let response = tokio::time::timeout(Duration::from_millis(750), receive(&mut socket)).await??;
    assert_eq!(&response[..4], &77i32.to_be_bytes());
    assert!(started_request(&mut started, 3).await? >= deadline);
    transport.shutdown().await?;
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    Ok(())
}
