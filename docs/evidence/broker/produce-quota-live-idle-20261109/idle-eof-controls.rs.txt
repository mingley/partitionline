//! Controls for idle peer closure and prefix retention during a pause.
use partitionline_broker::transport::{Config, ExchangeControl, ExchangeOutcome, Handler, Peer, Transport};
use std::{io, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, time::{sleep, timeout}};

struct PauseFirst {
    pause: Duration,
    calls: AtomicUsize,
}

impl Handler for PauseFirst {
    type Error = io::Error;

    async fn handle(&self, _request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::other("controlled exchange required"))
    }

    async fn exchange_with_control(
        &self,
        _peer: &Peer,
        request: Vec<u8>,
        spare: Vec<u8>,
    ) -> io::Result<ExchangeControl> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = ExchangeOutcome {
            response: Some(request.clone()),
            request,
            spare,
        };
        if call == 0 {
            let until = std::time::Instant::now()
                .checked_add(self.pause)
                .ok_or_else(|| io::Error::other("deadline overflow"))?;
            ExchangeControl::with_resume_at(outcome, until).map_err(io::Error::other)
        } else {
            Ok(ExchangeControl::new(outcome))
        }
    }
}

async fn send(socket: &mut TcpStream, body: u8) -> io::Result<()> {
    socket.write_all(&1i32.to_be_bytes()).await?;
    socket.write_all(&[body]).await
}

async fn receive(socket: &mut TcpStream) -> io::Result<u8> {
    if socket.read_i32().await? != 1 {
        return Err(io::Error::other("unexpected frame length"));
    }
    socket.read_u8().await
}

fn config() -> Result<Config, partitionline_broker::transport::Error> {
    let limit = Duration::from_secs(5);
    Config::new(1, 1, 1024, 1024, limit, limit, limit)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_closed_peer_releases_its_connection_slot_before_resume(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler = Arc::new(PauseFirst { pause: Duration::from_secs(10), calls: AtomicUsize::new(0) });
    let mut transport = Transport::bind(([127, 0, 0, 1], 0).into(), config()?, handler).await?;
    let mut first = TcpStream::connect(transport.local_addr()).await?;
    send(&mut first, b'a').await?;
    assert_eq!(timeout(Duration::from_secs(2), receive(&mut first)).await??, b'a');
    first.shutdown().await?;
    drop(first);

    let admitted = timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(mut next) = TcpStream::connect(transport.local_addr()).await {
                if send(&mut next, b'b').await.is_ok() {
                    if let Ok(Ok(b'b')) = timeout(Duration::from_millis(100), receive(&mut next)).await {
                        break;
                    }
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    }).await;
    let report = timeout(Duration::from_secs(2), transport.shutdown()).await??;
    assert!(admitted.is_ok(), "no replacement admitted while first peer was closed");
    assert!(report.peer_closes >= 1, "idle EOF was not reported");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_prefix_is_retained_until_after_the_pause(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler = Arc::new(PauseFirst { pause: Duration::from_secs(3), calls: AtomicUsize::new(0) });
    let mut transport = Transport::bind(([127, 0, 0, 1], 0).into(), config()?, handler.clone()).await?;
    let mut socket = TcpStream::connect(transport.local_addr()).await?;
    send(&mut socket, b'a').await?;
    assert_eq!(timeout(Duration::from_secs(2), receive(&mut socket)).await??, b'a');
    socket.write_all(&[0]).await?;
    sleep(Duration::from_millis(50)).await;
    socket.write_all(&[0, 0, 1, b'b']).await?;
    let mut byte = [0u8; 1];
    assert!(timeout(Duration::from_millis(200), socket.read(&mut byte)).await.is_err(),
            "a response arrived during the pause");
    assert_eq!(timeout(Duration::from_secs(5), receive(&mut socket)).await??, b'b');
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
    transport.shutdown().await?;
    Ok(())
}
