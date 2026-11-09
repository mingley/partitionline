//! Isolated observation of a closed peer during a controlled transport pause.
use partitionline_broker::transport::{Config, ExchangeControl, ExchangeOutcome, Handler, Peer, Transport};
use std::{io, sync::Arc, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, time::timeout};

struct Paused;

impl Handler for Paused {
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
        let until = std::time::Instant::now()
            .checked_add(Duration::from_secs(10))
            .ok_or_else(|| io::Error::other("deadline overflow"))?;
        ExchangeControl::with_resume_at(
            ExchangeOutcome { response: Some(b"ok".to_vec()), request, spare },
            until,
        ).map_err(io::Error::other)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_muted_peer_retains_the_one_connection_slot_until_shutdown()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let limit = Duration::from_secs(3);
    let config = Config::new(1, 1, 1024, 1024, limit, limit, limit)?;
    let mut transport = Transport::bind(([127, 0, 0, 1], 0).into(), config, Arc::new(Paused)).await?;
    let mut first = TcpStream::connect(transport.local_addr()).await?;
    first.write_all(&1i32.to_be_bytes()).await?;
    first.write_all(b"x").await?;
    assert_eq!(timeout(limit, first.read_i32()).await??, 2);
    let mut body = [0u8; 2];
    timeout(limit, first.read_exact(&mut body)).await??;
    assert_eq!(&body, b"ok");
    first.shutdown().await?;
    drop(first);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut second = TcpStream::connect(transport.local_addr()).await?;
    let mut byte = [0u8; 1];
    match timeout(Duration::from_secs(2), second.read(&mut byte)).await? {
        Ok(0) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
        other => return Err(format!("expected rejected socket, got {other:?}").into()),
    }
    let report = timeout(limit, transport.shutdown()).await??;
    assert_eq!(report.accepted_connections, 1);
    assert_eq!(report.overload_rejections, 1);
    assert_eq!(report.shutdown_connections, 1);
    Ok(())
}
