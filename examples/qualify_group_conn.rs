//! Local framed roundtrips for comparing the coordinator socket hot path.
use partitionline::net::{BrokerConn, Deadline};
use std::error::Error;
use std::io;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    const WARMUP: usize = 1_000;
    const REQUESTS: usize = 10_000;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let mut request = Vec::new();
        loop {
            let size = match socket.read_i32().await {
                Ok(size) => size,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => break,
                Err(error) => return Err(error),
            };
            if !(8..=4096).contains(&size) {
                return Err(io::Error::other("unexpected request frame size"));
            }
            request.resize(usize::try_from(size).map_err(io::Error::other)?, 0u8);
            let _read = socket.read_exact(&mut request).await?;
            let correlation = request
                .get(4..8)
                .ok_or_else(|| io::Error::other("missing correlation id"))?;
            socket.write_i32(4).await?;
            socket.write_all(correlation).await?;
        }
        Ok::<(), io::Error>(())
    });
    let mut conn = BrokerConn::connect(&addr.to_string(), "local-lock-probe", Duration::from_secs(5))
        .await?;
    for _ in 0..WARMUP {
        let _response = conn
            .roundtrip_deadline(3, 0, |_buf| Ok(()), Deadline::from_timeout(Duration::from_secs(5)))
            .await?;
    }
    let started = Instant::now();
    for _ in 0..REQUESTS {
        let _response = conn
            .roundtrip_deadline(3, 0, |_buf| Ok(()), Deadline::from_timeout(Duration::from_secs(5)))
            .await?;
    }
    println!("local_roundtrip requests={REQUESTS} elapsed_ns={}", started.elapsed().as_nanos());
    drop(conn);
    tokio::time::timeout(Duration::from_secs(5), peer).await??;
    Ok(())
}
