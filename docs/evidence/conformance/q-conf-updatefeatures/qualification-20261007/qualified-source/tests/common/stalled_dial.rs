//! Safe loopback fixture for a pending TCP connect, with no network policy changes.

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpSocket, TcpStream};

/// A listener whose unaccepted connection queue is full.
///
/// Linux admits two connections for a listen backlog of one. Holding both
/// connections without accepting makes a subsequent TCP connect remain pending.
/// Construction verifies the actual Tokio TCP connect stalls; unsupported
/// backlog behavior returns an error instead of substituting application delay.
pub struct StalledDial {
    _listener: TcpListener,
    _prefilled: [TcpStream; 2],
    addr: SocketAddr,
    verified_stall: Duration,
}

impl StalledDial {
    /// Fill a loopback accept queue and verify a third TCP dial times out.
    ///
    /// The fixture owns one listener and two prefilled sockets. The probe
    /// connection is cancelled after 50 ms and dropped. All sockets close
    /// when the fixture is dropped; no task, subprocess or external host is used.
    pub async fn new() -> io::Result<Self> {
        let socket = TcpSocket::new_v4()?;
        socket.bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let listener = socket.listen(1)?;
        let addr = listener.local_addr()?;
        let fill = || async {
            tokio::time::timeout(Duration::from_millis(100), TcpStream::connect(addr))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "cannot prefill accept queue")
                })?
        };
        let first = fill().await?;
        let second = fill().await?;
        let started = Instant::now();
        if tokio::time::timeout(Duration::from_millis(50), TcpStream::connect(addr))
            .await
            .is_ok()
        {
            return Err(io::Error::other(
                "loopback fixture did not stall a TCP connect",
            ));
        }
        Ok(Self {
            _listener: listener,
            _prefilled: [first, second],
            addr,
            verified_stall: started.elapsed(),
        })
    }

    /// Address of the stalled loopback listener.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Duration of the verified pending TCP connect before cancellation.
    pub fn verified_stall(&self) -> Duration {
        self.verified_stall
    }
}
