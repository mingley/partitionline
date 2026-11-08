
#[cfg(all(test, feature = "tls"))]
mod frame_network_microbench {
    use super::{frame_mut, new_frame, read_frame, Config, ReadAhead};
    use crate::security::tls::{Acceptor, ClientAuth, Limits};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::Instant;

    // This adapter intentionally fragments successful reads to one byte for
    // the pathological case, regardless of actual TCP segmentation.
    struct Limited<S> { inner: S, one_byte: bool }
    impl<S: AsyncRead + Unpin> AsyncRead for Limited<S> {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.one_byte && out.remaining() > 1 {
                let mut byte = [0u8; 1];
                let mut limited = ReadBuf::new(&mut byte);
                match Pin::new(&mut self.inner).poll_read(cx, &mut limited) {
                    Poll::Ready(Ok(())) => { out.put_slice(limited.filled()); Poll::Ready(Ok(())) }
                    Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                    Poll::Pending => Poll::Pending,
                }
            } else {
                Pin::new(&mut self.inner).poll_read(cx, out)
            }
        }
    }

    async fn case<C, S>(label: &str, mut client: C, server: S, length: usize, one_byte: bool, samples: usize)
    where
        C: AsyncRead + AsyncWrite + Unpin,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let warmup = 8usize;
        let total = warmup + samples;
        let mut server = Limited { inner: server, one_byte };
        let config = Config::new(
            1, 1, length, 1024,
            Duration::from_secs(30), Duration::from_secs(30), Duration::from_secs(30),
        ).unwrap();
        let receiver = async move {
            let mut ahead = ReadAhead::new();
            let mut spare = new_frame();
            for _ in 0..total {
                // Model the pipelined path with the real length prefix already
                // read into ReadAhead, then read the body through the socket.
                let mut prefix = [0u8; 4];
                server.read_exact(&mut prefix).await.unwrap();
                assert_eq!(i32::from_be_bytes(prefix), length as i32);
                frame_mut(&mut ahead.bytes).extend_from_slice(&prefix);
                spare = read_frame(
                    &mut server, config, Instant::now() + Duration::from_secs(30),
                    &mut ahead, spare,
                ).await.unwrap_or_else(|_| panic!("framed network read failed"));
                assert_eq!(spare.len(), length);
                assert!(spare.iter().all(|&byte| byte == 0x5a));
                server.inner.write_all(&[0x42]).await.unwrap();
                server.inner.flush().await.unwrap();
            }
        };
        let sender = async move {
            let mut frame = Vec::with_capacity(length + 4);
            frame.extend_from_slice(&(length as i32).to_be_bytes());
            frame.extend(std::iter::repeat_n(0x5a, length));
            let mut times = Vec::with_capacity(samples);
            let mut started = std::time::Instant::now();
            for i in 0..total {
                if i == warmup { started = std::time::Instant::now(); }
                let one = std::time::Instant::now();
                client.write_all(&frame).await.unwrap();
                client.flush().await.unwrap();
                let mut ack = [0u8; 1];
                client.read_exact(&mut ack).await.unwrap();
                assert_eq!(ack[0], 0x42);
                if i >= warmup { times.push(one.elapsed().as_nanos()); }
            }
            let elapsed = started.elapsed().as_nanos();
            times.sort_unstable();
            let p = |pc: usize| times[(times.len() - 1) * pc / 100];
            let variant = std::env::var("PL_FRAME_NETWORK_VARIANT").unwrap_or_default();
            println!("frame-network variant={variant} case={label} frames={samples} bytes={length} elapsed_ns={elapsed} p50_ns={} p95_ns={} p99_ns={}", p(50), p(95), p(99));
        };
        let (_, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(sender, receiver)
        }).await.unwrap();
    }

    async fn connected_tcp() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let client = client.unwrap();
        let server = accepted.unwrap().0;
        client.set_nodelay(true).unwrap();
        server.set_nodelay(true).unwrap();
        (client, server)
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "manual paired frame-over-loopback measurement"]
    async fn measure_frame_over_network() {
        for (label, length, one_byte, samples) in [
            ("tcp-256k", 256 * 1024usize, false, 128usize),
            ("tcp-4096-one", 4096, true, 256),
        ] {
            let (client, server) = connected_tcp().await;
            case(label, client, server, length, one_byte, samples).await;
        }
        for (label, length, one_byte, samples) in [
            ("tls-tcp-256k", 256 * 1024usize, false, 128usize),
            ("tls-tcp-4096-one", 4096, true, 256),
        ] {
            let acceptor = Acceptor::new(
                vec![include_bytes!("../tests/fixtures/tls/server1.cert.der").to_vec()],
                include_bytes!("../tests/fixtures/tls/server1.key.der").to_vec(),
                ClientAuth::ServerOnly, Limits::default(),
            ).unwrap();
            let mut roots = rustls::RootCertStore::empty();
            roots.add(rustls::pki_types::CertificateDer::from(
                include_bytes!("../tests/fixtures/tls/ca1.cert.der").to_vec(),
            )).unwrap();
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(rustls::DEFAULT_VERSIONS).unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            let (client, server) = connected_tcp().await;
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            let acceptor = tokio_rustls::TlsAcceptor::from(acceptor.snapshot().server.clone());
            let name = rustls::pki_types::ServerName::try_from("localhost".to_owned()).unwrap();
            let (client, server) = tokio::join!(
                connector.connect(name, client),
                acceptor.accept(server),
            );
            case(label, client.unwrap(), server.unwrap(), length, one_byte, samples).await;
        }
    }
}
