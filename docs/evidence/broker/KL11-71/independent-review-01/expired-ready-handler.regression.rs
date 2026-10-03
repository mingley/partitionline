// SOURCE-ONLY proposed addition to candidate tests/sasl_sessions.rs.
// Not compiled or executed. Reuses that file's closed fixture helpers.
// This checks the actual Transport path rather than only timeout_at in isolation.
struct ReadyAfterExpiryProbe;
impl Handler for ReadyAfterExpiryProbe {
    type Error = io::Error;
    async fn handle(&self, _: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::other("missing authenticated peer"))
    }
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>)
        -> io::Result<Option<Vec<u8>>> {
        assert!(peer.identity().is_some());
        // A Handler is allowed to perform synchronous work. Timeout cannot
        // preempt it, but its expired result must be discarded after return.
        std::thread::sleep(Duration::from_millis(300));
        Ok(Some(request))
    }
}
#[tokio::test(flavor = "current_thread")]
async fn ready_handler_after_expiry_does_not_emit_expired_reply() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let mut server = renewable_plaintext(
        store.clone(), Arc::new(ReadyAfterExpiryProbe),
        Duration::from_millis(100), Limits::default(),
    ).await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(scram_lifetime(
        &mut socket, Algorithm::Sha256, "user", "pencil", 2, false,
    ).await?, (0, 100));
    let request = header(3, 0, 201, false);
    send(&mut socket, &request).await?;
    let result = receive(&mut socket).await;
    let report = server.shutdown().await?;
    store.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    assert!(result.is_err(), "complete expired application reply was delivered");
    Ok(())
}
