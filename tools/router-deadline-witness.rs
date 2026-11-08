
// Diagnostic-only tests appended identically to the pinned baseline and candidate.
// They do not alter the production Router or the original failing test.
mod controlled_transport_deadline_witness {
    use super::*;
    use partitionline_broker::transport::{ExchangeOutcome, Handler, Peer};
    use tokio::sync::watch;

    #[derive(Clone, Copy)]
    enum Gate {
        BeforeProduceDispatch,
        AfterProduceCompletion,
    }

    struct ObservedRouter {
        router: Arc<Router>,
        gate: Gate,
        fetch_pending: watch::Sender<bool>,
        produce_gate_reached: watch::Sender<bool>,
    }

    impl Handler for ObservedRouter {
        type Error = metadata::Error;
        async fn handle(&self, request: Vec<u8>) -> Result<Option<Vec<u8>>, Self::Error> {
            self.router.dispatch(request).await
        }
        async fn exchange_request(
            &self,
            peer: &Peer,
            request: Vec<u8>,
            spare: Vec<u8>,
        ) -> Result<ExchangeOutcome, Self::Error> {
            let is_fetch = request.get(..2) == Some(&[0, 1]);
            let is_produce = request.get(..2) == Some(&[0, 0]);
            if is_produce && matches!(self.gate, Gate::BeforeProduceDispatch) {
                self.produce_gate_reached.send_replace(true);
                return std::future::pending::<Result<ExchangeOutcome, Self::Error>>().await;
            }
            let inner = Handler::exchange_request(self.router.as_ref(), peer, request, spare);
            tokio::pin!(inner);
            let output = std::future::poll_fn(|cx| {
                let result = std::future::Future::poll(inner.as_mut(), cx);
                if is_fetch && result.is_pending() {
                    self.fetch_pending.send_replace(true);
                }
                result
            }).await?;
            if is_produce && matches!(self.gate, Gate::AfterProduceCompletion) {
                let ack = output.response.as_ref().expect("Produce ack");
                exact_ack(ack);
                self.produce_gate_reached.send_replace(true);
                std::future::pending::<()>().await;
            }
            Ok(output)
        }
    }

    fn exact_ack(ack: &[u8]) {
        let mut expected = Vec::new();
        expected.extend_from_slice(&77i32.to_be_bytes());
        expected.extend_from_slice(&1i32.to_be_bytes());
        string(&mut expected, "alpha");
        expected.extend_from_slice(&1i32.to_be_bytes());
        expected.extend_from_slice(&0i32.to_be_bytes());
        expected.extend_from_slice(&0i16.to_be_bytes());
        expected.extend_from_slice(&0i64.to_be_bytes());
        expected.extend_from_slice(&(-1i64).to_be_bytes());
        expected.extend_from_slice(&0i32.to_be_bytes());
        assert_eq!(ack, expected.as_slice(), "complete Produce v3 acknowledgement");
    }

    async fn event(rx: &mut watch::Receiver<bool>) -> Result<(), Box<dyn StdError>> {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !*rx.borrow_and_update() {
                rx.changed().await?;
            }
            Ok::<_, watch::error::RecvError>(())
        }).await??;
        Ok(())
    }

    async fn run(gate: Gate, label: &str) -> Result<(), Box<dyn StdError>> {
        let root = scratch(label);
        seed(root.clone(), false, false).await?;
        let mut cfg = common();
        cfg.max_queued_requests = 1;
        let router = Arc::new(open(&root, cfg, fetch::Limits::default()).await?);
        let (fetch_tx, mut fetch_rx) = watch::channel(false);
        let (produce_tx, mut produce_rx) = watch::channel(false);
        let handler = Arc::new(ObservedRouter {
            router: Arc::clone(&router),
            gate,
            fetch_pending: fetch_tx,
            produce_gate_reached: produce_tx,
        });
        let config = transport::Config::new(
            4, 4, 128 * 1024, 128 * 1024,
            Duration::from_secs(10), Duration::from_secs(2), Duration::from_secs(10),
        )?;
        let mut transport = Transport::bind(([127, 0, 0, 1], 0).into(), config, handler).await?;
        let mut first = TcpStream::connect(transport.local_addr()).await?;
        send(&mut first, &fetch_request(6, 0, 30000, 1, 10000, &[(0, 10000)])).await?;
        event(&mut fetch_rx).await?;
        println!("WITNESS {label}: real Fetch reached pending");
        barrier(&router).await?;
        println!("WITNESS {label}: pre-probe FIFO barrier completed");
        assert!(matches!(
            router.respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)])).await,
            Err(metadata::Error::QueueFull)
        ), "the pending real Fetch must own the sole data slot");
        let mut byte = [0u8];
        assert_eq!(tokio::time::timeout(Duration::from_secs(10), first.read(&mut byte)).await??, 0);
        barrier(&router).await?;
        println!("WITNESS {label}: first handler timed out and FIFO metadata barrier completed");
        let empty = router.respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)])).await?;
        assert_eq!(fetch_error(&empty), 0);
        assert_eq!(watermark(&empty), 0);
        assert!(records(&empty, 6).is_empty());

        let batch = tokio::task::spawn_blocking(|| read(&base().join("4.3.1/log-batch-0.bin"))).await??;
        let mut second = TcpStream::connect(transport.local_addr()).await?;
        send(&mut second, &produce(&batch, 0)).await?;
        event(&mut produce_rx).await?;
        println!("WITNESS {label}: selected Produce gate reached");
        assert_eq!(tokio::time::timeout(Duration::from_secs(10), second.read(&mut byte)).await??, 0);
        let report = transport.shutdown().await?;
        assert_eq!(report.accepted_connections, 2);
        assert_eq!(report.joined_connections, 2);
        assert_eq!(report.handler_deadlines, 2);
        assert_eq!(report.handler_errors, 0);
        assert_eq!(report.worker_failures, 0);
        assert_eq!(report.read_deadlines, 0);
        assert_eq!(report.write_deadlines, 0);
        barrier(&router).await?;

        let before_recovery = router.respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)])).await?;
        assert_eq!(fetch_error(&before_recovery), 0);
        if matches!(gate, Gate::BeforeProduceDispatch) {
            assert_eq!(watermark(&before_recovery), 0);
            assert!(records(&before_recovery, 6).is_empty());
            println!("WITNESS {label}: no append after pre-dispatch timeout");
            let ack = router.respond(produce(&batch, 0)).await?;
            exact_ack(&ack);
        } else {
            assert_eq!(watermark(&before_recovery), 3);
            println!("WITNESS {label}: post-ack timeout left committed watermark 3");
        }
        let mut expected = batch;
        expected[..8].copy_from_slice(&0i64.to_be_bytes());
        expected[12..16].copy_from_slice(&0i32.to_be_bytes());
        let present = router.respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)])).await?;
        assert_eq!(fetch_error(&present), 0);
        assert_eq!(watermark(&present), 3);
        assert_eq!(records(&present, 6), expected);
        router.shutdown().await?;
        drop(router);
        let reopened = open(&root, common(), fetch::Limits::default()).await?;
        let recovered = reopened.respond(fetch_request(6, 0, 0, 0, 10000, &[(0, 10000)])).await?;
        assert_eq!(fetch_error(&recovered), 0);
        assert_eq!(watermark(&recovered), 3);
        assert_eq!(records(&recovered, 6), expected);
        reopened.shutdown().await?;
        drop(reopened);
        clean(root).await?;
        println!("WITNESS {label}: initial slot held, timeout released and actor drained; two handler deadlines with EOF; exact 3-record watermark and bytes persisted across reopen");
        Ok(())
    }

    #[tokio::test]
    async fn controlled_transport_deadline_before_produce_dispatch() -> Result<(), Box<dyn StdError>> {
        run(Gate::BeforeProduceDispatch, "before-produce-dispatch").await
    }

    #[tokio::test]
    async fn controlled_transport_deadline_after_produce_completion() -> Result<(), Box<dyn StdError>> {
        run(Gate::AfterProduceCompletion, "after-produce-completion").await
    }
}
