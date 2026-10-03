    async fn observer_addition_during_multichunk_reader_case(live_entries: usize) -> TestResult {
        let storage = StorageLimits {
            record_bytes: 64 * 1024,
            chunk_bytes: 256 * 1024,
            live_entries,
            live_bytes: 1024 * 1024,
            operations: 4096,
            wal_bytes: 8 * 1024 * 1024,
            fetch_bytes: 256 * 1024,
            images: snapshot::Limits::new(
                2 * 1024 * 1024, 64, 1024 * 1024, 64 * 1024, 32 * 1024, 8, 32,
            )?,
        };
        let source = LocalOwner::open_profile(0, 3, 2, 16, storage).await?;
        let peer = LocalOwner::open_profile(1, 3, 2, 16, storage).await?;
        let observer = LocalOwner::open_profile(2, 3, 2, 16, storage).await?;
        let result: TestResult = async {
            tokio::time::sleep(Duration::from_millis(3)).await;
            let jobs = match source.fixture("observer-campaign", Command::Tick).await?? {
                Reply::Votes(jobs) => jobs,
                _ => return Err("actual observer campaign".into()),
            };
            assert_eq!(jobs.len(), 1);
            for job in jobs {
                let Job::Vote(q) = job else { return Err("actual vote job".into()); };
                let response = grant(&peer, q).await?;
                source.fixture("observer-vote", Command::Acknowledge(response)).await??;
            }
            source.fixture("observer-activate", Command::Tick).await??;
            let key = peer.control.config.bootstrap.local().key();
            for _ in 0..2 { owner_append_exchange(&source, &peer, key).await?; }
            for byte in [71, 72] {
                source.fixture("observer-prefix-proposal", Command::Propose(vec![byte; 40 * 1024])).await??;
                for _ in 0..2 { owner_append_exchange(&source, &peer, key).await?; }
            }
            let image = match source.fixture("observer-selected-image", Command::Checkpoint([62;16])).await?? {
                Reply::Descriptor(d) => d,
                _ => return Err("selected image receipt".into()),
            };
            assert!(image.bytes > 64 * 1024);
            assert_eq!(image.base.index, 3);
            let observer_key = observer.control.config.bootstrap.local().key();
            source.fixture("observer-start-addition", Command::Add(observer_key)).await??;
            let feature = match source.fixture("observer-prepare-feature", Command::Prepare(observer_key)).await?? {
                Reply::Job(Some(Job::Feature(q))) => q,
                _ => return Err("actual feature request".into()),
            };
            let feature_reply = match observer.fixture("observer-feature-response", Command::Inbound(feature.leader, Message::Feature(feature))).await?? {
                Reply::Message(m @ Message::FeatureReply(_)) => m,
                _ => return Err("actual feature response".into()),
            };
            source.fixture("observer-consume-feature", Command::Acknowledge(feature_reply)).await??;
            let offer = match source.fixture("observer-prepare-image", Command::Prepare(observer_key)).await?? {
                Reply::Job(Some(Job::Image(q))) => q,
                _ => return Err("actual selected image offer".into()),
            };
            assert_eq!(offer.request.descriptor, image);
            observer.fixture("observer-begin-image", Command::Inbound(offer.context.leader, Message::Begin(offer))).await??;
            let before_busy = source.state().await?;
            let mut offset = 0;
            let mut chunks = 0;
            while offset < image.bytes {
                // Exercise the exact Owner Tick while its real Node image
                // reader is open. This used to make valid admission terminal.
                source.fixture("observer-tick-during-image", Command::Tick).await??;
                assert!(matches!(source.fixture("observer-pending-during-image", Command::Addition(observer_key)).await??, Reply::Addition(None)));
                let chunk = match source.fixture("observer-read-image-chunk", Command::Chunk(offer)).await?? {
                    Reply::Message(m @ Message::Chunk { .. }) => m,
                    _ => return Err("actual image chunk".into()),
                };
                if let Message::Chunk { offset: actual, bytes, .. } = &chunk {
                    assert_eq!(*actual, offset);
                    assert!(!bytes.is_empty());
                    offset = offset.checked_add(bytes.len() as u64).ok_or("offset overflow")?;
                    assert!(offset <= image.bytes);
                }
                assert!(matches!(observer.fixture("observer-apply-image-chunk", Command::Inbound(offer.context.leader, chunk)).await??, Reply::Message(Message::Chunked { .. })));
                chunks += 1;
            }
            assert!(chunks >= 3);
            let after_busy = source.state().await?;
            assert_eq!(after_busy.wal_durable_ops, before_busy.wal_durable_ops);
            assert_eq!(after_busy.last_position, before_busy.last_position);
            assert_eq!(after_busy.committed_end, before_busy.committed_end);
            let finished = match observer.fixture("observer-install-image", Command::Inbound(offer.context.leader, Message::Finish(offer))).await?? {
                Reply::Message(m @ Message::Finished(_)) => m,
                _ => return Err("actual durable image install".into()),
            };
            source.fixture("observer-consume-image-install", Command::Acknowledge(finished)).await??;
            let before_add = source.state().await?;
            source.fixture("observer-tick-after-catchup", Command::Tick).await??;
            if live_entries == 3 {
                // A real exhausted log budget is terminal, rather than being
                // blindly classified as Busy or repeatedly renewing admission.
                assert!(matches!(source.fixture("observer-terminal-log-budget", Command::Addition(observer_key)).await?, Err(Error::Deadline)));
                let after = source.state().await?;
                assert_eq!(after.wal_durable_ops, before_add.wal_durable_ops);
                assert_eq!(after.last_position, before_add.last_position);
                assert_eq!(after.committed_end, before_add.committed_end);
                assert!(after.ready && !after.poisoned);
            } else {
                let change = match source.fixture("observer-added-after-image", Command::Addition(observer_key)).await?? {
                    Reply::Addition(Some(change)) => change,
                    _ => return Err("actual added configuration receipt".into()),
                };
                assert_eq!(change.position.index, 4);
                assert!(!change.committed);
                for target in [&peer, &observer] {
                    let key = target.control.config.bootstrap.local().key();
                    for _ in 0..2 { owner_append_exchange(&source, target, key).await?; }
                }
                assert_eq!(source.state().await?.committed_end, 4);
                assert_eq!(peer.state().await?.committed_end, 4);
                assert_eq!(observer.state().await?.committed_end, 4);
                assert_eq!(observer.state().await?.base_position, image.base);
                assert!(matches!(source.fixture("observer-confirm-new-majority", Command::Change).await??, Reply::Change(r) if r.committed && r.position == change.position));
            }
            Ok(())
        }.await;
        let mut first_cleanup = None;
        for owner in [observer, peer, source] {
            if let Err(error) = owner.finish().await {
                if first_cleanup.is_none() { first_cleanup = Some(error); }
            }
        }
        result?;
        match first_cleanup { Some(error) => Err(error), None => Ok(()) }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observer_addition_retries_real_snapshot_busy_then_commits_new_set() -> TestResult {
        observer_addition_during_multichunk_reader_case(64).await
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observer_addition_keeps_real_log_budget_failure_terminal() -> TestResult {
        observer_addition_during_multichunk_reader_case(3).await
    }
