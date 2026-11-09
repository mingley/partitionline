from pathlib import Path

path = Path("src/producer.rs")
source = path.read_text(encoding="utf-8")
anchor = "    /// Test hook: sleeping reconnect timers.\n"
hook = """    /// Test-only: stage an old-generation rollback against current sequence state.
    #[cfg(stale_identity_witness)]
    #[doc(hidden)]
    pub fn __test_stale_identity_rollback(&self, topic: &str, partition: i32) -> i32 {
        let shared = &self.inner.shared;
        let name: Arc<str> = Arc::from(topic);
        let mut stale = self.new_pending(
            ProduceRecord::to(Arc::clone(&name)).partition(partition),
            None,
            Instant::now(),
        );
        stale.seq = Some(0);
        stale.batch_base_seq = Some(0);
        stale.batch_len = 1;
        stale.epoch_gen = 0;
        {
            let mut sequences = shared.seqs.lock();
            let _ = sequences.entry(Arc::clone(&name)).or_default().insert(partition, 1);
            shared.epoch_gen.store(1, Ordering::SeqCst);
        }
        let groups = [(Arc::clone(&name), partition, vec![stale])];
        rollback_sequences(
            &groups,
            shared.producer_id.load(Ordering::SeqCst),
            &shared.seqs,
            &shared.epoch_gen,
        );
        shared.seqs.lock().get(name.as_ref())
            .and_then(|parts| parts.get(&partition)).copied().unwrap_or(-1)
    }

"""
if source.count(anchor) != 1:
    raise SystemExit("unexpected hook anchor")
path.write_text(source.replace(anchor, hook + anchor), encoding="utf-8")
