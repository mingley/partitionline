from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text()
s=s.replace('use crate::partitioner::{to_positive, Partitioner, PartitionerBox};','use crate::partitioner::{partition_for_key, to_positive, Partitioner, PartitionerBox, StickyState, StickyTag};')
s=s.replace('struct Pending {\n','struct Pending {\n    sticky: Option<StickyTag>,\n',1)
s=s.replace('struct WorkerHandle {\n','struct WorkerHandle {\n    id: Arc<()>,\n',1)
s=s.replace('    partitioner: Arc<dyn Partitioner>,','    partitioner: Arc<dyn Partitioner>,\n    /// Present only for the explicit opt-in; mutable history is per producer.\n    sticky: Option<parking_lot::Mutex<StickyState>>,',1)
s=s.replace('        self.shared.interceptors.close();','        self.shared.clear_sticky();\n        self.shared.interceptors.close();',1)
s=s.replace('impl Shared {\n','''/// Owns a reserved payload budget until the real enqueue transfers ownership.
/// Dropping an async admission future before enqueue releases its reservation.
struct BufferReservation<'a> {
    shared: &'a Shared,
    bytes: u64,
    active: bool,
}
impl Drop for BufferReservation<'_> {
    fn drop(&mut self) { if self.active { self.shared.release_buffer(self.bytes); } }
}

impl Shared {
    fn clear_sticky(&self) {
        if let Some(state) = &self.sticky { state.lock().clear(); }
    }

    fn retire_sticky(&self, pending: &[Pending]) {
        let Some(state) = &self.sticky else { return; };
        // Lock order is always metadata -> policy; neither lock crosses an await.
        let cluster = self.cluster.lock();
        let mut state = state.lock();
        for p in pending { if let Some(tag) = p.sticky { state.release(tag, 1, &cluster); } }
        drop(state);
        drop(cluster);
        self.cache_nudge.notify_waiters();
    }
''',1)
s=s.replace('            partitioner: cfg.partitioner.arc(),','''            partitioner: cfg.partitioner.arc(),
            sticky: cfg.partitioner.arc().unkeyed_batching_policy().map(|policy| {
                parking_lot::Mutex::new(StickyState::new(policy, cfg.produce_batch_bytes(), cfg.batch_records))
            }),''',1)
# Existing initializer fields opt out; newly added constructors specify their own tags.
s=s.replace('w.data.send(Pending {\n','w.data.send(Pending {\n                    sticky: None,\n',1)
s=s.replace('w.data.try_send(Pending {\n','w.data.try_send(Pending {\n            sticky: None,\n',1)
s=s.replace('            self.ensure_ready(&mut rec, block_deadline).await?;','''            if self.inner.shared.sticky.is_some() {
                self.enqueue_sticky(rec, Some(tx), bytes, block_deadline).await?;
                rxs.push(rx);
                continue;
            }
            self.ensure_ready(&mut rec, block_deadline).await?;''',1)
s=s.replace('        let w = if let Some((p, w)) = self.fast_route(&rec) {','''        if self.inner.shared.sticky.is_some() {
            if !self.inner.shared.try_reserve_buffer(bytes) { return Err(Error::QueueFull); }
            let mut reservation = BufferReservation { shared: &self.inner.shared, bytes, active: true };
            let unkeyed = rec.partition.is_none() && rec.key.is_none();
            let now = Instant::now();
            let topic = Arc::clone(&rec.topic);
            let pending = self.new_pending(rec, None, now);
            match self.sticky_enqueue(pending, unkeyed) {
                Ok(()) => { reservation.active = false; self.inner.shared.note_queued_n(&topic, 1, bytes); return Ok(()); }
                Err((err, pending)) => { if matches!(err, Error::QueueFull) { self.nudge_topic(&pending.rec); } return Err(err); }
            }
        }
        let w = if let Some((p, w)) = self.fast_route(&rec) {''',1)
pos=s.index('    fn fast_route(&self, rec: &ProduceRecord)')
s=s[:pos]+'''    fn new_pending(&self, rec: ProduceRecord, tx: Option<oneshot::Sender<Result<RecordMetadata>>>, now: Instant) -> Pending {
        Pending { sticky: None, rec, tx, seq: None, batch_base_seq: None, batch_len: 1,
            epoch_gen: self.inner.shared.epoch_gen.load(Ordering::SeqCst),
            deadline: now + self.inner.shared.cfg.delivery_timeout, queued_at: now,
            retry: 0, skip_meta_refresh: false, retry_after: now }
    }

    /// The one accounting linearization point. Both route and plan are pure until
    /// the actual try_send succeeds. No callback, network operation, or await is
    /// performed while metadata/policy locks are held.
    fn sticky_enqueue(&self, mut pending: Pending, unkeyed: bool) -> std::result::Result<(), (Error, Pending)> {
        let shared = &self.inner.shared;
        let Some(policy) = &shared.sticky else { return Err((Error::Closed, pending)); };
        let cluster = shared.cluster.lock();
        let mut policy = policy.lock();
        if shared.closed.load(Ordering::SeqCst) { return Err((Error::Closed, pending)); }
        let Some(np) = cluster.partition_count(&pending.rec.topic) else { return Err((Error::QueueFull, pending)); };
        let partition = if unkeyed {
            let Some(route) = policy.route(&pending.rec.topic, &cluster) else { return Err((Error::QueueFull, pending)); };
            route.partition
        } else { pending.rec.partition.unwrap_or_else(|| partition_for_key(pending.rec.key.as_deref().unwrap_or_default(), np)) };
        pending.rec.partition = Some(partition);
        let Ok((node, _)) = cluster.leader(&pending.rec.topic, partition) else { return Err((Error::QueueFull, pending)); };
        let slot = usize::try_from(partition).unwrap_or(0) % shared.cfg.connections.max(1);
        let worker = shared.nodes.lock().get(&node).and_then(|slots| slots.get(slot)).cloned().flatten();
        let Some(worker) = worker else { return Err((Error::QueueFull, pending)); };
        let topic = Arc::clone(&pending.rec.topic);
        let plan = policy.plan(&topic, partition, &worker.id, estimate(&pending), &cluster);
        pending.sticky = Some(plan.tag);
        match worker.data.try_send(pending) {
            Ok(()) => { policy.commit(&topic, partition, &worker.id, unkeyed, plan, &cluster); Ok(()) }
            Err(mpsc::error::TrySendError::Full(mut pending)) => { pending.sticky = None; Err((Error::QueueFull, pending)) }
            Err(mpsc::error::TrySendError::Closed(mut pending)) => { pending.sticky = None; Err((Error::Closed, pending)) }
        }
    }

    async fn enqueue_sticky(&self, rec: ProduceRecord, tx: Option<oneshot::Sender<Result<RecordMetadata>>>, bytes: u64, deadline: Instant) -> Result<()> {
        let shared = &self.inner.shared;
        let unkeyed = rec.partition.is_none() && rec.key.is_none();
        let topic = Arc::clone(&rec.topic);
        let mut pending = self.new_pending(rec, tx, Instant::now());
        // Metadata waits consume the same absolute max_block budget as admission.
        if !shared.cluster.lock().topic_fresh(&topic, shared.cfg.metadata_max_age) {
            let rest = deadline.saturating_duration_since(Instant::now());
            if rest.is_zero() { return Err(Error::Timeout); }
            let _ = partitions_for_timeout(shared, &topic, shared.cfg.request_timeout.min(rest)).await?;
        }
        self.wait_buffer(bytes, deadline).await?;
        let mut reservation = BufferReservation { shared, bytes, active: true };
        loop {
            if shared.closed.load(Ordering::SeqCst) { return Err(Error::Closed); }
            if let Some(err) = peek_meta_err(shared) { return Err(err); }
            let now = Instant::now();
            if now >= deadline { return Err(Error::Timeout); }
            pending.queued_at = now;
            pending.retry_after = now;
            pending.deadline = now + shared.cfg.delivery_timeout;
            match self.sticky_enqueue(pending, unkeyed) {
                Ok(()) => { reservation.active = false; shared.note_queued_n(&topic, 1, bytes); return Ok(()); }
                Err((Error::QueueFull, returned)) => { pending = returned; self.nudge_topic(&pending.rec); }
                Err((err, _)) => return Err(err),
            }
            // Capacity/readiness notifications and the bounded fallback wake are
            // outside the policy lock. Cancellation drops the reserved budget.
            let notified = shared.cache_nudge.notified();
            tokio::pin!(notified);
            let rest = deadline.saturating_duration_since(Instant::now());
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep(rest.min(Duration::from_millis(5))) => {} }
        }
    }

    /// Test hook: bounded topic/cohort/text state, pressure admissions, and total
    /// admitted unkeyed conservative packed bytes (including new-cohort overhead).
    #[doc(hidden)]
    pub fn __test_sticky_state(&self) -> Option<(usize, usize, usize, u64, u64)> {
        self.inner.shared.sticky.as_ref().map(|state| state.lock().counts())
    }

'''+s[pos:]
s=s.replace('        if let Some(h) = self.inner.shared.retry_task.lock().take() {\n            h.abort();\n        }\n    }','        if let Some(h) = self.inner.shared.retry_task.lock().take() {\n            h.abort();\n        }\n        self.inner.shared.clear_sticky();\n    }',1)
s=s.replace('    let worker = Worker {','    let id = Arc::new(());\n    let worker = Worker {\n        id: Arc::clone(&id),',1)
s=s.replace('    Ok(WorkerHandle {\n','    Ok(WorkerHandle {\n        id,',1)
s=s.replace('struct Worker {\n','struct Worker {\n    id: Arc<()>,\n',1)
s=s.replace('                    self.shared.release_buffer(pendings_bytes(&pendings));','                    self.shared.retire_sticky(&pendings);\n                    self.shared.release_buffer(pendings_bytes(&pendings));',1)
s=s.replace('        shared.release_buffer(pendings_bytes(&pendings));','        shared.retire_sticky(&pendings);\n        shared.release_buffer(pendings_bytes(&pendings));',1)
s=s.replace('fn fail_pendings(shared: &Shared, pendings: Vec<Pending>, err: Error) {\n','fn fail_pendings(shared: &Shared, pendings: Vec<Pending>, err: Error) {\n    shared.retire_sticky(&pendings);\n',1)
p.write_text(s)
