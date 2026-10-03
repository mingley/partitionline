from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/partitioner.rs')
s=p.read_text()
s=s.replace('''    pub max_topic_bytes: usize,
    /// Reproducible draw seed''','''    pub max_topic_bytes: usize,
    /// Reserved admission slots plus accepted records, clamped to 1..=100,000.
    /// The bound includes empty/null records and unlimited payload-byte budgets.
    pub max_pending_records: usize,
    /// Reproducible draw seed''',1)
s=s.replace('''            max_topic_bytes: 256 * 1024,
            seed:''','''            max_topic_bytes: 256 * 1024,
            max_pending_records: 100_000,
            seed:''',1)
s=s.replace('''            max_topic_bytes: config.max_topic_bytes.min(256 * 1024),
            ..config''','''            max_topic_bytes: config.max_topic_bytes.min(256 * 1024),
            max_pending_records: config.max_pending_records.clamp(1, 100_000),
            ..config''',1)
p.write_text(s)
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text()
s=s.replace('''struct Pending {
    sticky: Option<StickyTag>,''','''/// A producer-wide record-slot bound, independent of payload byte accounting.
/// Fresh per Producer, including when cloned configs share a partitioner Arc.
struct StickyRecordBudget {
    limit: usize,
    used: AtomicUsize,
    nudge: Notify,
}

impl StickyRecordBudget {
    fn new(limit: usize) -> Self { Self { limit: limit.clamp(1, 100_000), used: AtomicUsize::new(0), nudge: Notify::new() } }
    fn acquire(self: &Arc<Self>) -> Option<StickyRecordPermit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            if used >= self.limit { return None; }
            match self.used.compare_exchange_weak(used, used + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(StickyRecordPermit { budget: Arc::clone(self) }),
                Err(current) => used = current,
            }
        }
    }
}

/// Non-Clone: one reservation moves with Pending through every owner and retry.
struct StickyRecordPermit { budget: Arc<StickyRecordBudget> }
impl Drop for StickyRecordPermit {
    fn drop(&mut self) { let _ = self.budget.used.fetch_sub(1, Ordering::AcqRel); self.budget.nudge.notify_waiters(); }
}

struct Pending {
    sticky_record: Option<StickyRecordPermit>,
    sticky: Option<StickyTag>,''',1)
s=s.replace('''    sticky: Option<parking_lot::Mutex<StickyState>>,
    producer_id:''','''    sticky: Option<parking_lot::Mutex<StickyState>>,
    sticky_records: Option<Arc<StickyRecordBudget>>,
    producer_id:''',1)
s=s.replace('''        if let Some(state) = &self.sticky {
            state.lock().clear();
        }
    }''','''        if let Some(state) = &self.sticky { state.lock().clear(); }
        if let Some(records) = &self.sticky_records { records.nudge.notify_waiters(); }
    }''',1)
s=s.replace('''        let shared = Arc::new(Shared {''','''        let sticky_policy = cfg.partitioner.arc().unkeyed_batching_policy();
        let shared = Arc::new(Shared {''',1)
s=s.replace('''            sticky: cfg
                .partitioner
                .arc()
                .unkeyed_batching_policy()
                .map(|policy| {''','''            sticky: sticky_policy.map(|policy| {''',1)
s=s.replace('''            producer_id: AtomicI64::new(producer_id),''','''            sticky_records: sticky_policy.map(|policy| Arc::new(StickyRecordBudget::new(policy.max_pending_records))),
            producer_id: AtomicI64::new(producer_id),''',1)
# Every construction starts with no record-slot permit; only opt-in admission attaches one.
s=s.replace('''                    sticky: None,
                    rec,''','''                    sticky_record: None,
                    sticky: None,
                    rec,''',1)
s=s.replace('''            sticky: None,
            rec,''','''            sticky_record: None,
            sticky: None,
            rec,''')
# Plain opt-in admission reserves its one record slot before the byte reservation.
s=s.replace('''        if self.inner.shared.sticky.is_some() {
            if !self.inner.shared.try_reserve_buffer(bytes)''','''        if self.inner.shared.sticky.is_some() {
            let record_permit = self.inner.shared.sticky_records.as_ref().and_then(StickyRecordBudget::acquire).ok_or(Error::QueueFull)?;
            if !self.inner.shared.try_reserve_buffer(bytes)''',1)
s=s.replace('''            let mut pending = Some(self.new_pending(rec, None, now));
            match self.sticky_enqueue''','''            let mut current = self.new_pending(rec, None, now);
            current.sticky_record = Some(record_permit);
            let mut pending = Some(current);
            match self.sticky_enqueue''',1)
# Async capacity wait keeps the original max_block and has no policy mutation.
index=s.index('    async fn enqueue_sticky(')
s=s[:index]+'''    async fn wait_sticky_record(&self, deadline: Instant) -> Result<StickyRecordPermit> {
        let shared = &self.inner.shared;
        let records = shared.sticky_records.as_ref().ok_or(Error::Closed)?;
        loop {
            if shared.closed.load(Ordering::SeqCst) { return Err(Error::Closed); }
            let now = Instant::now();
            if now >= deadline { return Err(Error::Timeout); }
            if let Some(permit) = records.acquire() { return Ok(permit); }
            let notified = records.nudge.notified();
            tokio::pin!(notified);
            tokio::select! { _ = notified => {}, _ = tokio::time::sleep(deadline.saturating_duration_since(now).min(Duration::from_millis(5))) => {} }
        }
    }

'''+s[index:]
s=s.replace('''        self.wait_buffer(bytes, deadline).await?;
        let mut reservation = BufferReservation''','''        let record_permit = self.wait_sticky_record(deadline).await?;
        pending.as_mut().ok_or(Error::Closed)?.sticky_record = Some(record_permit);
        self.wait_buffer(bytes, deadline).await?;
        let mut reservation = BufferReservation''',1)
index=s.index('    fn fast_route(')
s=s[:index]+'''    /// Test hook: currently reserved initial-admission slots plus accepted
    /// records, and the hard normalized record capacity. Includes byte-empty
    /// records and slots held by a pre-enqueue future; retries do not reacquire.
    #[doc(hidden)]
    pub fn __test_sticky_records(&self) -> Option<(usize, usize)> {
        self.inner.shared.sticky_records.as_ref().map(|records| (records.used.load(Ordering::Acquire), records.limit))
    }

'''+s[index:]
p.write_text(s)
# Named, exhaustive policy literals in existing new tests receive the unchanged default record cap.
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/tests/sticky_partitioner.rs')
s=p.read_text().replace('''            max_topic_bytes: 1,
            seed:''','''            max_topic_bytes: 1,
            max_pending_records: 100_000,
            seed:''').replace('''            max_topic_bytes: 0,
            seed:''','''            max_topic_bytes: 0,
            max_pending_records: 100_000,
            seed:''').replace('''                max_topic_bytes: 8,
                seed:''','''                max_topic_bytes: 8,
                max_pending_records: 100_000,
                seed:''')
p.write_text(s)
