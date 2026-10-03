from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/partitioner.rs')
s=p.read_text().replace('    pub(crate) fn route_current(&self, topic: &str, route: StickyRoute, cluster: &Cluster) -> bool { self.route(topic, cluster) == Some(route) }\n','')
s=s.replace('    pub(crate) fn seal(&mut self, tag: StickyTag)', '''    pub(crate) fn owned_by(&self, tag: StickyTag, worker: &Arc<()>) -> bool {
        tag.id == 0 || self.cohorts.get(&tag.id).is_none_or(|c| Arc::ptr_eq(&c.worker, worker))
    }
    pub(crate) fn rebind(&mut self, tag: StickyTag, worker: &Arc<()>) {
        if let Some(c) = self.cohorts.get_mut(&tag.id) { c.worker = Arc::clone(worker); c.sealed = true; }
    }
    pub(crate) fn seal(&mut self, tag: StickyTag)''')
s=s.replace('if let Some(c) = self.cohorts.remove(&tag.id) { self.maybe_rotate(&c.topic, cluster); }','if let Some(c) = self.cohorts.remove(&tag.id) { if self.topics.get(&c.topic).is_some_and(|t| t.lifetime == c.lifetime) { self.maybe_rotate(&c.topic, cluster); } }')
s=s.replace('let topic = self.cohorts.get_mut(&tag.id).map(|c| { c.sealed = true; Arc::clone(&c.topic) });\n        if let Some(topic) = topic { self.maybe_rotate(&topic, cluster); }','let row = self.cohorts.get_mut(&tag.id).map(|c| { c.sealed = true; (Arc::clone(&c.topic), c.lifetime) });\n        if let Some((topic, lifetime)) = row { if self.topics.get(&topic).is_some_and(|t| t.lifetime == lifetime) { self.maybe_rotate(&topic, cluster); } }')
p.write_text(s)
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text()
# Retry routing only opt-in. Existing default implementation and reservations remain untouched.
needle='    if p.rec.partition.is_none() {\n        if let Some(np) = shared.cluster.lock()'
index=s.index(needle,s.index('async fn retry_one('))
s=s[:index]+'''    if shared.sticky.is_some() {
        retry_sticky_one(shared, &mut guard).await;
        return;
    }
'''+s[index:]
index=s.index('\nstruct Worker {')
s=s[:index]+'''
/// Retry records stay pinned to their admitted partition and never enter the
/// accounting path again. Rebinding a cohort wakes displaced workers so a
/// metadata change between member transfers cannot strand half of a cohort.
async fn retry_sticky_one(shared: &Arc<Shared>, guard: &mut RetryGuard<'_>) {
    loop {
        let Some(pending) = guard.p.as_ref() else { return; };
        let deadline = pending.deadline;
        if shared.closed.load(Ordering::SeqCst) || Instant::now() >= deadline { return; }
        let Some(partition) = pending.rec.partition else { return; };
        let topic = Arc::clone(&pending.rec.topic);
        let outcome = {
            let cluster = shared.cluster.lock();
            let Some(policy) = &shared.sticky else { return; };
            let mut policy = policy.lock();
            if let Ok((node, _)) = cluster.leader(&topic, partition) {
                let slot = usize::try_from(partition).unwrap_or(0) % shared.cfg.connections.max(1);
                try_nudge_slot(&shared.connect_tx, node, slot);
                let worker = shared.nodes.lock().get(&node).and_then(|slots| slots.get(slot)).cloned().flatten();
                if let Some(worker) = worker {
                    if let Some(pending) = guard.p.take() {
                        let tag = pending.sticky;
                        match worker.data.try_send(pending) {
                            Ok(()) => { if let Some(tag) = tag { policy.rebind(tag, &worker.id); } Some(Ok(())) }
                            Err(mpsc::error::TrySendError::Full(pending)) => { guard.p = Some(pending); None }
                            Err(mpsc::error::TrySendError::Closed(pending)) => { guard.p = Some(pending); Some(Err(())) }
                        }
                    } else { Some(Ok(())) }
                } else { None }
            } else { None }
        };
        match outcome {
            Some(Ok(())) => { shared.cache_nudge.notify_waiters(); return; }
            Some(Err(())) => { if let Some(pending) = guard.p.take() { fail_pendings(shared, vec![pending], Error::Closed); } return; }
            None => {}
        }
        let notified = shared.cache_nudge.notified();
        tokio::pin!(notified);
        let rest = deadline.saturating_duration_since(Instant::now());
        tokio::select! { _ = notified => {}, _ = tokio::time::sleep(rest.min(Duration::from_millis(10))) => {} }
    }
}
'''+s[index:]
# Seal all queued records when abandoning an actor, before any retry transfer.
s=s.replace('        if !self.shared.closed.load(Ordering::SeqCst) {\n            while let Some(inf)', '''        if let Some(policy) = &self.shared.sticky {
            let mut policy = policy.lock();
            for p in &self.pending { if let Some(tag) = p.sticky { policy.seal(tag); } }
        }
        if !self.shared.closed.load(Ordering::SeqCst) {
            while let Some(inf)''',1)
# New worker helpers before purge; extract only one cohort, do not merge equal partition draws.
index=s.index('    fn purge_expired_pending(&mut self)')
s=s[:index]+'''    fn sticky_complete(&self) -> bool {
        let Some(policy) = &self.shared.sticky else { return true; };
        let Some(tag) = self.pending.first().and_then(|p| p.sticky) else { return true; };
        if tag.id == 0 { return true; }
        let policy = policy.lock();
        policy.owned_by(tag, &self.id) && self.pending.iter().filter(|p| p.sticky == Some(tag)).count() >= policy.members(tag)
    }

    fn requeue_displaced_sticky(&mut self) {
        let shared = Arc::clone(&self.shared);
        let Some(policy) = &shared.sticky else { return; };
        let policy = policy.lock();
        let mut retained = Vec::with_capacity(self.pending.len());
        let mut displaced = Vec::new();
        for p in std::mem::take(&mut self.pending) {
            if p.sticky.is_some_and(|tag| !policy.owned_by(tag, &self.id)) { displaced.push(p); } else { retained.push(p); }
        }
        self.pending = retained;
        drop(policy);
        if !displaced.is_empty() { self.requeue_pendings(displaced); }
    }

    async fn take_sticky_batch(&mut self) -> Vec<Pending> {
        let shared = Arc::clone(&self.shared);
        loop {
            self.pull_ready();
            self.purge_expired_pending();
            self.requeue_displaced_sticky();
            if self.pending.is_empty() { return Vec::new(); }
            {
                let cluster = shared.cluster.lock();
                let Some(policy) = &shared.sticky else { return Vec::new(); };
                let mut policy = policy.lock();
                // Every committed admission is now visible. Holding the policy
                // lock prevents extending a tail while its actual drain seals it.
                self.pull_ready();
                let Some(tag) = self.pending.first().and_then(|p| p.sticky) else { return Vec::new(); };
                let count = if tag.id == 0 { 1 } else { self.pending.iter().filter(|p| p.sticky == Some(tag)).count() };
                if policy.owned_by(tag, &self.id) && count >= policy.members(tag) {
                    policy.drain(tag, &cluster);
                    if tag.id == 0 { return vec![self.pending.remove(0)]; }
                    let mut batch = Vec::with_capacity(count);
                    let mut retained = Vec::with_capacity(self.pending.len().saturating_sub(count));
                    for p in std::mem::take(&mut self.pending) {
                        if p.sticky == Some(tag) { batch.push(p); } else { retained.push(p); }
                    }
                    self.pending = retained;
                    return batch;
                }
            }
            // A retry may be in transit or expire in another actor. Its terminal
            // callback shrinks membership and wakes this wait. No busy linger loop.
            let notified = shared.cache_nudge.notified();
            tokio::pin!(notified);
            let rest = self.earliest_pending_deadline().map_or(Duration::from_millis(10), |deadline| deadline.saturating_duration_since(Instant::now()));
            tokio::select! {
                n = self.data.recv_many(&mut self.pending, shared.cfg.batch_records.max(1)) => { if n == 0 { return Vec::new(); } }
                _ = notified => {}
                _ = tokio::time::sleep(rest.min(Duration::from_millis(10))) => {}
            }
        }
    }

'''+s[index:]
s=s.replace('        if let Some(first) = self.pending.first() {\n            if let Some(base)', '        if !self.sticky_complete() { return false; }\n        if let Some(first) = self.pending.first() {\n            if let Some(base)',1)
# Sticky exact membership supersedes legacy batch_len after terminal expiry of one member.
s=s.replace('        if let Some(first) = self.pending.first() {\n            if let Some(base)', '        if let Some(first) = self.pending.first().filter(|_| self.shared.sticky.is_none()) {\n            if let Some(base)',1)
s=s.replace('            self.pull_ready();\n            self.purge_expired_pending();\n            if self.pending.is_empty()', '            self.pull_ready();\n            self.purge_expired_pending();\n            self.requeue_displaced_sticky();\n            if self.pending.is_empty()',1)
s=s.replace('            let next_wake = if let Some(rest) = self.throttle.remaining() {','''            let next_wake = if !self.sticky_complete() {
                self.earliest_pending_deadline().map(|deadline| deadline.saturating_duration_since(now))
            } else if let Some(rest) = self.throttle.remaining() {''',1)
s=s.replace('            tokio::select! {\n                biased;\n                n = self.data.recv_many', '''            let wake_shared = Arc::clone(&self.shared);
            tokio::select! {
                biased;
                _ = wake_shared.cache_nudge.notified(), if wake_shared.sticky.is_some() => {}
                n = self.data.recv_many''',1)
s=s.replace('''        let n = take_count(
            &self.pending,
            self.shared.cfg.batch_records,
            self.shared.cfg.produce_batch_bytes(),
        );
        let batch: Vec<Pending> = self.pending.drain(..n).collect();''','''        let batch: Vec<Pending> = if self.shared.sticky.is_some() {
            self.take_sticky_batch().await
        } else {
            let n = take_count(&self.pending, self.shared.cfg.batch_records, self.shared.cfg.produce_batch_bytes());
            self.pending.drain(..n).collect()
        };''',1)
# On every requeue path (including closed socket / before-drain encode faults), stop tail appends.
s=s.replace('    fn requeue_pendings(&mut self, pendings: Vec<Pending>) {\n','''    fn requeue_pendings(&mut self, pendings: Vec<Pending>) {
        if let Some(policy) = &self.shared.sticky {
            let mut policy = policy.lock();
            for p in &pendings { if let Some(tag) = p.sticky { policy.seal(tag); } }
        }
''',1)
p.write_text(s)
