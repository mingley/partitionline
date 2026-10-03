from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text()
a=s.index('    fn sticky_enqueue('); b=s.index('    async fn enqueue_sticky(',a)
t=s[a:b]
t=t.replace('        mut pending: Pending,','        slot: &mut Option<Pending>,').replace('    ) -> std::result::Result<(), (Error, Pending)> {','    ) -> Result<()> {')
t=t.replace('        let shared = &self.inner.shared;','        let Some(pending) = slot.as_mut() else { return Err(Error::Closed); };\n        let shared = &self.inner.shared;',1)
t=t.replace('Err((Error::Closed, pending))','Err(Error::Closed)').replace('Err((Error::QueueFull, pending))','Err(Error::QueueFull)')
t=t.replace('estimate(&pending)','estimate(pending)')
t=t.replace('        match worker.data.try_send(pending) {','        let Some(pending) = slot.take() else { return Err(Error::Closed); };\n        match worker.data.try_send(pending) {')
t=t.replace('                pending.sticky = None;\n                Err(Error::QueueFull)','                pending.sticky = None;\n                *slot = Some(pending);\n                Err(Error::QueueFull)')
t=t.replace('                pending.sticky = None;\n                Err(Error::Closed)','                pending.sticky = None;\n                *slot = Some(pending);\n                Err(Error::Closed)')
s=s[:a]+t+s[b:]
s=s.replace('        let mut pending = self.new_pending(rec, tx, Instant::now());','        let mut pending = Some(self.new_pending(rec, tx, Instant::now()));',1)
s=s.replace('''            pending.queued_at = now;
            pending.retry_after = now;
            pending.deadline = now + shared.cfg.delivery_timeout;
            match self.sticky_enqueue(pending, unkeyed) {''','''            let current = pending.as_mut().ok_or(Error::Closed)?;
            current.queued_at = now;
            current.retry_after = now;
            current.deadline = now + shared.cfg.delivery_timeout;
            match self.sticky_enqueue(&mut pending, unkeyed) {''',1)
s=s.replace('''                Err((Error::QueueFull, returned)) => {
                    pending = returned;
                    self.nudge_topic(&pending.rec);
                }
                Err((err, _)) => return Err(err),''','''                Err(Error::QueueFull) => { if let Some(current) = &pending { self.nudge_topic(&current.rec); } }
                Err(err) => return Err(err),''',1)
s=s.replace('''            let pending = self.new_pending(rec, None, now);
            match self.sticky_enqueue(pending, unkeyed) {''','''            let mut pending = Some(self.new_pending(rec, None, now));
            match self.sticky_enqueue(&mut pending, unkeyed) {''',1)
s=s.replace('''                Err((err, pending)) => {
                    if matches!(err, Error::QueueFull) {
                        self.nudge_topic(&pending.rec);
                    }
                    return Err(err);
                }''','''                Err(err) => {
                    if matches!(err, Error::QueueFull) { if let Some(current) = &pending { self.nudge_topic(&current.rec); } }
                    return Err(err);
                }''',1)
p.write_text(s)
