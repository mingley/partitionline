from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/partitioner.rs')
s=p.read_text().replace('members: usize, sealed: bool }','members: usize, sealed: bool, cancelled: bool }')
s=s.replace('self.batch_records == 1 });','self.batch_records == 1, cancelled: false });')
s=s.replace('    pub(crate) fn owned_by(','''    pub(crate) fn cancelled(&self, tag: StickyTag) -> bool { self.cohorts.get(&tag.id).is_some_and(|c| c.cancelled) }
    pub(crate) fn cancel(&mut self, tag: StickyTag) { if let Some(c) = self.cohorts.get_mut(&tag.id) { c.cancelled = true; c.sealed = true; } }
    pub(crate) fn owned_by(''')
# Config batch_bytes==0 means no byte cap; no extra record/admission errors.
s=s.replace('batch_bytes: batch_bytes.max(1)', 'batch_bytes: if batch_bytes == 0 { usize::MAX } else { batch_bytes }')
p.write_text(s)
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text().replace('fn retire_sticky(&self, pending: &[Pending])','fn retire_sticky(&self, pending: &[Pending], failed: bool)')
s=s.replace('if let Some(tag) = p.sticky { state.release(tag, 1, &cluster); }','if let Some(tag) = p.sticky { if failed && p.seq.is_some() { state.cancel(tag); } state.release(tag, 1, &cluster); }')
s=s.replace('self.shared.retire_sticky(&pendings);','self.shared.retire_sticky(&pendings, false);')
s=s.replace('shared.retire_sticky(&pendings);','shared.retire_sticky(&pendings, false);')
s=s.replace('fn fail_pendings(shared: &Shared, pendings: Vec<Pending>, err: Error) {\n    shared.retire_sticky(&pendings, false);','fn fail_pendings(shared: &Shared, pendings: Vec<Pending>, err: Error) {\n    shared.retire_sticky(&pendings, true);')
s=s.replace('            let mut policy = policy.lock();\n            if let Ok((node, _))', '''            let mut policy = policy.lock();
            if guard.p.as_ref().and_then(|p| p.sticky).is_some_and(|tag| policy.cancelled(tag)) { return; }
            if let Ok((node, _))''',1)
s=s.replace('policy.owned_by(tag, &self.id) && self.pending.iter()', 'policy.cancelled(tag) || (policy.owned_by(tag, &self.id) && self.pending.iter()',1)
s=s.replace('.count() >= policy.members(tag)\n    }','.count() >= policy.members(tag))\n    }',1)
s=s.replace('        for p in std::mem::take(&mut self.pending) {\n            if p.sticky.is_some_and(|tag| !policy.owned_by(tag, &self.id))', '        for p in std::mem::take(&mut self.pending) {\n            if p.sticky.is_some_and(|tag| !policy.cancelled(tag) && !policy.owned_by(tag, &self.id))',1)
# Cancellation is evaluated at fire before collecting a shortened sequenced cohort.
s=s.replace('                if policy.owned_by(tag, &self.id) && count >= policy.members(tag) {','''                if policy.cancelled(tag) || (policy.owned_by(tag, &self.id) && count >= policy.members(tag)) {''',1)
s=s.replace('                    policy.drain(tag, &cluster);\n                    if tag.id == 0', '                    let cancelled = policy.cancelled(tag);\n                    policy.drain(tag, &cluster);\n                    if tag.id == 0',1)
s=s.replace('                    self.pending = retained;\n                    return batch;', '''                    self.pending = retained;
                    if cancelled {
                        drop(policy);
                        drop(cluster);
                        fail_pendings(&shared, batch, Error::Timeout);
                        continue;
                    }
                    return batch;''',1)
p.write_text(s)
