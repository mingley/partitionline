from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/partitioner.rs')
s=p.read_text()
s=s.replace('    rng: Option<u64>,\n    pub(crate) partition:', '    pub(crate) rng: Option<u64>,\n    pub(crate) partition:',1)
s=s.replace('    pub(crate) tag: StickyTag,\n', '    pub(crate) tag: StickyTag,\n    pub(crate) next_rng: Option<u64>,\n',1)
a=s.index('    fn choose('); b=s.index('    fn identity(',a)
s=s[:a]+'''    /// Uniform over eligible indices: the incomplete modulus bucket is rejected.
    /// Return the state after every attempted draw without mutating the policy.
    fn choose(seed: u64, leaders: &[i32]) -> (i32, u64) {
        let available = leaders.iter().filter(|&&leader| leader >= 0).count();
        let count = if available == 0 { leaders.len() } else { available };
        if count == 0 { return (0, seed); }
        // Kafka partition indices/counts are int32. Ignore an unrepresentable
        // suffix if called with a non-protocol, oversized metadata slice.
        let count = u64::try_from(count.min(2_147_483_647)).unwrap_or(1);
        let domain = 1u64 << 31;
        let accepted_domain = domain - domain % count;
        let mut post = seed;
        let index = loop {
            let (next, random) = Self::draw(post);
            post = next;
            let random = u64::try_from(random).unwrap_or(0);
            if random < accepted_domain { break usize::try_from(random % count).unwrap_or(0); }
        };
        let partition = if available == 0 { i32::try_from(index).unwrap_or(0) }
        else { leaders.iter().enumerate().filter(|(_, leader)| **leader >= 0).nth(index).and_then(|(p, _)| i32::try_from(p).ok()).unwrap_or(0) };
        (partition, post)
    }

'''+s[b:]
a=s.index('        Some(StickyRoute {',s.index('pub(crate) fn route(')); b=s.index('\n    }',a)
s=s[:a]+'''        let drawn = info.is_none().then(|| Self::choose(self.rng, leaders));
        Some(StickyRoute {
            lifetime: row.map(|row| row.lifetime),
            generation: info.map(|info| info.generation),
            identity,
            rng: drawn.map(|(_, post)| post),
            partition: info.map_or_else(|| drawn.map_or(0, |(partition, _)| partition), |info| info.partition),
        })'''+s[b:]
s=s.replace('        StickyAppend {\n            tag:', '        StickyAppend {\n            next_rng: None,\n            tag:',1)
s=s.replace('''        let partition = Self::choose(self.rng, leaders);
        self.rng = Self::draw(self.rng).0;''','''        let (partition, post) = Self::choose(self.rng, leaders);
        self.rng = post;''',1)
# Initial and pressure histories commit only the returned, actually used draw sequence.
s=s.replace('                self.rng = Self::draw(self.rng).0;', '                if let Some(post) = plan.next_rng { self.rng = post; }',1)
s=s.replace('            self.rng = Self::draw(self.rng).0;', '            if let Some(post) = plan.next_rng { self.rng = post; }',1)
# All source-prepared tests that emulate actual admission pass its pure route token.
s=s.replace('''        let plan = state.plan(&topic, partition, worker, bytes, cluster);
        let tag = plan.tag;''','''        let route = unkeyed.then(|| state.route(&topic, cluster)).flatten();
        let mut plan = state.plan(&topic, partition, worker, bytes, cluster);
        plan.next_rng = route.and_then(|route| route.rng);
        let tag = plan.tag;''',1)
s=s.replace('''            let plan = state.plan(&topic, route.partition, &worker, 100, &cluster);
            let tag = plan.tag;''','''            let mut plan = state.plan(&topic, route.partition, &worker, 100, &cluster);
            plan.next_rng = route.rng;
            let tag = plan.tag;''',1)
p.write_text(s)
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text()
a=s.index('        let partition = if unkeyed {',s.index('fn sticky_enqueue(')); b=s.index('        pending.rec.partition =',a)
s=s[:a]+'''        let route = if unkeyed {
            let Some(route) = policy.route(&pending.rec.topic, &cluster) else { return Err(Error::QueueFull); };
            debug_assert!(policy.route_current(&pending.rec.topic, route, &cluster));
            Some(route)
        } else { None };
        let partition = route.map_or_else(|| pending.rec.partition.unwrap_or_else(|| partition_for_key(pending.rec.key.as_deref().unwrap_or_default(), np)), |route| route.partition);
'''+s[b:]
s=s.replace('''        let plan = policy.plan(&topic, partition, &worker.id, estimate(pending), &cluster);
        pending.sticky''','''        let mut plan = policy.plan(&topic, partition, &worker.id, estimate(pending), &cluster);
        plan.next_rng = route.and_then(|route| route.rng);
        pending.sticky''',1)
p.write_text(s)
