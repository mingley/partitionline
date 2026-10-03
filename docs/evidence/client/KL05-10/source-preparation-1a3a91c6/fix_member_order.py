from pathlib import Path
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/partitioner.rs')
s=p.read_text()
s=s.replace('    pub(crate) id: u128,\n}', '    pub(crate) id: u128,\n    /// Original append order within this bounded cohort, preserved on retries.\n    pub(crate) index: usize,\n}',1)
s=s.replace('    members: usize,\n    sealed:', '    members: usize,\n    admitted: usize,\n    sealed:',1)
s=s.replace('c.members < self.batch_records', 'c.admitted < self.batch_records && !self.exhausted',1)
# rustfmt expanded tag initializer: use tag closure index by locating before old_tail field.
s=s.replace('''                },
            },
            old_tail,''','''                },
                index: if existing_tail { old_tail.and_then(|id| self.cohorts.get(&id)).map_or(0, |c| c.admitted) } else { 0 },
            },
            old_tail,''',1)
s=s.replace('                    c.members = c.members.saturating_add(1);','                    c.members = c.members.saturating_add(1);\n                    c.admitted = c.admitted.saturating_add(1);',1)
s=s.replace('c.members >= self.batch_records','c.admitted >= self.batch_records',1)
s=s.replace('                        members: 1,\n                        sealed:', '                        members: 1,\n                        admitted: 1,\n                        sealed:',1)
# Route token is rechecked under the same lock; no draw occurs before real admission.
index=s.index('    /// Plan is read-only')
s=s[:index]+'''    pub(crate) fn route_current(&self, topic: &str, route: StickyRoute, cluster: &Cluster) -> bool { self.route(topic, cluster) == Some(route) }

'''+s[index:]
p.write_text(s)
p=Path('/workspace/work/client-sticky-implementation-prep/candidate/src/producer.rs')
s=p.read_text().replace('            route.partition\n','            debug_assert!(policy.route_current(&pending.rec.topic, route, &cluster));\n            route.partition\n',1)
s=s.replace('p.sticky == Some(tag)','p.sticky.is_some_and(|member| member.id == tag.id)')
s=s.replace('                    return batch;','                    batch.sort_by_key(|p| p.sticky.map_or(0, |tag| tag.index));\n                    return batch;',1)
p.write_text(s)
