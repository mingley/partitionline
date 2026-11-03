#!/usr/bin/env python3
"""Exercise the proposed accounting rules; this is not a Rust runtime test."""
from random import Random
import json

class Model:
    def __init__(self):
        self.records = []
        self.earliest = None
        self.ordered = True
        self.valid = True
        self.total = 0
        self.cached_skips = 0
        self.scans = 0
        self.ordered_drains = 0
        self.fallback_drains = 0

    def rebuild(self):
        self.valid = False
        self.total = sum(r[1] for r in self.records)
        self.earliest = min((r[0] for r in self.records), default=None)
        self.ordered = all(a[0] <= b[0] for a,b in zip(self.records, self.records[1:]))
        self.valid = True

    def push(self, record):
        if not self.records:
            self.ordered = True
        elif self.records[-1][0] > record[0]:
            self.ordered = False
        self.total += record[1]
        self.earliest = record[0] if self.earliest is None else min(self.earliest, record[0])
        self.records.append(record)

    def append(self, additions, cancel):
        before = len(self.records)
        old_valid = self.valid
        self.records.extend(additions)
        self.valid = False
        if cancel:
            return
        if not old_valid:
            self.rebuild()
            return
        previous = self.records[before-1][0] if before else None
        total = 0
        earliest = self.earliest if before else None
        ordered = (not before) or self.ordered
        for deadline, size, _ in additions:
            total += size
            if previous is not None and previous > deadline:
                ordered = False
            previous = deadline
            earliest = deadline if earliest is None else min(earliest, deadline)
        self.total = self.total + total if before else total
        self.earliest = earliest
        self.ordered = ordered
        self.valid = True

    def first_expired(self, now, sticky):
        if self.valid and not sticky and self.earliest is not None and now < self.earliest:
            self.cached_skips += 1
            return None
        self.scans += 1
        return next((i for i,(d,_,cancelled) in enumerate(self.records)
                     if d <= now or (sticky and cancelled)), None)

    def drain(self, n):
        if not self.valid:
            self.rebuild()
        removed, rest = self.records[:n], self.records[n:]
        refresh = any(d == self.earliest for d,_,_ in removed)
        if refresh and self.ordered:
            self.ordered_drains += 1
            earliest = rest[0][0] if rest else None
        elif refresh:
            self.fallback_drains += 1
            earliest = min((d for d,_,_ in rest), default=None)
        else:
            earliest = self.earliest
        self.total -= sum(r[1] for r in removed)
        self.earliest = earliest
        self.records = rest
        if not rest:
            self.ordered = True
        return removed

    def split(self, selected):
        old_valid = self.valid
        self.valid = False
        removed = [r for i,r in enumerate(self.records) if i in selected]
        self.records = [r for i,r in enumerate(self.records) if i not in selected]
        if not removed:
            self.valid = old_valid
        else:
            self.rebuild()

    def validate(self, now, sticky):
        actual = next((i for i,(d,_,c) in enumerate(self.records)
                       if d <= now or (sticky and c)), None)
        assert self.first_expired(now, sticky) == actual
        if self.valid:
            assert self.total == sum(r[1] for r in self.records)
            assert self.earliest == min((r[0] for r in self.records), default=None)
            assert not self.ordered or all(a[0] <= b[0] for a,b in zip(self.records, self.records[1:]))

def main():
    rng = Random(0x6b61666b61)
    s = Model()
    operations = 0
    cancellations = 0
    for _ in range(30000):
        kind = rng.randrange(7)
        if kind == 0 and len(s.records) < 100:
            s.push((rng.randrange(12), rng.randrange(1, 500), bool(rng.randrange(10)==0)))
        elif kind in (1, 2) and len(s.records) < 100:
            count = rng.randrange(0, 8)
            additions = [(rng.randrange(12),rng.randrange(1,500),bool(rng.randrange(10)==0)) for _ in range(count)]
            cancel = kind == 2
            cancellations += int(cancel)
            s.append(additions, cancel)
        elif kind == 3:
            s.drain(rng.randrange(0, len(s.records)+1))
        elif kind == 4:
            s.split({i for i in range(len(s.records)) if rng.randrange(5)==0})
        elif kind == 5:
            s.rebuild()
        else:
            s.drain(len(s.records))
        operations += 1
        s.validate(rng.randrange(12), bool(rng.randrange(2)))
    print(json.dumps({'seed':'0x6b61666b61', 'operations':operations,
        'simulated_cancelled_appends':cancellations, 'expiry_fast_returns':s.cached_skips,
        'expiry_scans':s.scans, 'ordered_drains':s.ordered_drains,
        'unordered_fallback_drains':s.fallback_drains,
        'scope':'Model only; no Rust compilation or runtime test.'}, indent=2))

if __name__ == '__main__':
    main()
