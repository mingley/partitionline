#!/usr/bin/env python3
"""Finite abstract flush model; does not execute the Rust sketch or producer."""
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Record:
    generation: int
    outcome: int  # 0 pending; 1 success; 2 failure


@dataclass(frozen=True)
class Flush:
    start: int
    cutoff: int
    status: int  # 0 pending; 1 cancelled; 2 success; 3 delivery failure


@dataclass(frozen=True)
class State:
    current: int = 0
    acknowledged: int = -1
    records: tuple[Record, ...] = ()
    flushes: tuple[Flush, ...] = ()


def steps(state):
    # An accepted admission linearizes inside the currently open generation.
    if len(state.records) < 2:
        yield 'admit', replace(state, records=state.records + (Record(state.current, 0),))
    for i, record in enumerate(state.records):
        if record.outcome == 0:
            for outcome in (1, 2):
                records = list(state.records)
                records[i] = replace(record, outcome=outcome)
                yield 'complete', replace(state, records=tuple(records))
    if len(state.flushes) < 2:
        # Rotation seals exactly current and opens the next generation.
        flush = Flush(state.acknowledged, state.current, 0)
        yield 'begin', replace(state, current=state.current + 1, flushes=state.flushes + (flush,))
    for i, flush in enumerate(state.flushes):
        if flush.status != 0:
            continue
        cancelled = list(state.flushes)
        cancelled[i] = replace(flush, status=1)
        yield 'cancel', replace(state, flushes=tuple(cancelled))
        if any(record.generation <= flush.cutoff and record.outcome == 0 for record in state.records):
            continue
        errors = any(flush.start < record.generation <= flush.cutoff and record.outcome == 2
                     for record in state.records)
        finished = list(state.flushes)
        finished[i] = replace(flush, status=3 if errors else 2)
        yield 'report', replace(state, acknowledged=max(state.acknowledged, flush.cutoff),
                                flushes=tuple(finished))


def verify(before, action, after):
    if action == 'cancel':
        assert after.acknowledged == before.acknowledged
    if action == 'report':
        index = next(i for i, (old, new) in enumerate(zip(before.flushes, after.flushes))
                     if old.status == 0 and new.status in (2, 3))
        flush = before.flushes[index]
        assert not any(record.generation <= flush.cutoff and record.outcome == 0
                       for record in before.records)
        covered_errors = any(flush.start < record.generation <= flush.cutoff and record.outcome == 2
                             for record in before.records)
        assert (after.flushes[index].status == 3) == covered_errors
        assert after.acknowledged == max(before.acknowledged, flush.cutoff)


seen = {State()}
frontier = {State()}
edges = 0
for _depth in range(10):
    next_frontier = set()
    for state in frontier:
        for action, result in steps(state):
            verify(state, action, result)
            edges += 1
            if result not in seen:
                seen.add(result)
                next_frontier.add(result)
    frontier = next_frontier
    if not frontier:
        break
print(f'{len(seen)} abstract states and {edges} transitions checked')
print('Scope: two records and two ordinary rotating flushes, ideal atomic operations.')
print('No Rust code, Tokio, broker, generation-cap fallback, or resource cleanup was executed.')
