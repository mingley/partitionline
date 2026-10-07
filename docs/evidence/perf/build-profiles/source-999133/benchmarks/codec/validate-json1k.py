#!/usr/bin/env python3
"""Validate the complete retained KL10-03 local baseline (stdlib only)."""
import json
import math
import re
import sys
from pathlib import Path


def validate(root):
    host = json.loads((root / 'host.json').read_text())
    if not re.fullmatch(r'[0-9a-f]{40}', host['source_sha']):
        raise ValueError('missing immutable source SHA')
    expected = {f'{op}:{codec}:json1k/{size}k'
                for op in ('micro-compress', 'micro-decompress', 'raw-compress', 'raw-decompress')
                for codec in ('gzip', 'lz4', 'snappy') for size in (16, 256)}
    allocations = json.loads((root / 'allocations-and-ratios.json').read_text())
    rows = allocations['cells']
    if len(rows) != 24 or {row['cell'] for row in rows} != expected:
        raise ValueError('allocation cell inventory differs from all 24 required cells')
    for row in rows:
        if (row['records'] not in (16, 256) or row['value_bytes'] != row['records'] * 1024
                or not re.fullmatch(r'[0-9a-f]{64}', row['section_sha256'])
                or row['allocations'] <= 0 or row['allocated_bytes'] <= 0):
            raise ValueError(f'invalid allocation/input facts: {row["cell"]}')
        ratio = row['compressed_bytes'] / row['section_bytes']
        if abs(ratio - row['compressed_to_section_ratio']) > 1e-12:
            raise ValueError(f'invalid ratio: {row["cell"]}')
    instructions = [json.loads((root / f'instructions-{run}.json').read_text())
                    for run in ('first', 'repeat')]
    for table in instructions:
        if set(table) != expected or any(type(n) is not int or n <= 0 for n in table.values()):
            raise ValueError('instruction inventory/count failure')
    for cell in expected:
        if abs(instructions[1][cell] / instructions[0][cell] - 1) >= 0.01:
            raise ValueError(f'instruction baseline repeat differs by at least 1%: {cell}')
    timings = json.loads((root / 'timings.json').read_text())
    if len(timings) != 24 or {row['cell'] for row in timings} != expected:
        raise ValueError('timing inventory failure')
    raw_cells = set()
    for path in (root / 'criterion').glob('*/*/benchmark.json'):
        cell = json.loads(path.read_text())['full_id']
        sample = json.loads(path.with_name('sample.json').read_text())
        if cell in raw_cells or cell not in expected:
            raise ValueError(f'duplicate/unknown raw timing cell: {cell}')
        raw_cells.add(cell)
        if len(sample['times']) != 100 or len(sample['iters']) != 100:
            raise ValueError(f'missing raw samples: {cell}')
        if any(not math.isfinite(n) or n <= 0 for n in sample['times'] + sample['iters']):
            raise ValueError(f'invalid raw sample: {cell}')
    if raw_cells != expected:
        raise ValueError('missing raw timing cells')
    for row in timings:
        low, high = row['median_95pct_ci_ns']
        if not 0 < low <= row['median_ns_per_batch'] <= high or row['samples'] != 100:
            raise ValueError(f'invalid timing estimate: {row["cell"]}')
    return len(expected)


if __name__ == '__main__':
    root = Path(sys.argv[1]) if len(sys.argv) == 2 else Path(__file__).resolve().parents[2] / 'docs/evidence/perf/KL10-03'
    try:
        print(f'json1k-baseline: {validate(root)} complete cells validated (local unsigned)')
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as error:
        print(f'json1k-baseline: FAIL: {error}', file=sys.stderr)
        sys.exit(1)
