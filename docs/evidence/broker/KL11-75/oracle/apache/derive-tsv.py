#!/usr/bin/env python3
"""Derive the Rust replay index from authoritative actual Apache outcomes."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture-root', type=Path, required=True)
    args = parser.parse_args()
    total = 0
    for release in ['4.1.2', '4.2.1', '4.3.1']:
        directory = args.fixture_root / release
        manifest = json.loads((directory / 'goldens.json').read_text())
        assert manifest['release'] == release and len(manifest['cases']) == 36
        names, rows = set(), []
        for case in manifest['cases']:
            name, filename = case['name'], case['file']
            assert name not in names and Path(filename).name == filename
            names.add(name)
            raw = (directory / filename).read_bytes()
            assert len(raw) == case['bytes'] and hashlib.sha256(raw).hexdigest() == case['sha256']
            policy = case['handler_policy']
            if 'batches' in case:
                batches = case['batches']
                offsets = [record['offset'] for batch in batches for record in batch['records']]
                count = sum(batch['record_count'] for batch in batches)
                assert count == len(offsets)
                fields = [len(batches), count, batches[0]['base_offset'] if batches else '-',
                          batches[-1]['last_offset'] + 1 if batches else '-',
                          ','.join(str(offset) for offset in offsets) if offsets else '-']
            else:
                assert 'apache_outcome' in case and policy.startswith('reject_')
                fields = ['-'] * 5
            rows.append('\t'.join(str(field) for field in [name, filename, policy] + fields))
        (directory / 'cases.tsv').write_text('\n'.join(rows) + '\n')
        total += len(rows)
    print(json.dumps({'passed': True, 'derived_rows': total,
                      'columns': ['name', 'file', 'handler_policy', 'batch_count', 'record_count',
                                  'first_offset', 'next_offset', 'record_offsets_csv']}))


if __name__ == '__main__':
    main()
