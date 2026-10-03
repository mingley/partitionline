#!/usr/bin/env python3
"""Bind actual trace payloads and copied Journal bytes; no independent safety claim."""
import argparse
import hashlib
import json
from pathlib import Path


def sha(data):
    return hashlib.sha256(data).hexdigest()


def annotate(value):
    if isinstance(value, dict):
        if 'payload_hex' in value:
            payload = bytes.fromhex(value['payload_hex'])
            value['sha256'] = sha(payload)
        for child in list(value.values()):
            annotate(child)
    elif isinstance(value, list):
        for child in value:
            annotate(child)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('captures', type=Path)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--allow-development', action='store_true')
    args = parser.parse_args()
    if not args.allow_development:
        assert len(args.source_sha) == 40 and all(c in '0123456789abcdef' for c in args.source_sha)
    files = ['partitionline-broker/src/raft/replication.rs',
             'partitionline-broker/src/raft/election.rs',
             'partitionline-broker/src/raft/protocol.rs',
             'partitionline-broker/src/raft/mod.rs',
             'partitionline-broker/tests/raft_replication.rs']
    source_files = {name: sha((args.source_root / name).read_bytes()) for name in files}
    outcomes = []
    for root in sorted(args.captures.glob('history-*')):
        path = root / 'trace.json'
        raw = path.read_bytes()
        document = json.loads(raw)
        assert document['source_sha'] == args.source_sha
        document['source_files_sha256'] = source_files
        if args.allow_development:
            document['source_binding_scope'] = 'Development WORK: hashes at seal time; not an immutable compiled-source receipt.'
        for i, event in enumerate(document['events']):
            assert event['ordinal'] == i
            if i:
                assert event['now_ms'] >= document['events'][i-1]['now_ms']
        annotate(document)
        for receipt in document['final_journals']:
            assert receipt['group'] == document['group']
            for prefix in ['wal', 'election']:
                local = root / receipt[prefix + '_path']
                assert local.resolve().is_relative_to(root.resolve())
                blob = local.read_bytes()
                assert len(blob) <= 256 * 1024 * 1024
                receipt[prefix + '_sha256'] = sha(blob)
                receipt[prefix + '_bytes'] = len(blob)
        for event in document['events']:
            if event['kind'] == 'fault':
                fault = root / event['args']['file']
                event['args']['after_sha256'] = sha(fault.read_bytes())
                node_id = event['node_id']
                earlier = [r for r in document['final_journals'] if r['node_id'] == node_id
                           and r['event_ordinal'] < event['ordinal']]
                event['args']['before_sha256'] = earlier[-1]['wal_sha256']
        original = root / 'trace-raw.json'
        assert not original.exists(), 'use a fresh capture directory; never overwrite an attempt'
        original.write_bytes(raw)
        path.write_text(json.dumps(document, indent=2) + '\n')
        outcomes.append({'path': str(root.relative_to(args.captures)),
                         'events': len(document['events']),
                         'copied_journal_receipts': len(document['final_journals']),
                         'raw_trace_sha256': sha(raw), 'sealed_trace_sha256': sha(path.read_bytes())})
    assert len(outcomes) == 2
    (args.captures / 'seal.json').write_text(json.dumps({
        'schema_version': 1, 'source_sha': args.source_sha,
        'source_files_sha256': source_files,
        'purpose': 'hash/provenance binding only; independent safety checker is separately owned',
        'source_binding_scope': ('Development WORK: hashes at seal time; not an immutable compiled-source receipt.' if args.allow_development else 'Exact source SHA and source files supplied by immutable runner.'),
        'histories': outcomes}, indent=2) + '\n')
    print(json.dumps(outcomes))


if __name__ == '__main__':
    main()
