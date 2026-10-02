#!/usr/bin/env python3
"""Retain bounded exact Apache source members and already pinned RFC mirror text."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[5]
BASE = 'clients/src/main/java/org/apache/kafka/common/security/'
WANTED = {'LICENSE', 'NOTICE'} | {BASE + name for name in [
    'scram/internals/ScramFormatter.java', 'scram/internals/ScramMechanism.java',
    'scram/internals/ScramMessages.java', 'scram/internals/ScramSaslClient.java',
    'scram/internals/ScramSaslServer.java', 'scram/internals/ScramExtensions.java',
    'scram/ScramCredential.java', 'scram/ScramCredentialCallback.java',
    'plain/internals/PlainSaslServer.java', 'plain/PlainAuthenticateCallback.java',
]}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('Use normal Python without -O to enforce source pins.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archives', type=Path, required=True)
    parser.add_argument('--rfc-input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir()
    matrix = json.loads((ROOT / 'tests/conformance/broker/api-matrix.json').read_text())
    rows = []
    for release in matrix['releases']:
        source = args.archives / (release['version'] + '.tar.gz')
        assert source.stat().st_size < 32 * 1024 * 1024
        assert sha(source.read_bytes()) == release['source_archive_sha256']
        selected = {}
        prefix = 'kafka-' + release['commit'] + '/'
        with tarfile.open(source, 'r|gz') as archive:
            for member in archive:
                if not member.name.startswith(prefix):
                    continue
                name = member.name[len(prefix):]
                if name not in WANTED:
                    continue
                assert member.isfile() and 0 < member.size <= 128 * 1024 and name not in selected
                raw = archive.extractfile(member).read(128 * 1024 + 1)
                assert len(raw) == member.size
                selected[name] = raw
        assert selected.keys() == WANTED, sorted(WANTED - selected.keys())
        root = args.output / release['version']
        for name, raw in selected.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        rows.append({'version': release['version'], 'commit': release['commit'],
            'original_archive_url': release['source_archive_url'],
            'original_archive_sha256': release['source_archive_sha256'],
            'retained_files_sha256': {name: sha(raw) for name, raw in sorted(selected.items())}})
    rfc_root = args.output / 'rfc'
    rfc_root.mkdir()
    rfc_pins = json.loads((args.rfc_input / 'pins.json').read_text())
    assert [row['rfc'] for row in rfc_pins] == [4616, 5802, 7677]
    for row in rfc_pins:
        source = args.rfc_input / ('rfc' + str(row['rfc']) + '.txt')
        raw = source.read_bytes()
        assert 0 < len(raw) < 128 * 1024 and sha(raw) == row['sha256']
        assert ('RFC ' + str(row['rfc'])).encode() in raw
        (rfc_root / source.name).write_bytes(raw)
    (rfc_root / 'pins.json').write_text(json.dumps(rfc_pins, indent=2) + '\n')
    (args.output / 'pins.json').write_text(json.dumps(rows, indent=2) + '\n')
    print(json.dumps({'upstream_releases': 3, 'source_files_per_release': len(WANTED), 'rfc_texts': 3, 'verdict': 'passed'}))


if __name__ == '__main__':
    main()
