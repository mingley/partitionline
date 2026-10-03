#!/usr/bin/env python3
"""Retain bounded Apache references from exact source archives already on disk."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('Pin assertions require Python without -O.')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archives', type=Path, required=True)
    parser.add_argument('--selected-sources', type=Path, required=True)
    args = parser.parse_args()
    releases = json.loads((ROOT / 'tests/conformance/broker/api-matrix.json').read_text())['releases']
    pins = {'schema_version': 1, 'releases': []}
    for release in releases:
        version = release['version']
        archive_path = args.archives / (version + '.tar.gz')
        assert digest(archive_path.read_bytes()) == release['source_archive_sha256']
        selected = args.selected_sources / version
        wanted = {path.relative_to(selected).as_posix() for path in selected.rglob('*') if path.is_file()}
        wanted |= {'LICENSE', 'NOTICE', 'clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java',
            'clients/src/main/java/org/apache/kafka/common/protocol/Errors.java'}
        request_base = 'clients/src/main/java/org/apache/kafka/common/requests/'
        wanted |= {request_base + name + '.java' for name in ['AbstractRequest', 'AbstractResponse',
            'RequestHeader', 'ResponseHeader', 'ApiVersionsRequest', 'ApiVersionsResponse',
            'VoteRequest', 'VoteResponse', 'BeginQuorumEpochRequest', 'BeginQuorumEpochResponse',
            'EndQuorumEpochRequest', 'EndQuorumEpochResponse']}
        output = HERE / 'references' / version
        output.mkdir(parents=True, exist_ok=False)
        files = {}
        prefix = 'kafka-' + release['commit'] + '/'
        with tarfile.open(archive_path, 'r:gz') as archive:
            for name in sorted(wanted):
                member = archive.getmember(prefix + name)
                assert member.isfile() and member.size < 2 * 1024 * 1024
                data = archive.extractfile(member).read()
                if name in release['files_sha256']:
                    assert digest(data) == release['files_sha256'][name]
                if (selected / name).is_file():
                    assert data == (selected / name).read_bytes()
                destination = output / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(data)
                files[name] = digest(data)
        pins['releases'].append({'release': version, 'upstream_commit': release['commit'],
            'source_archive_url': release['source_archive_url'], 'source_archive_sha256': release['source_archive_sha256'],
            'retained_file_sha256': files})
    (HERE / 'reference-pins.json').write_text(json.dumps(pins, indent=2) + '\n')
    print('PASS retained', sum(len(row['retained_file_sha256']) for row in pins['releases']), 'exact Apache references')


if __name__ == '__main__':
    main()
