#!/usr/bin/env python3
"""Retain bounded election references from independently pinned Apache archives."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[4]
WANTED = {
    'LICENSE', 'NOTICE',
    'raft/src/main/java/org/apache/kafka/raft/QuorumState.java',
    'raft/src/main/java/org/apache/kafka/raft/ElectionState.java',
    'raft/src/main/java/org/apache/kafka/raft/CandidateState.java',
    'raft/src/main/java/org/apache/kafka/raft/FileQuorumStateStore.java',
    'raft/src/main/java/org/apache/kafka/raft/QuorumStateStore.java',
    'server-common/src/main/java/org/apache/kafka/server/common/OffsetAndEpoch.java',
    'raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java',
    'raft/src/main/java/org/apache/kafka/raft/internals/EpochElection.java',
    'clients/src/main/resources/common/message/VoteRequest.json',
    'clients/src/main/resources/common/message/BeginQuorumEpochRequest.json',
    'raft/src/main/resources/common/message/QuorumStateData.json',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('original_directory', type=Path)
    parser.add_argument('output_directory', type=Path)
    args = parser.parse_args()
    args.output_directory.mkdir()
    matrix = json.loads((ROOT / 'tests/conformance/broker/api-matrix.json').read_text())
    reports = []
    for release in matrix['releases']:
        original = args.original_directory / (release['version'] + '.tar.gz')
        assert original.stat().st_size < 32 * 1024 * 1024
        assert hashlib.sha256(original.read_bytes()).hexdigest() == release['source_archive_sha256']
        prefix = 'kafka-' + release['commit'] + '/'
        selected = {}
        with tarfile.open(original, mode='r|gz') as archive:
            for member in archive:
                if not member.name.startswith(prefix):
                    continue
                name = member.name[len(prefix):]
                if name not in WANTED:
                    continue
                assert member.isfile() and 0 < member.size <= 512 * 1024 and name not in selected
                selected[name] = archive.extractfile(member).read(512 * 1024 + 1)
                assert len(selected[name]) == member.size
        assert selected.keys() == WANTED, sorted(WANTED - selected.keys())
        output = args.output_directory / release['version']
        for name, data in selected.items():
            path = output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        reports.append({'version': release['version'], 'commit': release['commit'],
                        'original_archive_sha256': release['source_archive_sha256'],
                        'original_archive_url': release['source_archive_url'],
                        'retained_files_sha256': {name: hashlib.sha256(data).hexdigest() for name, data in sorted(selected.items())},
                        'scope': 'Pinned election/persistence references and wire distinctions, not Java behavior execution or KRaft compatibility'})
    (args.output_directory / 'pins.json').write_text(json.dumps(reports, indent=2) + '\n')
    print(json.dumps({'releases': len(reports), 'retained_files_per_release': len(WANTED), 'verdict': 'passed'}))


if __name__ == '__main__':
    main()
