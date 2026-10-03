"""Prepared offline checker. Only actual executed journals can produce a delivery receipt."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import struct
import time

MAGIC = b'PLSTK01\n'
ROW = struct.Struct('>QIiqQQ32sI')
MAX_RECORDS = 10_000_000
PROFILES = ('rust-rr-keyed', 'rust-uniform-keyed', 'java-uniform-keyed',
            'rust-rr-null', 'rust-uniform-null', 'java-uniform-null')


def rows(path):
    size = path.stat().st_size
    assert size >= len(MAGIC) and (size - len(MAGIC)) % ROW.size == 0
    count = (size - len(MAGIC)) // ROW.size
    assert count <= MAX_RECORDS
    with path.open('rb') as stream:
        assert stream.read(len(MAGIC)) == MAGIC
        for _ in range(count):
            raw = stream.read(ROW.size)
            assert len(raw) == ROW.size
            yield ROW.unpack(raw)
        assert not stream.read(1)



def expected_hash(seed, id_, keyed):
    mask = (1 << 64) - 1
    def mix(x):
        x = (x + 0x9e3779b97f4a7c15) & mask
        x = ((x ^ (x >> 30)) * 0xbf58476d1ce4e5b9) & mask
        x = ((x ^ (x >> 27)) * 0x94d049bb133111eb) & mask
        return x ^ (x >> 31)
    value = bytearray(b'PLSTKVAL' + seed.to_bytes(8, 'big') + id_.to_bytes(8, 'big'))
    word = mix(seed ^ id_)
    while len(value) < 100:
        value.extend(word.to_bytes(8, 'big')[:100-len(value)])
        word = mix(word)
    key = seed.to_bytes(8, 'big') + id_.to_bytes(8, 'big') if keyed else b''
    return hashlib.sha256(bytes([int(keyed)]) + key + value).digest()

def file_hash(path):
    sha = hashlib.sha256()
    with path.open('rb') as stream:
        while data := stream.read(1024 * 1024):
            sha.update(data)
    return sha.hexdigest()


def audit(out):
    deadline = time.monotonic() + 1200
    complete = json.loads((out / 'producer-complete.json').read_text())
    delivery = json.loads((out / 'delivery-complete.json').read_text())
    configuration = json.loads((out / 'configuration.json').read_text())
    assert not (out / 'failure.json').exists()
    assert complete['profile'] == delivery['profile'] == configuration['profile'] in PROFILES
    assert configuration['explicit_partition'] is False
    for key, expected in [('acks', -1), ('idempotence', True), ('max_in_flight', 5),
                          ('linger_ms', 5), ('batch_bytes', 1048576), ('compression', 'none'),
                          ('record_window', 8192), ('max_records', MAX_RECORDS)]:
        assert configuration[key] == expected, key
    phases = complete['phases']
    assert len(phases) == 2
    for phase, minimum, seconds in zip(phases, [10_000, 1_000_000], [15, 60]):
        assert phase['phase'] in (0, 1)
        assert phase['acknowledged'] >= minimum
        assert phase['seconds_including_admission_and_drain_flush'] >= seconds
        assert phase['end_monotonic_ns'] - phase['start_monotonic_ns'] >= seconds * 10**9
    assert [phase['phase'] for phase in phases] == [0, 1]
    total = complete['total_records']
    assert 1_010_000 <= total <= MAX_RECORDS
    assert total == delivery['verified_records'] == sum(p['acknowledged'] for p in phases)
    database = out / 'offline-audit.sqlite3'
    assert not database.exists(), 'Preserve every prior failed audit'
    conn = sqlite3.connect(database)
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.execute('PRAGMA cache_size=-16384')  # 16MiB bounded SQLite page cache.
    conn.execute('PRAGMA max_page_count=524288')  # 2GiB hard database cap.
    conn.execute('CREATE TABLE records(id INTEGER PRIMARY KEY,phase INTEGER,partition INTEGER,'
                 'offset INTEGER,hash BLOB,seen INTEGER DEFAULT0,UNIQUE(partition,offset))'.replace('DEFAULT0', 'DEFAULT 0'))
    try:
        counts = [0, 0]
        producer_count = 0
        for id_, phase, partition, offset, submitted, completed, digest, bound in rows(out / 'producer-acks.bin'):
            assert time.monotonic() < deadline
            assert 0 <= id_ < total and phase in (0, 1) and 0 <= partition < 6 and offset >= 0
            assert completed >= submitted
            assert phase == int(id_ >= phases[0]['acknowledged'])
            assert phases[phase]['start_monotonic_ns'] <= submitted <= completed <= phases[phase]['end_monotonic_ns']
            assert digest == expected_hash(configuration['seed'], id_, configuration['key_presence'] == 'non-null')
            assert bound == (141 if configuration['key_presence'] == 'non-null' else 125)
            conn.execute('INSERT INTO records(id,phase,partition,offset,hash) VALUES(?,?,?,?,?)',
                         (id_, phase, partition, offset, digest))
            counts[phase] += 1
            producer_count += 1
            if producer_count % 10_000 == 0:
                conn.commit()
        conn.commit()
        assert producer_count == total
        assert counts == [p['acknowledged'] for p in phases] == delivery['phase_counts']
        assert conn.execute('SELECT MIN(id),MAX(id),COUNT(*) FROM records').fetchone() == (0, total - 1, total)
        observed = 0
        for id_, phase, partition, offset, submitted, completed, digest, bound in rows(out / 'consumer-delivery.bin'):
            assert time.monotonic() < deadline
            assert submitted == completed == bound == 0, 'Consumer capture has no invented producer timing/bound'
            matched = conn.execute('UPDATE records SET seen=1 WHERE id=? AND phase=? AND partition=? '
                                   'AND offset=? AND hash=? AND seen=0', (id_, phase, partition, offset, digest)).rowcount
            assert matched == 1, 'Unknown/duplicate delivery or public ack offset/hash disagreement'
            observed += 1
            if observed % 10_000 == 0:
                conn.commit()
        conn.commit()
        assert observed == total and conn.execute('SELECT COUNT(*) FROM records WHERE seen<>1').fetchone() == (0,)
        skew = {}
        for phase in (0, 1):
            parts = []
            for partition in range(6):
                minimum, maximum, count = conn.execute('SELECT MIN(offset),MAX(offset),COUNT(*) FROM records WHERE partition=?',
                                                        (partition,)).fetchone()
                if count:
                    assert minimum == 0 and maximum == count - 1, 'Broker public offset hole'
                phase_count = conn.execute('SELECT COUNT(*) FROM records WHERE partition=? AND phase=?',
                                           (partition, phase)).fetchone()[0]
                parts.append(phase_count)
            mean = sum(parts) / 6
            skew[str(phase)] = {'counts': parts, 'max_to_mean': max(parts) / mean,
                                'min_to_mean': min(parts) / mean,
                                'coefficient_of_variation': (sum((x - mean)**2 for x in parts) / 6)**0.5 / mean}
        result = {'scope': 'actual public producer ack and independent consumer journal reconciliation only',
                  'profile': complete['profile'], 'records': total, 'phase_counts': counts,
                  'skew': skew, 'duplicates': 0, 'offset_holes': 0, 'ID_hash_ack_offset_disagreements': 0,
                  'producer_journal_sha256': file_hash(out / 'producer-acks.bin'),
                  'consumer_journal_sha256': file_hash(out / 'consumer-delivery.bin'),
                  'duration_acknowledgement_and_delivery_minima_met': True,
                  'latency': 'closed-loop invocation to ack; no CO-corrected percentile or open-loop claim',
                  'still_required': ['actual immutable source/build/image/config/CPU3 provenance',
                                     'five paired randomized six-profile blocks',
                                     'whole process closure and physical disk guard receipts',
                                     'paired uncertainty report; no fastest-client conclusion']}
        target = out / 'offline-delivery-audit.json'
        with target.open('x') as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write('\n')
        return result
    finally:
        conn.close()  # Keep the bounded database and all failed histories; never unlink.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('out')
    args = parser.parse_args()
    print(json.dumps(audit(Path(args.out)), indent=2))


if __name__ == '__main__':
    main()
