"""Hand-built Python decoder controls only; no broker/Rust/TCP qualification."""
import hashlib
import json
from pathlib import Path
import struct

import peer_bytes as p

U32 = lambda n: struct.pack('>I', n)
U64 = lambda n: struct.pack('>Q', n)
KEY1 = U32(1) + bytes([1]) * 16
KEY2 = U32(2) + bytes([2]) * 16
CONTEXT = KEY1 + KEY2 + U64(0)
VOTE = CONTEXT + U64(7) + U64(2) + U32(1) + U64(1) + U64(3)
ENTRY = U64(2) + U64(4) + b'\0' * 4 + U32(3) + b'row'
APPEND = CONTEXT + U32(1) + U32(2) + U64(8) + U64(2) + U64(1) + U64(3) + U64(3) + U32(1) + bytes(4) + ENTRY


def packet(body, kind=10, rpc=1, header_override=None):
    raw = b'PLPEER01' + struct.pack('>HBBQ', 1, kind, 0, rpc) + bytes(4) + body
    if header_override is not None:
        raw = header_override(raw)
    raw += U32(p.crc32c(raw))
    return U32(len(raw)) + raw


def replace(data, offset, value):
    return data[:offset] + value + data[offset + len(value):]


def main():
    assert p.crc32c(b'123456789') == 0xE3069283
    vote, append = packet(VOTE), packet(APPEND, 20)
    assert p.frame(vote)['message']['sequence'] == 7
    assert p.frame(append)['message']['entries'][0]['payload_hex'] == b'row'.hex()
    mutants = {
        'tcp-length': replace(vote, 0, U32(len(vote))),
        'crc': replace(vote, len(vote) - 1, bytes([vote[-1] ^ 1])),
        'magic': packet(VOTE, header_override=lambda raw: replace(raw, 0, b'X')),
        'version': packet(VOTE, header_override=lambda raw: replace(raw, 8, b'\0\2')),
        'flags': packet(VOTE, header_override=lambda raw: replace(raw, 11, b'\1')),
        'reserved': packet(VOTE, header_override=lambda raw: replace(raw, 20, b'\1')),
        'unknown-kind': packet(VOTE, kind=99),
        'vote-rpc-zero': packet(VOTE, rpc=0),
        'directory-zero': packet(replace(VOTE, 4, bytes(16))),
        'id-signed-overflow': packet(replace(VOTE, 0, U32(0x80000000))),
        'sequence-zero': packet(replace(VOTE, 48, U64(0))),
        'term-zero': packet(replace(VOTE, 56, U64(0))),
        'term-overflow': packet(replace(VOTE, 56, U64((1 << 31) + 1))),
        'candidate-mismatch': packet(replace(VOTE, 64, U32(2))),
        'log-not-prior-term': packet(replace(VOTE, 68, U64(2))),
        'position-term-zero': packet(replace(VOTE, 68, U64(0))),
        'trailing-body': packet(VOTE + b'\0'),
        'short-body': packet(VOTE[:-1]),
        'append-wrong-peer': packet(replace(APPEND, 52, U32(3)), 20),
        'append-reserved': packet(replace(APPEND, 100, b'\1'), 20),
        'append-count': packet(replace(APPEND, 96, U32(4097)), 20),
        'append-future-prefix': packet(replace(APPEND, 72, U64(3)), 20),
        'record-kind': packet(replace(APPEND, 120, b'\3'), 20),
        'record-length': packet(replace(APPEND, 124, U32(p.MAX_RECORD + 1)), 20),
        'barrier-nonempty': packet(replace(APPEND, 120, b'\1'), 20),
    }
    results = []
    for name, data in mutants.items():
        try:
            p.frame(data)
        except p.Rejected as error:
            results.append({'case': name, 'rejected': True, 'class': str(error),
                            'sha256': hashlib.sha256(data).hexdigest()})
        else:
            raise AssertionError(name)
    try:
        p.frame(append, max_records=0)
    except p.Rejected:
        results.append({'case': 'configured-record-count', 'rejected': True})
    else:
        raise AssertionError('configured-record-count')
    root = Path(__file__).resolve().parent
    receipt = {'scope': 'Hand-built Python decoder controls; no Rust compiler, broker, TCP or durable authority proof',
               'positive_packets': 2, 'negative_packets': len(results), 'crc_known_vector_passed': True,
               'all_rejected': True, 'negative_cases': results,
               'source_sha256': hashlib.sha256((root / 'peer_bytes.py').read_bytes()).hexdigest(),
               'limits': ['Hello genesis only size/magic checked here; full frozen Voters decoding must be composed.',
                          'Remote response identity and durable/current-config receipt authority are separate causal checks.',
                          'No actual runtime76 packet exists or is claimed by this receipt.']}
    (root / 'decoder-controls.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({'passed': True, 'positive': 2, 'negative': len(results)}))


if __name__ == '__main__':
    main()
