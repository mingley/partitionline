#!/usr/bin/env python3
"""Bounded independent codec spike; never runs partitionline's runtime codec."""
import argparse, hashlib, json, pathlib, random, struct, subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=pathlib.Path, required=True)
parser.add_argument('--out', type=pathlib.Path, required=True)
args = parser.parse_args()
root = args.out.resolve()
root.mkdir(parents=True, exist_ok=True)
binary = args.binary.resolve()

# Each subprocess has a finite input and a 30-second execution deadline.
commands = []
def run(argv, *, input=None):
    try:
        result = subprocess.run([str(x) for x in argv], input=input, capture_output=True, timeout=30)
        status = result.returncode
        stdout = result.stdout
        error = result.stderr.decode('utf-8', errors='replace').strip()
    except subprocess.TimeoutExpired:
        status, stdout, error = 124, b'', '30 second deadline exceeded'
    commands.append({'argv': [str(x).replace(str(root), '$CORPUS').replace(str(binary), '$SPIKE') for x in argv],
                     'exit_code': status, 'stderr': error.replace(str(root), '$CORPUS').replace(str(binary), '$SPIKE')})
    return status, stdout, error

def put(name, data):
    p = root / name
    p.write_bytes(data)
    return p

def header(data):
    if data[:4] != bytes.fromhex('28b52ffd'):
        return {'standard_magic': False}
    descriptor = data[4]
    single = bool(descriptor & 32)
    offset = 5
    window = None
    if not single:
        wd = data[offset]
        offset += 1
        base = 1 << (10 + (wd >> 3))
        window = base + (base // 8) * (wd & 7)
    offset += [0, 1, 2, 4][descriptor & 3]
    flag = descriptor >> 6
    size_bytes = [int(single), 2, 4, 8][flag]
    size = int.from_bytes(data[offset:offset + size_bytes], 'little') if size_bytes else None
    if size_bytes == 2:
        size += 256
    return {'standard_magic': True, 'single_segment': single, 'content_size': size,
            'checksum': bool(descriptor & 4), 'window_bytes': size if single else window}

rng = random.Random(8878)
text = (b'{"topic":"zstd-spike","partition":2,"key":"same-key","value":"bounded synthetic records"}\n' * 1600)[:131073]
payloads = {
    'empty': b'',
    'tiny': b'x',
    'runs': b'A' * 131072,
    'records': text,
    'entropy': bytes(rng.randrange(256) for _ in range(262144)),
    'mixed': (text + bytes(rng.randrange(256) for _ in range(262144)))[:393216],
}
profiles = [
    ('level1-known-check', ['-1', '--check', '--zstd=wlog=19'], False),
    ('level3-stream-check', ['-3', '--check', '--zstd=wlog=17'], True),
    ('level19-known-no-check', ['-19', '--no-check', '--zstd=wlog=17'], False),
]
frames = []
decodes = []
encodes = []
rejects = []
special = []
for name, payload in payloads.items():
    src = put(name + '.raw', payload)
    for profile, options, streaming in profiles:
        zst = root / (name + '-' + profile + '.zst')
        command = ['zstd', '-q', '-f', '--single-thread', *options, '-o', zst]
        command.append('-' if streaming else src)
        code, _, error = run(command, input=payload if streaming else None)
        if code:
            raise RuntimeError(error)
        raw_frame = zst.read_bytes()
        frames.append({'payload': name, 'profile': profile, 'frame': zst.name,
                       'raw_bytes': len(payload), 'frame_bytes': len(raw_frame),
                       'sha256': hashlib.sha256(raw_frame).hexdigest(), **header(raw_frame)})
        for candidate in ['ruzstd', 'zstd-rs']:
            dst = root / 'decoded.tmp'
            code, _, error = run([binary, candidate + '-decode', zst, dst, len(payload)])
            passed = code == 0 and dst.read_bytes() == payload
            decodes.append({'candidate': candidate, 'frame': zst.name, 'passed': passed, 'exit_code': code, 'error': error})
            if payload:
                code, _, error = run([binary, candidate + '-decode', zst, dst, len(payload) - 1])
                rejects.append({'candidate': candidate, 'frame': zst.name, 'case': 'output-cap-minus-one',
                                'rejected': code != 0 and code != 101, 'exit_code': code, 'error': error})
    for candidate, levels in [('ruzstd', [1]), ('zstd-rs', [1, 3, 19])]:
        for level in levels:
            dst = root / (name + '-' + candidate + '-level' + str(level) + '.zst')
            code, _, error = run([binary, candidate + '-encode', src, dst, level])
            if code == 0:
                peer_code, decoded, peer_error = run(['zstd', '-q', '-d', '--stdout', '--no-pass-through', dst])
                passed = peer_code == 0 and decoded == payload
                size = dst.stat().st_size
                digest = hashlib.sha256(dst.read_bytes()).hexdigest()
            else:
                peer_code, peer_error, passed, size, digest = None, None, False, None, None
            encodes.append({'candidate': candidate, 'payload': name, 'level': level, 'passed': passed,
                            'raw_bytes': len(payload), 'frame_bytes': size, 'sha256': digest,
                            'encode_exit_code': code, 'peer_exit_code': peer_code, 'error': error or peer_error})

# Mutations target a checksummed frame. Corruption is tested through our explicit
# ruzstd checksum adapter, not attributed to the upstream StreamingDecoder.
reference = (root / 'records-level1-known-check.zst').read_bytes()
mutations = {
    'checksum-flip': reference[:-1] + bytes([reference[-1] ^ 1]),
    'truncated-checksum': reference[:-2],
    'bad-magic': bytes([reference[0] ^ 1]) + reference[1:],
    # WD=0xf8 advertises 2^41 bytes, exceeding both candidates' internal ceiling.
    'excess-window': bytes.fromhex('28b52ffd00f8010000'),
}
for name, raw in mutations.items():
    src = put(name + '.zst', raw)
    for candidate in ['ruzstd', 'zstd-rs']:
        code, _, error = run([binary, candidate + '-decode', src, root / 'decoded.tmp', 512 * 1024])
        rejects.append({'candidate': candidate, 'frame': src.name, 'case': name,
                        'rejected': code != 0 and code != 101, 'exit_code': code, 'error': error})

one = (root / 'tiny-level1-known-check.zst').read_bytes()
two = (root / 'records-level3-stream-check.zst').read_bytes()
for name, data in [('concatenated', one + two),
                   ('skippable-prefix', struct.pack('<II', 0x184d2a50, 4) + b'test' + one + two)]:
    src = put(name + '.zst', data)
    for candidate in ['ruzstd', 'zstd-rs']:
        dst = root / 'decoded.tmp'
        code, _, error = run([binary, candidate + '-decode', src, dst, 512 * 1024])
        special.append({'candidate': candidate, 'case': name, 'passed': code == 0 and dst.read_bytes() == b'x' + text,
                        'exit_code': code, 'error': error})

(root / 'decoded.tmp').unlink(missing_ok=True)
summary = {
    'reference': 'Zstandard CLI 1.5.7',
    'bounded_payload_max_bytes': max(map(len, payloads.values())),
    'per_process_timeout_seconds': 30,
    'payload_count': len(payloads), 'reference_frame_count': len(frames),
    'reference_frames': frames, 'decoder_cases': decodes, 'encoder_cases': encodes,
    'rejection_cases': rejects, 'multiple_frame_cases': special,
    'counts': {
        'decoder_passes': sum(x['passed'] for x in decodes), 'decoder_cases': len(decodes),
        'encoder_peer_passes': sum(x['passed'] for x in encodes), 'encoder_cases': len(encodes),
        'rejections': sum(x['rejected'] for x in rejects), 'rejection_cases': len(rejects),
        'subprocess_commands': len(commands),
    },
    'limits': [
        'Raw standard zstd frames only; no Kafka record-batch envelope, Java/librdkafka broker or live Kafka qualification.',
        'No throughput, RSS, architecture comparison, fuzz campaign or downstream deployment claim.',
        'ruzstd adapter applies cap-plus-one and explicit checksum/trailing-byte checks; internal history is not bounded by this output cap.',
    ],
}
(root / 'results.json').write_text(json.dumps(summary, indent=2) + '\n')
(root / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
print(json.dumps(summary['counts'], sort_keys=True))
print(json.dumps({'failed_decodes': [x for x in decodes if not x['passed']],
                  'failed_encodes': [x for x in encodes if not x['passed']],
                  'failed_rejections': [x for x in rejects if not x['rejected']],
                  'multiple_frames': special}, indent=2))
