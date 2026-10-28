import json
from pathlib import Path
import socket
import struct

ROOT = Path('/workspace/partitionline')
OUT = Path(__file__).resolve().parent / 'apache-fetch'
OUT.mkdir(exist_ok=False)

def exchange(request):
    with socket.create_connection(('127.0.0.1', 19092), timeout=10) as connection:
        connection.sendall(struct.pack('>i', len(request)) + request)
        def read(length):
            output = bytearray()
            while len(output) < length:
                chunk = connection.recv(length - len(output))
                if not chunk:
                    raise EOFError('truncated response')
                output.extend(chunk)
            return bytes(output)
        size, = struct.unpack('>i', read(4))
        assert 0 <= size <= 1024 * 1024, size
        return read(size)

def text(value):
    encoded = value.encode()
    return struct.pack('>h', len(encoded)) + encoded

for batch in ['log-batch-0.bin', 'log-batch-3.bin']:
    payload = (ROOT / 'partitionline-broker/tests/fixtures/fetch/4.1.2' / batch).read_bytes()
    request = struct.pack('>hhi', 0, 3, 77) + text('goal') + struct.pack('>hhi', -1, 1, 10000)
    request += struct.pack('>i', 1) + text('alpha') + struct.pack('>iii', 1, 0, len(payload)) + payload
    response = exchange(request)
    (OUT / (batch + '.produce.request')).write_bytes(request)
    (OUT / (batch + '.produce.response')).write_bytes(response)
    error, = struct.unpack_from('>h', response, 21)
    assert error == 0, response.hex()

results = []
for version in [4, 5, 6]:
    for case in ['offset4', 'out-of-range-1', 'out-of-range5', 'out-of-range9223372036854775807']:
        name = f'fetch-v{version}-{case}'
        request = (ROOT / 'partitionline-broker/tests/fixtures/fetch/4.1.2' / (name + '.request.bin')).read_bytes()
        response = exchange(request)
        (OUT / (name + '.request.bin')).write_bytes(request)
        (OUT / (name + '.response.bin')).write_bytes(response)
        # Correlation, throttle, one topic named alpha, one partition.
        correlation, throttle, topics = struct.unpack_from('>iii', response)
        assert topics == 1
        length, = struct.unpack_from('>h', response, 12)
        assert response[14:14+length] == b'alpha'
        pos = 14 + length
        count, partition, error, high, stable = struct.unpack_from('>iihqq', response, pos)
        assert count == 1 and partition == 0
        start = struct.unpack_from('>q', response, pos + 26)[0] if version >= 5 else None
        results.append({'case': name, 'correlation': correlation, 'throttle_ms': throttle,
            'error': error, 'high_watermark': high, 'last_stable_offset': stable, 'log_start_offset': start})
print(json.dumps(results, indent=2))
(OUT / 'decoded.json').write_text(json.dumps(results, indent=2) + '\n')
