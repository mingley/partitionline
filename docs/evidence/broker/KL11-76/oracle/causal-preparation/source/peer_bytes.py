"""Independent WORK decoder for proposed private peer bytes; no runtime proof.

The packet length and CRC are checked before body parsing. This parser never
imports the Rust codec. Semantic durable-authority checks remain separate.
"""
import hashlib
import struct

MAX_FRAME = 4 * 1024 * 1024
MAX_CONFIGURATION = 132 * 1024
MAX_RECORDS = 4096
MAX_RECORD = 1024 * 1024
MAX_TERM = 1 << 31


class Rejected(ValueError):
    pass


def require(condition, label):
    if not condition:
        raise Rejected(label)


def crc32c(data):
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
        table.append(value)
    result = 0xFFFFFFFF
    for value in data:
        result = (result >> 8) ^ table[(result ^ value) & 255]
    return result ^ 0xFFFFFFFF


class Reader:
    def __init__(self, data):
        self.data, self.offset = data, 0

    def take(self, count):
        require(0 <= count <= len(self.data) - self.offset, 'truncated private body')
        value = self.data[self.offset:self.offset + count]
        self.offset += count
        return value

    def number(self, width, signed=False):
        return int.from_bytes(self.take(width), 'big', signed=signed)

    def zero(self, count):
        require(not any(self.take(count)), 'nonzero reserved private bytes')

    def finish(self):
        require(self.offset == len(self.data), 'trailing private body')

    def boolean(self):
        value = self.number(1)
        require(value in (0, 1), 'noncanonical boolean')
        return bool(value)

    def term(self):
        value = self.number(8)
        require(1 <= value <= MAX_TERM, 'term domain')
        return value

    def positive(self):
        value = self.number(8)
        require(value > 0, 'positive private correlation/index')
        return value

    def key(self):
        node, directory = self.number(4), self.take(16)
        require(node <= 0x7FFFFFFF and any(directory), 'full directory identity')
        return {'id': node, 'directory': directory.hex()}

    def text(self):
        size = self.number(2)
        require(1 <= size <= 249, 'bounded identity text')
        try:
            return self.take(size).decode('utf-8')
        except UnicodeDecodeError as error:
            raise Rejected('identity UTF8') from error

    def position(self):
        term, index = self.number(8), self.number(8)
        require((term == index == 0) or (1 <= term <= MAX_TERM and index > 0), 'position domain')
        return {'term': term, 'index': index}

    def context(self):
        return {'leader': self.key(), 'peer': self.key(), 'configuration_epoch': self.number(8)}

    def vote(self):
        context = self.context()
        sequence, term, candidate, log = self.positive(), self.term(), self.number(4), self.position()
        require(candidate == context['leader']['id'] and log['term'] < term, 'vote candidate/history domain')
        return {'context': context, 'sequence': sequence, 'term': term, 'candidate': candidate, 'log': log}

    def feature(self):
        return {'leader': self.key(), 'peer': self.key(), 'term': self.term(),
                'sequence': self.positive(), 'configuration_epoch': self.number(8)}

    def response(self):
        peer, leader, sequence, term = self.number(4), self.number(4), self.positive(), self.term()
        success = self.boolean()
        self.zero(7)
        return {'peer': peer, 'leader': leader, 'sequence': sequence, 'term': term,
                'success': success, 'matched': self.position(), 'conflict_index': self.number(8)}

    def descriptor(self):
        generation, base = self.take(16), self.position()
        records, payload_bytes, size, checksum = self.number(8), self.number(8), self.number(8), self.number(4)
        require(any(generation) and records <= MAX_RECORDS and base['index'] == records
                and payload_bytes <= 64 * 1024 * 1024 and size <= 128 * 1024 * 1024, 'image descriptor bounds')
        return {'generation': generation.hex(), 'base': base, 'records': records,
                'payload_bytes': payload_bytes, 'bytes': size, 'checksum': checksum}

    def offer(self):
        context = self.context()
        leader, peer, sequence = self.number(4), self.number(4), self.positive()
        term, commit, descriptor = self.term(), self.number(8), self.descriptor()
        require(leader == context['leader']['id'] and peer == context['peer']['id']
                and descriptor['base']['term'] <= term and descriptor['base']['index'] <= commit,
                'image request identity/authority domain')
        return {'context': context, 'leader': leader, 'peer': peer, 'sequence': sequence,
                'term': term, 'leader_commit': commit, 'descriptor': descriptor}

    def voter(self):
        key, minimum, maximum = self.key(), self.number(2, True), self.number(2, True)
        count = self.number(2)
        require(1 <= count <= 4 and 0 <= minimum <= maximum, 'feature voter bounds')
        endpoints = []
        for _ in range(count):
            listener, host, port = self.text(), self.text(), self.number(2)
            require(all(char in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for char in listener)
                    and '\0' not in host and port > 0, 'feature endpoint syntax')
            require(not endpoints or endpoints[-1]['listener'] < listener, 'canonical endpoint order')
            endpoints.append({'listener': listener, 'host': host, 'port': port})
        return {'key': key, 'kraft_min': minimum, 'kraft_max': maximum, 'endpoints': endpoints}


def body(kind, data, max_records=MAX_RECORDS):
    require(len(data) <= MAX_FRAME - 28 and 0 <= max_records <= MAX_RECORDS, 'body envelope')
    r = Reader(data)
    if kind in (1, 2):
        source, target, cluster, topic = r.key(), r.key(), r.text(), r.text()
        partition, size = r.number(4), r.number(4)
        require(40 <= size <= MAX_CONFIGURATION, 'complete canonical genesis size')
        genesis = r.take(size)
        require(genesis[:8] == b'PLVOTR01', 'genesis magic; full Voters decoder still required')
        result = {'source': source, 'target': target, 'cluster': cluster, 'topic': topic,
                  'partition': partition, 'genesis_hex': genesis.hex()}
    elif kind == 10:
        result = r.vote()
    elif kind == 11:
        result = {'request': r.vote(), 'term': r.term(), 'voter': r.number(4),
                  'candidate': r.number(4), 'granted': r.boolean()}
    elif kind == 12:
        result = r.feature()
    elif kind == 13:
        result = {'request': r.feature(), 'voter': r.voter()}
    elif kind == 20:
        context = r.context()
        leader, peer, sequence, term = r.number(4), r.number(4), r.positive(), r.term()
        previous, commit, count = r.position(), r.number(8), r.number(4)
        r.zero(4)
        require(count <= max_records and leader == context['leader']['id']
                and peer == context['peer']['id'] and previous['term'] <= term, 'append count/identity/domain')
        entries = []
        for _ in range(count):
            entry_term, index, tag = r.term(), r.positive(), r.number(1)
            r.zero(3)
            size = r.number(4)
            require(tag <= 2 and size <= MAX_RECORD and (tag == 1) == (size == 0), 'record kind/payload bound')
            payload = r.take(size)
            entries.append({'term': entry_term, 'index': index, 'kind': tag,
                            'payload_hex': payload.hex(), 'payload_sha256': hashlib.sha256(payload).hexdigest()})
        result = {'context': context, 'leader': leader, 'peer': peer, 'sequence': sequence,
                  'term': term, 'previous': previous, 'leader_commit': commit, 'entries': entries}
    elif kind == 21:
        result = {'context': r.context(), 'response': r.response()}
    elif kind in (30, 31, 34):
        result = r.offer()
    elif kind in (32, 33):
        offer, offset, size = r.offer(), r.number(8), r.number(4)
        result = {'offer': offer, 'offset': offset, 'length': size}
        if kind == 32:
            payload = r.take(size)
            result['bytes_hex'] = payload.hex()
            result['bytes_sha256'] = hashlib.sha256(payload).hexdigest()
    elif kind == 35:
        result = {'context': r.context(), 'response': r.response(), 'descriptor': r.descriptor()}
    elif kind == 40:
        request_kind, code = r.number(1), r.number(1)
        require(1 <= code <= 8, 'finite failure code')
        result = {'request_kind': request_kind, 'code': code}
    else:
        raise Rejected('unknown private message kind')
    r.finish()
    return result


def frame(data, with_tcp_length=True, max_records=MAX_RECORDS):
    if with_tcp_length:
        require(4 <= len(data) <= MAX_FRAME + 4, 'bounded TCP capture')
        size = int.from_bytes(data[:4], 'big')
        require(size == len(data) - 4, 'exact TCP private length')
        data = data[4:]
    require(28 <= len(data) <= MAX_FRAME, 'bounded private frame')
    require(crc32c(data[:-4]) == int.from_bytes(data[-4:], 'big'), 'private CRC32C')
    r = Reader(data[:-4])
    require(r.take(8) == b'PLPEER01' and r.number(2) == 1, 'private magic/version')
    kind = r.number(1)
    r.zero(1)
    rpc = r.number(8)
    r.zero(4)
    require((kind in (1, 2)) == (rpc == 0), 'Hello/private RPC correlation domain')
    payload = r.take(len(r.data) - r.offset)
    return {'kind': kind, 'rpc': rpc, 'message': body(kind, payload, max_records),
            'body_sha256': hashlib.sha256(payload).hexdigest(),
            'frame_sha256': hashlib.sha256(data).hexdigest()}
