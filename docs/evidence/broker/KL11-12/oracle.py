#!/usr/bin/env python3
"""Independent causal fixed-voter Raft model and actual-journal verifier.

No Rust code, generated Rust decisions or PRNG implementation is imported. Timer
draws are checked against declared bounds. Peer grants must have an earlier
actual voter decision, and winning candidates need distinct majority grants.
This checks election histories, not Kafka wire compatibility or committed logs.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import struct
import sys

ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location('independent_journal_crc', ROOT / 'docs/evidence/broker/KL11-03/verify-format.py')
CRC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CRC)
MAX_U64 = 2**64 - 1
MAX_HISTORY = 32 * 1024 * 1024
MAX_EVENTS = 100_000


def require(condition, message):
    if not condition:
        raise ValueError(message)


def position(term, index):
    require(type(term) is int and type(index) is int and 0 <= term <= MAX_U64 and 0 <= index <= MAX_U64, 'invalid log coordinates')
    require((term == 0) == (index == 0), 'mixed empty/nonempty log')
    return term, index


def durable(node):
    return node['term'], node['vote'], node['log_term'], node['log_index']


def parse_journal(path, local, members, budget):
    payload_bytes = 48 + 4 * len(members)
    maximum = 24 + (32 + payload_bytes) * budget
    require(path.stat().st_size <= maximum, 'journal exceeds declared bound')
    data = path.read_bytes()
    require(len(data) >= 24 and data[:8] == b'PLJRNL01', 'bad journal header')
    base, flags, checksum = struct.unpack_from('>QII', data, 8)
    require(base == flags == 0 and checksum == CRC.crc32c(data[:20]), 'journal header CRC/base/flags')
    result = []
    cursor = 24
    while cursor < len(data):
        header = data[cursor:cursor + 32]
        require(len(header) == 32 and header[:8] == b'PLENTRY1', 'partial/bad entry header')
        size, offset, records, payload_crc, header_crc = struct.unpack_from('>IQIII', header, 8)
        require(size == payload_bytes and offset == len(result) and records == 1, 'entry offset/count/size mismatch')
        require(header_crc == CRC.crc32c(header[:28]), 'entry header checksum')
        payload = data[cursor + 32:cursor + 32 + size]
        require(len(payload) == size and payload_crc == CRC.crc32c(payload), 'payload checksum/truncation')
        magic, node, count, reserved, term, vote, present, padding, log_term, log_index = struct.unpack_from('>8sIHHQIB3sQQ', payload)
        require(magic == b'PLELECT1' and reserved == 0 and padding == b'\0\0\0', 'election encoding/reserved bytes')
        require(node == local and count == len(members), 'persisted identity mismatch')
        ids = list(struct.unpack_from('>' + 'I' * count, payload, 48))
        require(ids == members and present in [0, 1], 'persisted membership/vote flag')
        require((present == 0 and vote == 0) or (present == 1 and vote in members), 'persisted voter identity')
        position(log_term, log_index)
        require(log_term <= term and (term != 0 or present == 0), 'persisted term/log invalid')
        state = (term, vote if present else None, log_term, log_index)
        if result:
            old = result[-1]
            require(term >= old[0], 'durable term regressed')
            require(term != old[0] or old[1] is None or state[1] == old[1], 'durable double vote or vote erased')
            require(log_term >= old[2] and log_index >= old[3] and (log_index != old[3] or log_term == old[2]), 'durable log summary regressed/rewritten')
        result.append(state)
        require(len(result) <= budget, 'journal snapshot budget')
        cursor += 32 + size
    require(result and result[0] == (0, None, 0, 0), 'missing initial durable state')
    return result


class Oracle:
    def __init__(self, directory):
        self.directory = directory
        self.config = None
        self.nodes = {}
        self.votes = {}
        self.winners = {}
        self.final = set()
        self.journals = set()
        self.isolated = 0
        self.totals = {'seeds': 0, 'events': 0, 'campaigns': 0, 'elections': 0, 'denials': 0, 'restarts': 0, 'journals': 0}

    def finish(self):
        require(self.config is not None and self.final == set(self.members) and self.journals == set(self.members), 'incomplete history/final journal set')

    def connected(self, source, target):
        return self.isolated == 0 or ((source == self.isolated) == (target == self.isolated))

    def persist(self, node, previous):
        current = durable(node)
        if current != previous:
            require(current[0] >= previous[0], 'term regressed')
            require(current[0] != previous[0] or previous[1] is None or current[1] == previous[1], 'double vote or same-term vote erased')
            node['durable'].append(current)

    def elected(self, local, node):
        term = node['term']
        require(len(node['voters']) >= self.majority, 'candidate elected without majority')
        require(self.winners.get(term, local) == local, 'two leaders elected in one term')
        self.winners[term] = local
        node['role'], node['leader'] = 'Leader', local
        self.totals['elections'] += 1

    def follower(self, node):
        node['role'], node['leader'], node['voters'] = 'Follower', None, set()

    def event(self, event):
        kind = event['kind']
        self.totals['events'] += 1
        if kind == 'config':
            if self.config is not None:
                self.finish()
            self.config = event
            self.members = event['members']
            require(1 <= len(self.members) <= 64 and sorted(set(self.members)) == self.members, 'invalid fixed membership')
            require(all(type(node) is int and 0 <= node <= 2**31 - 1 for node in self.members), 'invalid voter IDs')
            self.minimum, self.maximum = event['timeouts']
            require(1 <= self.minimum <= self.maximum <= 600_000, 'timeout bounds')
            self.budget = event['max_states']
            require(1 <= self.budget <= 65_536, 'state budget')
            self.majority = len(self.members) // 2 + 1
            self.nodes, self.votes, self.winners = {}, {}, {}
            self.final, self.journals = set(), set()
            self.isolated = 0
            self.totals['seeds'] += 1
            return
        require(self.config is not None, 'event before configuration')
        if kind == 'partition':
            require(event['isolated'] == 0 or event['isolated'] in self.members, 'partition identity')
            self.isolated = event['isolated']
            return
        local = event['node']
        require(type(local) is int and local in self.members, 'unknown event node')
        if kind == 'journal':
            name = event['path']
            require(Path(name).name == name and name == f'seed-{self.config["seed"]}-node-{local}.journal', 'journal path must be owned basename')
            require(local in self.final and local not in self.journals, 'journal before final or duplicate')
            states = parse_journal(self.directory / name, local, self.members, self.budget)
            require(states == self.nodes[local]['durable'], 'actual synchronized journal differs from causal model')
            self.journals.add(local)
            self.totals['journals'] += 1
            return
        now = event['now']
        require(type(now) is int and 0 <= now <= MAX_U64, 'invalid clock')
        reset_timer = False
        if kind == 'open':
            require(local not in self.nodes, 'duplicate initial open')
            node = {'term': 0, 'vote': None, 'log_term': 0, 'log_index': 0, 'role': 'Follower', 'leader': None,
                    'voters': set(), 'now': now, 'deadline': None, 'durable': [(0, None, 0, 0)]}
            self.nodes[local] = node
            reset_timer = True
        else:
            require(local in self.nodes, 'event before node open')
            node = self.nodes[local]
            require(now >= node['now'], 'clock moved backwards')
            previous = durable(node)
            if kind == 'restart':
                self.follower(node)
                reset_timer = True
                self.totals['restarts'] += 1
            elif kind == 'tick':
                due = node['role'] != 'Leader' and now >= node['deadline']
                require(event['outcome'] == ('campaign' if due else 'idle'), 'wrong timer/campaign decision')
                if due:
                    require(node['term'] < MAX_U64, 'term overflow')
                    node['term'] += 1
                    node['vote'], node['role'], node['leader'], node['voters'] = local, 'Candidate', None, {local}
                    expected = {'term': node['term'], 'candidate': local, 'log_term': node['log_term'], 'log_index': node['log_index']}
                    require(event['request'] == expected, 'outbound candidacy before durable self-vote')
                    self.votes[(node['term'], local)] = local
                    reset_timer = True
                    self.totals['campaigns'] += 1
                    if self.majority == 1:
                        self.elected(local, node)
            elif kind == 'request_vote':
                request, response = event['request'], event['response']
                candidate, term = request['candidate'], request['term']
                require(type(candidate) is int and candidate in self.members and self.connected(candidate, local), 'unknown/cross-partition request')
                log = position(request['log_term'], request['log_index'])
                require(type(term) is int and 0 < term <= MAX_U64 and log[0] <= term, 'invalid vote request')
                require(type(response['granted']) is bool, 'vote result must be boolean')
                if term > node['term']:
                    node['term'], node['vote'] = term, None
                    self.follower(node)
                    reset_timer = True
                eligible = term == node['term'] and log >= (node['log_term'], node['log_index'])
                eligible &= node['vote'] in [None, candidate] and node['leader'] in [None, candidate]
                require(response == {'term': node['term'], 'voter': local, 'candidate': candidate, 'granted': eligible}, 'invalid affirmative/negative vote response')
                if eligible:
                    require(self.votes.get((term, local), candidate) == candidate, 'one voter chose two candidates in one term')
                    self.votes[(term, local)] = candidate
                    node['vote'] = candidate
                    reset_timer = True
                else:
                    self.totals['denials'] += 1
            elif kind == 'receive_vote':
                response = event['response']
                voter, term = response['voter'], response['term']
                require(type(voter) is int and voter in self.members and self.connected(voter, local) and response['candidate'] == local and type(term) is int and 0 < term <= MAX_U64, 'invalid/misrouted/cross-partition response')
                require(type(response['granted']) is bool, 'vote result must be boolean')
                if response['granted']:
                    require(self.votes.get((term, voter)) == local, 'counted grant lacks an actual preceding durable vote')
                if term > node['term']:
                    node['term'], node['vote'] = term, None
                    self.follower(node)
                    reset_timer = True
                    outcome = 'SteppedDown'
                elif term < node['term'] or node['role'] != 'Candidate' or voter in node['voters']:
                    outcome = 'Ignored'
                elif not response['granted']:
                    outcome = 'Rejected'
                else:
                    node['voters'].add(voter)
                    outcome = 'Counted'
                    if len(node['voters']) >= self.majority:
                        self.elected(local, node)
                        outcome = 'Elected'
                require(event['outcome'] == outcome, 'invalid vote tally/duplicate election')
            elif kind == 'observe_leader':
                leader, term = event['leader'], event['term']
                require(leader in self.members and self.connected(leader, local), 'invalid/cross-partition leader assertion')
                require(self.winners.get(term) == leader, 'asserted leader has no actual majority election in this history')
                accepted = term >= node['term']
                require(event['accepted'] == accepted, 'stale leader accepted')
                if accepted:
                    require(term != node['term'] or node['leader'] in [None, leader], 'conflicting same-term leader')
                    if term > node['term']:
                        node['term'], node['vote'] = term, None
                    if leader != local:
                        self.follower(node)
                    else:
                        require(node['role'] == 'Leader', 'self promotion without election')
                    node['leader'] = leader
                    reset_timer = True
            elif kind == 'advance_log':
                log = position(event['log_term'], event['log_index'])
                require(log[0] <= node['term'] and log[0] >= node['log_term'] and log[1] >= node['log_index'], 'log regressed or exceeds current term')
                require(log[1] != node['log_index'] or log == (node['log_term'], node['log_index']), 'rewrote an existing log index')
                node['log_term'], node['log_index'] = log
            elif kind == 'lose_quorum':
                self.follower(node)
                reset_timer = True
            elif kind == 'final':
                require(local not in self.final, 'duplicate final node')
                self.final.add(local)
            else:
                raise ValueError(f'unknown history event {kind}')
            self.persist(node, previous)
        node['now'] = now
        actual = event['state']
        require(all(type(actual[key]) is int for key in ['term', 'log_term', 'log_index', 'deadline', 'grants', 'states']), 'state counters must be integers')
        require(type(actual['poisoned']) is bool, 'poison flag must be boolean')
        expected = {key: node[key] for key in ['term', 'vote', 'log_term', 'log_index', 'role', 'leader']}
        expected.update(grants=len(node['voters']), poisoned=False, states=len(node['durable']))
        require(all(actual.get(key) == value for key, value in expected.items()), f'actual state differs from independent model: expected {expected}, actual {actual}')
        deadline = actual['deadline']
        if reset_timer:
            require(now + self.minimum <= deadline <= min(MAX_U64, now + self.maximum), 'random timeout outside declared absolute bounds')
        else:
            require(deadline == node['deadline'], 'unjustified timeout reset')
        node['deadline'] = deadline


def verify(path):
    require(path.stat().st_size <= MAX_HISTORY, 'history file too large')
    model = Oracle(path.parent)
    with path.open() as source:
        for line_number, line in enumerate(source, 1):
            require(line_number <= MAX_EVENTS, 'history event count bound')
            try:
                model.event(json.loads(line))
            except (ValueError, KeyError, TypeError, struct.error, OSError) as error:
                return {'verdict': 'failed', 'line': line_number, 'error': str(error), 'counts': model.totals,
                        'production_qualification': False}
    model.finish()
    return {'verdict': 'passed', 'counts': model.totals, 'oracle': 'independent causal Raft state machine plus bitwise CRC32C actual-journal comparison',
            'scope': 'fixed-member election and supplied durable log summaries; no wire, replication, leases or full KRaft qualification',
            'production_qualification': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('history', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    try:
        result = verify(args.history)
    except (ValueError, KeyError, TypeError, OSError) as error:
        result = {'verdict': 'failed', 'error': str(error), 'production_qualification': False}
    text = json.dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as output:
            output.write(text)
    print(text, end='')
    return 0 if result['verdict'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
