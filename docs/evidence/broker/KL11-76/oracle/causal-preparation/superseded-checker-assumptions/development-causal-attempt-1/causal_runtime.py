"""Independent finite private-runtime capture replay; Python bytes only.

Owner-local clock epochs never order different processes. Exact packets, typed
owner messages and journal checkpoints establish the cross-process edges.
The earlier peer_bytes decoder and its 2/26 preparation controls stay unchanged.
"""
from __future__ import annotations
import collections
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import stat
import sys

sys.dont_write_bytecode = True
import peer_bytes as peer

MAX_FILES = 40000
MAX_CAPTURE_BYTES = 256 * 1024 * 1024
MAX_OWNER_BYTES = 16 * 1024 * 1024
MAX_EVENTS = 50000
MAX_FRAME_FILES = 20000

class Rejected(ValueError):
    pass

def need(value, message):
    if not value:
        raise Rejected(message)

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))

def identity(value):
    need(type(value) is dict and type(value.get('id')) is int and
         0 <= value['id'] <= 0x7fffffff and type(value.get('directory')) is str,
         'full key shape')
    d = value['directory']
    need(len(d) == 32 and d == bytes.fromhex(d).hex() and d != '00' * 16, 'full key directory')
    return value['id'], d

def integer(value, low=0, high=(1 << 64)-1):
    need(type(value) is int and low <= value <= high, 'integer domain')
    return value

def typed(value):
    need(type(value) is dict and set(value) == {'kind', 'body_hex'}, 'typed body shape')
    kind = integer(value['kind'], 1, 40)
    encoded = value['body_hex']
    need(type(encoded) is str and len(encoded) <= 2 * peer.MAX_FRAME, 'typed body byte bound')
    b = bytes.fromhex(encoded)
    need(b.hex() == encoded, 'canonical typed hex')
    return {'kind': kind, 'body_sha256': hashlib.sha256(b).hexdigest(),
            'message': peer.body(kind, b), 'body': b}

def audit_tree(root):
    root = root.resolve()
    need(root.is_dir(), 'capture root missing')
    rows, total = {}, 0
    for p in sorted(root.rglob('*')):
        need(not p.is_symlink(), 'capture symlink')
        if p.is_dir():
            continue
        s = p.stat()
        need(stat.S_ISREG(s.st_mode), 'capture nonregular file')
        total += s.st_size
        need(len(rows) < MAX_FILES and total <= MAX_CAPTURE_BYTES, 'capture total envelope')
        need(s.st_size <= 32 * 1024 * 1024, 'capture individual envelope')
        data = p.read_bytes()
        need(len(data) == s.st_size, 'capture read size changed')
        rows[str(p.relative_to(root))] = {'bytes': len(data), 'mode': stat.S_IMODE(s.st_mode),
                                         'sha256': hashlib.sha256(data).hexdigest()}
    return rows

def load_raw(oracle):
    """Reuse pinned independent dynamic WAL/election/image decoder, not Rust."""
    sys.path.insert(0, str(oracle.resolve()))
    import membership_raw
    return membership_raw

def validate_settings(s, genesis):
    need(type(s) is dict, 'runtime settings missing')
    names = ('frame_bytes record_bytes chunk_bytes live_entries live_bytes operations wal_bytes '
             'fetch_bytes image_bytes image_records image_record_bytes image_payload_bytes '
             'image_chunk_bytes image_decoded_bytes image_generations image_disk_bytes peer_count '
             'incoming_slots client_slots command_queue_capacity peer_mailbox_slots dns_admissions '
             'canonical_configuration_max_bytes task_bound socket_bound_with_listener quorum_ms '
             'quorum_timeout_ms election_min_ms election_max_ms tick_ms heartbeat_ms connect_ms '
             'rpc_ms transfer_ms command_ms transport_charge_bytes transport_pool_bytes managed_bytes '
             'managed_ceiling_bytes extra_test_encode_hex_bytes').split()
    need(set(s) == set(names), 'runtime settings complete exact shape')
    for n in names:
        integer(s[n])
    need(28 <= s['frame_bytes'] <= peer.MAX_FRAME and 1 <= s['record_bytes'] <= peer.MAX_RECORD and
         1 <= s['live_entries'] <= peer.MAX_RECORDS and 1 <= s['operations'] <= 65536 and
         s['record_bytes'] <= s['chunk_bytes'] and s['image_records'] <= s['live_entries'] and
         s['image_payload_bytes'] <= s['live_bytes'] and s['image_record_bytes'] <= s['record_bytes'],
         'actual storage/frame limits')
    p, inc, clients = s['peer_count'], s['incoming_slots'], s['client_slots']
    need(0 <= p <= 63 and 1 <= inc <= 64 and 1 <= clients <= 64 and
         s['peer_mailbox_slots'] == 1 and s['dns_admissions'] == 0 and
         s['canonical_configuration_max_bytes'] == 135168 and s['task_bound'] == 2+p+inc and
         s['socket_bound_with_listener'] == 1+p+inc and
         1 <= s['command_queue_capacity'] <= p+inc+clients+1,
         'actual task/socket/channel/zero-DNS envelope')
    # Native Record/Message ABI sizes are not inferred from a Rust source model.
    # Captured conservative charge must at least cover all three frame buffers;
    # exact pool multiplication and managed ceiling are independently checked.
    need(s['transport_charge_bytes'] >= 3*s['frame_bytes'] and
         s['transport_pool_bytes'] == (p+inc)*s['transport_charge_bytes'] and
         0 < s['managed_bytes'] <= s['managed_ceiling_bytes'] == 512*1024*1024 and
         s['managed_bytes'] + s['extra_test_encode_hex_bytes'] <= s['managed_ceiling_bytes'],
         'actual retained-byte envelope')
    need(1 <= s['election_min_ms'] <= s['election_max_ms'] <= 600000 and
         s['quorum_ms'] == s['quorum_timeout_ms'], 'actual quorum/election settings')
    for n in ('quorum_ms','tick_ms','heartbeat_ms','connect_ms','rpc_ms','transfer_ms','command_ms'):
        need(1 <= s[n] <= 600000, 'actual deadline bounds')
    need(s['tick_ms'] <= s['heartbeat_ms'] and s['heartbeat_ms']*3 <= s['quorum_ms'] and
         s['connect_ms'] <= s['rpc_ms'] <= s['quorum_ms'] and
         s['transfer_ms'] <= s['quorum_ms'], 'actual deadline relationships')
    for voter in genesis['voters']:
        for e in voter['endpoints']:
            need(not ipaddress.ip_address(e['host']).is_unspecified and e['port'] > 0,
                 'numeric immutable route zero-DNS')

class Owner:
    def __init__(self, root, raw, source_sha, overrides=None):
        self.root, self.name = root, root.name
        f = root/'owner.jsonl'
        need(f.stat().st_size <= MAX_OWNER_BYTES, 'owner trace byte envelope')
        lines = f.read_bytes().splitlines()
        need(2 <= len(lines) <= MAX_EVENTS+1, 'owner trace event envelope')
        rows = [json.loads(x) for x in lines]
        if overrides is not None:
            rows = overrides
        h, self.events = rows[0], rows[1:]
        self.header = h
        need(h['schema_version'] == 1 and h['source_sha'] == source_sha and
             h['clock_basis'] == 'process-local monotonic Instant; restart begins a new clock epoch' and
             h['trace_layer'] == 'exclusive Node owner; typed bodies are not TCP packet captures',
             'owner source/layer/clock epoch')
        integer(h['pid'], 1)
        self.local = {'id': h['local_id'], 'directory': h['directory']}
        identity(self.local)
        self.genesis = raw.decode_configuration(bytes.fromhex(h['genesis_hex']))
        need(self.genesis['epoch'] == 0, 'owner genesis initial epoch')
        validate_settings(h['runtime_settings'], self.genesis)
        self.settings = h['runtime_settings']
        self.clock_epoch = {'owner_trace': self.name, 'pid': h['pid'], 'key': self.local}
        self.states, self.checkpoints, self.decoded = [], {}, []
        last_clock = -1
        last_counts = None
        content = election = None
        total_payload = 0
        for ordinal, e in enumerate(self.events):
            need(e['ordinal'] == ordinal, 'owner ordinal gap/reorder')
            now = integer(e['now_ms'])
            need(now >= last_clock, 'owner-local clock regression')
            last_clock = now
            need(e['role'] in ('Follower','Candidate','Leader') and type(e['result_ok']) is bool and
                 type(e['ready']) is bool and type(e['poisoned']) is bool and
                 type(e['voted_directory_available']) is bool, 'owner state canonical types')
            counts = (integer(e['wal_ops'],1),integer(e['election_states'],1),e['poisoned'])
            cp = root/f'checkpoint-{ordinal}'
            if counts != last_counts:
                need(cp.is_dir(), 'changed durable counters missing raw checkpoint')
                content = raw.read_content(cp/'metadata.wal', cp/'images', self.local)
                election = raw.read_election(cp/'election.wal', content)
                need(content.group['genesis'] == self.genesis and
                     len(content.operations) == counts[0] and election['state_count'] == counts[1],
                     'actual durable counter/group binding')
                need((cp/'metadata.wal').stat().st_size <= self.settings['wal_bytes'] and
                     len(content.rows) <= self.settings['live_entries'] and
                     sum(len(r.payload) for r in content.rows) <= self.settings['live_bytes'],
                     'actual WAL/live limits')
                image_files = list((cp/'images').glob('snapshot-*.image'))
                need(len(image_files) <= self.settings['image_generations'] and
                     sum(p.stat().st_size for p in (cp/'images').iterdir() if p.is_file()) <=
                     self.settings['image_disk_bytes'], 'actual image generation/disk limits')
                self.checkpoints[ordinal] = (content, election)
            else:
                need(not cp.exists(), 'extra unbound checkpoint')
            need(content is not None and e['last_term'] == content.tail()[0] and
                 e['last_index'] == content.tail()[1] and e['committed_end'] == content.committed and
                 e['term'] == election['final']['term'] and
                 e['configuration_hex'] == content.view['canonical_hex'] and
                 e['voted_directory_available'] and e['voted_for'] == election['final']['vote'],
                 'owner summary differs from actual checkpoint bytes')
            inp = typed(e['input']) if e['input'] is not None else None
            need(type(e['typed_output']) is list and len(e['typed_output']) <= 64,
                 'owner output envelope')
            out = [typed(x) for x in e['typed_output']]
            need(e['result_ok'] or not out, 'failed owner command invents output')
            self.decoded.append((inp,out))
            self.states.append((content,election))
            last_counts = counts
        need(set(p.name for p in root.glob('checkpoint-*')) ==
             {f'checkpoint-{n}' for n in self.checkpoints}, 'checkpoint directory exact coverage')

def contextual(m):
    if 'context' in m:
        return m['context']
    if 'request' in m:
        return contextual(m['request'])
    if 'offer' in m:
        return contextual(m['offer'])
    if 'leader' in m and type(m['leader']) is dict:
        return {'leader':m['leader'],'peer':m['peer'],'configuration_epoch':m['configuration_epoch']}
    return None

def request_identity(kind, m):
    if kind in (11,13):
        kind, m = {11:10,13:12}[kind], m['request']
    if kind in (21,35):
        r, c = m['response'], m['context']
        return identity(c['leader']), identity(c['peer']), r['term'], r['sequence'], c['configuration_epoch']
    if kind in (32,33):
        m = m['offer']
    c = contextual(m)
    return identity(c['leader']), identity(c['peer']), m['term'], m['sequence'], c['configuration_epoch']

def authority(m):
    c = contextual(m)
    return {'kind':1, 'term':m['term'],'leader':c['leader'],'peer':c['peer'],
            'sequence':m['sequence'],'leader_commit':m['leader_commit'],
            'configuration_epoch':c['configuration_epoch']}

def eligible(raw, content, local, term):
    if raw.contains(content.view, local):
        return True
    p = content.view['position']
    return (p['index'] > content.committed and p['term'] == term and
            raw.contains(raw.configuration_in(content.rows[:p['index']-1], content.group['genesis']),local))

def as_records(raw, rows):
    return [raw.Record(r['index'],r['term'],r['kind'],bytes.fromhex(r['payload_hex'])) for r in rows]

class Graph:
    def __init__(self):
        self.edges = collections.defaultdict(set)
        self.nodes = set()
    def edge(self,a,b):
        need(a != b, 'causal self edge')
        self.nodes.update((a,b));self.edges[a].add(b)
    def finish(self):
        degree = {n:0 for n in self.nodes}
        for edges in self.edges.values():
            for n in edges: degree[n] += 1
        q = collections.deque(n for n,d in degree.items() if not d); seen=0
        while q:
            n=q.popleft();seen+=1
            for x in self.edges[n]:
                degree[x]-=1
                if not degree[x]:q.append(x)
        need(seen == len(self.nodes), 'causal owner/TCP graph cycle')
        return {'nodes':seen,'edges':sum(len(v) for v in self.edges.values())}

WIRE_RE = re.compile(r'^wire-(\d+)-(\d+)-(\d+)-(request|response)-(\d+)\.bin$')
HELLO_RE = re.compile(r'^hello-(\d+)-(\d+)-(\d+)\.bin$')

class Packets:
    def __init__(self, root, raw, overrides=None):
        self.root, self.sessions, self.rows = root, {}, []
        files = sorted((root/'wire').glob('*.bin'))
        need(1 <= len(files) <= MAX_FRAME_FILES, 'actual proxy packet count')
        self.group = None
        for f in files:
            data = overrides.get(str(f), f.read_bytes()) if overrides else f.read_bytes()
            d = peer.frame(data)
            h, w = HELLO_RE.fullmatch(f.name), WIRE_RE.fullmatch(f.name)
            need(h or w, 'proxy packet filename')
            source,target,connection = map(int,(h or w).groups()[:3])
            sess = self.sessions.setdefault((source,target,connection),{'hello':None,'request':{},'response':{}})
            p = {'file':str(f),'source':source,'target':target,'connection':connection,
                 'decoded':d,'id':('packet',str(f))}
            if h:
                need(d['kind'] == 1 and sess['hello'] is None, 'session initial Hello')
                m=d['message'];g=raw.decode_configuration(bytes.fromhex(m['genesis_hex']))
                need(g['epoch'] == 0 and m['source']['id'] == source and m['target']['id'] == target and
                     raw.contains(g,m['source']) and raw.contains(g,m['target']) and m['partition'] <= 0x7fffffff,
                     'Hello complete group/full-directory identity')
                group={'cluster_id':m['cluster'],'topic':m['topic'],'partition':m['partition'],'genesis':g}
                need(self.group is None or self.group == group, 'mixed proxy group')
                self.group=group;sess['hello']=p;p['direction']='hello';p['ordinal']=-1
            else:
                direction,ordinal=w.group(4),int(w.group(5));p.update(direction=direction,ordinal=ordinal)
                need(ordinal not in sess[direction], 'session directional ordinal collision')
                sess[direction][ordinal]=p
            self.rows.append(p)
        need(self.group is not None, 'proxy group Hello missing')
        for (source,target,connection),sess in self.sessions.items():
            need(sess['hello'] is not None, 'proxy packet without Hello')
            initial=sess['hello']['decoded']['message']
            for direction in ('request','response'):
                seq=sess[direction]
                need(sorted(seq) == list(range(len(seq))), 'proxy directional ordinal gap')
                last_rpc=-1
                for p in seq.values():
                    d=p['decoded'];m=d['message'];k=d['kind']
                    need(d['rpc'] > last_rpc, 'session RPC not strictly increasing')
                    last_rpc=d['rpc']
                    if direction=='response' and p['ordinal']==0:
                        need(k == 2 and d['rpc']==0 and m['source']==initial['target'] and
                             m['target']==initial['source'] and m['cluster']==initial['cluster'] and
                             m['topic']==initial['topic'] and m['partition']==initial['partition'] and
                             m['genesis_hex']==initial['genesis_hex'], 'Hello reply immutable identity')
                    else:
                        need(k not in (1,2), 'unexpected session Hello')
                        if k != 40:
                            c=contextual(m)
                            need(c['leader']==initial['source'] and c['peer']==initial['target'],
                                 'packet contradicts Hello full-directory identities')
            requests={p['decoded']['rpc']:p for p in sess['request'].values()}
            for p in sess['response'].values():
                d=p['decoded'];kind=d['kind'];rpc=d['rpc']
                if kind==2:continue
                need(rpc in requests,'response RPC lacks same-session request')
                q=requests[rpc]['decoded'];expected={10:11,12:13,20:21,30:31,32:33,34:35}
                need(q['kind'] in expected and (kind==expected[q['kind']] or kind==40),
                     'session response kind does not answer request')
                qm,m=q['message'],d['message']
                if kind==40:
                    need(m['request_kind']==q['kind'],'failure request-kind mismatch')
                elif kind in (11,13):
                    need(m['request']==qm and (m['voter'] if kind==11 else m['voter']['key']['id'])==target,
                         'echoed request/voter mismatch')
                elif kind==21:
                    need(m['context']==qm['context'] and m['response']['sequence']==qm['sequence'] and
                         m['response']['leader']==source and m['response']['peer']==target,
                         'Append response complete correlation')
                elif kind==31:
                    need(m==qm,'Begun offer mismatch')
                elif kind==33:
                    need(m['offer']==qm['offer'] and m['offset']==qm['offset'] and
                         m['length']==len(bytes.fromhex(qm['bytes_hex'])), 'Chunk response exact stream correlation')
                elif kind==35:
                    need(m['context']==qm['context'] and m['descriptor']==qm['descriptor'] and
                         m['response']['sequence']==qm['sequence'],'Finished descriptor/correlation mismatch')
                p['request']=requests[rpc]


def owner_proof(o, raw, graph):
    """Local order, exact durable summaries, election/commit admission and receipts."""
    counts=collections.Counter(); proofs=[]
    requests={}; used=set(); grants=set(); matched={}; contacts={}
    activated=None; old_term=None; previous=None; prior_counts=0
    remote_inputs=[]
    for n,e in enumerate(o.events):
        eid=('owner',o.name,n)
        if n:graph.edge(('owner',o.name,n-1),eid)
        content,election=o.states[n]; inp,outs=o.decoded[n]
        now=e['now_ms']; term=e['term']; localkey=identity(o.local)
        before=o.states[n-1][0] if n else None
        if old_term != term:
            grants={localkey} if e['voted_for']==o.local else set()
            matched={};contacts={};activated=None
            # Old requests remain as evidence, but cannot admit a current-term ACK.
        newops=content.operations[prior_counts:] if n else []
        need(prior_counts <= len(content.operations),'durable operation counter regression')
        for op in newops:
            auth=op['authority']
            if auth is None:continue
            if auth['kind']==1:
                candidates=[m for ord_,m in remote_inputs if ord_ <= n and authority(m)==auth]
                if inp and e['command']=='receive' and inp['kind'] in (20,34):
                    candidates += [inp['message']] if authority(inp['message'])==auth else []
                need(candidates,'remote WAL authority lacks exact received owner input')
                counts['remote_wal_authority_bindings']+=1
            elif op['opcode']==2:
                need(e['command'] in ('tick','propose','remove','add','prepare') and e['result_ok'] and
                     auth['leader']==o.local and auth['term']==term,
                     'local append lacks successful eligible owner command')
                if activated is None:
                    members={identity(v['key']) for v in content.group['genesis']['voters']}
                    if before:members={identity(v['key']) for v in before.view['voters']}
                    need(e['command']=='tick' and len(grants&members)>=len(members)//2+1,
                         'leader activation lacks consumed distinct current-view majority grants')
                    activated=(term,now+o.settings['quorum_ms'])
                    counts['majority_activations']+=1
            elif op['opcode']==4:
                if before is not None and auth['kind']==0:
                    voters={identity(v['key']) for v in op['view']['voters']}
                    commit=op['committed']
                    voters_matched={k for k,index in matched.items() if k in voters and index>=commit}
                    if localkey in voters:voters_matched.add(localkey)
                    # The triggering ACK is applied immediately below; defer this proof.
                    proofs.append({'kind':'commit','event':n,'term':term,'index':commit,'voters':voters})
        # Inputs and outputs are decoded independently, not accepted as verdicts.
        if inp and e['command']=='receive':
            m=inp['message'];k=inp['kind'];c=contextual(m)
            need(c is not None and c['peer']==o.local,'receive intended full directory')
            if k in (20,34):remote_inputs.append((n,m))
            for out in outs:
                r=out['message']
                if k==10:
                    need(out['kind']==11 and r['request']==m and r['voter']==localkey[0] and
                         r['candidate']==m['candidate'] and r['term']==term,
                         'Vote owner response exact request/term/voter')
                    if r['granted']:
                        need(e['voted_for']==c['leader'] and term==m['term'] and
                             raw.contains((before or content).view,c['leader']) and
                             (m['log']['term'],m['log']['index']) >= (before or content).tail(),
                             'granted Vote lacks synced fresh full-directory election state')
                        counts['synced_vote_grants']+=1
                elif k==20:
                    need(out['kind']==21 and r['context']==c and r['response']['peer']==localkey[0] and
                         r['response']['leader']==m['leader'] and r['response']['sequence']==m['sequence'] and
                         r['response']['term']==term,'Append owner response correlation')
                    if r['response']['success']:
                        records=as_records(raw,m['entries']);p=m['previous'];target=records[-1].index if records else p['index']
                        mt=records[-1].term if records else p['term']
                        need(r['response']['matched']=={'term':mt,'index':target} and
                             records==content.rows[p['index']:p['index']+len(records)] and
                             (p['index']==0 or content.rows[p['index']-1].term==p['term']) and
                             target<=len(content.rows) and term==m['term'],
                             'successful Append response lacks exact persisted prefix')
                        counts['synced_append_receipts']+=1
        if e['command']=='cancel-vote' and e['result_ok']:
            need(inp is not None and inp['kind']==10,'vote cancellation full input missing')
            tok=(10,canonical(inp['message']))
            need(tok in requests and tok not in used,'vote cancellation not one outstanding request')
            used.add(tok);counts['exact_vote_cancellations']+=1
        if e['command']=='ack' and e['result_ok']:
            need(inp is not None and inp['kind'] in (11,13,21,35),'ACK typed response missing')
            r=inp['message'];k=inp['kind'];ident=request_identity(k,r)
            candidates=[]
            for tok,(qn,qkind,q) in requests.items():
                qi=request_identity(qkind,q)
                if qi[:2]==ident[:2] and qi[3:]==ident[3:]:
                    candidates.append((tok,qn,qkind,q))
            need(len(candidates)==1,'successful ACK lacks unique earlier prepared exact request')
            tok,qn,qkind,q=candidates[0]
            need(tok not in used and qn<n and ident[0]==localkey,'ACK reused/canceled/not local')
            if k in (11,13):need(r['request']==q,'ACK request body forged')
            used.add(tok)
            if k==11:
                need(r['candidate']==localkey[0] and r['voter']==ident[1][0], 'vote ACK full peer correlation')
                if r['granted'] and r['term']==q['term']==term:
                    grants.add(ident[1]);counts['consumed_vote_grants']+=1
            elif k in (21,35):
                rr=r['response']
                if rr['success']:
                    need(rr['term']==q['term']==term and rr['matched']['index']<=len(content.rows) and
                         (rr['matched']['index']==0 or content.rows[rr['matched']['index']-1].term==rr['matched']['term']),
                         'ACK progress differs from exact current-term leader prefix')
                    matched[ident[1]]=max(matched.get(ident[1],0),rr['matched']['index'])
                    counts['consumed_durable_append_or_image_receipts']+=1
                if rr['term']==q['term']==term:contacts[ident[1]]=now
            counts['successful_owner_acks']+=1
        for out in outs:
            m,k=out['message'],out['kind']
            if k in (10,12,20,30):
                ident=request_identity(k,m)
                need(ident[0]==localkey and ident[2]==term and
                     ident[4]==content.view['epoch'],'prepared current-term full-directory/view authority')
                tok=(k,canonical(m))
                need(tok not in requests,'repeated owner request preparation')
                requests[tok]=(n,k,m)
                if k==10:
                    need(e['command']=='tick' and e['role']=='Candidate' and e['voted_for']==o.local and
                         m['log']=={'term':content.tail()[0],'index':content.tail()[1]},
                         'campaign current synced self-vote/freshness')
                    grants.add(localkey)
                elif k==20:
                    need(e['role']=='Leader' and activated is not None and eligible(raw,content,o.local,term),
                         'prepared append lacks eligible activated leader')
                    p=m['previous'];rs=as_records(raw,m['entries'])
                    need(m['leader_commit']==content.committed and p['index']<=len(content.rows) and
                         (p=={'term':0,'index':0} or content.rows[p['index']-1].term==p['term']) and
                         rs==content.rows[p['index']:p['index']+len(rs)],'prepared exact durable source prefix')
        for p in [p for p in proofs if p['event']==n]:
            voters=p['voters'];index=p['index'];peers={k for k,j in matched.items() if k in voters and j>=index}
            if localkey in voters:peers.add(localkey)
            need(len(peers)>=len(voters)//2+1 and content.rows[index-1].term==term,
                 'local commit lacks consumed distinct NEW-view current-term majority')
            p['voters']=sorted(voters);p['matched_voters']=sorted(peers)
            counts['new_view_commit_majorities']+=1
        if e['command'] in ('propose','remove','add') and e['result_ok']:
            need(previous is not None and previous['role']=='Leader' and activated is not None and
                 activated[0]==term and eligible(raw,before,o.local,term), 'write admitted without active eligible leader')
            if now>=activated[1]:
                voters={identity(v['key']) for v in before.view['voters']}
                recent={k for k,t in contacts.items() if k in voters and 0<=now-t<=o.settings['quorum_ms']}
                if localkey in voters:recent.add(localkey)
                need(len(recent)>=len(voters)//2+1,'write admission lacks fresh actual quorum contacts')
            counts['clock_epoch_bounded_write_admissions']+=1
        if e['poisoned']:need(not e['ready'],'poisoned owner ready')
        if e['role']!='Leader':activated=None
        previous=e;old_term=term;prior_counts=len(content.operations)
    return {'owner':o.name,'clock_epoch':o.clock_epoch,'events':len(o.events),
            'checkpoints':len(o.checkpoints),'counters':dict(counts),'commit_proofs':proofs}

def verify(root, source_sha, oracle, owner_overrides=None, packet_overrides=None):
    root=root.resolve();before=audit_tree(root);raw=load_raw(oracle)
    owners=[];ignored=[];groups=[];graph=Graph();statistics=collections.Counter()
    for d in sorted(root.iterdir()):
        if (d/'owner.jsonl').is_file():
            if re.fullmatch(r'\d+-node-\d+-\d+',d.name):
                o=Owner(d,raw,source_sha,(owner_overrides or {}).get(d.name));owners.append(o)
            else:ignored.append(d.name)
        elif d.name.startswith('partitionline76-tcp-'):
            groups.append(Packets(d,raw,packet_overrides))
    need(groups and owners,'actual TCP owners/proxy groups missing')
    need(len(owners)<=64 and len(groups)<=8,'owner/group bound')
    summaries=[]
    for o in owners:
        summaries.append(owner_proof(o,raw,graph))
        for key,count in summaries[-1]['counters'].items():statistics[key]+=count
    by_genesis=collections.defaultdict(list)
    for o in owners:by_genesis[o.genesis['canonical_hex']].append(o)
    bindings=[]
    for g in groups:
        oo=by_genesis[g.group['genesis']['canonical_hex']]
        need(len(g.group['genesis']['voters']) in (3,5) and oo,'actual three/five owner group missing')
        need(all(o.states[0][0].group==g.group for o in oo),'owner/Hello complete group binding')
        sent=collections.defaultdict(list);received=collections.defaultdict(list);acks=collections.defaultdict(list)
        for o in oo:
            for n,(inp,out) in enumerate(o.decoded):
                eid=('owner',o.name,n)
                for x in out:sent[(identity(o.local),x['kind'],x['body_sha256'])].append((o,n,eid,x))
                if inp:
                    table=received if o.events[n]['command']=='receive' else acks if o.events[n]['command']=='ack' else None
                    if table is not None:table[(identity(o.local),inp['kind'],inp['body_sha256'])].append((o,n,eid,inp))
        forwarded=collections.defaultdict(list)
        for p in g.rows:
            d=p['decoded'];kind=d['kind'];pid=p['id']
            forwarded[kind,d['body_sha256']].append(p)
            if kind in (1,2):continue
            direction=p['direction']; actor=p['source'] if direction=='request' else p['target']
            descriptor=next(v['key'] for v in g.group['genesis']['voters'] if v['key']['id']==actor)
            origin=sent.get((identity(descriptor),kind,d['body_sha256']),[])
            if kind==40:
                req=p['request'];q=req['decoded']
                failed=[x for x in received.get((identity(descriptor),q['kind'],q['body_sha256']),[])
                        if not x[0].events[x[1]]['result_ok']]
                need(failed,'Failure packet lacks actual rejected destination command')
                origin=failed
            need(len(origin)==1,'forwarded packet lacks unique actual owner generation/rejection')
            graph.edge(origin[0][2],pid)
            if direction=='response':graph.edge(p['request']['id'],pid)
        # A proxy forwarding attempt is not a consumption verdict. Require every
        # observed owner receive/ACK input to have its exact forwarded bytes.
        for table,direction in ((received,'request'),(acks,'response')):
            for (ownerkey,kind,digest),entries in table.items():
                for o,n,eid,x in entries:
                    candidates=[p for p in forwarded.get((kind,digest),[]) if p['direction']==direction and
                                (p['target'] if direction=='request' else p['source'])==ownerkey[0]]
                    need(candidates,'owner consumption lacks actual forwarded TCP response/request')
                    # Equal repeated bytes can be retried. The earliest source
                    # generation is unique; record the packet candidate set and
                    # add an edge only when transport identity is unambiguous.
                    if len(candidates)==1:graph.edge(candidates[0]['id'],eid)
                    else:statistics['ambiguous_retry_packet_matches']+=1
                    if direction=='response' and o.events[n]['result_ok']:
                        need(len(candidates)==1,'successful ACK lacks unique actual session response')
                        p=candidates[0];r=p['request']['decoded'];m=x['message']
                        origins=sent.get((identity(o.local),r['kind'],r['body_sha256']),[])
                        need(len(origins)==1 and origins[0][0] is o and origins[0][1]<n,
                             'consumed response precedes/escapes its same-clock request')
                        graph.edge(origins[0][2],eid)
                        bindings.append({'owner':o.name,'ordinal':n,'response':str(Path(p['file']).relative_to(root)),
                                         'request':str(Path(p['request']['file']).relative_to(root)),
                                         'request_owner_ordinal':origins[0][1],'kind':kind,
                                         'body_sha256':digest,'same_clock_epoch_latency_ms':
                                         o.events[n]['now_ms']-o.events[origins[0][1]]['now_ms']})
                        statistics['actual_tcp_response_consumptions']+=1
        # Every observed input/output channel has order inside a TCP direction;
        # no cross-PID wallclock ordering is invented.
        for session in g.sessions.values():
            for direction in ('request','response'):
                seq=sorted(session[direction].values(),key=lambda x:x['ordinal'])
                if seq:graph.edge(session['hello']['id'],seq[0]['id'])
                for a,b in zip(seq,seq[1:]):graph.edge(a['id'],b['id'])
        # Raw committed records must agree at every observed prefix, independent
        # of reported all-peer success or test assertion counts.
        commits={}
        for o in oo:
            for content,election in o.states:
                for r in content.rows[:content.committed]:
                    need(r.index not in commits or commits[r.index]==r,'committed cross-owner prefix divergence')
                    commits[r.index]=r
        statistics['actual_tcp_frames']+=len(g.rows)
        statistics['complete_hello_sessions']+=len(g.sessions)
        statistics['committed_unique_positions']+=len(commits)
        # Each restarted first raw state needs an earlier complete same-directory
        # raw state whose journal prefix is byte-identical. PID clocks stay local.
        for o in oo:
            c,e=o.states[0]
            if len(c.operations)>1:
                older=[]
                for other in oo:
                    if other is o or other.local!=o.local:continue
                    oc,oe=other.states[-1]
                    if [x['payload_sha256'] for x in oc.operations]==[x['payload_sha256'] for x in c.operations] and oe['final']==e['final']:
                        older.append(other)
                need(len(older)==1,'restart raw baseline lacks exact prior owner journal/election identity')
                graph.edge(('owner',older[0].name,len(older[0].events)-1),('owner',o.name,0))
                statistics['exact_restart_clock_epoch_bridges']+=1
    graph_receipt=graph.finish();after=audit_tree(root)
    need(before==after,'capture bytes/fullmodes changed during checking')
    source_inputs=[]
    for p in (Path(__file__),Path(peer.__file__),oracle/'membership_raw.py',
              oracle.parents[2]/'KL11-15/oracle/history/wal_oracle.py'):
        if p.is_file():
            source_inputs.append({'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
                                  'bytes':p.stat().st_size,'mode':stat.S_IMODE(p.stat().st_mode)})
    return {'schema_version':1,'source_sha':source_sha,'profile':'finite-private-TCP-owner-journal-causality',
            'passed':True,'captures':{'files':len(before),'bytes':sum(x['bytes'] for x in before.values()),
            'complete_sha256':hashlib.sha256(canonical(before).encode()).hexdigest(),
            'all_bytes_and_full_permission_modes_unchanged':True},'owners':summaries,'counters':dict(statistics),
            'graph':graph_receipt,'response_consumption_bindings':bindings,'checker_inputs':source_inputs,
            'excluded_owner_unit_traces':ignored,'limitations':[
                'Finite captured histories; not an exhaustive consensus proof or Kafka native peer compatibility.',
                'Typed owner trace inputs are not packets; only exact proxy packet bindings establish TCP consumption.',
                'Independent byte/pool/task configuration arithmetic does not measure OS socket buffers, allocator overhead or RSS.',
                'No global elapsed-time ordering across PID/restart clock epochs.',
                'Timeout/disconnect trace rows omit typed correlation; exact timeout targets and supervisor joins need separate lifecycle evidence.',
                'Private owner-only unit traces are inventoried but excluded from this actual-TCP causal profile.',
                'Unconsumed proxy responses are forwarding attempts, never counted as successful receipt authority.'
            ]}


def main():
    import argparse
    a=argparse.ArgumentParser();a.add_argument('--captures',type=Path,required=True)
    a.add_argument('--source-sha',required=True);a.add_argument('--membership-oracle',type=Path,required=True)
    a.add_argument('--out',type=Path,required=True);args=a.parse_args()
    need(re.fullmatch('[0-9a-f]{40}',args.source_sha),'exact source pin syntax')
    need(not args.out.exists(),'fresh output required')
    result=verify(args.captures,args.source_sha,args.membership_oracle)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'passed':True,'owners':len(result['owners']),'counters':result['counters'],'out':str(args.out)}))

if __name__=='__main__':
    main()
