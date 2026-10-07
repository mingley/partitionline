#!/usr/bin/env python3
"""Replay direct owner cancellation and lifetime captures without importing Rust."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True
import causal_runtime as c
import peer_bytes as peer


def verify(root, source, oracle, owner_overrides=None, documents=None):
    before = c.audit_tree(root)
    raw = c.load_raw(oracle)
    graph = c.Graph()
    counters = {}
    owners = []
    for path in sorted(root.glob('*/owner.jsonl')):
        owner = c.Owner(path.parent, raw, source, (owner_overrides or {}).get(path.parent.name),
                        direct_control=(path.parent/'lifecycle-direct-owner-joined.json').exists())
        proof = c.owner_proof(owner, raw, graph)
        for name, count in proof['counters'].items():
            counters[name] = counters.get(name, 0) + count
        direct = path.parent/'lifecycle-direct-owner-joined.json'
        if direct.exists():
            row = (documents or {}).get(str(direct.relative_to(root)), json.loads(direct.read_bytes()))
            c.need(row is not None and row['schema_version'] == 1 and row['capture_revision'] == 2 and
                   row['source_sha'] == source and row['pid'] == owner.header['pid'] and
                   row['local_id'] == owner.local['id'] and row['directory'] == owner.local['directory'] and
                   row['stage'] == 'direct-owner-joined' and row['owner_joined'] is True and
                   row['supervisor_joined'] is None and row['listener_closed'] is None and
                   row['network_workers_spawned'] == row['network_workers_joined'] == 0 and
                   row['stopping'] is True and row['actual_mpsc_capacity'] == owner.settings['command_queue_capacity'] and
                   row['command_envelope_owner_bound'] == owner.settings['command_owner_bound'] and
                   row['available_client_permits'] == owner.settings['client_slots'] and
                   row['available_transport_permits'] == owner.settings['transport_pool_bytes'],
                   'direct owner thread join, identity, channel and restored permits')
            c.resources(row['resource_owners'], owner.settings, joined=True)
            c.need(all(row['resource_owners']['gauges'][i]['peak'] == 0 for i in (2,3)),
                   'direct owner creates no network workers or sockets')
        else:
            c.need(len(owner.lifecycle) == 2, 'TCP rejection control joins all workers and supervisor')
        rejected = []
        old_timeouts = []
        for n, event in enumerate(owner.events):
            inp, _ = owner.decoded[n]
            if event['command'] == 'ack' and not event['result_ok'] and inp:
                c.need(n > 0 and owner.states[n][0].operations == owner.states[n-1][0].operations and
                       owner.states[n][1]['final'] == owner.states[n-1][1]['final'],
                       'rejected genuine ACK must not mutate durable WAL/election state')
                rejected.append(n)
            if event['command'] == 'timeout' and event['result_ok'] and not event['cleanup']['append_or_image_released']:
                old_timeouts.append(n)
        counters['rejected_actual_acknowledgements'] = counters.get('rejected_actual_acknowledgements',0)+len(rejected)
        counters['nonreleasing_old_timeouts'] = counters.get('nonreleasing_old_timeouts',0)+len(old_timeouts)
        proof.update(direct_owner=direct.exists(), rejected_ack_ordinals=rejected, nonreleasing_timeout_ordinals=old_timeouts)
        owners.append(proof)
    c.need(len(owners) >= 10 and counters.get('exact_vote_cancellations',0) >= 8 and
           counters.get('exact_timeout_cancellations',0) >= 1 and
           counters['rejected_actual_acknowledgements'] >= 9 and counters['nonreleasing_old_timeouts'] >= 1,
           'required real abandoned-vector, full-route, late-ACK and old-timeout controls')
    c.need(before == c.audit_tree(root), 'owner control capture bytes/full modes unchanged')
    return dict(passed=True, counters=counters, owners=owners, graph=graph.finish(),
                scope='Direct owned Node controls plus one TCP unauthorized-leader rejection; no direct-owner input is counted as a captured TCP packet.')


def main():
    a = argparse.ArgumentParser()
    a.add_argument('--producer-proof',type=Path,required=True)
    a.add_argument('--producer-command',required=True)
    a.add_argument('--captures',type=Path,required=True)
    a.add_argument('--source-sha',required=True)
    a.add_argument('--membership-oracle',type=Path,required=True)
    a.add_argument('--out-dir',type=Path,required=True)
    args = a.parse_args()
    c.need(not args.out_dir.exists(), 'fresh owner-control output required')
    args.out_dir.mkdir(parents=True)
    paths = [Path(__file__),Path(c.__file__),Path(peer.__file__),Path(__file__).with_name('check-runtime-capture.py')]
    def audit_sources():
        return {str(p):dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=p.stat().st_size,mode=stat.S_IMODE(p.stat().st_mode)) for p in paths}
    sources = audit_sources()
    spec = importlib.util.spec_from_file_location('producer_checker',Path(__file__).with_name('check-runtime-capture.py'))
    producer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(producer)
    binding, postguard = producer.verify_producer(args.producer_proof,args.captures,args.producer_command,args.source_sha)
    positive = verify(args.captures,args.source_sha,args.membership_oracle)
    controls = []
    def negative(name, owners=None, documents=None):
        try:
            verify(args.captures,args.source_sha,args.membership_oracle,owners,documents)
        except (c.Rejected,peer.Rejected,ValueError) as error:
            row=dict(name=name,rejected=True,reason=str(error))
            controls.append(row)
            (args.out_dir/(name+'.json')).write_text(json.dumps(row,indent=2)+'\n')
        else:
            raise AssertionError(name)
    owner_files = sorted(args.captures.glob('*/owner.jsonl'))
    failed = next(p for p in owner_files if any(e['command']=='ack' and not e['result_ok'] and e['input'] and e['input']['kind']==21 for e in [json.loads(x) for x in p.read_text().splitlines()][1:]))
    rows = [json.loads(x) for x in failed.read_text().splitlines()]
    ix = next(i for i,e in enumerate(rows) if i and e['command']=='ack' and not e['result_ok'] and e['input'] and e['input']['kind']==21)
    mutant=copy.deepcopy(rows);mutant[ix]['result_ok']=True
    negative('cancelled-real-append-ACK-counted-as-success',{failed.parent.name:mutant})
    old = next(i for i,e in enumerate(rows) if i and e['command']=='timeout' and not e['cleanup']['append_or_image_released'])
    mutant=copy.deepcopy(rows);mutant[old]['cleanup']['append_or_image_released']=True
    negative('old-timeout-pretends-to-release-newer-owner',{failed.parent.name:mutant})
    vote = next(p for p in owner_files if any(e['command']=='ack' and not e['result_ok'] and e['input'] and e['input']['kind']==11 for e in [json.loads(x) for x in p.read_text().splitlines()][1:]))
    rows = [json.loads(x) for x in vote.read_text().splitlines()]
    ix = next(i for i,e in enumerate(rows) if i and e['command']=='ack' and not e['result_ok'] and e['input'] and e['input']['kind']==11)
    mutant=copy.deepcopy(rows);mutant[ix]['result_ok']=True
    negative('abandoned-vector-real-vote-counted-as-success',{vote.parent.name:mutant})
    joined = next(args.captures.glob('*/lifecycle-direct-owner-joined.json'))
    row=json.loads(joined.read_bytes());key=str(joined.relative_to(args.captures))
    mutant=copy.deepcopy(row);mutant['available_client_permits']-=1
    negative('direct-owner-admission-permit-leak',documents={key:mutant})
    mutant=copy.deepcopy(row);mutant['owner_joined']=False
    negative('direct-owner-thread-unjoined',documents={key:mutant})
    negative('direct-owner-join-receipt-missing',documents={key:None})
    binding['independent_source_after'] = postguard()
    c.need(binding['independent_source_before'] == binding['independent_source_after'],
           'original immutable owner-control source changed')
    c.need(sources == audit_sources(), 'independent owner checker sources changed')
    result=dict(source_sha=args.source_sha,passed=True,producer=binding,positive=positive,negative_controls=controls,checker_sources=sources)
    (args.out_dir/'validation.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    print(json.dumps(dict(passed=True,counters=positive['counters'],negative_controls=len(controls))))

if __name__ == '__main__':
    main()
