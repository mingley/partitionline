#!/usr/bin/env python3
"""Actual retained-history controls; never launches broker or imports Rust.

All packet mutants repair the private Castagnoli checksum and retain the TCP
length. Their rejection tests causal/identity/session facts after valid framing.
Original evidence is read-only; trace substitutions stay in checker memory.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

sys.dont_write_bytecode=True
import causal_runtime as c
import peer_bytes as peer

def patched_packet(data,offset,size,value):
    b=bytearray(data)
    replacement=value if isinstance(value,bytes) else value.to_bytes(size,'big')
    assert len(replacement)==size
    b[offset:offset+size]=replacement
    b[-4:]=peer.crc32c(bytes(b[4:-4])).to_bytes(4,'big')
    assert int.from_bytes(b[:4],'big')==len(b)-4
    peer.frame(bytes(b))  # Must reach semantic/causal checking with valid framing.
    return bytes(b)

def main():
    a=argparse.ArgumentParser();a.add_argument('--captures',type=Path,required=True)
    a.add_argument('--source-sha',required=True);a.add_argument('--membership-oracle',type=Path,required=True)
    a.add_argument('--out-dir',type=Path,required=True);args=a.parse_args()
    assert not args.out_dir.exists();args.out_dir.mkdir(parents=True)
    original_audit=c.audit_tree(args.captures)
    source_paths=[Path(__file__),Path(c.__file__),Path(peer.__file__),Path(c.__file__).with_name('profiles.json'),args.membership_oracle/'membership_raw.py',
                  args.membership_oracle.parents[2]/'KL11-15/oracle/history/wal_oracle.py']
    def source_audit():
        return {str(p):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size,
                        'mode':stat.S_IMODE(p.stat().st_mode)} for p in source_paths}
    source_before=source_audit()
    initial={}
    for name in ('peer_bytes.py','check-peer-bytes.py','decoder-controls.json'):
        p=Path(__file__).parent/name
        initial[name]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size,
                       'mode':stat.S_IMODE(p.stat().st_mode)}
    receipt={'schema_version':1,'source_sha':args.source_sha,'scope':'WORK development actual retained-history replay',
             'actual_new_broker_or_Cargo_executions':0,'actual_checker_affinity':sorted(os.sched_getaffinity(0)),'argv':sys.argv,'checker_sources_before':source_before,'initial_decoder_files_before':initial,'controls':[]}
    def run(name,owners=None,packets=None,accepted=False,documents=None):
        start=time.monotonic(); entry={'name':name,'expected_accepted':accepted}
        try:
            result=c.verify(args.captures,args.source_sha,args.membership_oracle,owners,packets,documents)
            entry['accepted']=True;entry['counters']=result['counters']
        except (c.Rejected,peer.Rejected,ValueError) as error:
            entry['accepted']=False;entry['rejection']=str(error)
        entry['seconds']=round(time.monotonic()-start,6)
        entry['passed']=entry['accepted']==accepted
        receipt['controls'].append(entry)
        (args.out_dir/(name+'.json')).write_text(json.dumps(entry,indent=2,sort_keys=True)+'\n')
        print(json.dumps(entry),flush=True)
        assert entry['passed'],name
        return result if entry['accepted'] else None
    positive = run('positive-original-actual-three-five',accepted=True)
    # Explicit clock epochs are local: shift an entire restarting process clock
    # into a disjoint numerical range. Equal local elapsed relationships remain
    # valid; no fabricated cross-PID comparison may reject that proof.
    owner_files=sorted(args.captures.glob('*-node-*/owner.jsonl'))
    first=owner_files[0];original=[json.loads(x) for x in first.read_text().splitlines()]
    restart=next(f for f in owner_files if json.loads(f.read_text().splitlines()[1])['wal_ops']>1)
    rr=[json.loads(x) for x in restart.read_text().splitlines()]
    shifted=copy.deepcopy(rr)
    for e in shifted[1:]:e['now_ms']+=10000000
    run('positive-disjoint-restart-clock-epoch',{restart.parent.name:shifted},accepted=True)
    def owner_mutation(name,fn,source=original,path=first):
        rows=copy.deepcopy(source);fn(rows);run(name,{path.parent.name:rows})
    owner_mutation('negative-owner-source-pin',lambda x:x[0].update(source_sha='00'*20))
    owner_mutation('negative-owner-clock-regression',lambda x:x[3].update(now_ms=0))
    owner_mutation('negative-owner-ordinal-reorder',lambda x:x[3].update(ordinal=1))
    owner_mutation('negative-owner-committed-summary',lambda x:x[1].update(committed_end=x[1]['committed_end']+1))
    owner_mutation('negative-owner-term-summary',lambda x:x[1].update(term=x[1]['term']+1))
    owner_mutation('negative-owner-full-directory',lambda x:x[0].update(directory='fe'*16))
    owner_mutation('negative-owner-PID-path-epoch',lambda x:x[0].update(pid=x[0]['pid']+1))
    owner_mutation('negative-owner-global-clock-claim',lambda x:x[0].update(clock_basis='global monotonic clock'))
    for name,field,value in [('tasks','task_bound',100),('socket','socket_bound_with_listener',100),
                             ('queue','command_queue_capacity',100),('dns','dns_admissions',1),
                             ('pool','transport_pool_bytes',1),('managed','managed_bytes',2**30),
                             ('frame','frame_bytes',28),
                             ('quorum','quorum_timeout_ms',1)]:
        owner_mutation('negative-actual-'+name+'-budget',lambda x,f=field,v=value:x[0]['runtime_settings'].update({f:v}))
    owner_mutation('negative-actual-record-parser-budget',lambda x:x[0]['runtime_settings'].update(record_bytes=1,image_record_bytes=1))
    # An otherwise synced grant/cancellation trace is not sufficient: ACK must
    # consume a previously prepared request exactly once in the same epoch.
    ackowner=next(f for f in owner_files if any(e.get('command')=='ack' and e['result_ok'] and
                   e['input']['kind']==21 for e in [json.loads(y) for y in f.read_text().splitlines()][1:]))
    ao=[json.loads(y) for y in ackowner.read_text().splitlines()]
    ai=next(i for i,e in enumerate(ao) if i and e['command']=='ack' and e['result_ok'] and e['input']['kind']==21)
    def forged_ack(x):
        b=bytearray.fromhex(x[ai]['input']['body_hex']);b[56:64]=(int.from_bytes(b[56:64],'big')+100000).to_bytes(8,'big')
        peer.body(21,bytes(b));x[ai]['input']['body_hex']=bytes(b).hex()
    owner_mutation('negative-ACK-unprepared-sequence',forged_ack,ao,ackowner)
    def late_ack(x):
        for e in x[ai:]:e['now_ms']+=3*x[0]['runtime_settings']['quorum_ms']
    owner_mutation('negative-ACK-after-local-correlation-deadline',late_ack,ao,ackowner)
    def dispatch_only(x):
        # The receiving Node did dispatch the response, but remove the exact
        # owner request origin while keeping its synced state and packet bytes.
        q=peer.body(21,bytes.fromhex(x[ai]['input']['body_hex']));seq=q['response']['sequence'];ctx=q['context']
        removed=0
        for e in x[1:ai]:
            kept=[]
            for out in e['typed_output']:
                m=peer.body(out['kind'],bytes.fromhex(out['body_hex']))
                if out['kind']==20 and m['sequence']==seq and m['context']==ctx:removed+=1
                else:kept.append(out)
            e['typed_output']=kept
        assert removed==1
    owner_mutation('negative-dispatched-without-owner-request',dispatch_only,ao,ackowner)
    # Choose exact actual successful Append reply and its connection request.
    response=None
    for f in sorted(args.captures.glob('partitionline76-tcp-*/wire/*response*.bin')):
        z=peer.frame(f.read_bytes())
        if z['kind']==21 and z['message']['response']['success']:
            prefix=f.stem.rsplit('-',1)[0]
            last=max(int(q.stem.rsplit('-',1)[1]) for q in f.parent.glob(prefix+'-*.bin'))
            if int(f.stem.rsplit('-',1)[1])==last:
                response=f;break
    assert response is not None
    data=response.read_bytes();d=peer.frame(data);m=d['message'];base=28
    packets=[('response-wrong-RPC',16,8,d['rpc']+100000),
             ('response-leader-directory',base+4,16,b'\xfe'*16),
             ('response-peer-directory',base+24,16,b'\xfd'*16),
             ('response-configuration-epoch',base+40,8,m['context']['configuration_epoch']+1),
             ('response-sequence',base+56,8,m['response']['sequence']+100000),
             ('response-leader-id',base+52,4,99),
             ('response-fabricated-match',base+88,8,m['response']['matched']['index']+1),
             ('response-fabricated-refusal',base+72,1,0)]
    for name,offset,size,value in packets:
        changed=patched_packet(data,offset,size,value)
        entry={'original_path':str(response),'original_sha256':hashlib.sha256(data).hexdigest(),
               'changed_sha256':hashlib.sha256(changed).hexdigest(),'offset':offset,'bytes':size,
               'private_CRC_recomputed':True,'valid_independent_frame_decode':True}
        (args.out_dir/(name+'.mutation.json')).write_text(json.dumps(entry,indent=2,sort_keys=True)+'\n')
        (args.out_dir/(name+'.bin')).write_bytes(changed)
        run('negative-checksum-valid-'+name,packets={str(response):changed})
    req=next(f for f in sorted(args.captures.glob('partitionline76-tcp-*/wire/*request*.bin'))
             if peer.frame(f.read_bytes())['kind']==20 and any(r['kind']==0 and r['payload_hex'] for r in peer.frame(f.read_bytes())['message']['entries']))
    rd=req.read_bytes();r=peer.frame(rd)
    # context48 + IDs8 + seq8 + term8 + previous16 + commit8 + count/reserved8 =104.
    # First record header24 then payload; flip an opaque/barrier-independent byte.
    payload_offset=28+104
    for record in r['message']['entries']:
        payload_offset+=24
        if record['kind']==0 and record['payload_hex']:break
        payload_offset+=len(bytes.fromhex(record['payload_hex']))
    if True:
        changed=patched_packet(rd,payload_offset,1,rd[payload_offset]^1)
        (args.out_dir/'request-opaque-payload.bin').write_bytes(changed)
        run('negative-checksum-valid-source-record-bytes',packets={str(req):changed})
    hello=next(args.captures.glob('partitionline76-tcp-*/wire/hello-*.bin'))
    hd=hello.read_bytes();changed=patched_packet(hd,base+20,4,99)
    (args.out_dir/'hello-unknown-target.bin').write_bytes(changed)
    run('negative-checksum-valid-Hello-unknown-target',packets={str(hello):changed})
    # Remove successful current-term grant consumption while leaving all packet
    # and synced election bytes intact. A reported Leader must not substitute
    # for distinct consumed voter grants.
    elect_owner=next(f for f in owner_files if any(e['role']=='Leader' for e in [json.loads(y) for y in f.read_text().splitlines()][1:]))
    er=[json.loads(y) for y in elect_owner.read_text().splitlines()]
    def erase_grants(x):
        end=next(i for i,e in enumerate(x) if i and e['role']=='Leader')
        for e in x[1:end+1]:
            if e['command']=='ack' and e['result_ok'] and e['input']['kind']==11:
                e['result_ok']=False
    owner_mutation('negative-reported-Leader-without-consumed-majority',erase_grants,er,elect_owner)
    # Forge an elapsed local deadline while preserving local monotonic order.
    writer=next(f for f in owner_files if any(e['command']=='propose' and e['result_ok'] for e in [json.loads(y) for y in f.read_text().splitlines()][1:]))
    wr=[json.loads(y) for y in writer.read_text().splitlines()]
    def expired_write(x):
        begin=next(i for i,e in enumerate(x) if i and e['command']=='propose' and e['result_ok'])
        for e in x[begin:]:e['now_ms']+=3*x[0]['runtime_settings']['quorum_ms']
    owner_mutation('negative-expired-local-quorum-admission',expired_write,wr,writer)
    for index,name in enumerate(c.RESOURCE_NAMES):
        def exceed(rows,i=index):
            rows[1]['resource_owners']['gauges'][i]['peak']=2**63
        owner_mutation('negative-retained-owner-bound-'+name,exceed)
    owner_mutation('negative-command-owner-envelope',lambda rows:rows[0]['runtime_settings'].update(command_owner_bound=1))
    timeout_owner=next(f for f in owner_files if any(e.get('command')=='timeout' and
                       e.get('cleanup',{}).get('append_or_image_released') is True
                       for e in [json.loads(y) for y in f.read_text().splitlines()][1:] if e.get('cleanup')))
    tr=[json.loads(y) for y in timeout_owner.read_text().splitlines()]
    ti=next(i for i,e in enumerate(tr) if i and e['command']=='timeout' and e['cleanup']['append_or_image_released'])
    owner_mutation('negative-timeout-wrong-sequence',lambda rows:rows[ti]['target'].update(sequence=rows[ti]['target']['sequence']+100000),tr,timeout_owner)
    owner_mutation('negative-timeout-wrong-directory',lambda rows:rows[ti]['target']['peer'].update(directory='fe'*16),tr,timeout_owner)
    owner_mutation('negative-timeout-wrong-image-fallback',lambda rows:rows[ti]['cleanup'].update(image_fallback_set=not rows[ti]['cleanup']['image_fallback_set']),tr,timeout_owner)
    joined=next(args.captures.glob('partitionline76-tcp-*/joined-node-*-pid-*.json'))
    crashed=next(args.captures.glob('partitionline76-tcp-*/crashed-node-*-pid-*.json'))
    lifecycle=next(args.captures.glob('*-node-*/lifecycle-supervisor-joined.json'))
    def document_mutation(name,path,mutate):
        value=json.loads(path.read_bytes());mutate(value)
        run(name,documents={str(path.relative_to(args.captures)):value})
    document_mutation('negative-supervisor-shutdown-did-not-return',joined,lambda row:row.update(supervisor_shutdown_returned=False))
    document_mutation('negative-shutdown-retains-task',joined,lambda row:row.update(final_network_tasks=1))
    document_mutation('negative-crash-parent-did-not-wait',crashed,lambda row:row.update(parent_waited=False))
    document_mutation('negative-crash-treated-as-runtime-join',crashed,lambda row:row.update(runtime_joined=True))
    document_mutation('negative-crash-unexpected-exit-code',crashed,lambda row:row.update(exit_code=0))
    document_mutation('negative-supervisor-worker-unjoined',lifecycle,lambda row:row.update(network_workers_joined=row['network_workers_spawned']+1))
    document_mutation('negative-supervisor-transport-permit-leak',lifecycle,lambda row:row.update(available_transport_permits=row['available_transport_permits']-1))
    document_mutation('negative-supervisor-resource-current-leak',lifecycle,lambda row:row['resource_owners']['gauges'][2].update(current=1))
    run('negative-supervisor-lifecycle-receipt-absent',documents={str(lifecycle.relative_to(args.captures)):None})
    binding=next(row for row in positive['response_consumption_bindings'] if row['kind']==21)
    forwarded=args.captures/(binding['response']+'.forward.json')
    document_mutation('negative-consumed-ACK-proxy-write-error',forwarded,lambda row:row.update(disposition='forward-write-error',forward_write_ok=False))
    document_mutation('negative-consumed-ACK-proxy-partition-drop',forwarded,lambda row:row.update(disposition='partition-drop',forward_write_ok=None,forward_started_ms=None,forward_finished_ms=None))
    document_mutation('negative-proxy-receipt-wrong-RPC',forwarded,lambda row:row.update(rpc=row['rpc']+100000))
    document_mutation('negative-proxy-receipt-wrong-PID',forwarded,lambda row:row.update(proxy_pid=row['proxy_pid']+1))
    document_mutation('negative-proxy-write-completes-before-start',forwarded,lambda row:row.update(forward_finished_ms=row['forward_started_ms']-1))
    run('negative-proxy-forward-receipt-missing',documents={str(forwarded.relative_to(args.captures)):None})
    delayed=next(path for path in args.captures.glob('partitionline76-tcp-*/wire/*.forward.json') if json.loads(path.read_bytes())['selected_delay_ms']==800)
    document_mutation('negative-declared-delayed-packet-omitted',delayed,lambda row:row.update(selected_delay_ms=0))
    document_mutation('negative-delayed-packet-forward-before-held-deadline',delayed,lambda row:row.update(forward_started_ms=row['received_ms']))
    document_mutation('negative-undeclared-proxy-delay',delayed,lambda row:row.update(selected_delay_ms=801))
    assert original_audit==c.audit_tree(args.captures)
    after={}
    for name in initial:
        p=Path(__file__).parent/name;after[name]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
                      'bytes':p.stat().st_size,'mode':stat.S_IMODE(p.stat().st_mode)}
    assert initial==after
    source_after=source_audit();assert source_before==source_after
    receipt.update(initial_decoder_files_after=after,initial_decoder_2_positive_26_negative_files_unchanged=True,
                   original_actual_capture_bytes_and_full_modes_unchanged=True,
                   passed=True,checker_sources_after=source_after,all_checker_source_bytes_and_full_modes_unchanged=True,positive_controls=sum(e['expected_accepted'] for e in receipt['controls']),
                   negative_controls=sum(not e['expected_accepted'] for e in receipt['controls']),
                   checker_sha256=hashlib.sha256(Path(c.__file__).read_bytes()).hexdigest(),
                   control_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (args.out_dir/'validation.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')

if __name__=='__main__':main()
