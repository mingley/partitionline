"""Independent deterministic expectations and generic KL03-18 record-history gate."""
from __future__ import annotations
import hashlib
from collections import Counter
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
MASK = 2**64-1

def mix(x):
    x = (x+0x9e3779b97f4a7c15)&MASK
    x = ((x^(x>>30))*0xbf58476d1ce4e5b9)&MASK
    x = ((x^(x>>27))*0x94d049bb133111eb)&MASK
    return x^(x>>31)

def record(seed, index, size):
    key = index.to_bytes(8,'big')+mix(seed^index).to_bytes(8,'big')
    state = seed ^ ((index*0x9e3779b97f4a7c15)&MASK)
    payload = bytearray()
    while len(payload)<size:
        state = mix(state); payload.extend(state.to_bytes(8,'big'))
    return key, bytes(payload[:size])

def read_rows(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line]

def verify(c, raw, attempts, receipts):
    if c['key_mode'] != 'id' or c['payload_mode'] != 'seeded':
        raise ValueError('unique ID history verification requires seeded payload and ID keys')
    problems=[]; consumed=[]; seen=set(); offsets=set()
    fence={p['partition']:p for p in raw['high_watermarks']['partitions']}
    attempt_ids=set()
    for attempt in attempts:
        index=int(attempt['id']); attempt_ids.add(index)
        if not 0<=index<c['count']:
            problems.append('attempt ID outside requested phase'); continue
        key,value=record(c['record_seed'],index,c['payload_bytes'])
        if (attempt['topic']!=c['topic'] or attempt['partition']!=index%c['partitions']
                or attempt['key']!=key.hex() or attempt['payload_hash']!=hashlib.sha256(value).hexdigest()):
            problems.append('producer history differs from independently derived expectation')
    for receipt in receipts:
        key=bytes.fromhex(receipt['key']) if receipt['key'] is not None else b''
        value=bytes.fromhex(receipt['value']) if receipt['value'] is not None else b''
        if len(key)!=16:
            problems.append('receipt lacks 16-byte ID key'); continue
        index=int.from_bytes(key[:8],'big')
        expected_key,expected_value=record(c['record_seed'],index,c['payload_bytes'])
        part=receipt['partition']; offset=receipt['offset']; p=fence.get(part)
        if (not 0<=index<c['count'] or index not in attempt_ids or part!=index%c['partitions']
                or key!=expected_key or value!=expected_value):
            problems.append('independent receipt ID/partition/key/value mismatch')
        if p is None or not p['start_offset']<=offset<p['end_offset']:
            problems.append('receipt outside high-watermark fence')
        if (part,offset) in offsets:
            problems.append('duplicate partition offset')
        offsets.add((part,offset)); seen.add(index)
        consumed.append(dict(id=str(index),topic=c['topic'],partition=part,offset=offset,
                             key=key.hex(),payload_hash=hashlib.sha256(value).hexdigest()))
    # In-fence unique offsets plus the exact per-partition cardinality prove
    # complete coverage without allocating another set proportional to backlog.
    offset_counts=Counter(part for part,_ in offsets)
    if any(offset_counts[p['partition']]!=p['end_offset']-p['start_offset']
           for p in fence.values()):
        problems.append('independent receipts do not cover every fenced offset exactly once')
    # Callback order across partitions differs from send order; attempt_index is the
    # actual original offer sequence. The checker compares each partition in that order.
    attempts=sorted(attempts,key=lambda row:row['attempt_index'])
    history=dict(history_id='rust-rdkafka-independent-java-receipts',
                 config=dict(acks=c['acks'],idempotent=c['idempotence'],transactional=False,
                             isolation_level=c['isolation_level'],delivery='partition'),
                 attempted=attempts,consumed=consumed)
    spec=importlib.util.spec_from_file_location('rust_peer_history_gate', ROOT/'scripts/check-record-history.py')
    module=importlib.util.module_from_spec(spec); sys.modules[spec.name]=module; spec.loader.exec_module(module)
    try:
        verdict=module.verify_history(history,'independent-java-receipts').to_dict()
    except module.HistoryValidationError as error:
        verdict=dict(valid=False,validation_error=str(error),
                     minimal_counterexample=dict(type='HISTORY_VALIDATION_ERROR',message=str(error)))
    duplicates=len(consumed)-len(seen)
    complete=(len(seen)==c['count'] and raw['timed']['acknowledged']==c['count'])
    return history, dict(valid=verdict['valid'] and not problems and complete,
                         history_gate=verdict,independent_byte_errors=problems,
                         complete_expected_ids=complete,verified_ids=len(seen),
                         duplicate_ids=duplicates,bad_records=len(problems))
