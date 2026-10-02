#!/usr/bin/env python3
"""Replay fail-before-connect checks for the benchmark examples."""
import json, os, subprocess, sys
base = os.environ.copy()
for key in ['COUNT','WARMUP_SECS','MEASURE_SECS','ACKS','IDEMPOTENT','PAYLOAD_BYTES','CONNECTIONS','MAX_IN_FLIGHT','VERIFY','VERIFY_HEADERS','SEED','ISOLATION','MAX_WAIT_MS','MAX_BYTES','MIN_BYTES','RECORD_HISTORY']:
    base.pop(key, None)
base.update(KAFKA_BOOTSTRAP='127.0.0.1:1',WARMUP_SECS='0')
cases = [
 ('produce', {'COUNT':'0'}, 'COUNT must be positive'),
 ('produce', {'COUNT':'bad'}, 'invalid COUNT'),
 ('produce', {'COUNT':'-1'}, 'invalid COUNT'),
 ('produce', {'MEASURE_SECS':'0'}, 'MEASURE_SECS must be positive'),
 ('produce', {'PAYLOAD_BYTES':'0'}, 'PAYLOAD_BYTES must be positive'),
 ('produce', {'CONNECTIONS':'0'}, 'CONNECTIONS must be positive'),
 ('produce', {'MAX_IN_FLIGHT':'0'}, 'MAX_IN_FLIGHT must be positive'),
 ('produce', {'ACKS':'2'}, 'ACKS must be'),
 ('produce', {'ACKS':'bad'}, 'invalid ACKS'),
 ('produce', {'IDEMPOTENT':'true'}, 'IDEMPOTENT must be'),
 ('produce', {'IDEMPOTENT':'1','ACKS':'1'}, 'IDEMPOTENT=1 requires'),
 ('produce', {'WARMUP_SECS':'bad'}, 'invalid WARMUP_SECS'),
 ('produce', {'RECORD_HISTORY':'unused.jsonl','COUNT':'1','PAYLOAD_BYTES':'23'}, 'RECORD_HISTORY requires'),
 ('produce', {'RECORD_HISTORY':'unused.jsonl'}, 'RECORD_HISTORY requires'),
 ('fetch', {'COUNT':'0'}, 'COUNT must be positive'),
 ('fetch', {'COUNT':'bad'}, 'invalid COUNT'),
 ('fetch', {'COUNT':'-1'}, 'invalid COUNT'),
 ('fetch', {'VERIFY':'true'}, 'VERIFY must be'),
 ('fetch', {'SEED':'bad'}, 'invalid SEED'),
 ('fetch', {'ISOLATION':'typo'}, 'ISOLATION must be'),
 ('fetch', {'MAX_WAIT_MS':'-1'}, 'invalid MAX_WAIT_MS/MAX_BYTES/MIN_BYTES'),
 ('fetch', {'MAX_BYTES':'0'}, 'invalid MAX_WAIT_MS/MAX_BYTES/MIN_BYTES'),
 ('fetch', {'MIN_BYTES':'0'}, 'invalid MAX_WAIT_MS/MAX_BYTES/MIN_BYTES'),
 ('fetch', {'RECORD_HISTORY':'unused.jsonl','VERIFY':'1'}, 'RECORD_HISTORY requires'),
]
binaries={'produce':sys.argv[1],'fetch':sys.argv[2]}
results=[]
for kind, settings, expected in cases:
    process=subprocess.run([binaries[kind]],env=base|settings,text=True,capture_output=True,timeout=5)
    assert process.returncode != 0 and expected in process.stderr, (kind,settings,process.stderr)
    assert 'Connection refused' not in process.stderr, (kind,settings,process.stderr)
    results.append({'kind':kind,'settings':settings,'rejected_before_connection':True})
print(json.dumps({'status':'passed','case_count':len(results),'cases':results},indent=2))
