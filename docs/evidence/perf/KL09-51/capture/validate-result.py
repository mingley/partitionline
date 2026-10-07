#!/usr/bin/env python3
"""Validate the retained acquisition-lookup measurement and checksum."""
import json
import math
from pathlib import Path
import sys


def unique(pairs):
    result={}
    for k,v in pairs:
        if k in result:raise ValueError('duplicate result key')
        result[k]=v
    return result


def validate(path):
    path=Path(path)
    if path.stat().st_size>4096:raise ValueError('oversize measurement')
    row=json.loads(path.read_text(),object_pairs_hook=unique,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite JSON')))
    fixed=dict(schema_version=1,cell='micro-share-ranges',range_count=1000,lookups_per_iteration=5000,
        acquired_offsets_per_iteration=3000,gap_offsets_per_iteration=2000,excluded_warmup_iterations=100)
    for name,value in fixed.items():
        if row.get(name)!=value or type(row.get(name))!=type(value):raise ValueError('fixture differs: '+name)
    n=row['iterations'];elapsed=row['elapsed_ns'];metric=row['ns_per_record']
    if type(n)!=int or not 1<=n<=1_000_000 or type(elapsed)!=int or not 0<elapsed<600*10**9:raise ValueError('invalid count/clock')
    expected=sum(3*(i%7+1) for i in range(1000))*n
    if type(row['checksum'])!=int or row['checksum']!=expected or row['expected_checksum']!=expected:raise ValueError('actual lookup checksum differs')
    if type(metric) not in (int,float) or not math.isfinite(metric) or not math.isclose(metric,elapsed/(n*5000),rel_tol=1e-12,abs_tol=0):raise ValueError('time per lookup differs')
    for name in ('lookup_allocation_count','lookup_allocated_bytes'):
        if type(row[name])!=int or row[name]<0:raise ValueError('invalid allocation census')
    return row


if __name__=='__main__':
    print(json.dumps(dict(status='valid',ns_per_record=validate(sys.argv[1])['ns_per_record'])))
