#!/usr/bin/env python3
"""Synthetic stale-proof predicate control, not an actual/native history."""
import importlib.util
from pathlib import Path
import sys

directory = Path('/workspace/partitionline/docs/evidence/broker/KL11-74/oracle/history')
sys.path.insert(0, str(directory))
spec = importlib.util.spec_from_file_location('admission_control', directory/'check-membership-history.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
owner = (1, '02'*16)
check = module.Admission({'quorum_timeout_ms':1000,'election_min_ms':10,'election_max_ms':10}, [owner])
check.elected.add((owner,5))
check.activate(owner,5,10)
check.after({owner:{'term':5,'open':True,'ready':True,'poisoned':False,'role':'Follower','active_term':None}})
check.after({owner:{'term':5,'open':True,'ready':True,'poisoned':False,'role':'Leader','active_term':None}})
try:
    check.activate(owner,5,20)
except module.raw.Rejected as error:
    assert str(error)=='activation lacks actual majority election'
    print('Synthetic stale-election proof correctly rejected.')
else:
    raise AssertionError('Synthetic unproved same-term reactivation reused a revoked election proof.')
