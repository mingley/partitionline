#!/usr/bin/env python3
"""Run the final lookup checks with owned command trees and source guards."""
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

b=Path('/workspace/work/open-cards-20261006/share-ranges-20261007')
r=Path('/workspace/work/open-cards-20261006/share-ranges-source-candidate')
out=b/'checks-final-01';out.mkdir()
owned_path=Path('/workspace/work/open-cards-20261006/local-baseline-source-c4b915/scripts/run-benchmark-matrix.py')
parent=Path('/workspace/work/open-cards-20261006/local-baseline-20261007/parent-bound-exec.py')
spec=importlib.util.spec_from_file_location('range_check_owner',owned_path)
owned=importlib.util.module_from_spec(spec);spec.loader.exec_module(owned)
if ctypes.CDLL(None).prctl(36,1,0,0,0):raise OSError('cannot become subreaper')
def interrupt(sig,frame):raise InterruptedError(f'owner signal{sig}')
for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,interrupt)
pinset=json.loads((b/'candidate-source-pins-final.json').read_text())
pins={str(r/name):digest for group in ('source_files','zstd_fixture_files') for name,digest in pinset[group].items()}
for path in (Path(__file__).resolve(),parent,owned_path):pins[str(path)]=owned.sha(path)
def guard():
    for path,digest in pins.items():
        if owned.sha(path)!=digest:raise ValueError('check input changed: '+path)
    if subprocess.check_output(['git','-C',str(r),'rev-parse','HEAD'],text=True).strip()!=pinset['source_commit']:raise ValueError('source commit changed')
    if subprocess.check_output(['git','-C',str(r),'status','--porcelain']):raise ValueError('source dirty')
commands=[
 ('share',['cargo','+stable','test','--locked','--lib','share'], '/workspace/work/target'),
 ('full-surface',['cargo','+stable','test','--locked','--test','full_surface'], '/workspace/work/target'),
 ('zstd-fixtures',['cargo','+stable','test','--locked','--features','zstd','--test','zstd_decode','--test','zstd_encode','--test','fuzz_decode_smoke'], '/workspace/work/target'),
 ('core-fmt',['cargo','+stable','fmt','--all','--','--check'], '/workspace/work/target'),
 ('bench-fmt',['cargo','+stable','fmt','--manifest-path','benchmarks/codec/Cargo.toml','--','--check'], '/workspace/work/target-codec'),
 ('bench-clippy',['cargo','+stable','clippy','--locked','--manifest-path','benchmarks/codec/Cargo.toml','--all-targets','--all-features','--','-D','warnings'], '/workspace/work/target-codec'),
 ('bench-shared-tests',['cargo','+stable','test','--locked','--manifest-path','benchmarks/codec/Cargo.toml','--bin','share-ranges'], '/workspace/work/target-codec'),
]
guard();owned.atomic(out/'plan.json',dict(source_commit=pinset['source_commit'],source_and_tool_pins=pins,commands=commands))
env=owned.base_env();env.update(CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
os.chdir(r)
for label,argv,target in commands:
    guard();print('start '+label,flush=True)
    owned.execute([sys.executable,'-B',str(parent),str(os.getpid()),*argv],dict(env,CARGO_TARGET_DIR=target),out,label,600)
    guard();print('passed '+label,flush=True)
owned.atomic(out/'completion.json',dict(passed=True,commands=len(commands),source_guards_passed=True,owned_process_groups_empty=True))
