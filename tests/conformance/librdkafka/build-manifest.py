#!/usr/bin/env python3
"""Hash the candidate's actual source bytes before/after compilation."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[2]
BUILD=Path(os.environ['CASE_BUILD_DIR']);NATIVE=Path(os.environ.get('C_PEER_BUILD_DIR',ROOT/'work/librdkafka-peer'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def command(args):return subprocess.check_output(args,text=True).strip()
def snapshot():
 files=list((ROOT/'src').rglob('*'))+[ROOT/'Cargo.toml',ROOT/'Cargo.lock',HERE/'rust/Cargo.toml',HERE/'rust/Cargo.lock',HERE/'rust/src/main.rs']
 facts={str(p.relative_to(ROOT)):sha(p) for p in files if p.is_file()}
 return dict(core_and_adapter_files=facts,source_digest=hashlib.sha256(json.dumps(facts,sort_keys=True).encode()).hexdigest(),
             git_commit=command(['git','-C',str(ROOT),'rev-parse','HEAD']),git_clean=not command(['git','-C',str(ROOT),'status','--porcelain']))
if sys.argv[1]=='snapshot':
 (BUILD/'source-before.json').write_text(json.dumps(snapshot(),indent=2)+'\n')
else:
 before=json.load(open(BUILD/'source-before.json'));after=snapshot()
 assert before['core_and_adapter_files']==after['core_and_adapter_files'],'build input changed during compilation; rerun with stable source'
 manifest=dict(source_snapshot=before,source_unchanged_during_build=True,git_commit_after_build=after['git_commit'],
               rust_binary_sha256=sha(BUILD/'rust-target/debug/partitionline-librdkafka-0125'),native_binary_sha256=sha(NATIVE/'0125-peer'),
               native_library_build=json.load(open(NATIVE/'build-manifest.json')),
               shim_sources={p.name:sha(p) for p in (HERE/'shim').glob('*')},upstream_pin=json.load(open(HERE/'pin.json')),
               rust_compiler=command(['rustc','--version']),cargo=command(['cargo','--version']),
               c_compiler=command([os.environ.get('CC','cc'),'--version']).splitlines()[0])
 (BUILD/'behavior-build-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
