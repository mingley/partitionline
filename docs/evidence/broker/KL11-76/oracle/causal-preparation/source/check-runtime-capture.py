#!/usr/bin/env python3
"""Bind a finite causal replay to actual Cargo capture/source/harness receipts.

This program performs no builds and imports no Rust. A passed diagnostic remains
that diagnostic's exact toolchain/profile/source scope. No prior full403 result
is transferred to the newly implemented private runtime.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import stat
import sys

sys.dont_write_bytecode=True
import causal_runtime as c

MAX_PROOF_BYTES=16*1024*1024
MAX_SOURCE_MAP_BYTES=32*1024*1024

def regular(p,cap):
    c.need(p.is_file() and not p.is_symlink() and p.stat().st_size<=cap,'regular bounded provenance input')
    return p.read_bytes()

def digest_gzip(p,maximum):
    c.need(p.is_file() and not p.is_symlink(),'regular retained compressed input')
    digest=hashlib.sha256();size=0;prefix=b''
    with gzip.open(p,'rb') as f:
        while True:
            block=f.read(1024*1024)
            if not block:break
            size+=len(block);c.need(size<=maximum,'retained decompression ceiling')
            if not prefix:prefix=block[:4]
            digest.update(block)
    return digest.hexdigest(),size,prefix

def verify_producer(path,capture_root,command_name,source_sha):
    encoded=regular(path,MAX_PROOF_BYTES);proof=json.loads(encoded)
    c.need(proof['source_commit']==source_sha,'producer exact source pin')
    command=next((x for x in proof['commands'] if x['name']==command_name),None)
    c.need(command is not None and command['exit_code']==0,'actual completed producer command missing')
    env=command['environment_additions']
    c.need(Path(env['PL_PEER_RUNTIME_CAPTURE_DIR']).resolve()==capture_root.resolve() and
           env['PL_PEER_RUNTIME_SOURCE_SHA']==source_sha,'producer capture environment binding')
    before=command['source_before']
    c.need(before==command['source_after']==command['source_after_retention']==proof['final_source'] and
           before['all_git_blobs_match'] and before['all_baseline_permission_modes_match'],
           'producer complete original command source guards')
    argv=command['argv']
    c.need(argv[:3]==['taskset','-c','2,4'] and 'test' in argv and '--offline' in argv and
           '--locked' in argv and '--manifest-path' in argv and 'partitionline-broker/Cargo.toml' in argv,
           'actual broker compiler/locked/offline/CPU invocation')
    source_origin=proof['source_origin'];origin_path=Path(source_origin['path'])
    c.need(hashlib.sha256(regular(origin_path,MAX_PROOF_BYTES)).hexdigest()==source_origin['sha256'],
           'original materialization lineage receipt hash')
    origin=json.loads(origin_path.read_bytes())
    c.need(origin['source_commit']==source_sha and Path(origin['source_directory']).resolve()==Path(proof['source_tree']).resolve() and
           origin['verified_files']==before['file_count'] and origin['complete_path_set_and_git_blobs_verified'] and
           origin['all_full_modes_verified'] and origin['no_inode_shared_with_repository_worktree'],
           'actual materialization source pin/tree/fullmode/disjoint lineage')
    m=source_origin['manifest'];map_path=Path(m['path'])
    compressed=regular(map_path,MAX_SOURCE_MAP_BYTES)
    c.need(hashlib.sha256(compressed).hexdigest()==m['compressed_sha256'],'compressed source map hash')
    digest,size,_=digest_gzip(map_path,MAX_SOURCE_MAP_BYTES)
    c.need(digest==m['uncompressed_sha256'] and size==m['uncompressed_bytes'],'complete source map raw bytes')
    with gzip.open(map_path,'rb') as f:source_map=json.loads(f.read(MAX_SOURCE_MAP_BYTES+1))
    tree=Path(proof['source_tree']).resolve()
    c.need(len(source_map)==before['file_count'],'complete source map count')
    def source_guard():
        paths=set()
        for p in tree.rglob('*'):
            c.need(not p.is_symlink(),'immutable source symlink')
            if p.is_file():paths.add(str(p.relative_to(tree)))
        c.need(paths==set(source_map),'immutable complete source path set')
        for name,row in source_map.items():
            p=tree/name;data=regular(p,64*1024*1024)
            c.need(len(data)==row['bytes'] and hashlib.sha256(data).hexdigest()==row['sha256'] and
                   hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==row['git_blob_sha1'] and
                   stat.S_IMODE(p.stat().st_mode)==row['full_permission_mode'],
                   'immutable source bytes/Git/full07777 permission mismatch')
        return {'files':len(paths),'all_actual_git_blob_sha1_sha256_bytes_and_full_modes_match':True,
                'complete_manifest_uncompressed_sha256':digest}
    independent_before=source_guard()
    log_path=path.parent/command_name/'command.log';log=regular(log_path,MAX_PROOF_BYTES)
    c.need(hashlib.sha256(log).hexdigest()==command['log_sha256'],'actual executed compiler/test log hash')
    actual_capture=c.audit_tree(capture_root)
    declared=proof['capture_files'][command_name]
    expected={name:{'sha256':r['sha256'],'bytes':r['bytes'],'mode':r['full_permission_mode']} for name,r in declared.items()}
    c.need(actual_capture==expected,'producer original complete capture identity binding')
    objects={r['path']:r for r in proof['retained_elf_objects']}
    harnesses=[]
    for e in command['executed_elfs']:
        c.need(e['command']==command_name and e['retained_object'] in objects,'Cargo executed harness original retention identity')
        obj=objects[e['retained_object']];p=path.parent/e['retained_object']
        c.need(hashlib.sha256(regular(p,64*1024*1024)).hexdigest()==obj['sha256'],'retained actual harness compressed hash')
        sha,n,magic=digest_gzip(p,64*1024*1024)
        c.need(sha==e['sha256']==obj['uncompressed_sha256'] and n==e['bytes']==obj['uncompressed_bytes'] and
               magic==b'\x7fELF' and 0<=e['original_mode']<=0o7777 and e['original_mode']&0o111,
               'retained actual executed ELF byte/full-mode restore binding')
        harnesses.append({'original_path':e['original_path'],'retained_object':str(p),
                          'sha256':sha,'bytes':n,'original_full_mode':e['original_mode']})
    c.need(harnesses,'no actual executed harness receipt')
    return {'path':str(path),'sha256':hashlib.sha256(encoded).hexdigest(),'bytes':len(encoded),
            'mode':stat.S_IMODE(path.stat().st_mode),'command_name':command_name,'argv':argv,'exit_code':0,
            'toolchain':proof['toolchain'],'scope':proof['scope'],'original_full_source_guards':before,
            'independent_source_before':independent_before,'source_origin':source_origin,
            'compiler_log':{'path':str(log_path),'sha256':command['log_sha256'],'bytes':len(log)},
            'actual_executed_harnesses':harnesses,'captured_files':len(actual_capture)},source_guard

def main():
    a=argparse.ArgumentParser();a.add_argument('--producer-proof',type=Path,required=True)
    a.add_argument('--producer-command',required=True);a.add_argument('--captures',type=Path,required=True)
    a.add_argument('--source-sha',required=True);a.add_argument('--membership-oracle',type=Path,required=True)
    a.add_argument('--out',type=Path,required=True);args=a.parse_args()
    c.need(re.fullmatch('[0-9a-f]{40}',args.source_sha) and not args.out.exists(),'exact source/fresh output')
    provenance,postguard=verify_producer(args.producer_proof,args.captures,args.producer_command,args.source_sha)
    result=c.verify(args.captures,args.source_sha,args.membership_oracle)
    provenance['independent_source_after']=postguard()
    c.need(provenance['independent_source_before']==provenance['independent_source_after'],'producer complete immutable source changed')
    result['actual_producer_provenance']=provenance
    result['actual_new_broker_or_Cargo_executions']=0
    args.out.parent.mkdir(parents=True,exist_ok=True);args.out.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'passed':True,'source':args.source_sha,'scope':provenance['scope'],'counters':result['counters'],'out':str(args.out)}))

if __name__=='__main__':main()
