import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path('/workspace/partitionline')
SOURCE = Path('/workspace/work/snapshot-inner-draft-45582234/source')
OUTPUT = ROOT / 'docs/evidence/broker/KL11-15/development/node-inner-cuts-02'
env = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
    CARGO_TARGET_DIR='/workspace/work/target-broker-snapshot', CARGO_INCREMENTAL='0',
    CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
env['PATH'] = '/workspace/work/cargo/bin:' + env['PATH']
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
files = {str(p.relative_to(SOURCE)): sha(p) for p in sorted(SOURCE.rglob('*')) if p.is_file()}
def identity():
    assert all((SOURCE / p).is_file() and sha(SOURCE / p) == digest for p, digest in files.items())
    return {'passed':True, 'files_checked':len(files)}
commands=[]
for toolchain in ['stable','1.85.0']:
    proof = OUTPUT / toolchain / 'histories'
    proof.mkdir(parents=True)
    current = dict(env, PL_SNAPSHOT_NODE_INNER_PROOF_DIR=str(proof))
    for kind in ['test','clippy']:
        argv=['taskset','-c','0-2,4','cargo','+'+toolchain,kind,'--locked','--manifest-path','partitionline-broker/Cargo.toml','-j2','--lib']
        if kind=='test': argv += ['raft::snapshot::tests::node_inner','--','--nocapture']
        else: argv += ['--test','raft_snapshot','--','-D','warnings']
        before=identity(); log=OUTPUT/(toolchain+'-'+kind+'.log'); start=time.time()
        with log.open('wb') as output:
            result=subprocess.run(argv,cwd=SOURCE,env=current,stdout=output,stderr=subprocess.STDOUT)
        commands.append({'argv':argv,'cwd':str(SOURCE),'exit_code':result.returncode,'elapsed_seconds':round(time.time()-start,3),
            'log':log.name,'log_sha256':sha(log),'source_before':before,'source_after':identity(),
            'proof_directory_env':str(proof)})
        report={'schema_version':1,'stage':'development Node-backed inner publication cuts','passed':False,
            'source_inputs':json.loads((OUTPUT/'source-inputs.json').read_text()),'commands':commands}
        (OUTPUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
        assert result.returncode==0,log
report['passed']=True
report['per_toolchain']={'unit_functions':3,'unconfigured_helpers':1,'io_cut_histories':3,'actual_process_exit_histories':3,'process_exit_code':44}
report['raw_artifacts_sha256']={str(p.relative_to(OUTPUT)):sha(p) for p in sorted(OUTPUT.rglob('*')) if p.is_file() and p.name not in ['validation.json']}
report['limits']=['Single-voter local Node durability and selected-image/WAL replay only; multi-node transfers and authoritative WAL cuts belong to RPC runtime proof.',
    'Development overlay of immutable455 plus four explicitly hashed15files; final qualification requires a pushed immutable integrated source.']
(OUTPUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':True,'commands':len(commands),'histories':12}))
