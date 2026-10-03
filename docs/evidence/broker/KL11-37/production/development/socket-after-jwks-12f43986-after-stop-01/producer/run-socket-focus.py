import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

repo=Path('/workspace/partitionline'); base=sys.argv[1]; snapshot=Path('/workspace/work/broker-oidc/socket-overlay-after-jwks-'+base[:8]); suffix=('-'+sys.argv[2])if len(sys.argv)>2 else ''; out=repo/('docs/evidence/broker/KL11-37/production/development/socket-after-jwks-'+base[:8]+suffix); expected=json.loads(gzip.decompress((out/'source-before.json.gz').read_bytes()))['files']; results=[]; checks=[]
env=os.environ.copy(); env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+env['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR='/workspace/work/broker-oidc/target')
def verify(label,phase):
    digest=hashlib.sha256()
    for row in expected:
        p=snapshot/row['path']; actual=hashlib.sha256(p.read_bytes()).hexdigest(); mode=p.stat().st_mode&0o777
        if actual!=row['sha256'] or mode!=row['mode']: raise RuntimeError('isolated source changed '+row['path'])
        digest.update(row['path'].encode()+b'\0'+actual.encode()+b'\0'+str(mode).encode()+b'\n')
    checks.append(dict(command=label,phase=phase,files=len(expected),bytes_and_full_modes_identical=True,aggregate_sha256=digest.hexdigest()));(out/'source-per-command.json').write_text(json.dumps(dict(base_source_sha=base,checks=checks),indent=2)+'\n')
for toolchain in ['stable','1.85.0']:
    cargo=['taskset','-c','0,1','cargo','+'+toolchain]; manifest=['--manifest-path',str(snapshot/'partitionline-broker/Cargo.toml')]
    commands=[(toolchain+'-rustc',['taskset','-c','0,1','rustc','+'+toolchain,'-vV']),(toolchain+'-fmt',cargo+['fmt']+manifest+['--','--check'])]
    for profile in ['default','tls','sasl','sasl,tls','oidc']:
        flags=[] if profile=='default' else ['--no-default-features','--features',profile]; common=manifest+['--locked','--offline']+flags; label=profile.replace(',','-')
        commands.extend([(toolchain+'-'+label+'-tests',cargo+['test']+common+['--lib','--test','oidc_http','--test','oidc_validation','--test','oidc_sessions','--test','sasl_sessions']),(toolchain+'-'+label+'-clippy',cargo+['clippy']+common+['--all-targets','--','-Dwarnings'])])
    for label,argv in commands:
        if shutil.disk_usage('/workspace').free<350*1024*1024:raise RuntimeError('HOLD before new command: less than350MiB free')
        verify(label,'before');print('start '+label,flush=True);log=out/(label+'.log');started=time.monotonic()
        resource_holds=[];minimum_free=shutil.disk_usage('/workspace').free;monitor=out/(label+'-disk-monitor.json')
        with log.open('wb')as stream:
            child=subprocess.Popen(argv,cwd=snapshot,env=env,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True);paused=False;held_at=None
            while child.poll()is None:
                free=shutil.disk_usage('/workspace').free;minimum_free=min(minimum_free,free);monitor.write_text(json.dumps(dict(command=label,minimum_sampled_free_bytes=minimum_free,last_sampled_free_bytes=free,sampling_interval_s=0.5,resource_holds=resource_holds,status='running'))+'\n')
                if not paused and free<350*1024*1024:
                    os.killpg(child.pid,signal.SIGSTOP);paused=True;held_at=time.monotonic();resource_holds.append(dict(started_at_s=held_at,free_bytes=free,reason='generated build paused at disk guard'));print('HOLD '+label+' process group paused: disk below350MiB',flush=True)
                elif paused and free>=450*1024*1024:
                    os.killpg(child.pid,signal.SIGCONT);paused=False;resource_holds[-1].update(resumed_at_s=time.monotonic(),free_bytes_on_resume=free);print('RESUME '+label+' headroom restored',flush=True)
                time.sleep(0.5)
            r=child
            monitor.write_text(json.dumps(dict(command=label,minimum_sampled_free_bytes=minimum_free,last_sampled_free_bytes=shutil.disk_usage('/workspace').free,sampling_interval_s=0.5,resource_holds=resource_holds,status='completed'))+'\n')

        data=log.read_bytes()
        executed=[]
        for raw in re.findall(rb'^\s*Running .+ \(([^\)]+)\)$',data,re.MULTILINE):
            binary=Path(raw.decode());saved=Path('/workspace/work/broker-oidc/socket-executables-after-jwks-'+base[:8])/(label+suffix)/binary.name;saved.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(binary,saved)
            binary_bytes=binary.read_bytes();mode=binary.stat().st_mode&0o777
            if saved.read_bytes()!=binary_bytes or saved.stat().st_mode&0o777!=mode:raise RuntimeError('executed ELF retention differs')
            executed.append(dict(executed_path=str(binary),retained_path=str(saved),sha256=hashlib.sha256(binary_bytes).hexdigest(),full_mode=mode,bytes=len(binary_bytes),source_scope='same six-source overlay and pushed base as this command'))
        (out/(label+'-executed-ELFs.json')).write_text(json.dumps(dict(command=label,invocations=executed),indent=2)+'\n')
        counts=re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out',data);row=dict(label=label,argv=argv,exit_code=r.returncode,duration_s=time.monotonic()-started,resource_holds=resource_holds,minimum_sampled_free_bytes=minimum_free,log=log.name,sha256=hashlib.sha256(data).hexdigest(),free_bytes_after=shutil.disk_usage('/workspace').free)
        if counts:row['test_totals']=dict(zip(['passed','failed','ignored','measured','filtered'],[sum(int(t[k])for t in counts)for k in range(5)]))
        verify(label,'after');results.append(row);(out/'commands.json').write_text(json.dumps(dict(base_source_sha=base,scope='six frozen socket source overlays on actual pushed JWKS baseline; selected library and owned HTTP/public-validation/OAuth/SASL socket behavior plus all-target strict, active/inactive feature coverage; no whole broker test matrix or official peer claim',commands=results),indent=2)+'\n');print('end '+label+' exit='+str(r.returncode),flush=True)
        if r.returncode:raise SystemExit(1)
print(json.dumps(dict(commands=len(results),all_passed=True)),flush=True)
