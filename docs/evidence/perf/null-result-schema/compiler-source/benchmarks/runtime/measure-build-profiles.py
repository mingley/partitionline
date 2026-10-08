#!/usr/bin/env python3
"""Build source-bound benchmark profiles and assemble their measurement matrix."""
import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

NAMES=('runtime','nb-serve','native-produce-runtime','native-latency-runtime')
ARMS=[('none_16_portable','false',16,'x86-64',None),
      ('thin_16_portable','thin',16,'x86-64',None),
      ('fat_16_portable','fat',16,'x86-64',None),
      ('thin_1_portable','thin',1,'x86-64',None),
      ('thin_16_native','thin',16,'native',None),
      ('pgo_generate','thin',16,'native','generate'),
      ('pgo_use','thin',16,'native','use')]


def sha(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def save(path,value):
    with path.open('x') as stream:
        json.dump(value,stream,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())


def module(path,name):
    spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('stage',choices=('build','matrix','merge'))
    for name in ('source','source-pins','output'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--commit',required=True);p.add_argument('--target-dir',type=Path)
    p.add_argument('--only',choices=('initial','pgo_use'),default='initial')
    p.add_argument('--profile-data',type=Path);p.add_argument('--training',action='store_true')
    p.add_argument('--build-manifest',type=Path,action='append',default=[])
    p.add_argument('--training-directory',type=Path,action='append',default=[])
    a=p.parse_args();source=a.source.resolve();output=a.output.resolve()
    output.mkdir(parents=True,exist_ok=False)
    pins=json.loads(a.source_pins.read_text())
    external={str(a.source_pins.resolve()):sha(a.source_pins),str(Path(__file__).resolve()):sha(__file__)}
    if a.profile_data:external[str(a.profile_data.resolve())]=sha(a.profile_data)
    owner=module(source/'scripts/run-benchmark-matrix.py','profile_process_owner')
    if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0):raise OSError('owned child subreaper unavailable')
    def interrupt(signum,frame):raise InterruptedError('profile stage owner interrupted')
    for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,interrupt)
    deadline=time.monotonic()+7200
    tools=source/'benchmarks/runtime/tools'
    def guard():
        if time.monotonic()>=deadline:raise TimeoutError('overall profile-stage deadline')
        if any(sha(source/name)!=digest for name,digest in pins.items()):raise ValueError('source bytes changed')
        if any(sha(name)!=digest for name,digest in external.items()):raise ValueError('input bytes changed')
        if subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()!=a.commit:
            raise ValueError('source commit differs')
        if subprocess.check_output(['git','-C',str(source),'status','--porcelain']):raise ValueError('source is dirty')
    def command(argv,directory,label,timeout,env=None):
        guard()
        result=owner.execute(['python3','-B',str(tools/'parent-bound-exec.py'),str(os.getpid()),*map(str,argv)],
            env or owner.base_env(),directory,label,min(timeout,deadline-time.monotonic()))
        guard();return result
    guard()
    rustc=command(['rustc','+stable','-vV'],output,'rustc',15).read_text()
    if 'release: 1.99.0\n' not in rustc or 'LLVM version: 23.1.1\n' not in rustc:
        raise ValueError('declared latest-stable Rust/LLVM identity differs')
    save(output/'source-inputs.json',dict(commit=a.commit,files=pins,external_inputs=external,rustc=rustc))
    if a.stage=='build':
        if a.target_dir is None:raise ValueError('explicit generated target directory required')
        target=a.target_dir.resolve()
        if target.parent!=Path('/workspace/work') or not target.name.startswith('target-build-profiles'):
            raise ValueError('generated target must be a dedicated /workspace/work/target-build-profiles directory')
        if target.exists() and any(target.iterdir()):raise ValueError('new or empty generated compilation directory required')
        configs=[]
        for name,lto,units,cpu,pgo in ARMS:
            if (a.only=='initial' and pgo=='use') or (a.only=='pgo_use' and pgo!='use'):continue
            directory=output/name;directory.mkdir()
            env=owner.base_env()
            for key in list(env):
                if key in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','CARGO_BUILD_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','RUSTC_BOOTSTRAP','LLVM_PROFILE_FILE') or (key.startswith('CARGO_TARGET_') and key.endswith('_RUSTFLAGS')):
                    env.pop(key,None)
            flags=['-C','target-cpu='+cpu]
            if pgo=='generate':flags+=['-C','profile-generate='+str(directory/'profile-defaults')]
            if pgo=='use':
                if a.profile_data is None:raise ValueError('actual merged profile data required')
                flags+=['-C','profile-use='+str(a.profile_data.resolve())]
            settings=dict(CARGO_INCREMENTAL='0',CARGO_PROFILE_RELEASE_OPT_LEVEL='3',CARGO_PROFILE_RELEASE_LTO=lto,
                CARGO_PROFILE_RELEASE_CODEGEN_UNITS=str(units),CARGO_PROFILE_RELEASE_DEBUG='0',CARGO_PROFILE_RELEASE_STRIP='none',RUSTFLAGS=' '.join(flags))
            env.update(settings)
            argv=['cargo','+stable','build','--offline','--locked','--release','--verbose','--manifest-path',source/'benchmarks/runtime/Cargo.toml',
                '--bins','--target','x86_64-unknown-linux-gnu','--target-dir',target]
            specification=dict(lto=lto,codegen_units=units,target_cpu=cpu,pgo=pgo,environment=settings,
                target='x86_64-unknown-linux-gnu',source_commit=a.commit,rustc=rustc,
                profile_data_sha256=sha(a.profile_data) if pgo=='use' else None)
            save(directory/'declared-build.json',specification)
            print('build '+name,flush=True)
            command(argv,directory,'cargo-build',900,env)
            binaries={}
            for binary in NAMES:
                original=target/'x86_64-unknown-linux-gnu/release'/binary;copy=directory/binary;shutil.copy2(original,copy)
                binaries[binary]=dict(path=str(copy),sha256=sha(copy),bytes=copy.stat().st_size)
            configs.append(dict(name=name,flavor='current_thread',workers=0,build=specification,binaries=binaries,
                instrumented_training=pgo=='generate'))
            save(directory/'completed-build.json',configs[-1])
            command(['cargo','+stable','clean','--manifest-path',source/'benchmarks/runtime/Cargo.toml','--target-dir',target],directory,'cargo-clean',60,env)
            print('retained '+name,flush=True)
        save(output/'build-manifest.json',dict(source_commit=a.commit,configs=configs))
    elif a.stage=='matrix':
        configs=[]
        for path in a.build_manifest:
            external[str(path.resolve())]=sha(path);data=json.loads(path.read_text())
            if data['source_commit']!=a.commit:raise ValueError('build source differs')
            configs.extend(data['configs'])
        fixed=[c for c in configs if c['name']=='none_16_portable']
        if len(fixed)!=1:raise ValueError('one portable baseline required')
        baseline=fixed[0]['binaries']['nb-serve']
        selected=[c for c in configs if c['instrumented_training']==a.training]
        if not selected:raise ValueError('nonempty selected matrix required')
        for config in selected:
            config['binaries']['nb-serve']=baseline
            for binary in config['binaries'].values():
                if sha(binary['path'])!=binary['sha256']:raise ValueError('retained build bytes differ')
        save(output/'matrix.json',dict(scope='local/unsigned',source_commit=a.commit,configs=selected,training=a.training,
            fixed_null_broker_binary=baseline,notes='All measured clients use current_thread and the same broker build. No compiler build runs during measurement.'))
    else:
        raws=sorted(path for root in a.training_directory for path in root.rglob('*.profraw'))
        if not raws:raise ValueError('actual completed training raw profiles required')
        for path in raws:external[str(path.resolve())]=sha(path)
        sysroot=command(['rustc','+stable','--print','sysroot'],output,'sysroot',15).read_text().strip()
        profdata=Path(sysroot)/'lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata'
        external[str(profdata)]=sha(profdata)
        version=command([profdata,'--version'],output,'llvm-profdata-version',15).read_text()
        if '23.1.1-rust-1.99.0-stable' not in version:raise ValueError('matching stable LLVM tools required')
        merged=output/'merged.profdata'
        command([profdata,'merge','--failure-mode=any','-o',merged,*raws],output,'merge-profiles',60)
        show=command([profdata,'show','--all-functions','--counts',merged],output,'show-profiles',60)
        if not show.stat().st_size:raise ValueError('merged profile has no recorded summary')
        save(output/'merge-manifest.json',dict(source_commit=a.commit,raws=[dict(path=str(p),sha256=sha(p)) for p in raws],
            merged=dict(path=str(merged),sha256=sha(merged)),llvm_version=version,inputs=external))
    guard()
    save(output/'completion.json',dict(status='completed',source_guards_passed=True,stage=a.stage,ended_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))


if __name__=='__main__':main()
