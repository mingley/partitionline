#!/usr/bin/env python3
"""Build exact immutable peer inputs; this does not execute an OAuth peer."""
import argparse
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import shlex
import shutil
import signal
import subprocess
import sys
import time

FLOOR=350*1024*1024
sys.dont_write_bytecode=True

def require(ok,label):
    if not ok:raise RuntimeError(label)

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        while data:=f.read(1048576):h.update(data)
    return h.hexdigest()

def write(path,value):
    Path(path).write_text(json.dumps(value,indent=2)+'\n')

class Build:
    def __init__(self,args):
        self.args=args
        self.source=Path(args.source_root).resolve()
        self.output=Path(args.output).resolve()
        self.output.mkdir(mode=0o700,parents=True,exist_ok=False)
        self.target=Path(args.target).resolve()
        require(not self.target.is_relative_to(self.source) and not self.output.is_relative_to(self.source),
                'build writes outside immutable input')
        self.environment=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',
            PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_TARGET_DIR=str(self.target),
            CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',
            CARGO_NET_OFFLINE='true',PYTHONDONTWRITEBYTECODE='1')
        self.results=[]
        self.elf_objects={}
        self.peer_base=self.source/'docs/evidence/broker/KL11-37/peers'
        path=self.peer_base/'driver/run-live.py'
        require(sha(path)=='7097d0001acde3563ebd70cb56324cbca308a7268e1132aaa06a2cc1d81c85f6','published driver source')
        spec=importlib.util.spec_from_file_location('frozen_peer_driver',path)
        module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.guard=module.Run({'source_root':str(self.source),'source_sha':args.source_sha,
                    'git_repository':'/workspace/partitionline'},self.output,
                    self.output.parent/(self.output.name+'-unused-private'))
        origin=Path(args.origin)
        require(sha(origin)==args.origin_sha256,'actual normalized origin bytes')
        value=json.loads(origin.read_text())
        require(value['source_sha']==args.source_sha and value['source_root']==str(self.source)
                and value['complete_git_tree_verified'] and value['mutable_worktree_inodes_disjoint'],
                'complete immutable origin')
        self.baseline=self.guard.source_guard()
        data=json.dumps(self.baseline,separators=(',',':')).encode()+b'\n'
        encoded=gzip.compress(data,mtime=0)
        require(gzip.decompress(encoded)==data,'complete source audit roundtrip')
        (self.output/'source-before.json.gz').write_bytes(encoded)
        write(self.output/'source-before.restore.json',{'original_path':'source-before.json','original_bytes':len(data),
             'original_sha256':hashlib.sha256(data).hexdigest(),'original_mode':'0o600',
             'compressed_sha256':sha(self.output/'source-before.json.gz'),'roundtrip_verified':True})
        self.source_digest=hashlib.sha256(data).hexdigest()
        self.save()

    def source_check(self):
        rows=self.guard.source_guard()
        require(rows==self.baseline,'whole source byte/fullmode/pathset unchanged')
        return {'file_count':len(rows),'raw_sha256':self.source_digest,'identical_to_before':True,
                'scope':'every Git blob and exact actual regular/symlink pathset/full07777'}

    def owners(self):
        target=str(self.target)
        found=[]
        for entry in Path('/proc').iterdir():
            if not entry.name.isdigit() or int(entry.name)==os.getpid():continue
            categories=set()
            for kind in ('cwd','exe'):
                try:
                    value=os.readlink(entry/kind).removesuffix(' (deleted)')
                    if value==target or value.startswith(target+'/'):categories.add(kind)
                except OSError:pass
            try:
                if b'CARGO_TARGET_DIR='+target.encode() in (entry/'environ').read_bytes().split(b'\0'):
                    categories.add('CARGO_TARGET_DIR')
            except OSError:pass
            try:
                if target+'/' in (entry/'maps').read_text():categories.add('maps')
            except OSError:pass
            try:
                for fd in (entry/'fd').iterdir():
                    try:
                        value=os.readlink(fd).removesuffix(' (deleted)')
                        if value==target or value.startswith(target+'/'):categories.add('fd')
                    except OSError:pass
            except OSError:pass
            if categories:found.append({'pid':int(entry.name),'categories':sorted(categories)})
        require(not found,'no live own-target reference before retention/overwrite')
        return found

    def retain(self,label):
        self.owners()
        files=[]
        if self.target.exists():
            for path in sorted(self.target.rglob('*')):
                if not path.is_file() or path.is_symlink():continue
                with path.open('rb') as f:
                    if f.read(4)==b'\x7fELF':files.append(path)
        # Worst-case gzip overhead plus metadata; hold before any retention write.
        unknown=[p for p in files if sha(p) not in self.elf_objects]
        bound=sum(p.stat().st_size+p.stat().st_size//1000+1024 for p in unknown)+1048576
        require(shutil.disk_usage('/workspace').free>=FLOOR+bound,'retention forecast respects disk floor')
        directory=self.output/'retained-elf-objects'
        directory.mkdir(mode=0o700,exist_ok=True)
        rows=[]
        for path in files:
            before=path.stat()
            checksum=sha(path)
            if checksum not in self.elf_objects:
                destination=directory/(checksum+'.gz')
                with path.open('rb') as source,destination.open('wb') as target:
                    with gzip.GzipFile(fileobj=target,mode='wb',mtime=0) as encoded:
                        shutil.copyfileobj(source,encoded,1048576)
                restored=hashlib.sha256()
                total=0
                with gzip.open(destination,'rb') as decoded:
                    while chunk:=decoded.read(1048576):restored.update(chunk);total+=len(chunk)
                require(restored.hexdigest()==checksum and total==before.st_size,'retained ELF exact decompression')
                self.elf_objects[checksum]={'path':str(destination),'gzip_sha256':sha(destination),
                    'original_sha256':checksum,'bytes':total,'roundtrip_verified':True}
            after=path.stat()
            require(sha(path)==checksum and (before.st_mode&0o7777)==(after.st_mode&0o7777),'ELF unchanged during retention')
            rows.append({'original_path':str(path),'sha256':checksum,'bytes':before.st_size,
                         'full_mode':oct(before.st_mode&0o7777),'object':self.elf_objects[checksum]})
        self.owners()
        write(self.output/(label+'.elf-retention.json'),{'owners_before_after':[], 'files':rows})
        return rows

    def command(self,name,argv,timeout=600):
        before=self.source_check()
        self.retain(name+'-before')
        require(shutil.disk_usage('/workspace').free>=FLOOR,'disk floor before compiler')
        directory=self.output/name
        directory.mkdir(mode=0o700)
        samples=[]
        began=time.monotonic()
        with (directory/'command.log').open('wb') as log:
            process=subprocess.Popen(['taskset','-c','0,1',*argv],cwd=self.source,env=self.environment,
                  stdout=log,stderr=subprocess.STDOUT,start_new_session=True,umask=0o077)
            stopped=None
            while process.poll() is None:
                free=shutil.disk_usage('/workspace').free
                samples.append({'elapsed_seconds':time.monotonic()-began,'free_bytes':free})
                if free<FLOOR or time.monotonic()-began>timeout:
                    stopped='disk-floor' if free<FLOOR else 'compiler-deadline'
                    os.killpg(process.pid,signal.SIGTERM)
                    try:process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid,signal.SIGKILL)
                        process.wait(timeout=5)
                    break
                time.sleep(.5)
            code=process.wait()
        row={'name':name,'command':['taskset','-c','0,1',*argv],'cwd':str(self.source),'exit_code':code,
             'elapsed_seconds':time.monotonic()-began,'stopped':stopped,'disk_samples':samples,
             'minimum_free_bytes':min((s['free_bytes'] for s in samples),default=shutil.disk_usage('/workspace').free),
             'log_path':str(directory/'command.log'),'log_sha256':sha(directory/'command.log'),
             'source_before':before,'source_after':self.source_check()}
        self.results.append(row)
        self.retain(name+'-after')
        self.save()
        print(json.dumps({'command':name,'exit_code':code,'stopped':stopped}),flush=True)
        return code==0 and not stopped

    def save(self):
        write(self.output/'validation.json',{'source_sha':self.args.source_sha,'source_root':str(self.source),
              'origin_sha256':self.args.origin_sha256,'driver_sha256':sha(Path(__file__)),
              'scope':'actual peer compiler qualification only; zero SDK/broker live runs',
              'commands':self.results,'retained_elf_objects':list(self.elf_objects.values()),
              'environment':{k:self.environment[k] for k in ('CARGO_HOME','RUSTUP_HOME','CARGO_TARGET_DIR',
               'CARGO_INCREMENTAL','CARGO_BUILD_JOBS','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_NET_OFFLINE')},
              'passed':bool(self.results) and all(r['exit_code']==0 and not r['stopped'] for r in self.results)})

    def copy_binary(self,path,relative):
        destination=self.output/relative
        destination.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        shutil.copy2(path,destination)
        require(sha(path)==sha(destination),'copied normal binary exact')
        return {'path':str(destination),'sha256':sha(destination),'bytes':destination.stat().st_size,
                'full_mode':oct(destination.stat().st_mode&0o7777)}

    def run(self):
        rust_manifest=str(self.peer_base/'rust/Cargo.toml')
        rust_ok=True
        for toolchain in ('stable','1.85.0'):
            if not rust_ok:break  # Do not repeat unchanged rejected source.
            argv=['cargo','+'+toolchain,'build','--offline','--locked','--manifest-path',rust_manifest,
                  '--message-format=json-render-diagnostics']
            rust_ok=self.command(toolchain+'-rust-build',argv)
            if rust_ok:
                artifact=self.copy_binary(self.target/'debug/partitionline-oidc-live-peer','bin/'+toolchain+'/rust-peer')
                write(self.output/(toolchain+'-rust-artifact.json'),{'source_sha':self.args.source_sha,
                      'artifact_kind':'normal-peer-executable','build_command':argv,'artifact':artifact})
                rust_ok=self.command(toolchain+'-rust-strict',['cargo','+'+toolchain,'clippy','--offline','--locked',
                                      '--manifest-path',rust_manifest,'--bins','--','-D','warnings'])
        sdk=Path('/workspace/work/c-peer/source/src')
        native=self.output/'native'
        native.mkdir(mode=0o700)
        cjson_ok=self.command('native-cjson-object',['gcc','-std=c11','-O2','-I'+str(sdk),'-c',str(sdk/'cJSON.c'),'-o',str(native/'cJSON.o')])
        flags=shlex.split(subprocess.check_output(['pkg-config','--cflags','--libs','libcurl','openssl'],text=True))
        if cjson_ok:
            self.command('native-strict-link',['gcc','-std=c11','-O2','-Wall','-Wextra','-Wpedantic','-Werror',
                '-isystem',str(sdk),str(self.peer_base/'native/oidc-native-peer.c'),str(native/'cJSON.o'),
                '-L/workspace/work/c-peer/lib','-Wl,-rpath,/workspace/work/c-peer/lib','-lrdkafka',*flags,
                '-o',str(native/'oidc-native-peer')])
        pins=json.loads((self.peer_base/'java/pins.json').read_text())
        for release in pins['releases']:
            name=release['release']
            jars=[]
            for item in release['runtime_jars']:
                require(sha(item['path'])==item['sha256'],'official Java runtime JAR pin')
                jars.append(item['path'])
            classes=self.output/'java'/name/'classes'
            classes.mkdir(mode=0o700,parents=True)
            self.command('java-'+name+'-strict',['/usr/bin/java','-Xmx128m','--add-modules','jdk.compiler',
                 'com.sun.tools.javac.Main','-Xlint:all','-Werror','-cp',':'.join(jars),'-d',str(classes),
                 str(self.peer_base/'java/OAuthMetadataPeer.java')],timeout=60)
        self.save()

def main():
    parser=argparse.ArgumentParser()
    for key in ('source-sha','source-root','origin','origin-sha256','output','target'):
        parser.add_argument('--'+key,required=True)
    args=parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    os.umask(0o077)
    os.sched_setaffinity(0,{0,1})
    build=Build(args)
    build.run()

if __name__=='__main__':main()
