#!/usr/bin/env python3
"""Synthetic standard-cache-marker/dry-run gates; never touches actual targets."""
from pathlib import Path as RealPath,PurePosixPath
import ast,copy,hashlib,json,os,re,stat,types,gzip
BASE=RealPath('/workspace/work/client-capability-qa-preparation-e90efb49');PREP=BASE/'cache-tag-protection-preparation-v4';TAG_PREP=BASE/'cache-tag-protection-preparation'
RUNNER=BASE/'run-forced-baseline403-standard-marker-v4.py';raw=RUNNER.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='8065020485bec6865e09ffb567130194ccf9bdc6f3121fa5efc032234ea908fe'
TAG=(TAG_PREP/'standard-CACHEDIR.TAG.proposed').read_bytes();assert hashlib.sha256(TAG).hexdigest()=='6d9d1d216e0f83abc5e5662ca62c92b4f23009466b54fa27321a69acdb778bb2'
oldraw=(BASE/'run-forced-baseline403-platform-exception-v2.py').read_bytes();assert hashlib.sha256(oldraw).hexdigest()=='231425e452e5f1045b5eb6a8006a459d6f8ed6f4bd3dca3dc44afc4a6b3ba07c'
newtree=ast.parse(raw);oldtree=ast.parse(oldraw)
inherited=['verify_platform_daemons','verify_docker_cli','docker_zero_running_workloads','process_reference_guard','forecast','run','prove_forced_old_library','source_guard']
for name in inherited:
 a=next(n for n in oldtree.body if isinstance(n,ast.FunctionDef) and n.name==name);b=next(n for n in newtree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 assert ast.dump(a,include_attributes=False)==ast.dump(b,include_attributes=False),('inherited function changed',name)
names={'verify_complete_cache_map','repair_standard_owned_cache_marker','exact_dryrun_package_paths','guard_package_outputs_and_processes'}
code=compile(ast.fix_missing_locations(ast.Module(body=[n for n in newtree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[])),str(RUNNER),'exec')
TARGET='/workspace/work/client-share-target'
class Model:
 def __init__(self):
  self.data={};self.modes={};self.mtimes={};self.expected={};self.calls=[];self.fds={};self.nextfd=10;self.dir_uid=1000;self.dir_mode=0o700;self.dir_inode=524404;self.dir_type=stat.S_IFDIR;self.fsync_fail=False
  for i in range(608):
   name=('pkg/' if i<25 else 'deps/')+('f%03d'%i);path=TARGET+'/'+name;data=('synthetic-%d'%i).encode();self.put(path,data)
   self.expected[name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'full_mode':0o600,'mtime_ns':123}
  self.initial=copy.deepcopy(self.expected);self.own={name:{} for name in self.expected if name.startswith('pkg/')}
  self.data['/mock.stdout']=b'';self.data['/mock.stderr']=b''
 def put(self,path,data):self.data[path]=data;self.modes[path]=stat.S_IFREG|0o600;self.mtimes[path]=123
 def path(self,value):return Node(self,str(value))
 def files(self,root):return {name[len(TARGET)+1:]:self.path(name).lstat() for name in self.data if name.startswith(TARGET+'/')}
 def open(self,path,flags,mode=None):
  path=str(path);self.calls.append(('open',path,flags,mode));fd=self.nextfd;self.nextfd+=1
  if path!=TARGET:
   assert flags&os.O_EXCL and flags&os.O_CREAT and flags&os.O_NOFOLLOW
   if path in self.data:raise FileExistsError(17,'synthetic exists')
   self.put(path,b'');self.modes[path]=stat.S_IFREG|mode
  self.fds[fd]=path;return fd
 def fdopen(self,fd,mode):return Output(self,fd)
 def fsync(self,fd):
  self.calls.append(('fsync',self.fds[fd]))
  if self.fsync_fail:raise OSError(5,'synthetic fsync failure')
 def close(self,fd):self.calls.append(('close',self.fds[fd]))
 def replace(self,source,dest):
  source,dest=str(source),str(dest);self.calls.append(('replace',source,dest));self.data[dest]=self.data.pop(source);self.modes[dest]=self.modes.pop(source);self.mtimes[dest]=self.mtimes.pop(source)
 def namespace(self):
  fakeos=types.SimpleNamespace(path=types.SimpleNamespace(lexists=lambda p:str(p) in self.data),getuid=lambda:1000,open=self.open,fdopen=self.fdopen,fsync=self.fsync,close=self.close,replace=self.replace)
  for name in ['O_WRONLY','O_CREAT','O_EXCL','O_NOFOLLOW','O_RDONLY','O_DIRECTORY']:setattr(fakeos,name,getattr(os,name))
  ns={'Path':self.path,'TARGET':self.path(TARGET),'RUN':self.path('/mock-run'),'stat':stat,'files':self.files,'digest':lambda d:hashlib.sha256(d).hexdigest(),'os':fakeos,'ARGS':types.SimpleNamespace(run_label='synthetic'),'CACHE_TAG_BYTES':TAG,'json':json,'re':re,'INITIAL_PACKAGE_IDENTITIES':self.own,'persist':lambda:None}
  exec(code,ns);return ns
 def row(self,lines):
  self.data['/mock.stdout']=('\n'.join(lines)+'\n').encode();return {'stdout_path':'/mock.stdout','stderr_path':'/mock.stderr'}
class Output:
 def __init__(self,m,fd):self.m=m;self.fd=fd
 def __enter__(self):return self
 def __exit__(self,*args):return False
 def write(self,data):self.m.data[self.m.fds[self.fd]]+=data;return len(data)
 def flush(self):self.m.calls.append(('flush',self.m.fds[self.fd]))
 def fileno(self):return self.fd
class Node:
 def __init__(self,m,path):self.m=m;self.path=path
 def __str__(self):return self.path
 def __truediv__(self,other):return Node(self.m,self.path.rstrip('/')+'/'+str(other))
 @property
 def parts(self):return PurePosixPath(self.path).parts
 def relative_to(self,other):return PurePosixPath(self.path).relative_to(str(other))
 def read_bytes(self):return self.m.data[self.path]
 def read_text(self):return self.read_bytes().decode()
 def write_text(self,value):self.m.put(self.path,value.encode())
 def chmod(self,mode):self.m.modes[self.path]=(self.m.modes[self.path]&~0o7777)|mode
 def is_dir(self):return self.path==TARGET or any(name.startswith(self.path.rstrip('/')+'/') for name in self.m.data)
 def lstat(self):
  if self.path==TARGET:return types.SimpleNamespace(st_mode=self.m.dir_type|self.m.dir_mode,st_uid=self.m.dir_uid,st_dev=27,st_ino=self.m.dir_inode)
  return types.SimpleNamespace(st_mode=self.m.modes[self.path],st_mtime_ns=self.m.mtimes[self.path],st_uid=1000)
RESULTS=[]
def control(name,fn):fn();RESULTS.append({'name':name,'passed':True})
def rejects(fn):
 try:fn()
 except (AssertionError,OSError,KeyError):return
 raise AssertionError('synthetic hostile input accepted')
def repair_positive():
 m=Model();ns=m.namespace();before=copy.deepcopy(m.expected);result=ns['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected})
 assert result['original_marker']['present'] is False and len(result['new609_complete_cache_identity_map'])==609
 assert m.data[TARGET+'/CACHEDIR.TAG']==TAG and all(m.expected[k]==v for k,v in before.items())
 assert [x[0] for x in m.calls].count('replace')==1 and [x[0] for x in m.calls].count('fsync')==2
 assert not any('.tmp' in p for p in m.data)
control('standard177B-repair-only-after-complete608-map-and-owned-directory',repair_positive)
for label,changes in [('wrong_owner',{'dir_uid':1001}),('wrong_mode',{'dir_mode':0o755}),('different_inode',{'dir_inode':524405}),('symlink_target',{'dir_type':stat.S_IFLNK})]:
 def bad(changes=changes):
  m=Model();[setattr(m,k,v) for k,v in changes.items()];rejects(lambda:m.namespace()['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected}));assert not m.calls
 control('reject-target-'+label+'-before-write',bad)
for data in [b'invalid-original-marker',TAG]:
 def exists(data=data):
  m=Model();m.put(TARGET+'/CACHEDIR.TAG',data);rejects(lambda:m.namespace()['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected}));assert m.data[TARGET+'/CACHEDIR.TAG']==data and not m.calls
 control('refuse-unreviewed-existing-marker-'+('standard' if data==TAG else 'invalid'),exists)
def cache_changed():
 m=Model();m.data[TARGET+'/deps/f025']=b'changed';rejects(lambda:m.namespace()['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected}));assert not m.calls
control('reject-changed-dependency-cache-hash-before-marker-write',cache_changed)
def cache_extra():
 m=Model();m.put(TARGET+'/unreviewed',b'extra');rejects(lambda:m.namespace()['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected}));assert not m.calls
control('reject-unreviewed-cache609-before-repair',cache_extra)
def cache_short():
 m=Model();m.data.pop(TARGET+'/deps/f607');m.expected.pop('deps/f607');rejects(lambda:m.namespace()['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected}));assert not m.calls
control('reject607-even-when-map-consistent-before-write',cache_short)
def fsync_failure():
 m=Model();m.fsync_fail=True;rejects(lambda:m.namespace()['repair_standard_owned_cache_marker']({'complete_cache_identity_map':m.expected}));assert TARGET+'/CACHEDIR.TAG' not in m.data and not any(x[0]=='replace' for x in m.calls)
control('fsync-failure-prevents-marker-publication-retains-temp-state',fsync_failure)
for form in ['bare','Removing','backtick','directory']:
 def good(form=form):
  m=Model();m.put(TARGET+'/CACHEDIR.TAG',TAG);m.expected['CACHEDIR.TAG']={'sha256':hashlib.sha256(TAG).hexdigest(),'bytes':177,'full_mode':0o600,'mtime_ns':123}
  paths=[TARGET+'/'+x for x in m.own]
  if form=='Removing':paths=['Removing '+x for x in paths]
  elif form=='backtick':paths=['Removing `'+x+'`' for x in paths]
  elif form=='directory':paths=['Removing '+TARGET+'/pkg']
  result=m.namespace()['exact_dryrun_package_paths'](m.row(paths),m.expected);assert result['expanded_regular_file_count']==25 and result['CACHEDIR_TAG_removal_planned'] is False
 control('dryrun-exact25-positive-'+form,good)
for scenario in ['missing_one','marker','unknown','root','deps','unparseable','none','cache_changed']:
 def bad(scenario=scenario):
  m=Model();m.put(TARGET+'/CACHEDIR.TAG',TAG);m.expected['CACHEDIR.TAG']={'sha256':hashlib.sha256(TAG).hexdigest(),'bytes':177,'full_mode':0o600,'mtime_ns':123}
  paths=['Removing '+TARGET+'/'+x for x in m.own]
  if scenario=='missing_one':paths.pop()
  elif scenario=='marker':paths.append('Removing '+TARGET+'/CACHEDIR.TAG')
  elif scenario=='unknown':paths.append('Removing '+TARGET+'/unreviewed')
  elif scenario=='root':paths=['Removing '+TARGET]
  elif scenario=='deps':paths.append('Removing '+TARGET+'/deps')
  elif scenario=='unparseable':paths.append('note: unexpected target '+TARGET+'/pkg')
  elif scenario=='none':paths=['Removed 25 files total','no files deleted due to --dry-run']
  elif scenario=='cache_changed':m.data[TARGET+'/deps/f025']=b'changed-during-dryrun'
  rejects(lambda:m.namespace()['exact_dryrun_package_paths'](m.row(paths),m.expected))
 control('dryrun-reject-'+scenario+'-before-real-clean',bad)
def frozen_guard_namespace(m):
 ns=m.namespace();frozen=copy.deepcopy(m.expected)
 identities={}
 for name in m.own:
  row=copy.deepcopy(m.expected[name]);row['gzip_path']='/mock-gzip/'+name
  m.put(row['gzip_path'],gzip.compress(m.data[TARGET+'/'+name],mtime=0));identities[name]=row
 ns.update(PACKAGE_MAP={'complete_cache_file_inventory':copy.deepcopy(m.expected)},FULL_CACHE_REFERENCE=frozen,
           INITIAL_PACKAGE_IDENTITIES=identities,gzip=gzip,source_guard=lambda *args:{'synthetic_only':True},
           OLD_SOURCE=m.path('/mock-original403'),OLD_MANIFEST={},SOURCE=m.path('/mock-source04f'),MANIFEST={},
           process_reference_guard=lambda:{'synthetic_only':True})
 return ns
def frozen_all608_positive():
 m=Model();ns=frozen_guard_namespace(m);r=ns['guard_package_outputs_and_processes']()
 assert len(r['complete_cache_identity_map'])==608 and all('mtime_ns' in row for row in r['complete_cache_identity_map'].values())
control('frozen608-SHA-bytes-fullmode-mtime-positive-retained25-gzip',frozen_all608_positive)
for field in ['sha256','bytes','full_mode','mtime_ns']:
 def corrupt_reference(field=field):
  m=Model();ns=frozen_guard_namespace(m);ns['FULL_CACHE_REFERENCE']['deps/f025'][field]=('0'*64 if field=='sha256' else 1)
  rejects(ns['guard_package_outputs_and_processes'])
 control('reject-frozen-nonpackage-'+field+'-mismatch-before-marker',corrupt_reference)

assert len(RESULTS)==28,len(RESULTS)
receipt={'schema_version':1,'runner_sha256':hashlib.sha256(raw).hexdigest(),'control_source_sha256':hashlib.sha256(RealPath(__file__).read_bytes()).hexdigest(),'passed':True,'synthetic_marker_dryrun_controls':len(RESULTS),'controls':RESULTS,'inherited43control_function_ASTs_unchanged':inherited,'scope':'synthetic extracted helper functions only; no actual cache tag write/Cargo dry-run/cleanup/compiler/API behavior','actual_target_writes':0,'actual_Cargo_commands':0,'actual_Docker_queries':0}
p=PREP/'marker-dryrun-controls.json';p.write_text(json.dumps(receipt,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'passed':True,'synthetic_controls':len(RESULTS),'receipt_sha256':hashlib.sha256(p.read_bytes()).hexdigest()}))
