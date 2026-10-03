#!/usr/bin/env python3
"""Extracted exact quarantine helpers on a bounded in-memory filesystem."""
from pathlib import Path as RealPath,PurePosixPath
import ast,copy,gzip,hashlib,json,os,stat,types
BASE=RealPath('/workspace/work/client-capability-qa-preparation-e90efb49');PREP=BASE/'package-quarantine-preparation-v1'
RUNNER=BASE/'run-forced-baseline403-quarantine-v1.py';raw=RUNNER.read_bytes();tree=ast.parse(raw)
NAMES={'observe_quarantine_identity','require_quarantine_identity','quarantine_location_snapshot','quarantine_log_event','quarantine_own_package_outputs','verify_complete_cache_map'}
code=compile(ast.fix_missing_locations(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in NAMES],type_ignores=[])),str(RUNNER),'exec')
TARGET='/workspace/work/client-share-target';Q='/mock-work/quarantine';RUN='/mock-work/run'
class Model:
 def __init__(self):
  self.nodes={};self.nextino=600000;self.renames=[];self.opens=[];self.mkdir_calls=[];self.rename_fail_before=None;self.rename_fail_after=None;self.fail_destination=None;self.intrude_after=None;self.ledger_fail_event=None;self.ledger_events=0
  self.make(TARGET,stat.S_IFDIR|0o700,inode=524404);self.make('/mock-work',stat.S_IFDIR|0o700);self.make(RUN,stat.S_IFDIR|0o700)
  self.expected={};self.own={}
  for i in range(608):
   name=('debug/.fingerprint/p%02d/f%d'%(i//4,i) if i<16 else 'debug/deps/owned%d'%i) if i<25 else 'debug/deps/dep%d'%i
   data=('synthetic-%d'%i).encode();self.make(TARGET+'/'+name,stat.S_IFREG|0o600,data)
   n=self.nodes[TARGET+'/'+name];row=self.identity(n);self.expected[name]={k:row[k] for k in ['sha256','bytes','full_mode','mtime_ns']}
   if i<25:
    row['gzip_path']='/mock-gzip/'+str(i);obj=gzip.compress(data,mtime=0);self.make(row['gzip_path'],stat.S_IFREG|0o600,obj);row['gzip_sha256']=hashlib.sha256(obj).hexdigest();self.own[name]=row
  self.make(TARGET+'/CACHEDIR.TAG',stat.S_IFREG|0o600,b'synthetic-standard-marker');self.expected['CACHEDIR.TAG']={k:v for k,v in self.identity(self.nodes[TARGET+'/CACHEDIR.TAG']).items() if k in ['sha256','bytes','full_mode','mtime_ns']}
  self.frozen={k:v for k,v in self.expected.items() if k!='CACHEDIR.TAG'}
 def make(self,p,mode,data=b'',inode=None):
  if inode is None:self.nextino+=1;inode=self.nextino
  self.nodes[p]={'mode':mode,'data':data,'mtime':123,'inode':inode,'dev':27,'uid':1000}
 def identity(self,n):return {'sha256':hashlib.sha256(n['data']).hexdigest(),'bytes':len(n['data']),'full_mode':stat.S_IMODE(n['mode']),'mtime_ns':n['mtime'],'inode':[n['dev'],n['inode']]}
 def path(self,p):return Node(self,str(p))
 def files(self,p):
  prefix=str(p).rstrip('/')+'/'
  return {k[len(prefix):]:self.path(k).lstat() for k,n in self.nodes.items() if k.startswith(prefix) and not stat.S_ISDIR(n['mode'])}
 def rename(self,source,dest):
  source,dest=str(source),str(dest);ordinal=len(self.renames)
  if ordinal==self.rename_fail_before:raise OSError('synthetic before-rename error')
  assert dest not in self.nodes
  n=self.nodes.pop(source);self.nodes[dest]=n;self.renames.append((source,dest))
  if ordinal==self.rename_fail_after:raise OSError('synthetic ambiguous-after-rename error')
  if ordinal==self.fail_destination:
   future=sorted(self.own)[ordinal+1];self.make(Q+'/'+future,stat.S_IFLNK|0o777,b'foreign')
  if ordinal==self.intrude_after:self.make(TARGET+'/unknown-new-path',stat.S_IFREG|0o600,b'foreign')
 def namespace(self):
  fakeos=types.SimpleNamespace(path=types.SimpleNamespace(lexists=lambda p:str(p) in self.nodes),getuid=lambda:1000,rename=self.rename,fsync=lambda fd:None)
  ns={'Path':self.path,'os':fakeos,'stat':stat,'digest':lambda data:hashlib.sha256(data).hexdigest(),'json':json,'gzip':gzip,
      'TARGET':self.path(TARGET),'QUARANTINE':self.path(Q),'RUN':self.path(RUN),'INITIAL_PACKAGE_IDENTITIES':self.own,
      'FULL_CACHE_REFERENCE':self.frozen,'files':self.files,'INTERRUPTED':None,'PLAN':{'source_sha':'synthetic-only'}}
  exec(code,ns);return ns
 def checked(self):return {'complete_cache_identity_map':copy.deepcopy(self.expected)}
class Node:
 def __init__(self,m,p):self.m=m;self.p=p
 def __str__(self):return self.p
 def __truediv__(self,p):return Node(self.m,self.p.rstrip('/')+'/'+str(p))
 @property
 def parent(self):return Node(self.m,str(PurePosixPath(self.p).parent))
 @property
 def parts(self):return PurePosixPath(self.p).parts
 def is_absolute(self):return PurePosixPath(self.p).is_absolute()
 def read_bytes(self):return self.m.nodes[self.p]['data']
 def read_text(self):return self.read_bytes().decode()
 def lstat(self):
  n=self.m.nodes[self.p];return types.SimpleNamespace(st_mode=n['mode'],st_uid=n['uid'],st_dev=n['dev'],st_ino=n['inode'],st_size=len(n['data']),st_mtime_ns=n['mtime'])
 def mkdir(self,mode=0o777):
  self.m.mkdir_calls.append(self.p)
  if self.p in self.m.nodes:raise FileExistsError('synthetic existing directory')
  self.m.make(self.p,stat.S_IFDIR|mode)
 def chmod(self,mode):self.m.nodes[self.p]['mode']=(self.m.nodes[self.p]['mode']&~0o7777)|mode
 def write_text(self,data):
  if self.p not in self.m.nodes:self.m.make(self.p,stat.S_IFREG|0o600)
  self.m.nodes[self.p]['data']=data.encode();return len(data)
 def open(self,mode):
  self.m.opens.append((self.p,mode))
  if mode=='x':
   if self.p in self.m.nodes:raise FileExistsError('synthetic existing ledger')
   self.m.make(self.p,stat.S_IFREG|0o600)
  elif mode=='a':assert self.p in self.m.nodes
  else:raise AssertionError(mode)
  return Output(self.m,self.p)
class Output:
 def __init__(self,m,p):self.m=m;self.p=p
 def __enter__(self):return self
 def __exit__(self,*args):return False
 def write(self,value):
  if self.p.endswith('quarantine-moves.jsonl'):
   i=self.m.ledger_events;self.m.ledger_events+=1
   if i==self.m.ledger_fail_event:raise OSError('synthetic ledger append failure')
  self.m.nodes[self.p]['data']+=value.encode();return len(value)
 def flush(self):pass
 def fileno(self):return 10
RESULTS=[]
def control(name,fn):fn();RESULTS.append({'name':name,'passed':True})
def rejects(fn):
 try:fn()
 except (AssertionError,OSError,KeyError,InterruptedError):return
 raise AssertionError('hostile control accepted')
def positive():
 m=Model();before=copy.deepcopy(m.nodes);ns=m.namespace();r=ns['quarantine_own_package_outputs'](m.checked())
 assert len(m.renames)==25 and len(m.files(m.path(TARGET)))==584 and len(m.files(m.path(Q)))==25
 assert r['actual_reclaimed_file_bytes']==0 and not r['raw_original_bytes_deleted_copied_or_reencoded']
 assert r['retained_locations']['all25_present_exactly_once_and_intact']
 assert all(m.nodes[dest]==before[src] for src,dest in m.renames)
 events=[json.loads(x) for x in m.nodes[RUN+'/quarantine-moves.jsonl']['data'].decode().splitlines()]
 assert len(events)==50 and sum(x['operation']=='rename_planned' for x in events)==25 and sum(x['operation']=='rename_completed' for x in events)==25
 assert m.nodes[TARGET+'/CACHEDIR.TAG']==before[TARGET+'/CACHEDIR.TAG']
control('exact609-to584-and25-same-inodes-fullmode-mtime-zero-reclaim',positive)
for field in ['sha256','bytes','full_mode','mtime_ns','inode']:
 def bad(field=field):
  m=Model();name=sorted(m.own)[0];m.own[name][field]=('0'*64 if field=='sha256' else [27,1] if field=='inode' else 1)
  rejects(lambda:m.namespace()['quarantine_own_package_outputs'](m.checked()));assert not m.renames and Q not in m.nodes
 control('reject-owned-'+field+'-mismatch-before-any-rename',bad)
for scenario in ['added_unknown','symlink_owned','symlink_dependency','missing_dependency','existing_quarantine','existing_ledger','wrong_target_owner','wrong_target_mode','wrong_target_inode','different_filesystem','gzip_corrupt','wrong_marker','source_path_traversal']:
 def bad(scenario=scenario):
  m=Model();first=sorted(m.own)[0]
  if scenario=='added_unknown':m.make(TARGET+'/unknown',stat.S_IFREG|0o600,b'new')
  elif scenario=='symlink_owned':m.nodes[TARGET+'/'+first]['mode']=stat.S_IFLNK|0o600
  elif scenario=='symlink_dependency':m.nodes[TARGET+'/debug/deps/dep025' if False else TARGET+'/debug/deps/dep25']['mode']=stat.S_IFLNK|0o600
  elif scenario=='missing_dependency':m.nodes.pop(TARGET+'/debug/deps/dep25')
  elif scenario=='existing_quarantine':m.make(Q,stat.S_IFDIR|0o700)
  elif scenario=='existing_ledger':m.make(RUN+'/quarantine-moves.jsonl',stat.S_IFREG|0o600,b'old-ledger')
  elif scenario=='wrong_target_owner':m.nodes[TARGET]['uid']=1001
  elif scenario=='wrong_target_mode':m.nodes[TARGET]['mode']=stat.S_IFDIR|0o755
  elif scenario=='wrong_target_inode':m.nodes[TARGET]['inode']=524405
  elif scenario=='different_filesystem':m.nodes['/mock-work']['dev']=28
  elif scenario=='gzip_corrupt':m.nodes[m.own[first]['gzip_path']]['data']=b'notgzip'
  elif scenario=='wrong_marker':m.nodes[TARGET+'/CACHEDIR.TAG']['data']=b'changed'
  elif scenario=='source_path_traversal':m.own['../escape']=m.own.pop(first)
  rejects(lambda:m.namespace()['quarantine_own_package_outputs'](m.checked()));assert not m.renames
 control('reject-'+scenario+'-before-any-rename',bad)
for scenario in ['before_first','before_tenth','after_tenth','completed_ledger_failure','existing_future_destination','new_target_path','controlled_interruption']:
 def partial(scenario=scenario):
  m=Model();ns=m.namespace()
  if scenario=='before_first':m.rename_fail_before=0
  elif scenario=='before_tenth':m.rename_fail_before=9
  elif scenario=='after_tenth':m.rename_fail_after=9
  elif scenario=='completed_ledger_failure':m.ledger_fail_event=1
  elif scenario=='existing_future_destination':m.fail_destination=0
  elif scenario=='new_target_path':m.intrude_after=0
  elif scenario=='controlled_interruption':ns['INTERRUPTED']=15
  rejects(lambda:ns['quarantine_own_package_outputs'](m.checked()))
  failure=json.loads(m.nodes[RUN+'/package-quarantine-failure.json']['data'])
  assert failure['automatic_rollback_or_retry'] is False and failure['following_compiler_or_test_launch_permitted'] is False
  original_intact=[]
  for name,row in m.own.items():
   found=[p for p in [TARGET+'/'+name,Q+'/'+name] if p in m.nodes and m.identity(m.nodes[p])=={k:row[k] for k in ['sha256','bytes','full_mode','mtime_ns','inode']}]
   assert len(found)==1;original_intact.append(name)
  assert len(original_intact)==25
  if scenario=='after_tenth':assert len(m.renames)==10 and failure['locations']['completed_rename_count']==9 and failure['locations']['all25_present_exactly_once_and_intact']
  if scenario=='completed_ledger_failure':assert len(m.renames)==1 and failure['locations']['completed_rename_count']==1
  if scenario=='existing_future_destination':assert not failure['locations']['all25_present_exactly_once_and_intact']
 control('partial-'+scenario+'-preserves-originals-location-ledger-no-retry',partial)
assert len(RESULTS)==26,len(RESULTS)
old=ast.parse((BASE/'run-forced-baseline403-standard-marker-v5.py').read_bytes())
inherited=['run','source_guard','selected_origin_guard','materialize_baseline','test_result','seed_retention','verify_platform_daemons','verify_docker_cli','docker_zero_running_workloads','process_reference_guard','retain_cache','verify_complete_cache_map']
for name in inherited:
 a=next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name==name);b=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 assert ast.dump(a,include_attributes=False)==ast.dump(b,include_attributes=False),name
receipt={'schema_version':1,'scope':'26 extracted exact-helper in-memory controls; no actual target/quarantine/Docker/Cargo/compiler/tests','runner_sha256':hashlib.sha256(raw).hexdigest(),'control_source_sha256':hashlib.sha256(RealPath(__file__).read_bytes()).hexdigest(),'passed':True,'synthetic_controls':len(RESULTS),'controls':RESULTS,'inherited_function_ASTs_unchanged':inherited,'actual_cache_operations':0,'actual_Cargo_or_Docker_commands':0}
p=PREP/'quarantine-controls.json';assert not p.exists();p.write_text(json.dumps(receipt,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'passed':True,'controls':len(RESULTS),'receipt_sha256':hashlib.sha256(p.read_bytes()).hexdigest()}))
