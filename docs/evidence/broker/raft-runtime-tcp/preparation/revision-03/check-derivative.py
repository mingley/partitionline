"""Offline source/semantic controls only; never launches Rust, peers or Docker."""
import ast,hashlib,importlib.util,itertools,json,os,pathlib,re,sys,tempfile
from unittest.mock import patch
os.umask(0o077);os.sched_setaffinity(0,{2,4});sys.dont_write_bytecode=True
R=pathlib.Path(__file__).parent;B=R.with_name('tcp-qualification-02')
def digest(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
p=B/'handoff.json';assert digest(p)=='e4250889d4cd2e68818a21b19e6a68a8caeb7b516ad54643a24b34880a54591b';old=json.loads(p.read_bytes())
def verify_old():
 for row in old['rows']:
  p=pathlib.Path(row['source']);assert digest(p)==row['sha256'] and p.stat().st_size==row['bytes'] and p.stat().st_mode&0o7777==row['full07777']
verify_old();s=(R/'candidate/partitionline-broker/src/raft/runtime.rs').read_text();before=(B/'candidate/partitionline-broker/src/raft/runtime.rs').read_text()
def order(text):return re.findall(r'^    (_\w+):',re.search(r'struct TestPermit \{(.*?)^\}',text,re.M|re.S).group(1),re.M)
assert order(before)==['_permit','_counter'];assert order(s)==['_counter','_permit']
# Explicit Rust declaration-order model, not an observed Tokio scheduling run.
# The new owner can acquire only after returned capacity; publication follows
# that acquire. Enumerate every legal ordering of the two old/new drop/publish
# steps, so the fixed claim does not depend on sampling a favorable schedule.
model=[]
for label,fields in [('old',order(before)),('candidate',order(s))]:
 for amount in [1,4096]:
  legal=[]
  for events in itertools.permutations(['old-counter-drop','old-permit-return','new-permit-acquire','new-counter-publish']):
   i={x:events.index(x) for x in events}
   if not (i['new-permit-acquire']<i['new-counter-publish']):continue
   expected=(i['old-permit-return']<i['old-counter-drop']) if fields[0]=='_permit' else (i['old-counter-drop']<i['old-permit-return'])
   if not expected:continue
   current=4*amount;peak=current;occupied=4;timeline=[];valid=True
   for event in events:
    if event=='old-counter-drop':current-=amount
    elif event=='old-permit-return':occupied-=1
    elif event=='new-permit-acquire':
     if occupied>=4:valid=False;break
     occupied+=1
    elif event=='new-counter-publish':current+=amount;peak=max(peak,current)
    assert occupied<=4
    timeline.append({'event':event,'genuine_occupied_permits':occupied,'published_current':current,'published_peak':peak})
   if valid:legal.append({'events':list(events),'timeline':timeline,'peak':peak})
  assert legal
  if label=='old':assert max(x['peak'] for x in legal)==5*amount
  else:assert max(x['peak'] for x in legal)==4*amount
  model.append({'source':label,'gauge_amount':amount,'legal_schedules':legal,'max_peak':max(x['peak'] for x in legal),'bound':4*amount})
# The literal scanner uses only Envelope's field-only blocks. Definition and
# destructuring are deliberately excluded; no string bodies occur in them.
def literal_controls(text):
 result=[]
 for m in re.finditer(r'Envelope \{',text):
  start=m.end();depth=1;i=start
  while depth:
   c=text[i];depth+=(c=='{')-(c=='}');i+=1
  block=text[start:i-1];prefix=text[max(0,m.start()-25):m.start()]
  if re.search(r'(struct|let)\s+$',prefix):continue
  if not re.search(r'^\s*command(?::|,)',block,re.M):continue
  result.append({'line':text[:m.start()].count('\n')+1,'counter_present':'_counter:' in block})
 return result
original_literals=literal_controls(before);new_literals=literal_controls(s)
assert len(original_literals)==len(new_literals)==5
assert sum(not r['counter_present'] for r in original_literals)==1
assert all(r['counter_present'] for r in new_literals)
assert s.count('self.assert_joined()?;')==1 and s.count('source.assert_joined()?;')==1
assert '.commands.max_capacity()' in s and '"direct blocking owner without network workers or supervisor"' not in s # Rust string escaped
assert '\\"supervisor_joined\\":null' in s and '\\"listener_closed\\":null' in s
# Post-command preservation must lexically finish before harness parsing and
# before source/hash qualification that may refuse. This is a source control,
# not an actual cache-write/clean test.
def runner_order(text):
 at=text.index('receipt[\'commands\'].append(item);save()');tail=text[at:];return {k:tail.index(v) for k,v in {'elf':'item[\'post_command_elfs\']=retain_cache','whole':'retained=preserve_whole_cache','raw':'item[\'captures\']=','tmp':'item[\'temporary_files\']=','parse':'item[\'executed_elfs\']=executed_elfs'}.items()}
bad=runner_order((B/'run-next-focused.py').read_text());good=runner_order((R/'run-next-focused.py').read_text());assert bad['parse']<bad['elf'];assert all(good[k]<good['parse'] for k in ('elf','whole','raw','tmp'))
for p in R.glob('*.py'):ast.parse(p.read_text(),str(p))
spec=importlib.util.spec_from_file_location('local_process_guards',R/'process_guards.py');pg=importlib.util.module_from_spec(spec);spec.loader.exec_module(pg)
process=[]
# All process controls use invented local fixture files. No real /proc scan,
# Docker query, cache-object read, cleanup or child launch occurs here.
with tempfile.TemporaryDirectory(prefix='process-control-',dir=R) as d:
 root=pathlib.Path(d);proc=root/'proc';proc.mkdir();target=root/'cache';target.mkdir();p=proc/'313';p.mkdir();(p/'fd').mkdir()
 def restore(state='S'):
  for q in (p/'cwd',p/'exe'):
   if q.is_symlink():q.unlink()
  for q in (p/'fd').iterdir():q.unlink()
  fields=[state]+['0']*19;fields[1]='1';fields[19]='777';(p/'stat').write_text('313 (unit) '+' '.join(fields)+'\n');(p/'status').write_text('Name:\tunit\nState:\t'+state+'\nPPid:\t1\nUid:\t1000 1000 1000 1000\n')
  (p/'cmdline').write_bytes(b'unit\0');(p/'environ').write_bytes(b'X=x\0');(p/'maps').write_text('0-1 r-x 0 0 0 /other\n');(p/'cwd').symlink_to(root);(p/'exe').symlink_to('/other/unit')
 def run(name,expected):
  report=pg.cache_references(proc,target,-1);assert expected(report),report;process.append({'name':name,'owners':len(report['owners']),'faults':len(report['live_inspection_faults']),'zombies':report['zombies_skipped']})
 restore();run('readable-no-reference',lambda r:not r['owners'] and not r['live_inspection_faults'])
 for field in ('cwd','exe','cmdline','environ','maps','fd'):
  restore()
  if field in ('cwd','exe'):(p/field).unlink();(p/field).symlink_to(target/'object')
  elif field=='fd':(p/'fd'/'8').symlink_to(target/'object')
  else:(p/field).write_bytes(str(target/'object').encode())
  run('actual-reference-'+field,lambda r:len(r['owners'])==1 and not r['live_inspection_faults'])
 original_read=pg.limited_read
 for field in ('stat','status','cmdline','environ','maps'):
  restore()
  def denied(q,limit,field=field):
   if pathlib.Path(q)==p/field:raise PermissionError('synthetic live unreadability')
   return original_read(q,limit)
  with patch.object(pg,'limited_read',denied):run('unreadable-live-'+field,lambda r:bool(r['live_inspection_faults']))
 restore();original_link=os.readlink
 def denied_link(q):
  if pathlib.Path(q)==p/'fd'/'8':raise PermissionError('synthetic live FD unreadability')
  return original_link(q)
 (p/'fd'/'8').symlink_to('/other')
 with patch.object(pg.os,'readlink',denied_link):run('unreadable-live-FD',lambda r:bool(r['live_inspection_faults']))
 restore();(p/'cmdline').unlink();run('missing-field-still-live',lambda r:bool(r['live_inspection_faults']))
 restore('Z');(p/'cmdline').unlink();run('zombie-skip',lambda r:r['zombies_skipped']==1 and not r['live_inspection_faults'])
 restore();original_identity=pg.process_identity;calls=0
 def replaced(q):
  global calls
  calls+=1;row=original_identity(q)
  if calls>1:row['starttime_ticks']+=1
  return row
 with patch.object(pg,'process_identity',replaced):run('PID-replacement-refusal',lambda r:bool(r['live_inspection_faults']))
 restore()
 def oversized(q,limit):
  if pathlib.Path(q)==p/'cmdline':raise ValueError('synthetic inspection bound exceeded')
  return original_read(q,limit)
 with patch.object(pg,'limited_read',oversized):run('bounded-read-refusal',lambda r:bool(r['live_inspection_faults']))
verify_old()
receipt={'schema_version':1,'scope':'offline source/semantic and invented-process controls; no Rust/Docker/runtime/cache operation','parent_handoff_sha256':digest(B/'handoff.json'),'all20_prior_files_bytes_lengths_full07777_unchanged':True,'runtime_source_sha256':digest(R/'candidate/partitionline-broker/src/raft/runtime.rs'),'semantic_drop_models':model,'Envelope_original_literals':original_literals,'Envelope_candidate_literals':new_literals,'runner_original_order':bad,'runner_candidate_order':good,'process_controls':process,'actual_rust_tests_executed':0,'actual_process_cache_inspections':0,'actual_cleanup_operations':0,'limitations':['Drop interleavings are a declaration-order semantic model, not observed Rust or Tokio scheduling.','New cfg(test) Rust paths remain uncompiled pending root source checkpoint and lease.','Process controls use fixture process files and synthetic read errors; optional exact daemon/Docker branch remains source-only and requires root review and actual future joined queries.']}
p=R/'derivative-controls.json';assert not p.exists();p.write_text(json.dumps(receipt,indent=2)+'\n');print('PASS offline controls:',len(model),'drop-model rows;',len(process),'fixture-process cases; no Cargo/runtime/cache/Docker.')
