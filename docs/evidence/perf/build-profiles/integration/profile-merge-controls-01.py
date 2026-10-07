import ctypes,hashlib,importlib.util,json,os,signal
from pathlib import Path
s=Path('/workspace/work/open-cards-20261006/build-profiles-20261007');q=Path('/workspace/partitionline/docs/evidence/perf/build-profiles/integration/strict-merge-01');q.mkdir(exist_ok=False)
source=Path('/workspace/work/open-cards-20261006/build-profiles-source-999133');spec=importlib.util.spec_from_file_location('strict_merge_owner',source/'scripts/run-benchmark-matrix.py');owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0):raise OSError('subreaper unavailable')
def interrupt(signum,frame):raise InterruptedError('strict merge owner interrupted')
for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,interrupt)
def sha(path):
 with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
manifest=json.loads((s/'merge-profiles-01/merge-manifest.json').read_text());raws=[Path(x['path']) for x in manifest['raws']]
prof=Path('/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata')
pins={**manifest['inputs'],manifest['merged']['path']:manifest['merged']['sha256']}
helper=source/'benchmarks/runtime/tools/parent-bound-exec.py'
def run(label,mode,files,code):
 assert all(sha(p)==digest for p,digest in pins.items())
 out=q/(label+'.profdata')
 try:owner.execute(['python3','-B',str(helper),str(os.getpid()),str(prof),'merge','--failure-mode='+mode,'-o',str(out),*map(str,files)],owner.base_env(),q,label,30)
 except ValueError:
  if code==0:raise
 assert all(sha(p)==digest for p,digest in pins.items())
 receipt=json.loads((q/(label+'.process.json')).read_text());assert receipt['parent_waited'] and receipt['exit_code']==code and not receipt.get('failure')
 return out,receipt
valid,first=run('strict-valid','any',raws,0);assert sha(valid)==manifest['merged']['sha256']
bad=q/'captured-profile-truncated.profraw';bad.write_bytes(raws[0].read_bytes()[:16])
previous,second=run('previous-all-corrupt','all',raws+[bad],0)
strict,third=run('strict-any-corrupt','any',raws+[bad],1)
with (q/'summary.json').open('x') as f:json.dump({'strict_valid_merge_byte_identical_to_compiler_input':True,'merged_sha256':sha(valid),'previous_mode_failed_to_reject_corrupt_input':True,'strict_mode_rejected_corrupt_input':True,'processes':[first,second,third],'raw_input_count':len(raws),'scope':'Actual captured profile truncation; no compile, training or timing repeated.'},f,indent=2);f.write('\n')
print('Strict valid merge matches compiler input; prior all-mode accepted a truncated input; any-mode rejected it')
