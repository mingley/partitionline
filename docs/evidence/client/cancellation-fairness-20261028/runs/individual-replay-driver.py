from pathlib import Path
import datetime,hashlib,json,re,subprocess,time
p=Path('/workspace/work/goal-qualification-20261028'); source=p/'client-default-matrix-02.log'; binary=Path('/workspace/work/qualification-20261027/broker-check-target/debug/deps/partitionline-b07aa256026bc081'); data=source.read_text(); names=list(dict.fromkeys(re.findall(r'^test ([^\n]+does_not_hold_the_caller) \.\.\. FAILED$',data,re.M)))
root=p/'fairness-failures-individual'; root.mkdir(exist_ok=False)
r={'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'source_log_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'names':names,'test_timeout_seconds':30,'test_threads':1,'results':[],'scope':'Exact old matrix executable on shared x86 host; source may have changed since.'}
for i,name in enumerate(names):
 t=time.monotonic()
 try:
  run=subprocess.run([str(binary),'--exact',name,'--test-threads=1','--nocapture'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=30)
  code=run.returncode; out=run.stdout
 except subprocess.TimeoutExpired as e:
  code=None;out=e.stdout or b''
 (root/f'{i:03d}.log').write_bytes(out); r['results'].append({'name':name,'returncode':code,'elapsed_seconds':time.monotonic()-t,'log':f'{i:03d}.log','sha256':hashlib.sha256(out).hexdigest()});(root/'progress.json').write_text(json.dumps(r,indent=2)+'\n')
 if (i+1)%10==0: print(f'{i+1}/{len(names)}',flush=True)
(root/'result.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps({'cases':len(names),'passes':sum(x['returncode']==0 for x in r['results']),'failures':sum(x['returncode']!=0 for x in r['results'])}),flush=True)
