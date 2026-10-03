import hashlib,json,os,signal,subprocess,time
from pathlib import Path
O=Path('/workspace/work/integration/client-capabilities-source-04f6bc29-supervision')
D=Path('/workspace/work/raft-runtime-76/materializer-exact-audit-preparation-01/materialize-from-baseline-exact-audit.py')
assert hashlib.sha256(D.read_bytes()).hexdigest()=='88ec429775f08895affc40f918d85d4043286bb8cd1a78c591259e9f3961a589'
args=['taskset','-c','0,1','python3',str(D),'--source','04f6bc2968c1d721c6815a6389897a62e4ca76f1','--destination','/workspace/work/client-capabilities-source-04f6bc29','--receipt-directory','/workspace/work/integration/client-capabilities-source-04f6bc29','--baseline-commit','7942fbac784b35220ae02f6983e6454399f42ba5','--baseline-root','/workspace/work/client-capabilities-source-7942fbac','--baseline-origin','/workspace/work/integration/client-capabilities-source-7942fbac/receipt.json','--baseline-origin-sha256','6b32de0a48931c1a7fcb85ae15165661f9303f501e5936c17c36cb1c570f4007']
def free():
 s=os.statvfs(O);return s.f_bavail*s.f_frsize
assert free()>=854958080
start=time.monotonic();minimum=free(); cutoff=None
with (O/'stdout.log').open('wb') as out,(O/'stderr.log').open('wb') as err:
 p=subprocess.Popen(args,stdout=out,stderr=err,start_new_session=True)
 while p.poll() is None:
  minimum=min(minimum,free())
  if minimum<350*1024*1024 or time.monotonic()-start>600:
   cutoff='disk' if minimum<350*1024*1024 else 'timeout';os.killpg(p.pid,signal.SIGTERM);time.sleep(.2)
   try:os.killpg(p.pid,signal.SIGKILL)
   except ProcessLookupError:pass
   break
  time.sleep(.2)
 exit=p.wait()
 try:os.killpg(p.pid,0);joined=False
 except ProcessLookupError:joined=True
 r=dict(args=args,exit=exit,cutoff=cutoff,elapsed=time.monotonic()-start,minimum_free=minimum,free_after=free(),group_joined=joined,passed=exit==0 and cutoff is None and joined)
 (O/'validation.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r));assert r['passed']
