import gzip,hashlib,json,shutil,subprocess
from pathlib import Path
r=Path('/workspace/partitionline');base=Path('/workspace/work/open-cards-20261006');s=base/'update-features-retries-20261008';q=r/'docs/evidence/conformance/q-conf-updatefeatures/qualification-20261008'
assert json.loads((s/'final-qualification-01.json').read_text())['native_brokers_joined']==6
roots=[('study',s),('before-deadline-source',base/'update-features-retry-source-e2a75b'),('before-deadline-driver',base/'update-features-retry-driver-c858fc'),('before-input-source',base/'update-features-retry-source-0cf6c1'),('before-input-driver',base/'update-features-retry-driver-35cbe4'),('first-fixed-source',base/'update-features-fixed-ec264d'),('before-empty-source',base/'update-features-fixed-268c46'),('before-feature-driver',base/'update-features-peer-575115'),('controller-source',base/'update-features-qualified-9eb1bb'),('verified-source',base/'update-features-verified-2be86c'),('upstream',base/'update-features-20261007/upstream')]
for name,root in roots:
 if name not in ['study','upstream']:assert not subprocess.check_output(['git','status','--porcelain'],cwd=root),root
q.mkdir(exist_ok=False);rows=[];excluded=[]
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for name,root in roots:
 for p in sorted(root.rglob('*')):
  if not p.is_file():continue
  rel=p.relative_to(root)
  if '.git' in rel.parts or '__pycache__' in rel.parts or p.name.endswith(('.index','.paths')):
   excluded.append(dict(path=str(p),reason='Git metadata or inactive generated publication/Python cache'));continue
  with p.open('rb') as f:elf=f.read(4)==b'\x7fELF'
  compressed=p.stat().st_size>=1048576 or elf;out=q/name/rel
  if compressed:out=out.with_name(out.name+'.gz')
  out.parent.mkdir(parents=True,exist_ok=True);digest=sha(p);size=p.stat().st_size
  with p.open('rb') as source,out.open('xb') as sink:
   if compressed:
    with gzip.GzipFile(filename='',mode='wb',fileobj=sink,mtime=0,compresslevel=6) as dest:shutil.copyfileobj(source,dest)
   else:shutil.copyfileobj(source,sink)
  assert sha(p)==digest
  rows.append(dict(root=name,source_path=str(p),stored_path=str(out.relative_to(q)),gzip=compressed,bytes=size,stored_bytes=out.stat().st_size,sha256=digest,stored_sha256=sha(out)))
(q/'archive-manifest.json').write_text(json.dumps(dict(scope='Finite API57 source-case/controller/native qualification and complete retained failed preparations; immutable earlier partial archive is referenced separately.',files=rows,excluded=excluded,external_inputs='Three genuine Kafka distributions, SDK jars and SLF4J remain checksum-pinned external dependencies; native bindings retain every executed distribution input hash.'),indent=2)+'\n')
shutil.copy2(Path(__file__),q/'freeze-evidence.py');shutil.copy2(s/'final-qualification-01.json',q/'qualification.json');shutil.copy2(s/'source-case-applicability-qualified-02.json',q/'applicability.json')
verifier='''import gzip,hashlib,json
from pathlib import Path
q=Path(__file__).resolve().parent;rows=json.loads((q/'archive-manifest.json').read_text())['files']
for x in rows:
 p=q/x['stored_path'];assert p.stat().st_size==x['stored_bytes']
 with p.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==x['stored_sha256'],p
 with (gzip.open(p,'rb') if x['gzip'] else p.open('rb')) as f:assert hashlib.file_digest(f,'sha256').hexdigest()==x['sha256'],p
print(json.dumps(dict(mapped_files=len(rows),stored_bytes=sum(x['stored_bytes'] for x in rows),original_bytes=sum(x['bytes'] for x in rows),stored_and_decoded_bytes_verified=True)))
'''
(q/'verify-evidence.py').write_text(verifier)
result=subprocess.run(['python3','-B',str(q/'verify-evidence.py')],check=True,capture_output=True,text=True);(q/'archive-verification.json').write_text(result.stdout)
(q/'README.md').write_text('''# UpdateFeatures qualification

The declared API57 v0–v2 cohort passes on default and all-feature builds with
Apache Kafka SDKs and brokers 4.1.2, 4.2.1 and 4.3.1. The checks cover 48
parameter invocations from 36 source methods, 606 independently parsed Rust
bodies, 84 native public caller cases, 84 bounded SDK TCP peers, 72 source-case
bodies and 60 malformed bodies. All owned peers joined and their ports rebound.

Five additional compatibility fixes follow the earlier v0 validation-only fix:
one operation deadline with decreasing wire budgets, Java feature-name trim
rules, rejection of null nonnullable fields, success rows for empty successful
responses, and preservation of per-feature controller errors. Actual old-source
failures and every failed preparation remain under `study/`.

Current Java public Admin fails before dispatch when only v0 is available. Rust
supports the v0 AllowDowngrade representation; each actual SDK independently
parses its public request and constructs its response. `applicability.json`
records this projection and all source-method mappings. No Java public v0
success is claimed.

Controller checks compiled source `9eb1bbba82bc997d314deadfd4bd48fa76ea4a09`.
Final checks and native runs compiled `2be86c09ba171dcef41e5dd50a24d2aef7aaf3c0`.
Only fixture I/O changed between them. Product code and existing caller tests are
byte-identical; `study/core-and-caller-source-identity-01.json` records the check.
Both final builds pass selected tests, formatting and strict Clippy on stable
Rust 1.99.0. These finite checks do not establish production readiness or speed.

The [earlier partial archive](../qualification-20261007/README.md) is unchanged.
`archive-manifest.json` maps every retained file to its original and stored hashes.
Large files and executables use deterministic gzip. Kafka distributions and SDK
jars are external checksum-pinned inputs. Verify the mapping with
`python3 -B verify-evidence.py`, then run `sha256sum -c SHA256SUMS` here.
''')
paths=sorted(p for p in q.rglob('*') if p.is_file());(q/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(q))+'\n' for p in paths))
print(result.stdout.strip(),flush=True)
