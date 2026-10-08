import pathlib,json,hashlib,gzip,shutil
r=pathlib.Path('/workspace/partitionline');w=pathlib.Path('/workspace/work/open-cards-20261006/matrix-tiers-20261008');q=r/'docs/evidence/perf/matrix-tiers';assert not (q/'archive-manifest.json').exists();rows=[];sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
roots=[('study',w),('before-source',w.parent/'matrix-before-90286')]
for label,root in roots:
 for p in sorted(root.rglob('*')):
  if not p.is_file() or '.git' in p.relative_to(root).parts or '__pycache__' in p.relative_to(root).parts:continue
  rel=p.relative_to(root);compressed=p.stat().st_size>=1048576 or p.read_bytes()[:4]==b'\x7fELF';dest=q/label/rel
  if compressed:dest=dest.with_name(dest.name+'.gz')
  dest.parent.mkdir(parents=True,exist_ok=True);digest=sha(p)
  with p.open('rb') as source,dest.open('xb') as output:
   if compressed:
    with gzip.GzipFile(filename='',mode='wb',fileobj=output,mtime=0,compresslevel=6) as gz:shutil.copyfileobj(source,gz)
   else:shutil.copyfileobj(source,output)
  assert digest==sha(p);rows.append(dict(stored_path=str(dest.relative_to(q)),source_path=str(p),bytes=p.stat().st_size,stored_bytes=dest.stat().st_size,gzip=compressed,sha256=digest,stored_sha256=sha(dest)))
(q/'archive-manifest.json').write_text(json.dumps(dict(scope='Partial KL09-68: actual synthetic tier-mixing failure, bounded real-process guards and real pinned Go driver configuration refusal. No Kafka, client-ceiling campaign or comparative qualification.',files=rows),indent=2)+'\n');shutil.copy2(r/'docs/evidence/conformance/q-conf-updatefeatures/qualification-20261008/verify-evidence.py',q/'verify-evidence.py');shutil.copy2(pathlib.Path(__file__),q/'freeze-evidence.py')
(q/'README.md').write_text('''# Orchestrator tier checks

KL09-68 is in progress. The orchestrator now refuses a null-broker cell in a
Kafka manifest, client-ceiling result labels in Kafka cells, and client-ceiling
artifacts returned by a Kafka peer. Null-broker orchestration remains disabled
until its fixture lifecycle and adapters are qualified.

A real process test reproduced the previous error: two explicitly synthetic
Kafka-shaped results were accepted in a cell declaring a null-broker target and
client-ceiling result kind. The copied process outputs and receipts are under
`study/before-mixing-processes-01/`. They are test fixtures, with no broker or
performance evidence.

Twenty-four process tests pass, including retained failures, mismatched settings,
crash, timeout and resume under the franz-go peer ID. These use fake adapters.
A separately compiled, pinned franz-go 1.22.0 driver actually ran `emit-config`
and `scenarios` on Go 1.26.0. Its config omits queue capacities, delivery/flush/run/
consume timeouts, payload/key modes, connection count and Nagle configuration.
The real output and explicit refusal are retained; missing settings are not
filled from another client or guessed. No native Go comparison is qualified.

The remaining work is to supply truthful, applied driver settings and owned
null-broker lifecycle handling, then rehearse the tier with genuine result
artifacts. Source and executable hashes, build logs and failed preparation
outputs are retained. Verify this archive with `python3 -B verify-evidence.py`
and `sha256sum -c SHA256SUMS` from this directory.
''')
(q/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(q))+'\n' for p in sorted(q.rglob('*')) if p.is_file()));print(len(rows),'mapped files')
