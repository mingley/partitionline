import pathlib,json,gzip,shutil,hashlib,subprocess
r=pathlib.Path('/workspace/partitionline');w=pathlib.Path('/workspace/work/open-cards-20261006/nullbroker-peers-20261008');q=r/'docs/evidence/perf/nullbroker-peers';assert json.load(open(w/'qualification-01.json'))['joined_qualification_children']==6
roots=[('study',w),('before-source',w.parent/'nullbroker-peers-before-18e12b'),('qualified-source',w.parent/'nullbroker-peers-final')]
for name,root in roots[1:]:assert not subprocess.check_output(['git','status','--porcelain'],cwd=root)
q.mkdir(exist_ok=False);rows=[];excluded=[]
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,label,rel):
 original=sha(p);size=p.stat().st_size;compressed=size>=1048576 or p.read_bytes()[:4]==b'\x7fELF';out=q/label/rel
 if compressed:out=out.with_name(out.name+'.gz')
 out.parent.mkdir(parents=True,exist_ok=True)
 with p.open('rb') as inp,out.open('xb') as sink:
  if compressed:
   with gzip.GzipFile(filename='',fileobj=sink,mode='wb',mtime=0,compresslevel=6) as gz:shutil.copyfileobj(inp,gz)
  else:shutil.copyfileobj(inp,sink)
 assert sha(p)==original
 rows.append(dict(root=label,source_path=str(p),stored_path=str(out.relative_to(q)),gzip=compressed,bytes=size,stored_bytes=out.stat().st_size,sha256=original,stored_sha256=sha(out)))
for name,root in roots:
 for p in sorted(root.rglob('*')):
  if not p.is_file():continue
  rel=p.relative_to(root)
  if '.git' in rel.parts or '__pycache__' in rel.parts or p.name.endswith(('.index','.paths')) or (name=='study' and (rel.parts[0]=='go-cache' or str(rel).startswith('go-preparation-01/go/') or p.name.endswith('.tar.gz'))):
   excluded.append(dict(path=str(p),reason='Git/cache/compiler distribution; toolchain URL, official digest, metadata and execution version are retained.'));continue
  save(p,name,rel)
external=[pathlib.Path('/workspace/work/open-cards-20261006/init-v6-peers/kafka-clients-4.3.1.jar'),pathlib.Path('/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar'),pathlib.Path('/workspace/work/open-cards-20261006/zstd-c-peer/lib/librdkafka.so.1')]
for p in external:save(p,'sdks',pathlib.Path(p.name))
croot=pathlib.Path('/workspace/work/open-cards-20261006/zstd-c-peer')
for n in ['build-manifest.json','source/LICENSE','source/src/rdkafka.h','source/src/rdkafka_request.c','source/src/rdkafka_fetcher.c','source/src/rdkafka_feature.c']:
 p=croot/n
 if p.is_file():save(p,'librdkafka-source',pathlib.Path(n))
modules=w/'go-cache/modules'
for module in ['github.com/twmb/franz-go@v1.22.0','github.com/twmb/franz-go/pkg/kmsg@v1.14.0','github.com/klauspost/compress@v1.20.0','github.com/pierrec/lz4/v4@v4.1.30']:
 for p in sorted((modules/module).rglob('*')):
  if p.is_file():save(p,'go-sdk-source',p.relative_to(modules))
for p in sorted((r/'docs/evidence/broker').rglob('*.json')):
 if '/4.3.1/' in str(p) and p.name in ['ProduceRequest.json','ProduceResponse.json','FetchRequest.json','FetchResponse.json','MetadataRequest.json','MetadataResponse.json','ListOffsetsRequest.json','ListOffsetsResponse.json']:save(p,'apache-schema',pathlib.Path(p.name))
for n in ['FindCoordinatorRequest.json','FindCoordinatorResponse.json']:
 p=r/'docs/evidence/client/legacy-discovery-v0/qualification/validation/schemas/4.3.1'/n;save(p,'apache-schema',pathlib.Path(n))
(q/'archive-manifest.json').write_text(json.dumps(dict(scope='KL09-66 finite genuine SDK compatibility, retained failures and exact source/input bindings.',files=rows,excluded=excluded),indent=2)+'\n');shutil.copy2(w/'qualification-01.json',q/'qualification.json');shutil.copy2(pathlib.Path(__file__),q/'freeze-evidence.py');shutil.copy2(r/'docs/evidence/conformance/q-conf-updatefeatures/qualification-20261008/verify-evidence.py',q/'verify-evidence.py')
result=subprocess.run(['python3','-B',str(q/'verify-evidence.py')],capture_output=True,text=True,check=True);(q/'archive-verification.json').write_text(result.stdout)
(q/'README.md').write_text('''# Null-broker peer compatibility

Kafka Java clients 4.3.1, librdkafka 2.15.0 and franz-go 1.22.0 each acknowledge
512 records and validate 512 Fetch records with zero validation failures.
All six qualification processes joined, their groups emptied, and all three
listener ports rebound. The broker checks CRC, record counts and batch structure;
each SDK checks every fetched key, value, timestamp, header count and offset.

| Peer | Produce | Fetch | Metadata | Other observed requests |
|---|---:|---:|---:|---|
| Java 4.3.1 | 12 | 17 | 13 | ApiVersions 4 |
| librdkafka 2.15.0 | 10 | 16 | 13 | FindCoordinator 2; ApiVersions 3 |
| franz-go 1.22.0 | 12 | 17 | 13 | ApiVersions 5 rejected with v0 error 35; retries 4 |

The fixture serves a seeded Fetch stream independently of accepted Produce
batches. Produced keys and values match that stream; Produce uses current-time
SDK timestamps and Fetch uses its synthetic batch clock. These checks establish
protocol compatibility for one partition, uncompressed 100-byte values, zero
headers and non-idempotent Produce. They do not establish persisted readback,
consumer-group support, transaction durability or comparative speed.

The original pinned C client acknowledged no records because Produce v10 was
advertised but unhandled. The broker now advertises implemented ranges, handles
classic coordinator discovery and provides the standard ApiVersions downgrade
response. Socket rejection also shuts down the descriptor immediately. Optional
API-version tracing is explicit and disabled by default.

Source `42edda4ec0a903f4595fad42ebe89c8cfe488a71` supplies the qualified peers and
broker. Thirty Rust tests, formatting and strict Clippy pass on stable Rust 1.99.0.
Their Rust inputs are byte-identical to checked source
`68278185d202d9bf309f0df6f021b4d2a4b429de`; only peer timestamp settings and README
changed afterward. The actual source and binary comparison is retained.

`qualification.json` records limits and outcomes. `study/` retains the actual
old-source failures, intermediate failures, compiler logs, executable bindings,
SDK logs and raw artifacts. SDK binaries, Go module sources and Apache schemas
are retained. The official Go compiler archive remains an external input pinned
by its downloaded metadata and digest. `archive-manifest.json` maps stored and
original hashes. Run `python3 -B verify-evidence.py` and `sha256sum -c SHA256SUMS`
from this directory to verify the archive.
''')
(q/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(q))+'\n' for p in sorted(q.rglob('*')) if p.is_file()))
print(result.stdout.strip())
