from pathlib import Path
import ast,gzip,hashlib,json,os,stat,sys,time
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49')
RUNNER=BASE/'run-baseline403-after-7942.py'
assert hashlib.sha256(RUNNER.read_bytes()).hexdigest()=='8cd64667d1de746abb483ae45c89afcad4ee47b2f6acb8a1273613d95fc3247b'
ORIGIN=Path('/workspace/work/integration/broker-merged-source-403be1e3/receipt.json')
b=ORIGIN.read_bytes();assert hashlib.sha256(b).hexdigest()=='d6e9a3578b31fe76ba8e764303a83cc615437744498e4874b37ef0c8af2d2fff'
j=json.loads(b);assert j['source_commit']=='403be1e3db073df86921d6fb21189f695c4f1eaf'
m=j['source_manifest']; compressed=Path(m['path']).read_bytes();assert hashlib.sha256(compressed).hexdigest()==m['compressed_sha256']
raw=gzip.decompress(compressed);assert hashlib.sha256(raw).hexdigest()==m['uncompressed_sha256'] and len(raw)==m['uncompressed_bytes']
manifest=json.loads(raw);assert len(manifest)==71885
nodes=[n for n in ast.parse(RUNNER.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name in ['files','digest','source_guard']]
ns={'Path':Path,'os':os,'stat':stat,'hashlib':hashlib};exec(compile(ast.Module(body=nodes,type_ignores=[]),str(RUNNER),'exec'),ns)
phase=sys.argv[1];assert phase in ['before','after']
guard=ns['source_guard'](Path(j['source_directory']),manifest)
directory=BASE/'baseline403-full-source-guards-7942';directory.mkdir(mode=0o700,exist_ok=True)
out=directory/(phase+'.json');assert not out.exists()
row={'schema_version':1,'phase':phase,'sampled_at_unix':time.time(),'scope':'entire unchanged original403 tree before/after the separate selective baseline experiment','source_sha':j['source_commit'],'source_origin_receipt_sha256':hashlib.sha256(b).hexdigest(),'source_manifest_compressed_sha256':m['compressed_sha256'],'source_manifest_uncompressed_sha256':m['uncompressed_sha256'],'guard':guard,'helper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
out.write_text(json.dumps(row,indent=2)+'\n');out.chmod(0o600)
if phase=='after':
 previous=json.loads((directory/'before.json').read_bytes());assert previous['guard']==guard
print(json.dumps({'phase':phase,'path':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'guard':guard}))
