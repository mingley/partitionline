import pathlib,subprocess,hashlib,json,os,importlib.util
w=pathlib.Path(__file__).parent;r=pathlib.Path('/workspace/partitionline');source=w.parent/'matrix-before-90286';exe=w/'franz-peer.elf';sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();rows=[]
for name in ['emit-config','scenarios']:
 cmd=[str(exe),name]
 with (w/(name+'.stdout')).open('wb') as stdout,(w/(name+'.stderr')).open('wb') as stderr:
  result=subprocess.run(cmd,env=dict(os.environ,KAFKA_BOOTSTRAP='127.0.0.1:19092',KAFKA_TOPIC='matrix-config-only',COUNT='512',WARMUP='0',PARTITIONS='1',PAYLOAD_BYTES='100',ACKS='all',IDEMPOTENT='false',COMPRESSION='none'),stdout=stdout,stderr=stderr,timeout=3)
 rows.append(dict(command=cmd,exit=result.returncode,joined=True));result.check_returncode()
spec=importlib.util.spec_from_file_location('matrix',r/'scripts/run-benchmark-matrix.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);cfg=json.load(open(w/'emit-config.stdout'))
try:m.settings(cfg);raise AssertionError('incomplete driver configuration was accepted')
except ValueError as exc:refusal=str(exc)
pins={str(p.relative_to(source)):sha(p) for p in (source/'benchmarks/peers/franz-go').rglob('*') if p.is_file()};assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
with (w/'franz-build-info.log').open('wb') as log:subprocess.run(['/workspace/work/open-cards-20261006/nullbroker-peers-20261008/go-preparation-01/go/bin/go','version','-m',str(exe)],stdout=log,check=True)
(w/'franz-config-refusal-01.json').write_text(json.dumps(dict(scope='Actual pinned SDK driver config only; no broker or performance measurement.',source_commit='90286f76673616f02abccec53b9cf52501673319',source_sha256=pins,binary_sha256=sha(exe),process_receipts=rows,emitted_settings=cfg,missing_settings=[k for k in m.MATCH if k not in cfg],refusal=refusal,comparability_qualified=False),indent=2)+'\n');print(refusal)
