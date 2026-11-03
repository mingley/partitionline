import json,os,pathlib,shutil,subprocess,sys,tempfile
D=pathlib.Path('/workspace/work/add-partitions-fatal-v6-20261103');R=pathlib.Path('/workspace/partitionline');env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')};env['PYTHONDONTWRITEBYTECODE']='1'
with tempfile.TemporaryDirectory(prefix='add-partitions-fake-inputs-',dir='/workspace/work') as t:
 fake=pathlib.Path(t)/'checkout';paths=['src/error.rs','src/protocol/txn.rs','docs/evidence/client/candidate-20261030/v16/fixed-inputs.json','docs/evidence/client/candidate-20261030/v17/fixed-inputs.json']
 for name in paths:
  target=fake/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(R/name,target)
 code="import pathlib,runpy,sys; real=pathlib.Path; fake=sys.argv[1]; pathlib.Path=lambda *a,**k: real(fake) if a==('/workspace/partitionline',) and not k else real(*a,**k); runpy.run_path(sys.argv[2],run_name='__main__')"
 def check():return subprocess.run([sys.executable,'-B','-c',code,str(fake),str(D/'verify.py')],env=env,cwd='/',capture_output=True,text=True,timeout=30)
 assert check().returncode==0
 error=fake/'src/error.rs';before=error.read_bytes();error.write_bytes(bytes([before[0]^1])+before[1:]);bad=check();assert bad.returncode!=0 and 'checkout src/error.rs' in bad.stderr;error.write_bytes(before)
 for version in ('v16','v17'):
  path=fake/'docs/evidence/client/candidate-20261030'/version/'fixed-inputs.json';before=path.read_bytes();j=json.loads(before);j['src/error.rs']='0'*64;path.write_text(json.dumps(j));bad=check();assert bad.returncode!=0 and version in bad.stderr;path.write_bytes(before)
 print(json.dumps({'valid_fake_checkout_passed':True,'changed_error_source_rejected':True,'changed_v16_catalog_rejected':True,'changed_v17_catalog_rejected':True,'scope':'exact verifier with fake checkout path; no root edits or Rust execution'}))
