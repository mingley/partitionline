import hashlib,json,os,pathlib,shutil,subprocess,sys,tarfile,tempfile
D=pathlib.Path(sys.argv[1]);R=pathlib.Path('/workspace/partitionline');sha=lambda b:hashlib.sha256(b).hexdigest();digest=lambda p:sha(p.read_bytes());m=json.loads((D/'manifest.json').read_text());paths=['src/producer.rs','src/protocol/txn.rs','src/producer/prepare_tests.rs']
assert digest(D/'candidate.patch')==m['patch_sha256'] and digest(D/'README.md')==m['readme_sha256'] and digest(D/'verify.py')==m['verifier_sha256']
for name in paths:
 for stage in ('before','after'):assert digest(D/stage/name)==m[stage][name]
for version in ('v16','v17'):
 catalog=json.loads((R/'docs/evidence/client/candidate-20261030'/version/'fixed-inputs.json').read_text())
 for name in paths:assert digest(D/'before'/name)==catalog[name]
 assert digest(R/'src/error.rs')==catalog['src/error.rs']
commit=m['main_source_commit']; git=lambda path:subprocess.run(['git','-C',str(R),'show',commit+':'+path],stdout=subprocess.PIPE,check=True).stdout
assert sha(git('src/producer.rs'))==m['main_producer_sha256']; assert git('src/protocol/txn.rs')==(D/'before/src/protocol/txn.rs').read_bytes();assert sha(git('src/error.rs'))==digest(R/'src/error.rs')
assert (D/'after/src/producer/prepare_tests.rs').read_bytes().startswith((D/'before/src/producer/prepare_tests.rs').read_bytes())
required={'README.md','candidate.patch','manifest.json','verify.py'}|{stage+'/'+p for stage in ('before','after') for p in paths}
with tarfile.open(D/'evidence.tar.gz','r:gz') as archive:
 members=archive.getmembers();files=[x for x in members if x.isfile()];assert len(files)==len(required)==len({x.name for x in files});assert set(x.name for x in files)==required;assert all(x.isfile() or x.isdir() for x in members)
 for item in files:assert archive.extractfile(item).read()==(D/item.name).read_bytes(),item.name
 with tempfile.TemporaryDirectory(prefix='add-partitions-evidence-check-',dir='/workspace/work') as temp:
  root=pathlib.Path(temp)
  for item in files:
   dst=root/item.name;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(archive.extractfile(item).read())
  env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')};env['PYTHONDONTWRITEBYTECODE']='1'
  check=subprocess.run([sys.executable,'-B',str(root/'verify.py')],cwd='/',env=env,check=True,capture_output=True,text=True,timeout=60)
  assert 'PASS: v16/v17 pins' in check.stdout
v3=pathlib.Path('/workspace/work/add-partitions-fatal-v3-20261103');assert (D/'candidate.patch').read_bytes()==(v3/'candidate.patch').read_bytes()
for stage in ('before','after'):
 for path in paths:assert (D/stage/path).read_bytes()==(v3/stage/path).read_bytes()
old=(D/'before/src/producer/prepare_tests.rs').read_text();new=(D/'after/src/producer/prepare_tests.rs').read_text();added=new[len(old):]
assert all(added.count('async fn '+name+'(')==1 for name in m['added_tests']) and len(m['added_tests'])==4
reducer=(D/'after/src/producer.rs').read_text();start=reducer.index('async fn decode_add_partitions_producer_error');end=reducer.index('async fn txn_roundtrip',start);body=reducer[start:end]
assert body.index('decode_add_partitions_to_txn_topics_response_cooperative')<body.index('if !body.is_empty()')<body.index('on_fatal(code)')<body.index('tokio::task::yield_now().await')
assert 'decode_add_partitions_and_record_fatal' in reducer and reducer.count('decode_add_partitions_and_record_fatal(')>=3
# An unvalidated count window remains: an 8,192-item prefix can yield before a later fence.
assert 'first_fatal == 0 && add_partitions_error_is_fatal' in body and 'if walked == 8 * 1024' in body
print(json.dumps({'archive_sha256':digest(D/'evidence.tar.gz'),'manifest_sha256':digest(D/'manifest.json'),'patch_sha256':digest(D/'candidate.patch'),'archive_file_members':len(required),'catalogs_and_exact_archived_sparse_sources':'pass','standalone_archive_verifier':'pass within documented workspace dependencies','source_identical_to_v3':True,'four_appended_tests_source_only':True,'fatal_callback_before_scan_yield':'after the reducer observes fatal; later fatal may remain unobserved at an earlier yield','error_source_sha256':digest(R/'src/error.rs'),'qualification':'not performed'},indent=2))
