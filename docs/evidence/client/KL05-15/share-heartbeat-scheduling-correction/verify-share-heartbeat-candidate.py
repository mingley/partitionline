from pathlib import Path
import datetime,hashlib,json,stat,sys
w=Path('/workspace/work/client-share-assessment');r=json.loads((w/'final-0057972d/source-verification.json').read_text());v=json.loads((w/'share-heartbeat-sync-correction-prep/candidate-variants.json').read_text());source=w/'preparation-share-heartbeat-sync-0057972d';output,phase=sys.argv[1:];count=0
for name,row in r['files'].items():
    p=source/name;info=p.lstat();mode=row['mode'];assert stat.S_ISLNK(info.st_mode)==(mode=='120000'),name
    if mode!='120000':assert stat.S_ISREG(info.st_mode) and bool(info.st_mode&0o111)==(mode=='100755'),name
    data=p.readlink().as_posix().encode() if mode=='120000' else p.read_bytes();sha256=hashlib.sha256(data).hexdigest()
    assert sha256==v['candidate_source_sha256'].get(name,row['sha256']),name
    if name not in v['candidate_source_sha256']:assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==row['git_blob'],name
    count+=1
frozen=json.loads((source/'docs/evidence/client/KL05-15/msrv-lint-correction/source-freeze-msrv-lint-correction.json').read_text());expected={**frozen['source_sha256'],**frozen['unchanged_held_source_sha256']}
for name,digest in expected.items():assert hashlib.sha256((source/name).read_bytes()).hexdigest()==digest,name
Path(output).write_text(json.dumps({'schema_version':1,'phase':phase,'base_source_sha':r['source_sha'],'verified_source_files_and_modes':count,'candidate_source_sha256':v['candidate_source_sha256'],'unchanged_ten_source_sha256':expected,'root_clippy_toml_sha256':hashlib.sha256((source/'clippy.toml').read_bytes()).hexdigest(),'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n');print(f'PASS: {count} candidate files/modes with one declared test variant; ten production/held source hashes unchanged')
