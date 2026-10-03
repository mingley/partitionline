from pathlib import Path
import datetime,hashlib,json,stat,subprocess,sys
receipt_path,source_path,output_path,phase=sys.argv[1:]
receipt_file=Path(receipt_path);receipt=json.loads(receipt_file.read_text());source=Path(source_path)
mode_counts={}
for name,row in receipt['files'].items():
    path=source/name;mode=row['mode'];info=path.lstat()
    assert stat.S_ISLNK(info.st_mode)==(mode=='120000'),name
    if mode!='120000':assert stat.S_ISREG(info.st_mode) and bool(info.st_mode & 0o111)==(mode=='100755'),name
    data=path.readlink().as_posix().encode() if mode=='120000' else path.read_bytes()
    assert hashlib.sha256(data).hexdigest()==row['sha256'],name
    assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==row['git_blob'],name
    mode_counts[mode]=mode_counts.get(mode,0)+1
repo=Path('/workspace/partitionline')
frozen=json.loads((source/'docs/evidence/client/KL05-15/msrv-lint-correction/source-freeze-msrv-lint-correction.json').read_text())
base_expected={**frozen['source_sha256'],**frozen['unchanged_held_source_sha256']}
additional_freeze_candidates=[source/'docs/evidence/client/KL05-15/share-heartbeat-scheduling-correction/source-freeze-share-heartbeat-scheduling-correction.json',source/'docs/evidence/client/KL05-15/heartbeat-scheduling-correction/source-freeze-heartbeat-scheduling-correction.json']
additional_freeze=next((path for path in additional_freeze_candidates if path.exists()),None)
additional_expected=json.loads(additional_freeze.read_text())['source_sha256'] if additional_freeze is not None else {}
oracle_freeze=source/'docs/evidence/client/KL05-14/msrv-oracle-lint-correction/source-freeze-msrv-oracle-lint-correction.json'
if oracle_freeze.exists():additional_expected.update(json.loads(oracle_freeze.read_text())['source_sha256'])
expected={**base_expected,**additional_expected}
for name,digest in expected.items():assert hashlib.sha256((source/name).read_bytes()).hexdigest()==digest,name
Path(output_path).write_text(json.dumps({'schema_version':1,'phase':phase,'source_sha':receipt['source_sha'],'source_path':str(source),'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'verified_git_blobs_and_modes':len(receipt['files']),'git_modes':mode_counts,'archive_receipt_sha256':hashlib.sha256(receipt_file.read_bytes()).hexdigest(),'ten_source_sha256':base_expected,'additional_frozen_test_sha256':additional_expected,'all_frozen_source_sha256':expected,'worktree_git_snapshot':{'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'remote_main':subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0],'status_porcelain':subprocess.check_output(['git','status','--porcelain=v1'],cwd=repo,text=True).splitlines()}},indent=2)+'\n')
print(f"PASS: all {len(receipt['files'])} frozen blobs/modes and {len(expected)} frozen hashes unchanged at {phase}")
