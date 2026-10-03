import hashlib
import json
from pathlib import Path
import shutil
import stat
import zipfile

R=Path('/workspace/partitionline')
W=Path(__file__).parent
S=W/'stage'
ZIP=Path('/workspace/attachments/1cdf1e86-1b06-407e-9c4b-b95907858b12/fetch-session-recovery-e8fc0b72.zip')
P='docs/evidence/conformance/fetch-recovery-ci-e8fc-01'
FIX='tests/fixtures/fetch-session-recovery/e8fc0b72'

def write(path,data,mode=0o600):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    assert not path.exists()
    path.write_bytes(data);path.chmod(mode)

assert hashlib.sha256(ZIP.read_bytes()).hexdigest()=='ce20dc2c38ed7635ed503c93055f750104b085927fd5c3a71f4428b088cfbc39'
rows=[]
with zipfile.ZipFile(ZIP) as z:
    assert len(z.infolist())==71 and sum(i.file_size for i in z.infolist())==348921
    for i in z.infolist():
        relative=Path(i.filename)
        assert not relative.is_absolute() and '..' not in relative.parts and not i.is_dir()
        data=z.read(i);assert len(data)==i.file_size
        dest=S/P/'artifact-members'/relative
        write(dest,data)
        rows.append(dict(zip_member=i.filename, bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), zip_external_attr=i.external_attr, copied_physical_full_mode=0o600))
        if i.filename.startswith('fetch-session-recovery/') and relative.name in ['runtime.stdout.log','identity.json','java-records.stdout.log']:
            write(S/FIX/relative.name,data)
receipt=dict(source_sha='e8fc0b72724ebfcef99d4742c7870f95a7a9a47f',repository='mingley/partitionline',run_id=37156631589,job_id=111301245657,artifact_id=11286412224,
    tool='mcp__codex_apps__github_download_workflow_artifact',original_zip_sha256=hashlib.sha256(ZIP.read_bytes()).hexdigest(),original_zip_bytes=ZIP.stat().st_size,original_zip_full_mode=stat.S_IMODE(ZIP.stat().st_mode),original_zip_retained_WORK=str(ZIP),members=rows,
    scope='Complete original71 ZIP members preserved byte-for-byte with archive attributes mapped separately from copied physical0600. Runtime executed1 required test,65 records,32 positions,7 successful Fetch18 phases. Original wrapper failed validator and original identity failure is retained; no rewritten identity or new passing complete SDK/CI qualification claim.')
write(S/P/'artifact-provenance.json',(json.dumps(receipt,indent=2)+'\n').encode())
for name in ['scripts/report-fetch-session-recovery.py','tests/conformance/test_fetch_session_recovery.py']:
    original=R/name
    write(W/'original'/name,original.read_bytes(),stat.S_IMODE(original.stat().st_mode))
    write(S/name,original.read_bytes(),stat.S_IMODE(original.stat().st_mode))
write(S/P/'prepare.py',Path(__file__).read_bytes())
print(json.dumps(dict(stage=str(S),zip_members=71,bytes=348921,original_identity_failure_retained=True)))
