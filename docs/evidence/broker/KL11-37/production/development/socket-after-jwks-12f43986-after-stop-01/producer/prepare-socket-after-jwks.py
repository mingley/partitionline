import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

repo=Path('/workspace/partitionline');base=sys.argv[1];old=Path('/workspace/work/broker-oidc/http-final-0b9797ef');dest=Path('/workspace/work/broker-oidc/socket-overlay-after-jwks-'+base[:8]);out=repo/('docs/evidence/broker/KL11-37/production/development/socket-after-jwks-'+base[:8]);retention=Path('/workspace/work/broker-oidc/socket-audit-retention-'+base[:8]);out.mkdir(parents=True,exist_ok=True);dest.mkdir(parents=True,exist_ok=True);retention.mkdir(parents=True,exist_ok=True)
socket=json.loads((repo/'docs/evidence/broker/KL11-37/production/socket-checkpoint-source20/manifest.json').read_text())['source'];overlays={row['path']:row for row in socket};jwks=json.loads((repo/'docs/evidence/broker/KL11-37/production/development/jwks-media-01/validation.json').read_text())['changed_source'];rows=[];links=0;seen=set();tree=subprocess.check_output(['git','ls-tree','-r','-z',base],cwd=repo);batch=subprocess.Popen(['git','cat-file','--batch'],cwd=repo,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
def load(oid):
    batch.stdin.write(oid.encode()+b'\n');batch.stdin.flush();received,kind,size=batch.stdout.readline().decode().split();assert received==oid and kind=='blob';data=batch.stdout.read(int(size));assert batch.stdout.read(1)==b'\n';return data
for entry in tree.split(b'\0'):
    if not entry:continue
    metadata,name=entry.split(b'\t',1);mode,kind,oid=metadata.decode().split();name=name.decode();assert mode in('100644','100755')and kind=='blob';permissions=0o755 if mode=='100755'else 0o644;source=old/name;path=dest/name;path.parent.mkdir(parents=True,exist_ok=True);seen.add(name);linked=False
    if name in overlays:
        data=(repo/name).read_bytes();assert hashlib.sha256(data).hexdigest()==overlays[name]['sha256'];assert mode=='100644'
    elif source.is_file()and not source.is_symlink()and source.stat().st_mode&0o777==permissions:
        candidate=source.read_bytes()
        if hashlib.sha1(b'blob '+str(len(candidate)).encode()+b'\0'+candidate).hexdigest()==oid:
            data=candidate
            if not path.exists():os.link(source,path)
            linked=True;links+=1
        else:data=load(oid)
    else:data=load(oid)
    if not linked:
        if path.exists():path.unlink()
        path.write_bytes(data);path.chmod(permissions)
    assert path.read_bytes()==data and path.stat().st_mode&0o777==permissions
    rows.append(dict(path=name,base_git_oid=oid,base_git_mode=mode,mode=permissions,sha256=hashlib.sha256(data).hexdigest(),overlay=name in overlays))
for name,row in overlays.items():
    if name in seen:continue
    data=(repo/name).read_bytes();assert hashlib.sha256(data).hexdigest()==row['sha256'];path=dest/name;path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():path.unlink()
    path.write_bytes(data);path.chmod(0o644);rows.append(dict(path=name,base_git_oid=None,base_git_mode=None,mode=0o644,sha256=row['sha256'],overlay=True))
batch.stdin.close();assert batch.wait()==0
by_path={row['path']:row for row in rows}
for row in jwks:assert by_path[row['path']]['sha256']==row['sha256']and not by_path[row['path']]['overlay'], 'actual pushed JWKS bytes differ: '+row['path']
assert sum(row['overlay']for row in rows)==6
manifest=dict(base_source_sha=base,scope='actual pushed JWKS main source plus exactly six unchanged OAuth socket source20 overlays; no mutable hardlinks',files=rows);raw=(json.dumps(manifest,indent=2)+'\n').encode();sealed=retention/'source-before.json';sealed.write_bytes(raw);sealed.chmod(0o600);compressed=gzip.compress(raw,mtime=0);(out/'source-before.json.gz').write_bytes(compressed);assert gzip.decompress(compressed)==raw
preparation=dict(base_source_sha=base,base_Git_files=len(seen),candidate_files=len(rows),immutable_hardlinks=links,all_links_git_bytes_and_full_modes_verified=True,linked_only_from=str(old),six_overlays_unchanged=socket,JWKS_four_actual_pushed_blobs_match_frozen_followup=True,raw_audit={'original_sha256':hashlib.sha256(raw).hexdigest(),'original_bytes':len(raw),'original_mode':'0o600','original_scratch_path':str(sealed),'gzip_sha256':hashlib.sha256(compressed).hexdigest(),'stored_path':'source-before.json.gz','lossless_roundtrip':True},qualification='development preparation only; no Cargo/runtime or actual socket source push yet')
(out/'preparation.json').write_text(json.dumps(preparation,indent=2)+'\n');print(json.dumps({'base_source_sha':base,'candidate_files':len(rows),'immutable_hardlinks':links,'six_overlays_unchanged':True,'JWKS_four_pushed_blobs_match':True},indent=2))
