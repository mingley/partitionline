import hashlib,json,tarfile,urllib.request
from pathlib import Path
s=Path(__file__).parent;out=s/'go-preparation-01';out.mkdir(exist_ok=False)
try:
 url='https://go.dev/dl/?mode=json&include=all'
 with urllib.request.urlopen(url,timeout=30) as response:data=response.read(16*1024*1024+1)
 assert len(data)<=16*1024*1024
 (out/'official-release-metadata.json').write_bytes(data)
 release=next(x for x in json.loads(data) if x['version']=='go1.26.0');item=next(x for x in release['files'] if x['os']=='linux' and x['arch']=='amd64' and x['kind']=='archive');assert item['size']<=256*1024*1024
 archive=out/item['filename'];digest=hashlib.sha256();total=0
 with urllib.request.urlopen('https://go.dev/dl/'+item['filename'],timeout=30) as response,archive.open('xb') as sink:
  while chunk:=response.read(1024*1024):
   total+=len(chunk);assert total<=item['size'];digest.update(chunk);sink.write(chunk)
 assert total==item['size'] and digest.hexdigest()==item['sha256']
 with tarfile.open(archive) as source:source.extractall(out,filter='data')
 (out/'toolchain-binding.json').write_text(json.dumps(dict(metadata_url=url,archive_url='https://go.dev/dl/'+item['filename'],version=release['version'],bytes=total,sha256=digest.hexdigest()),indent=2)+'\n')
 print('Official pinned Go archive verified and isolated under work',flush=True)
except BaseException as error:
 (out/'failure.json').write_text(json.dumps(dict(error=type(error).__name__,message=str(error)),indent=2)+'\n');raise
