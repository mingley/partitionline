from pathlib import Path
import hashlib,json,subprocess
root=Path('/workspace/partitionline')
dest=Path('/workspace/work/v17-helper-pin-evidence-20261103')
commit='9b796378395982405baa84188dba463d218506f9'
manifest_name='docs/evidence/client/candidate-20261030/v17/import-manifest.json'
manifest=json.loads((root/manifest_name).read_bytes())
files=sorted([*manifest['required_existing_root_files'],manifest_name,'.github/workflows/candidates.yml'])
pins={}
for name in files:
    data=subprocess.run(['git','show',commit+':'+name],cwd=root,check=True,capture_output=True).stdout
    assert (root/name).read_bytes()==data, name
    p=dest/'source'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
    pins[name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
(dest/'source-pins.json').write_text(json.dumps({'commit':commit,'source':'git show, byte-compared to working files','files':pins},indent=2,sort_keys=True)+'\n')
