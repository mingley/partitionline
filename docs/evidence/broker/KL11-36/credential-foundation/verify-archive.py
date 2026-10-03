#!/usr/bin/env python3
"""Verify every archived Git blob against an immutable source commit."""
import hashlib,json,os,subprocess,sys
from pathlib import Path
repo=Path(sys.argv[1]);archive=Path(sys.argv[2]);sha=sys.argv[3];output=Path(sys.argv[4])
objects=subprocess.check_output(['git','ls-tree','-rz','--full-tree',sha],cwd=repo).split(b'\0')
checked=0;mismatches=[];inputs={}
for row in objects:
    if not row:continue
    fields,path=row.split(b'\t',1);mode,kind,object_id=fields.split()
    if kind!=b'blob':continue
    name=path.decode();local=archive/name
    expected=subprocess.check_output(['git','cat-file','blob',object_id],cwd=repo)
    actual=os.readlink(local).encode() if mode==b'120000' else local.read_bytes()
    checked+=1
    if actual!=expected:mismatches.append(name)
    if name in ['partitionline-broker/src/security/credentials.rs','partitionline-broker/src/security/sasl.rs','partitionline-broker/tests/sasl_credentials.rs']:
        inputs[name]=hashlib.sha256(actual).hexdigest()
result={'source_sha':sha,'tree':subprocess.check_output(['git','rev-parse',sha+'^{tree}'],cwd=repo,text=True).strip(),'checked_git_blobs':checked,'mismatches':mismatches,'inputs':inputs}
output.write_text(json.dumps(result,indent=2)+'\n')
if mismatches:raise SystemExit('archive differs from immutable source')
print(json.dumps({'checked_git_blobs':checked,'mismatches':len(mismatches)}))
