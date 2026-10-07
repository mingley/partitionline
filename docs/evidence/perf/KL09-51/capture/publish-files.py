#!/usr/bin/env python3
"""Commit selected review files while preserving the working HEAD and index."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

p=argparse.ArgumentParser(description=__doc__)
for name in ('repo','receipt','files','message'):p.add_argument('--'+name,type=Path,required=True)
p.add_argument('--parent',required=True);p.add_argument('--branch',required=True)
a=p.parse_args();r=a.repo.resolve();os.chdir(r)
run=lambda argv,**kw:subprocess.check_output(argv,**kw)
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
if a.receipt.exists():raise ValueError('fresh receipt required')
assert run(['git','rev-parse','refs/heads/'+a.branch],text=True).strip()==a.parent
files=json.loads(a.files.read_text());assert files and len(files)==len(set(files))
for name in files:
 path=Path(name)
 assert not path.is_absolute() and '..' not in path.parts and (r/path).is_file()
head=run(['git','rev-parse','HEAD'],text=True).strip();index=sha(r/'.git/index')
env=dict(os.environ,GIT_INDEX_FILE=str(a.receipt.resolve())+'.index')
subprocess.run(['git','read-tree',a.parent],env=env,check=True)
names=Path(str(a.receipt.resolve())+'.paths');names.write_bytes(b''.join(n.encode()+b'\0' for n in sorted(files)))
subprocess.run(['git','add','--pathspec-from-file='+str(names),'--pathspec-file-nul'],env=env,check=True)
tree=run(['git','write-tree'],env=env,text=True).strip()
commit_env=dict(env,GIT_AUTHOR_NAME='Codex',GIT_AUTHOR_EMAIL='codex@localhost',GIT_COMMITTER_NAME='Codex',GIT_COMMITTER_EMAIL='codex@localhost')
commit=subprocess.run(['git','commit-tree',tree,'-p',a.parent],input=a.message.read_text(),text=True,env=commit_env,check=True,capture_output=True).stdout.strip()
pins={name:sha(r/name) for name in files}
for name,digest in pins.items():assert hashlib.sha256(run(['git','show',commit+':'+name])).hexdigest()==digest
subprocess.run(['git','update-ref','refs/heads/'+a.branch,commit,a.parent],check=True)
assert sha(r/'.git/index')==index and run(['git','rev-parse','HEAD'],text=True).strip()==head
receipt=dict(commit=commit,parent=a.parent,branch=a.branch,working_head=head,working_index_unchanged=True,files_sha256=pins,scope='Review branch only; no merge or release')
with a.receipt.open('x') as f:json.dump(receipt,f,indent=2);f.write('\n')
print(commit)
