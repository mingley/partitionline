import hashlib,io,json,pathlib,subprocess,tarfile
D=pathlib.Path(__file__).resolve().parent; R=pathlib.Path('/workspace/partitionline');C='c3b17722a76d3ddb5689811c55ce53cb5d64be9a'; receipt=json.loads((D/'binary/receipt.json').read_text());build=json.loads((D/'integrated-rf1-01.json').read_text())
raw=subprocess.check_output(['git','archive','--format=tar',C,'partitionline-broker','Cargo.toml','Cargo.lock','rust-toolchain.toml'],cwd=R)
if raw!=(D/'candidate-inputs.tar').read_bytes():raise ValueError('commit archive differs')
files={}
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as t:
 for entry in t:
  if not entry.isfile():continue
  data=t.extractfile(entry).read();expected=hashlib.sha256(data).hexdigest();path=R/entry.name
  if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:raise ValueError('worktree differs: '+entry.name)
  files[entry.name]=expected
if files!=receipt['source_files']:raise ValueError('source receipt differs')
a={k:v for k,v in files.items() if k.startswith('partitionline-broker/src/') and k.endswith('.rs')};b={k:v for k,v in build['source_files'].items() if k.startswith('partitionline-broker/src/') and k.endswith('.rs')}
if a!=b:raise ValueError('build source receipt differs')
if hashlib.sha256((D/'binary/operator.elf').read_bytes()).hexdigest()!=receipt['binaries']['operator.elf']:raise ValueError('copied binary differs')
print('Commit and worktree inputs:',len(files),'matched; broker Rust build receipt:',len(a),'matched; post-build executable snapshot matches')
