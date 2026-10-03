#!/usr/bin/env python3
"""Inspect private path types/modes only, never bytes, hashes, argv or env."""
import argparse,hashlib,json,os,pathlib,stat
p=argparse.ArgumentParser();p.add_argument('--private-scratch',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);args=p.parse_args()
root=args.private_scratch
assert root.exists() and root.is_dir() and not root.is_symlink()
rows=[]
for path in [root,*sorted(root.rglob('*'))]:
 info=path.lstat();mode=info.st_mode&0o7777
 kind='directory' if stat.S_ISDIR(info.st_mode) else 'regular' if stat.S_ISREG(info.st_mode) else 'other'
 expected=0o700 if kind=='directory' else 0o600 if kind=='regular' else None
 rows.append({'path':str(path.relative_to(root)),'kind':kind,'full_mode':oct(mode),'expected_full_mode':oct(expected) if expected else None,'passed':expected is not None and mode==expected})
value={'passed':bool(rows) and all(r['passed'] for r in rows),'scope':'post-joined private filesystem mode inventory only; no credential/key/token bytes or hashes inspected','path_count':len(rows),'directory_count':sum(r['kind']=='directory' for r in rows),'file_count':sum(r['kind']=='regular' for r in rows),'paths':rows}
args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(value,indent=2)+'\n');os.chmod(args.output,0o600)
print(json.dumps({'passed':value['passed'],'path_count':len(rows),'receipt_sha256':hashlib.sha256(args.output.read_bytes()).hexdigest()}))
raise SystemExit(0 if value['passed'] else 1)
