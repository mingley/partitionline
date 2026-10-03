#!/usr/bin/env python3
"""Verify a source-only publication inventory before copying or after installation."""
from pathlib import Path
import argparse,gzip,hashlib,json,os,stat
p=argparse.ArgumentParser();p.add_argument('--inventory',required=True);p.add_argument('--inventory-sha256',required=True);p.add_argument('--installed-root');p.add_argument('--exact-installed-fullmode',action='store_true');a=p.parse_args()
raw=Path(a.inventory).read_bytes();assert hashlib.sha256(raw).hexdigest()==a.inventory_sha256
inventory=json.loads(raw);rows=inventory['files'];assert len(rows)==inventory['file_count'];total=0;seen=set()
for row in rows:
 name=row['proposed_repo_target'];assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in seen;seen.add(name)
 target=(Path(a.installed_root)/name) if a.installed_root else Path(row['source_path']);st=target.lstat();assert stat.S_ISREG(st.st_mode)
 data=target.read_bytes();assert len(data)==row['bytes'] and hashlib.sha256(data).hexdigest()==row['sha256'] and len(data)<=12*1024*1024
 assert not data.startswith((b'\x7fELF',b'PK\x03\x04',b'\xca\xfe\xba\xbe'))
 if a.installed_root:
  assert bool(stat.S_IMODE(st.st_mode)&0o111)==bool(row['full_mode']&0o111)
  if a.exact_installed_fullmode:assert stat.S_IMODE(st.st_mode)==row['full_mode']
 else:assert stat.S_IMODE(st.st_mode)==row['full_mode'] and st.st_mtime_ns==row['mtime_ns']
 if row['payload_kind']=='gzipJSON_source_audit_only':json.loads(gzip.decompress(data))
 else:data.decode('utf-8')
 total+=len(data)
assert total==inventory['file_bytes']
print(json.dumps({'passed':True,'files':len(rows),'bytes':total,'source_fullmode_and_mtime_checked':not bool(a.installed_root),'installed_Git_executable_class_checked':bool(a.installed_root),'exact_installed_fullmode_checked':bool(a.installed_root and a.exact_installed_fullmode),'compiled_or_binarygzip_payloads':False,'mutations':0}))
