#!/usr/bin/env python3
"""Verify the exact external helper bytes pinned by a client candidate."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tempfile
import unittest

REQUIRED = frozenset({
    'scripts/ci-qualify-client-candidate.py',
    'tests/ci/test_client_candidate_qualification.py',
    'scripts/candidate-command-ownership.py',
    'tests/ci/test_candidate_command_ownership.py',
    'scripts/ci-qualify-broker-candidate.py',
    'scripts/ci-verify-client-candidate-helpers.py',
})

def checked_file(root, relative):
    path=PurePosixPath(relative)
    if path.is_absolute() or '..' in path.parts or str(path)!=relative or '\\' in relative:
        raise ValueError('unsafe helper path: '+relative)
    current=root
    for part in path.parts:
        current=current/part
        if current.is_symlink():raise ValueError('symlink in helper path: '+relative)
    if not current.is_file() or not current.resolve().is_relative_to(root):
        raise ValueError('missing or unsafe helper: '+relative)
    return current

def unique_pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('duplicate JSON key: '+key)
        result[key]=value
    return result

def reject_constant(value):
    raise ValueError('invalid JSON constant: '+value)

def verify(checkout,manifest):
    checkout=Path(checkout)
    if checkout.is_symlink() or not checkout.is_dir():raise ValueError('checkout must be a real directory')
    root=checkout.resolve();manifest=Path(manifest)
    if not manifest.is_absolute():manifest=root/manifest
    try:relative=manifest.relative_to(root).as_posix()
    except ValueError:raise ValueError('manifest is outside checkout') from None
    manifest_file=checked_file(root,relative)
    try:doc=json.loads(manifest_file.read_text(),object_pairs_hook=unique_pairs,parse_constant=reject_constant)
    except (OSError,ValueError) as e:raise ValueError('invalid helper manifest: '+str(e)) from e
    pins=doc.get('required_existing_root_files') if isinstance(doc,dict) else None
    if not isinstance(pins,dict) or set(pins)!=REQUIRED:raise ValueError('missing or extra helper paths')
    for name in sorted(REQUIRED):
        item=pins[name]
        if not isinstance(item,dict) or set(item)!={'sha256','bytes'} or not isinstance(item['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',item['sha256']) or type(item['bytes']) is not int or item['bytes']<0:
            raise ValueError('invalid helper pin: '+name)
        path=checked_file(root,name);h=hashlib.sha256();size=0
        with path.open('rb') as stream:
            for data in iter(lambda:stream.read(1024*1024),b''):
                h.update(data);size+=len(data)
        if size!=item['bytes'] or h.hexdigest()!=item['sha256']:
            raise ValueError('helper size or hash mismatch: '+name)
    return len(REQUIRED)

class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='candidate-helper-guard-')
        self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.manifest=self.root/'manifest.json';self.pins={}
        for name in REQUIRED:
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);data=('fixture:'+name).encode();p.write_bytes(data)
            self.pins[name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
        self.write_pins(self.pins)
    def write_pins(self,pins):self.manifest.write_text(json.dumps({'required_existing_root_files':pins}))
    def test_exact_helpers_pass(self):self.assertEqual(verify(self.root,self.manifest),6)
    def test_missing_extra_and_unsafe_names_fail(self):
        one=next(iter(REQUIRED));missing=dict(self.pins);missing.pop(one)
        for pins in (missing,{**self.pins,'scripts/extra.py':self.pins[one]},
                     {**self.pins,'../outside':self.pins[one]}):
            self.write_pins(pins)
            with self.assertRaises(ValueError):verify(self.root,self.manifest)
    def test_missing_tampered_and_size_mismatch_fail(self):
        name=sorted(REQUIRED)[0];path=self.root/name;data=path.read_bytes()
        path.unlink()
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
        path.write_bytes(data+b'!')
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
        path.write_bytes(bytes([data[0]^1])+data[1:])
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
        path.write_bytes(data);wrong=dict(self.pins);wrong[name]={**wrong[name],'bytes':len(data)+1};self.write_pins(wrong)
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
    def test_ownership_test_and_broker_driver_same_size_mutation_fails(self):
        for name in ('tests/ci/test_candidate_command_ownership.py','scripts/ci-qualify-broker-candidate.py'):
            path=self.root/name;before=path.read_bytes();path.write_bytes(bytes([before[0]^1])+before[1:])
            with self.assertRaises(ValueError,msg=name):verify(self.root,self.manifest)
            path.write_bytes(before)

    def test_file_and_parent_symlinks_fail(self):
        name='scripts/ci-qualify-client-candidate.py';p=self.root/name;target=self.root/'target';target.write_bytes(p.read_bytes());p.unlink();p.symlink_to(target)
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
        p.unlink();p.write_bytes(target.read_bytes());directory=self.root/'tests/ci';moved=self.root/'moved-ci';directory.rename(moved);directory.symlink_to(moved,target_is_directory=True)
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
    def test_invalid_pins_and_manifest_symlink_fail(self):
        name=sorted(REQUIRED)[0]
        for item in ({'sha256':'bad','bytes':1},{'sha256':'0'*64,'bytes':True},{'sha256':'0'*64,'bytes':0,'extra':1}):
            pins=dict(self.pins);pins[name]=item;self.write_pins(pins)
            with self.assertRaises(ValueError):verify(self.root,self.manifest)
        self.write_pins(self.pins);target=self.root/'actual-manifest.json';self.manifest.rename(target);self.manifest.symlink_to(target)
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
    def test_duplicate_keys_and_nonfinite_constants_fail(self):
        one=sorted(REQUIRED)[0];pairs=json.dumps(self.pins);entry=json.dumps(one)+':'+json.dumps(self.pins[one])
        duplicate_helper=pairs[:-1]+','+entry+'}'
        first_hash=json.dumps(self.pins[one]['sha256'])
        duplicate_field=pairs.replace('"sha256": '+first_hash,'"sha256": '+first_hash+', "sha256": '+first_hash,1)
        first_size=str(self.pins[one]['bytes'])
        duplicate_bytes=pairs.replace('"bytes": '+first_size,'"bytes": '+first_size+', "bytes": '+first_size,1)
        payloads=(
            '{"required_existing_root_files":'+pairs+',"required_existing_root_files":'+pairs+'}',
            '{"required_existing_root_files":'+duplicate_helper+'}',
            '{"required_existing_root_files":'+duplicate_field+'}',
            '{"required_existing_root_files":'+duplicate_bytes+'}',
            '{"required_existing_root_files":'+pairs+',"unrelated":NaN}',
            '{"required_existing_root_files":'+pairs+',"unrelated":Infinity}',
            '{"required_existing_root_files":'+pairs+',"unrelated":-Infinity}',
        )
        for payload in payloads:
            self.manifest.write_text(payload)
            with self.assertRaises(ValueError):verify(self.root,self.manifest)

    def test_invalid_or_external_manifest_fails(self):
        self.manifest.write_text('{bad')
        with self.assertRaises(ValueError):verify(self.root,self.manifest)
        with tempfile.TemporaryDirectory(prefix='external-manifest-') as temp:
            other=Path(temp)/'manifest.json';other.write_text('{}')
            with self.assertRaises(ValueError):verify(self.root,other)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true')
    parser.add_argument('--checkout',type=Path,default=Path.cwd())
    parser.add_argument('--manifest',type=Path)
    args=parser.parse_args()
    if args.self_test:
        result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(GuardTests))
        return 0 if result.wasSuccessful() else 1
    if args.manifest is None:parser.error('--manifest is required')
    try:count=verify(args.checkout,args.manifest)
    except (OSError,ValueError) as e:print('candidate helper pins: '+str(e),file=sys.stderr);return 1
    print(f'candidate helper pins: {count} exact files verified')
    return 0

if __name__=='__main__':raise SystemExit(main())
