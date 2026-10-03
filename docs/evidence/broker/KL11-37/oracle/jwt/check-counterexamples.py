#!/usr/bin/env python3
"""Require genuine corruption controls to fail the independent fixture checker."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
def sha(data):
    return hashlib.sha256(data).hexdigest()
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True)
    baseline = subprocess.run(['python3', str(HERE / 'check-fixtures.py'), str(args.fixtures)], capture_output=True, text=True)
    assert baseline.returncode == 0
    controls = []
    for name in ['missing-case', 'duplicate-case', 'unsafe-path', 'duplicate-manifest-key', 'private-jwk',
                 'forged-signature-and-hash', 'forged-signature-verdict', 'rewritten-header-sidecar', 'forged-spki']:
        target = args.work / name
        shutil.copytree(args.fixtures, target)
        manifest = target / 'fixtures.json'
        value = json.loads(manifest.read_text())
        row = next(row for row in value['cases'] if row['id'] == 'valid-rsa2048')
        if name == 'missing-case': value['cases'].pop()
        elif name == 'duplicate-case': value['cases'][-1] = dict(value['cases'][0])
        elif name == 'unsafe-path': row['compact_jws_file'] = '../outside.jws'
        elif name == 'private-jwk':
            path = target / 'jwks.json'; jwks = json.loads(path.read_text()); jwks['keys'][0]['d'] = 'public-negative-control'
            path.write_text(json.dumps(jwks)); value['jwks_sha256'] = sha(path.read_bytes())
        elif name == 'forged-signature-and-hash':
            path = target / row['compact_jws_file']; header, payload, proof = path.read_text().split('.')
            raw = bytearray(base64.urlsafe_b64decode(proof + '=' * (-len(proof) % 4))); raw[0] ^= 1
            path.write_text(header + '.' + payload + '.' + base64.urlsafe_b64encode(raw).decode().rstrip('='))
            row['compact_jws_sha256'] = sha(path.read_bytes())
        elif name == 'forged-signature-verdict': row['expected_signature_valid'] = False
        elif name == 'rewritten-header-sidecar':
            path = target / row['header_file']; path.write_text('{"alg":"none"}')
            row['header_utf8'] = path.read_text(); row['header_sha256'] = sha(path.read_bytes())
        elif name == 'forged-spki':
            path = target / 'rsa2048.spki.der'; raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
            for item in value['cases']:
                if item['signer_kid'] == 'rsa2048': item['public_key_sha256'] = sha(raw)
        manifest.write_text(json.dumps(value))
        if name == 'duplicate-manifest-key':
            raw = manifest.read_text(); manifest.write_text('{"schema_version":1,' + raw[1:])
        result = subprocess.run(['python3', str(HERE / 'check-fixtures.py'), str(target)], capture_output=True, text=True)
        controls.append({'name': name, 'exit_code': result.returncode, 'rejected': result.returncode != 0,
            'stderr_tail': result.stderr.splitlines()[-1:]})
        args.report.write_text(json.dumps({'schema_version':1, 'passed':False, 'baseline_passed':True,
            'checker_sha256':sha((HERE/'check-fixtures.py').read_bytes()), 'controls':controls}, indent=2)+'\n')
        assert result.returncode != 0, name
    args.report.write_text(json.dumps({'schema_version':1, 'passed':True, 'baseline_passed':True,
        'checker_sha256':sha((HERE/'check-fixtures.py').read_bytes()), 'controlled_rejections':len(controls), 'controls':controls}, indent=2)+'\n')
    print(json.dumps({'passed':True, 'baseline_cases':70, 'controlled_rejections':len(controls)}))
if __name__ == '__main__': main()
