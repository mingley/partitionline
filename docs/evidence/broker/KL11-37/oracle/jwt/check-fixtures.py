#!/usr/bin/env python3
"""Independently check public fixture integrity and exact OpenSSL proof outcomes."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

def strict_json(path):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate manifest/JWKS key')
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)

def checksum(data):
    return hashlib.sha256(data).hexdigest()

def take_file(root, name, maximum=65536):
    if not isinstance(name, str) or Path(name).name != name or name in ('.', '..'):
        raise ValueError('Unsafe fixture filename')
    data = (root / name).read_bytes()
    if len(data) > maximum:
        raise ValueError('Oversized retained fixture')
    return data

def decode(value):
    return base64.urlsafe_b64decode(value + '=' * (-len(value) % 4))

def ec_der(raw):
    if len(raw) != 64:
        raise ValueError('Wrong raw EC signature size')
    integers = []
    for value in (raw[:32], raw[32:]):
        value = value.lstrip(b'\0') or b'\0'
        if value[0] & 128:
            value = b'\0' + value
        integers.append(b'\2' + bytes([len(value)]) + value)
    body = b''.join(integers)
    return b'\x30' + bytes([len(body)]) + body

def verify(root):
    fixture = strict_json(root / 'fixtures.json')
    if fixture.get('schema_version') != 1 or fixture.get('epoch_anchor') != 1800000000:
        raise ValueError('Fixture schema/epoch mismatch')
    cases = fixture.get('cases')
    if not isinstance(cases, list) or len(cases) != 70:
        raise ValueError('Incomplete fixture matrix')
    jwks_bytes = take_file(root, fixture['jwks_file'])
    if checksum(jwks_bytes) != fixture['jwks_sha256']:
        raise ValueError('JWKS hash mismatch')
    jwks = strict_json(root / fixture['jwks_file'])
    if len(jwks['keys']) != 3 or {row['kid'] for row in jwks['keys']} != {'rsa2048', 'rsa4096', 'p256'}:
        raise ValueError('Unexpected public keys')
    for row in jwks['keys']:
        if any(field in row for field in ['d', 'p', 'q', 'dp', 'dq', 'qi', 'oth', 'k']):
            raise ValueError('Private material in public fixture authority')
        kid = row['kid']
        public = root / (kid + '.public.pem')
        actual_spki = subprocess.run(['openssl', 'pkey', '-pubin', '-in', str(public), '-outform', 'DER'], capture_output=True, check=True).stdout
        if actual_spki != take_file(root, kid + '.spki.der', 2048):
            raise ValueError('PEM/SPKI key mismatch')
    identifiers = set()
    valid = 0
    with tempfile.TemporaryDirectory(prefix='jwt-public-check-') as tmp:
        proof_path = Path(tmp) / 'proof.bin'
        for row in cases:
            if row['id'] in identifiers:
                raise ValueError('Duplicate fixture identity')
            identifiers.add(row['id'])
            if row['expected_policy_decision'] not in ['accept', 'reject'] or type(row['expected_signature_valid']) is not bool:
                raise ValueError('Invalid declared expectation')
            raw = take_file(root, row['compact_jws_file'])
            header = take_file(root, row['header_file'])
            payload = take_file(root, row['payload_file'])
            for data, field in [(raw, 'compact_jws_sha256'), (header, 'header_sha256'), (payload, 'payload_sha256')]:
                if checksum(data) != row[field]:
                    raise ValueError('Retained byte hash mismatch')
            if header.decode() != row['header_utf8'] or payload.decode() != row['payload_utf8']:
                raise ValueError('Retained UTF8 mismatch')
            token = raw.decode().split('.')
            if len(token) < 2 or decode(token[0]) != header or decode(token[1]) != payload:
                raise ValueError('Compact/header/payload mismatch')
            signing = (token[0] + '.' + token[1]).encode()
            if checksum(signing) != row['signing_input_sha256']:
                raise ValueError('Signing input hash mismatch')
            kid = row['signer_kid']
            if kid not in ['rsa2048', 'rsa4096', 'p256']:
                raise ValueError('Unknown independent signer')
            public = root / (kid + '.public.pem')
            spki = take_file(root, kid + '.spki.der', 2048)
            if checksum(spki) != row['public_key_sha256']:
                raise ValueError('Public key hash mismatch')
            observed = False
            if len(token) == 3:
                try:
                    proof = decode(token[2])
                    if row['signer_algorithm'] == 'ES256':
                        proof = ec_der(proof)
                    elif row['signer_algorithm'] != 'RS256':
                        raise ValueError('Unsupported independent signer algorithm')
                    proof_path.write_bytes(proof)
                    result = subprocess.run(['openssl', 'dgst', '-sha256', '-verify', str(public), '-signature', str(proof_path)], input=signing, capture_output=True)
                    observed = result.returncode == 0
                except ValueError:
                    pass
            if observed != row['expected_signature_valid']:
                raise ValueError('Independent signature verdict mismatch:' + row['id'])
            valid += observed
    return {'passed': True, 'cases': len(cases), 'actual_signature_verifications': len(cases), 'signature_valid': valid,
        'policy_decisions_scope': 'Declared independent strict-policy test inputs; this checker does not implement or certify JWT claims policy.'}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('fixtures', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    result = verify(args.fixtures)
    if args.report:
        args.report.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))

if __name__ == '__main__':
    main()
