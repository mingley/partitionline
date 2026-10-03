#!/usr/bin/env python3
"""Generate public synthetic JWT fixtures with independent OpenSSL signatures.

Private test keys are created only under --work and never copied to output.
Reusing that private scratch directory reproduces RSA signatures exactly; EC
signatures are randomized and the retained manifest pins each actual result.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess

ISSUER = 'https://issuer.example/realm'
AUDIENCE = 'partitionline'
ANCHOR = 1800000000

def b64(value):
    return base64.urlsafe_b64encode(value).decode().rstrip('=')

def digest(value):
    return hashlib.sha256(value).hexdigest()

def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()

def run(argv, data=None):
    return subprocess.run(argv, input=data, capture_output=True, check=True).stdout

def tlv(data, position=0):
    tag, length = data[position:position + 2]
    position += 2
    if length & 128:
        count = length & 127
        assert 0 < count <= 4
        length = int.from_bytes(data[position:position + count], 'big')
        position += count
    assert position + length <= len(data)
    return tag, data[position:position + length], position + length

def fields(data):
    result = []
    position = 0
    while position < len(data):
        tag, body, position = tlv(data, position)
        result.append((tag, body))
    return result

def unsigned(value):
    return value.lstrip(b'\x00') or b'\x00'

def der_integer(value):
    value = unsigned(value)
    if value[0] & 128:
        value = b'\x00' + value
    assert len(value) < 128
    return b'\x02' + bytes([len(value)]) + value

def ec_der(value):
    assert len(value) == 64
    body = der_integer(value[:32]) + der_integer(value[32:])
    return b'\x30' + bytes([len(body)]) + body

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--epoch', type=int, default=ANCHOR)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    assert not any(args.output.iterdir()), 'Use a fresh output directory'
    keys = {}
    jwks = []
    for kid, algorithm, bits in [('rsa2048', 'RS256', 2048), ('rsa4096', 'RS256', 4096), ('p256', 'ES256', 256)]:
        private = args.work / (kid + '.private.pem')
        if not private.exists():
            argv = ['openssl', 'genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:' + str(bits)] if algorithm == 'RS256' else ['openssl', 'genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256']
            private.write_bytes(run(argv))
            private.chmod(0o600)
        public = run(['openssl', 'pkey', '-in', str(private), '-pubout', '-outform', 'DER'])
        tag, outer, end = tlv(public)
        assert tag == 48 and end == len(public)
        spki = fields(outer)
        assert len(spki) == 2 and spki[1][0] == 3 and spki[1][1][0] == 0
        material = spki[1][1][1:]
        if algorithm == 'RS256':
            tag, body, end = tlv(material)
            assert tag == 48 and end == len(material)
            values = fields(body)
            assert len(values) == 2 and all(tag == 2 for tag, _ in values)
            jwk = {'kid': kid, 'kty': 'RSA', 'alg': algorithm, 'use': 'sig', 'key_ops': ['verify'], 'n': b64(unsigned(values[0][1])), 'e': b64(unsigned(values[1][1]))}
            assert len(unsigned(values[0][1])) * 8 == bits
        else:
            assert len(material) == 65 and material[0] == 4
            jwk = {'kid': kid, 'kty': 'EC', 'alg': algorithm, 'use': 'sig', 'key_ops': ['verify'], 'crv': 'P-256', 'x': b64(material[1:33]), 'y': b64(material[33:65])}
        public_path = args.output / (kid + '.public.pem')
        public_path.write_bytes(run(['openssl', 'pkey', '-in', str(private), '-pubout']))
        (args.output / (kid + '.spki.der')).write_bytes(public)
        keys[kid] = {'private': private, 'public': public_path, 'algorithm': algorithm, 'spki_sha256': digest(public), 'jwk_sha256': digest(encoded(jwk))}
        jwks.append(jwk)
    (args.output / 'jwks.json').write_bytes(encoded({'keys': jwks}) + b'\n')
    rows = []
    base = {'iss': ISSUER, 'aud': AUDIENCE, 'sub': 'fixture-user', 'iat': args.epoch, 'exp': args.epoch + 300, 'scope': 'read write', 'jti': 'synthetic-fixture'}

    def add(name, *, kid='rsa2048', header=None, payload=None, accept=False, reason='', validation_epoch=None,
            alternate=False, mutate=None, signature_valid=True, raw_header=None, raw_payload=None):
        key = keys[kid]
        protected = {'alg': key['algorithm'], 'kid': kid, 'typ': 'at+jwt'} if header is None else header
        claims = dict(base) if payload is None else payload
        header_bytes = encoded(protected) if raw_header is None else raw_header
        payload_bytes = encoded(claims) if raw_payload is None else raw_payload
        signing = (b64(header_bytes) + '.' + b64(payload_bytes)).encode()
        proof = run(['openssl', 'dgst', '-sha256', '-sign', str(key['private'])], signing)
        original_der = proof
        if key['algorithm'] == 'ES256':
            tag, body, end = tlv(proof)
            assert tag == 48 and end == len(proof)
            values = fields(body)
            assert len(values) == 2 and all(tag == 2 for tag, _ in values)
            proof = b''.join(unsigned(value).rjust(32, b'\x00') for _, value in values)
            assert len(proof) == 64
        token = signing.decode() + '.' + b64(proof)
        if mutate is not None:
            token = mutate(token, original_der)
        token_path = args.output / (name + '.jws')
        token_path.write_text(token, encoding='utf-8')
        hpath, ppath = args.output / (name + '.header.json'), args.output / (name + '.payload.json')
        hpath.write_bytes(header_bytes)
        ppath.write_bytes(payload_bytes)
        # Verify the exact retained compact's signature independently of claims.
        exact = token.split('.')
        crypto_valid = False
        if len(exact) == 3:
            try:
                proof = base64.urlsafe_b64decode(exact[2] + '=' * (-len(exact[2]) % 4))
                if key['algorithm'] == 'ES256':
                    proof = ec_der(proof)
                proof_path = args.work / 'verify-signature.bin'
                proof_path.write_bytes(proof)
                result = subprocess.run(['openssl', 'dgst', '-sha256', '-verify', str(key['public']), '-signature', str(proof_path)], input=(exact[0] + '.' + exact[1]).encode(), capture_output=True)
                crypto_valid = result.returncode == 0
            except (ValueError, AssertionError):
                pass
        assert crypto_valid == signature_valid, (name, crypto_valid, signature_valid)
        rows.append({'id': name, 'algorithm': protected.get('alg') if isinstance(protected, dict) else None,
            'signer_algorithm': key['algorithm'], 'kid': protected.get('kid') if isinstance(protected, dict) else None,
            'signer_kid': kid, 'compact_jws_file': token_path.name, 'compact_jws_sha256': digest(token.encode()),
            'issued_epoch': args.epoch, 'validation_epoch': args.epoch if validation_epoch is None else validation_epoch,
            'epoch_anchor': args.epoch, 'expected_signature_valid': signature_valid,
            'signature_scope': 'Exact retained signing input verified with the named independent signer public key, irrespective of declared alg/kid or strict token syntax.',
            'expected_policy_decision': 'accept' if accept else 'reject', 'reason': reason,
            'alternate_policy': 'token_use=access' if alternate else None,
            'header_utf8': header_bytes.decode(), 'payload_utf8': payload_bytes.decode(),
            'header_file': hpath.name, 'header_sha256': digest(header_bytes), 'payload_file': ppath.name, 'payload_sha256': digest(payload_bytes),
            'signing_input_sha256': digest((exact[0] + '.' + exact[1]).encode()) if len(exact) >= 2 else None,
            'public_key_sha256': key['spki_sha256'], 'public_jwk_sha256': key['jwk_sha256']})

    for kid in keys:
        add('valid-' + kid, kid=kid, accept=True, reason='Supported algorithm and key-size boundary with strict signed access claims')
    add('valid-audience-array', payload=dict(base, aud=['other', AUDIENCE]), accept=True, reason='Configured audience present in bounded string array')
    add('valid-unicode-subject', payload=dict(base, sub='fixture-\u03bb'), accept=True, reason='Bounded Unicode subject')
    add('valid-no-jti', payload={k: v for k, v in base.items() if k != 'jti'}, accept=True, reason='Optional token identifier absent')
    add('valid-no-scope', payload={k: v for k, v in base.items() if k != 'scope'}, accept=True, reason='Scope is not part of local JWT identity policy')
    add('valid-iat-skew-boundary', payload=dict(base, iat=args.epoch + 5), accept=True, reason='Exactly five seconds future iat allowed')
    add('valid-nbf-skew-boundary', payload=dict(base, nbf=args.epoch + 5), accept=True, reason='Exactly five seconds future nbf allowed')
    add('valid-lifetime-boundary', payload=dict(base, exp=args.epoch + 3600), accept=True, reason='Exactly3600seconds signed lifetime allowed')
    add('valid-before-expiry', validation_epoch=args.epoch + 299, accept=True, reason='One second before expiration')
    for name, change, reason in [
        ('wrong-issuer', {'iss': 'https://other.example/realm'}, 'Issuer must exactly match authority'),
        ('wrong-audience', {'aud': 'other'}, 'Audience must include configured relying party'),
        ('audience-empty-array', {'aud': []}, 'Audience array must be nonempty'),
        ('audience-non-string', {'aud': [AUDIENCE, 3]}, 'Every audience must be string'),
        ('audience-too-many', {'aud': [AUDIENCE] * 9}, 'Default audience count bound8'),
        ('subject-empty', {'sub': ''}, 'Subject must be nonempty'),
        ('subject-control', {'sub': 'bad\nsubject'}, 'Control character forbidden in identity'),
        ('subject-numeric', {'sub': 3}, 'Subject must be a string'),
        ('subject-oversized', {'sub': 'x' * 257}, 'Default identity byte bound256'),
        ('jti-empty', {'jti': ''}, 'Present token identifier must be nonempty'),
        ('jti-numeric', {'jti': 7}, 'Present token identifier must be a string'),
        ('expired', {'exp': args.epoch - 1, 'iat': args.epoch - 301}, 'Expired token is rejected without skew grace'),
        ('iat-future', {'iat': args.epoch + 60}, 'Issued time beyond future skew bound'),
        ('nbf-future', {'nbf': args.epoch + 60}, 'Not-before beyond future skew bound'),
        ('nbf-equals-exp', {'nbf': args.epoch + 300}, 'Not-before must precede expiration'),
        ('iat-equals-exp', {'iat': args.epoch + 300}, 'Issued time must precede expiration'),
        ('lifetime-too-long', {'exp': args.epoch + 3601}, 'Default signed lifetime bound3600'),
        ('exp-string', {'exp': str(args.epoch + 300)}, 'NumericDate must be integral unsigned JSON integer'),
        ('exp-fractional', {'exp': args.epoch + 300.5}, 'Fractional NumericDate rejected by local policy'),
        ('iat-negative', {'iat': -1}, 'Negative NumericDate rejected'),
        ('nbf-string', {'nbf': str(args.epoch)}, 'Not-before must be integral unsigned integer'),
    ]:
        add(name, payload=dict(base, **change), reason=reason)
    add('at-expiry', validation_epoch=args.epoch + 300, reason='At expiration is rejected without grace')
    for claim in ['iss', 'aud', 'sub', 'iat', 'exp']:
        add('missing-' + claim, payload={k: v for k, v in base.items() if k != claim}, reason='Required signed claim absent')
    for typ in ['JWT', 'id+jwt', '', 7]:
        add('wrong-type-' + str(typ or 'empty'), header={'alg': 'RS256', 'kid': 'rsa2048', 'typ': typ}, reason='Default profile requires exact protected typ=at+jwt')
    add('missing-type', header={'alg': 'RS256', 'kid': 'rsa2048'}, reason='Default protected access type is mandatory')
    add('alternative-valid-access', header={'alg': 'RS256', 'kid': 'rsa2048', 'typ': 'JWT'}, payload=dict(base, token_use='access'), accept=True, alternate=True, reason='Explicit alternate profile requires signed token_use=access')
    add('alternative-id-token', header={'alg': 'RS256', 'kid': 'rsa2048', 'typ': 'JWT'}, payload=dict(base, token_use='id'), alternate=True, reason='Signed ID-token discriminator rejected')
    add('alternative-missing-discriminator', header={'alg': 'RS256', 'kid': 'rsa2048', 'typ': 'JWT'}, alternate=True, reason='Alternate profile requires signed discriminator')
    add('unknown-key', header={'alg': 'RS256', 'kid': 'unknown', 'typ': 'at+jwt'}, reason='Token cannot select unknown verification key')
    add('algorithm-key-confusion', header={'alg': 'ES256', 'kid': 'rsa2048', 'typ': 'at+jwt'}, reason='Declared algorithm must match configured key type')
    add('unsupported-algorithm', header={'alg': 'RS512', 'kid': 'rsa2048', 'typ': 'at+jwt'}, reason='Explicit local algorithm allowlist RS256/ES256')
    add('none-algorithm', header={'alg': 'none', 'kid': 'rsa2048', 'typ': 'at+jwt'}, reason='Unsecured algorithm rejected even with a valid underlying RSA proof')
    for name, value in [('crit', []), ('jku', 'https://other.example/jwks'), ('jwk', {'kty': 'RSA'}), ('x5u', 'https://other.example/cert'), ('b64', True)]:
        add('forbidden-header-' + name, header={'alg': 'RS256', 'kid': 'rsa2048', 'typ': 'at+jwt', name: value}, reason='Unconfigured critical or token-selected authority header forbidden')
    add('duplicate-header', raw_header=b'{"alg":"RS256","alg":"RS256","kid":"rsa2048","typ":"at+jwt"}', reason='Duplicate protected member rejected despite valid signature')
    add('duplicate-claim', raw_payload=encoded(base)[:-1] + b',"iss":"https://issuer.example/realm"}', reason='Duplicate identity claim rejected despite valid signature')
    add('escaped-duplicate-claim', raw_payload=encoded(base)[:-1] + b',"\\u0069ss":"https://issuer.example/realm"}', reason='Escaped duplicate JSON member rejected')
    add('malformed-header', raw_header=b'{', reason='Malformed protected JSON rejected')
    add('malformed-payload', raw_payload=b'{', reason='Malformed signed claims JSON rejected')
    add('array-payload', raw_payload=b'[]', reason='Claims must be a JSON object')
    add('nested-payload', payload=dict(base, extra=json.loads('[' * 17 + '0' + ']' * 17)), reason='Default JSON depth bound16')
    add('oversized-header', header={'alg': 'RS256', 'kid': 'rsa2048', 'typ': 'at+jwt', 'extra': 'x' * 2100}, reason='Default decoded header byte bound2048')
    add('oversized-token', payload=dict(base, extra='x' * 9000), reason='Default compact token byte bound8192')
    def corrupt_proof(token, _):
        header, payload, proof = token.split('.')
        raw = bytearray(base64.urlsafe_b64decode(proof + '=' * (-len(proof) % 4)))
        raw[0] ^= 1
        return header + '.' + payload + '.' + b64(raw)
    add('tampered-signature-rsa', mutate=corrupt_proof, signature_valid=False, reason='Changed RSA signature byte')
    add('tampered-signature-ec', kid='p256', mutate=lambda t, _: t[:t.rfind('.') + 1] + b64(bytes(64)), signature_valid=False, reason='Changed fixed-format EC signature')
    add('ec-der-signature', kid='p256', mutate=lambda t, der: t[:t.rfind('.') + 1] + b64(der), signature_valid=False, reason='JOSE requires64byte R||S, not DER sequence')
    add('padded-header', mutate=lambda t, _: t.replace('.', '=.', 1), signature_valid=False, reason='Padded base64url protected segment forbidden')
    add('padded-proof', mutate=lambda t, _: t + '=', signature_valid=True, reason='Local compact encoding requires canonical unpadded base64url proof')
    add('trailing-segment', mutate=lambda t, _: t + '.extra', signature_valid=False, reason='Compact token must contain exactly three segments')
    fixture = {'schema_version': 1, 'scope': 'Public synthetic independently OpenSSL-signed local-policy fixtures; fixed private decoder epoch, not live OIDC/provider evidence.',
        'epoch_anchor': args.epoch, 'issuer': ISSUER, 'audiences': [AUDIENCE], 'subject': 'fixture-user',
        'jwks_file': 'jwks.json', 'jwks_sha256': digest((args.output / 'jwks.json').read_bytes()),
        'default_policy': {'access_token': 'typ=at+jwt', 'algorithms': ['RS256', 'ES256'], 'token_bytes': 8192, 'header_bytes': 2048,
            'json_depth': 16, 'identity_bytes': 256, 'audiences': 8, 'token_lifetime_seconds': 3600, 'future_skew_seconds': 5, 'expiration_grace_seconds': 0},
        'cases': rows}
    (args.output / 'fixtures.json').write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + '\n')
    (args.output / 'cases.tsv').write_text(''.join(row['id'] + '\t' + row['compact_jws_file'] + '\t' + row['expected_policy_decision'] + '\n' for row in rows))
    result = {'schema_version': 1, 'passed': True, 'openssl_version': run(['openssl', 'version']).decode().strip(),
        'generator_sha256': digest(Path(__file__).read_bytes()), 'epoch_anchor': args.epoch, 'cases': len(rows),
        'signature_valid': sum(row['expected_signature_valid'] for row in rows), 'policy_accept': sum(row['expected_policy_decision'] == 'accept' for row in rows),
        'files_sha256': {p.name: digest(p.read_bytes()) for p in sorted(args.output.iterdir()) if p.is_file()}}
    (args.output / 'generation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ['passed', 'epoch_anchor', 'cases', 'signature_valid', 'policy_accept']}))

if __name__ == '__main__':
    main()
