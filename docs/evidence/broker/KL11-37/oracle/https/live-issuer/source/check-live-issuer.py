#!/usr/bin/env python3
"""Actual bounded HTTPS policy and independent OpenSSL signature checks.

Only synthetic public fields and hashes enter reports. JWTs and credentials
remain in memory/private scratch. This is a component test, not SDK/broker QA.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

sys.dont_write_bytecode = True


def require(condition, label):
    if not condition:
        raise RuntimeError(label)


def interrupt(_signal, _frame):
    raise RuntimeError('finite component deadline')


def run(scratch, output):
    source = Path(__file__).with_name('live-issuer.py')
    spec = importlib.util.spec_from_file_location('signed_live_issuer', source)
    issuer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(issuer)
    issuer.prepare(scratch)
    server = issuer.Server(scratch)
    owner = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
    owner.start()
    root = server.state.issuer.rsplit('/issuer', 1)[0]
    trust = ssl.create_default_context(cafile=str(scratch / 'ca.pem'))
    checks, cases = [], []
    passed, failure = False, None

    def check(condition, label):
        require(condition, label)
        checks.append(label)

    def request(path, data=None, username=None, secret=None, form=False, trusted=True):
        headers = {}
        if username is not None:
            headers['Authorization'] = 'Basic ' + base64.b64encode(
                (username + ':' + secret).encode()).decode()
        if data is not None:
            headers['Content-Type'] = ('application/x-www-form-urlencoded' if form
                                       else 'application/json')
        req = urllib.request.Request(root + path, data=data, headers=headers)
        try:
            response = urllib.request.urlopen(req, context=trust if trusted else
                                              ssl.create_default_context(), timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read(65537)
            require(len(body) <= 65536, 'finite component response')
            return response.status, json.loads(body)

    def control(value, expected=200, raw=False, authorized=True):
        data = value if raw else json.dumps(value, separators=(',', ':')).encode()
        status, body = request('/control', data, 'control',
                               server.state.control_secret if authorized else 'incorrect')
        check(status == expected, 'HTTPS control status ' + str(expected))
        check(body == ({'changed': True} if expected == 200 else
                       {'error': 'invalid control credential'} if expected == 401 else
                       {'error': 'invalid_request'}), 'fixed control envelope')

    def state_fingerprint():
        state = server.state
        with state.lock:
            return (state.generation, state.ttl, state.token_policy, set(state.outage),
                    set(state.revoked), state.control_count)

    def token():
        status, envelope = request('/issuer/token', b'grant_type=client_credentials',
                                   'partitionline-probe', server.state.client_secret, form=True)
        check(status == 200, 'actual client-credentials HTTPS token response')
        check(set(envelope) == {'access_token', 'token_type', 'expires_in', 'scope'}
              and envelope['token_type'] == 'Bearer' and envelope['scope'] == 'kafka'
              and type(envelope['expires_in']) is int and envelope['expires_in'] > 0,
              'fixed OAuth response schema')
        return envelope['access_token']

    def introspect(proof):
        data = urllib.parse.urlencode({'token': proof, 'token_type_hint': 'access_token'}).encode()
        status, result = request('/issuer/introspect', data, 'partitionline-probe',
                                 server.state.client_secret, form=True)
        check(status == 200, 'actual authenticated HTTPS introspection')
        return result

    def decode(part):
        return base64.urlsafe_b64decode(part + '=' * ((4 - len(part) % 4) % 4))

    def verify(proof, generation, signature=None):
        header, claims, encoded_signature = proof.split('.')
        signing = (header + '.' + claims).encode('ascii')
        signature_path = scratch / 'verification.signature'
        signature_path.write_bytes(decode(encoded_signature) if signature is None else signature)
        result = subprocess.run(['openssl', 'dgst', '-sha256', '-verify',
            str(scratch / ('signing-' + str(generation) + '.public.pem')),
            '-signature', str(signature_path)], input=signing, capture_output=True, timeout=5)
        return result.returncode == 0 and result.stdout.strip() == b'Verified OK'

    try:
        status, discovery = request('/issuer/.well-known/openid-configuration')
        check(status == 200 and discovery['issuer'] == server.state.issuer
              and discovery['token_endpoint'] == root + '/issuer/token',
              'actual trusted discovery')
        untrusted_failed = False
        try:
            request('/issuer/keys', trusted=False)
        except urllib.error.URLError:
            untrusted_failed = True
        check(untrusted_failed, 'untrusted CA rejected before HTTP')
        for generation in (0, 1):
            status, _ = request('/issuer/keys')
            check(status == 200, 'actual HTTPS JWKS response')
            for policy in issuer.TOKEN_POLICIES:
                control({'generation': generation, 'token_policy': policy, 'ttl': 60})
                before = int(time.time())
                proof = token()
                after = int(time.time())
                parts = proof.split('.')
                check(len(parts) == 3, 'compact RS256 token schema')
                header, claims = json.loads(decode(parts[0])), json.loads(decode(parts[1]))
                expected_header_keys = {'alg', 'kid'} if policy == 'missing_typ' else {'alg', 'kid', 'typ'}
                expected_claim_keys = {'iss', 'aud', 'sub', 'iat', 'exp', 'jti', 'scope', 'token_use'}
                if policy == 'missing_subject':
                    expected_claim_keys.remove('sub')
                if policy == 'future_nbf':
                    expected_claim_keys.add('nbf')
                check(set(header) == expected_header_keys and set(claims) == expected_claim_keys,
                      'exact declared policy schema')
                check(header['alg'] == 'RS256' and header['kid'] == 'synthetic-' + str(generation)
                      and ('typ' not in header if policy == 'missing_typ' else
                           header['typ'] == ('access+jwt' if policy == 'wrong_typ' else 'at+jwt')),
                      'exact protected token header')
                check(claims['iss'] == server.state.issuer + ('/untrusted' if policy == 'wrong_issuer' else '')
                      and claims['aud'] == (['untrusted-partitionline'] if policy == 'wrong_audience'
                                            else ['partitionline'])
                      and claims.get('sub') == (None if policy == 'missing_subject' else 'issuer-probe')
                      and claims['scope'] == 'kafka' and claims['token_use'] == 'access',
                      'exact declared identity audience subject scope')
                check(type(claims['iat']) is int and type(claims['exp']) is int
                      and before - (120 if policy == 'expired' else 0) <= claims['iat']
                          <= after - (120 if policy == 'expired' else 0)
                      and claims['exp'] - claims['iat'] == (120 if policy == 'future_nbf' else 60)
                      and (policy != 'future_nbf' or (claims['nbf'] == claims['iat'] + 60
                                                    and claims['nbf'] < claims['exp'])),
                      'exact bounded time policy')
                check(verify(proof, generation), 'actual independent OpenSSL RS256 verify')
                token_hash = hashlib.sha256(proof.encode()).hexdigest()
                check(server.state.issued[token_hash] == claims
                      and server.state.issued_metadata[token_hash]['policy'] == policy
                      and server.state.issued_metadata[token_hash]['generation'] == generation,
                      'exact issued hash policy generation binding')
                observed = introspect(proof)
                check(observed == ({'active': False} if policy == 'expired' else {'active': True, **claims}),
                      'introspection binds exact issued claims and actual expiration')
                cases.append({'generation': generation, 'token_policy': policy,
                    'token_sha256': token_hash, 'protected_header': header, 'claims': claims,
                    'signature_verified_by_openssl': True,
                    'introspection_active': observed['active'],
                    'broker_or_sdk_verdict': None})
                if generation == 0 and policy == 'valid':
                    valid_proof = proof
                    signature = bytearray(decode(parts[2]))
                    signature[0] ^= 1
                    check(not verify(proof, generation, bytes(signature)), 'damaged signature rejected by OpenSSL')
                    check(not verify(proof, 1), 'wrong generation public key rejected by OpenSSL')
        control({'token_policy': 'valid', 'generation': 0})
        invalid = [None, [], {}, {'token_policy': 'arbitrary'}, {'token_policy': True},
            {'token_policy': None}, {'token_policy': {}}, {'token_policy': []},
            {'generation': True}, {'generation': 2}, {'ttl': 0}, {'ttl': 301}, {'ttl': True},
            {'outage': ['/not-declared']}, {'outage': ['/issuer/token', '/issuer/token']},
            {'outage': [None]}, {'outage': 'all'}, {'revoke_sha256': '0' * 64},
            {'revoke_sha256': 'X' * 64}, {'claims': {'sub': 'arbitrary'}},
            {'generation': 1, 'token_policy': 'invalid'},
            {'generation': 1, 'revoke_sha256': '0' * 64}]
        for value in invalid:
            before = state_fingerprint()
            control(value, expected=400)
            check(state_fingerprint() == before, 'invalid control is atomic')
        for raw in (b'{"token_policy":"valid","token_policy":"wrong_typ"}',
                    b'{"token_policy":', b'x' * 16385, b'[' * 2000 + b'0' + b']' * 2000):
            before = state_fingerprint()
            control(raw, expected=400, raw=True)
            check(state_fingerprint() == before, 'duplicate malformed or oversized control is atomic')
        before = state_fingerprint()
        control({'token_policy': 'wrong_typ'}, expected=401, authorized=False)
        check(state_fingerprint() == before, 'unauthorized control cannot mutate policy')
        status, _ = request('/issuer/token', b'grant_type=client_credentials',
                            'partitionline-probe', 'incorrect', form=True)
        check(status == 401, 'token client authentication required')
        check(introspect(valid_proof + 'x') == {'active': False}, 'modified unissued token hash inactive')
        control({'revoke_sha256': hashlib.sha256(valid_proof.encode()).hexdigest()})
        check(introspect(valid_proof) == {'active': False}, 'actual issued hash revocation inactive')
        control({'outage': ['/issuer/token']})
        status, _ = request('/issuer/token', b'grant_type=client_credentials',
                            'partitionline-probe', server.state.client_secret, form=True)
        check(status == 503, 'actual declared token outage')
        control({'outage': []})
        check(introspect(token())['active'], 'positive acquisition recovery after controlled outage')
        # Guard exhaustion is an explicitly injected component precondition;
        # no claim that 256 actual tokens/controls were generated is made.
        exhausted = issuer.State(scratch)
        exhausted.issuer = server.state.issuer
        exhausted.control_count = 256
        blocked = False
        try:
            exhausted.apply_control({'token_policy': 'wrong_typ'})
        except RuntimeError:
            blocked = True
        check(blocked and exhausted.token_policy == 'valid', 'injected finite control ceiling')
        exhausted.issued = {str(index): {} for index in range(256)}
        blocked = False
        try:
            exhausted.token()
        except RuntimeError:
            blocked = True
        check(blocked, 'injected finite issued ceiling before signing')
        exhausted.events = [{}] * 4096
        blocked = False
        try:
            exhausted.record('/issuer/token', 200)
        except RuntimeError:
            blocked = True
        check(blocked and len(exhausted.events) == 4096, 'injected finite event ceiling')
        passed = True
    except BaseException:
        failure = 'controlled component assertion or bounded HTTPS/OpenSSL failure'
    finally:
        server.shutdown()
        owner.join(timeout=3)
        server.server_close()
        joined = not owner.is_alive()
        public = server.state.public_metadata()
        record = {'schema_version': 1, 'passed': passed and joined,
            'test_kind': 'actual synthetic issuer HTTPS/OpenSSL component execution',
            'source_sha': '6e498e3054180449520767ac040270bb1ba0e61b',
            'inputs': [{'path': path.name, 'bytes': len(path.read_bytes()),
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                       'full_mode': oct(path.stat().st_mode & 0o7777)}
                      for path in (source, source.with_name('original-issuer.py'), Path(__file__))],
            'assertion_count': len(checks), 'checks': checks, 'cases': cases,
            'actual_signed_policy_cases': len(cases), 'authority_public_metadata': public,
            'issuer_owner_joined': joined, 'failure_classification': failure,
            'limits': {'issued_tokens': 256, 'controls': 256, 'http_events': 4096,
                       'http_handlers': 8, 'request_body_bytes': 16384,
                       'response_bytes': 65536, 'socket_seconds': 3,
                       'openssl_command_seconds': 15, 'component_deadline_seconds': 120},
            'limitations': ['No SDK or broker was launched; no SASL rejection result',
                           'Provider-local rejection must remain separate from observed broker SASL rejection',
                           'Introspection active only means exact issued hash, no revocation and unexpired exp; it does not validate issuer/audience/typ/nbf/sub',
                           'Budget guards use labeled injected exhaustion preconditions',
                           'Private keys credentials signatures and compact tokens are excluded from reports']}
        (output / 'validation.json').write_text(json.dumps(record, indent=2) + '\n')
    return passed and joined


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    require(not args.scratch.resolve().is_relative_to(args.output.resolve())
            and not args.output.resolve().is_relative_to(args.scratch.resolve()),
            'private scratch and public reports are disjoint')
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    signal.signal(signal.SIGALRM, interrupt)
    signal.alarm(120)
    passed = run(args.scratch, args.output)
    signal.alarm(0)
    print(json.dumps({'passed': passed, 'validation': str(args.output / 'validation.json')}))
    raise SystemExit(0 if passed else 1)


if __name__ == '__main__':
    main()
