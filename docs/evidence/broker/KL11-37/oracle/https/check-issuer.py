#!/usr/bin/env python3
"""Exercise real trusted HTTPS and independently verify synthetic JWT signatures."""
import argparse
import base64
import hashlib
import http.client
import importlib.util
import json
from pathlib import Path
import ssl
import threading
import urllib.parse


ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('synthetic_issuer', ROOT / 'issuer.py')
issuer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(issuer)


def require(value, reason):
    if not value:
        raise ValueError(reason)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    issuer.prepare(args.scratch)
    server = issuer.Server(args.scratch)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05})
    thread.start()
    checks = []
    context = ssl.create_default_context(cafile=str(args.scratch / 'ca.pem'))

    def request(path, form=None, control=None, authorized=True, trusted=True):
        client = http.client.HTTPSConnection('localhost', server.server_port,
                                             context=context if trusted else ssl.create_default_context(),
                                             timeout=3)
        headers = {}
        body = None
        method = 'GET'
        if form is not None or control is not None:
            method = 'POST'
            if control is not None:
                body = json.dumps(control).encode()
                username, secret = 'control', server.state.control_secret
                headers['Content-Type'] = 'application/json'
            else:
                body = urllib.parse.urlencode(form).encode()
                username, secret = 'partitionline-probe', server.state.client_secret
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
            if authorized:
                headers['Authorization'] = 'Basic ' + base64.b64encode((username + ':' + secret).encode()).decode()
        try:
            client.request(method, path, body, headers)
            response = client.getresponse()
            data = response.read(65537)
            require(len(data) <= 65536, 'bounded actual response')
            return response.status, json.loads(data)
        finally:
            client.close()

    def verified(token, ordinal):
        parts = token.split('.')
        require(len(parts) == 3, 'JWT compact segments')
        signature = base64.urlsafe_b64decode(parts[2] + '=' * (-len(parts[2]) % 4))
        signature_file = args.scratch / 'verification.sig'
        signature_file.write_bytes(signature)
        output = issuer.command(['openssl', 'dgst', '-sha256', '-verify',
                                 str(args.scratch / ('signing-' + str(ordinal) + '.public.pem')),
                                 '-signature', str(signature_file)], input=(parts[0] + '.' + parts[1]).encode())
        require(output.strip() == b'Verified OK', 'independent OpenSSL verify result')
        header = json.loads(base64.urlsafe_b64decode(parts[0] + '=' * (-len(parts[0]) % 4)))
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + '=' * (-len(parts[1]) % 4)))
        require(header == {'alg': 'RS256', 'kid': 'synthetic-' + str(ordinal), 'typ': 'at+jwt'}, 'signed access header')
        require(claims['iss'] == server.state.issuer and claims['aud'] == ['partitionline'] and
                claims['sub'] == 'issuer-probe' and claims['exp'] - claims['iat'] == 60,
                'independent claims expectations')
        checks.append({'case': 'signature-generation-' + str(ordinal), 'passed': True,
                       'token_sha256': issuer.digest(token.encode()), 'key_id': header['kid'],
                       'public_key_sha256': issuer.digest((args.scratch / ('signing-' + str(ordinal) + '.public.pem')).read_bytes())})
        return claims

    try:
        try:
            request('/issuer/.well-known/openid-configuration', trusted=False)
        except ssl.SSLCertVerificationError:
            checks.append({'case': 'untrusted-ca-rejected', 'passed': True})
        else:
            raise ValueError('untrusted test CA accepted')
        status, discovery = request('/issuer/.well-known/openid-configuration')
        require(status == 200 and discovery['issuer'] == server.state.issuer and
                discovery['jwks_uri'] == server.state.issuer + '/keys', 'real HTTPS discovery binding')
        checks.append({'case': 'trusted-https-discovery', 'passed': True})
        status, keys = request('/issuer/keys')
        require(status == 200 and keys == {'keys': [server.state.keys[0]]}, 'initial JWKS generation')
        checks.append({'case': 'initial-jwks', 'passed': True})
        status, denied = request('/issuer/token', {'grant_type': 'client_credentials'}, authorized=False)
        require(status == 401 and denied == {'error': 'invalid_client'}, 'token client authentication')
        checks.append({'case': 'unauthenticated-token-rejected', 'passed': True})
        status, acquired = request('/issuer/token', {'grant_type': 'client_credentials'})
        require(status == 200 and acquired['token_type'] == 'Bearer', 'real HTTPS client credential acquisition')
        token = acquired['access_token']
        claims = verified(token, 0)
        status, authority = request('/issuer/introspect', {'token': token, 'token_type_hint': 'access_token'})
        require(status == 200 and authority == {'active': True, **claims}, 'exact positive introspection binding')
        checks.append({'case': 'positive-introspection', 'passed': True})
        status, changed = request('/control', control={'revoke_sha256': issuer.digest(token.encode())})
        require(status == 200 and changed == {'changed': True}, 'revocation control')
        status, authority = request('/issuer/introspect', {'token': token})
        require(status == 200 and authority == {'active': False}, 'actual issued token revocation')
        checks.append({'case': 'revoked-introspection-inactive', 'passed': True})
        request('/control', control={'generation': 1})
        status, keys = request('/issuer/keys')
        require(status == 200 and keys == {'keys': [server.state.keys[1]]}, 'atomic new JWKS generation')
        status, acquired = request('/issuer/token', {'grant_type': 'client_credentials'})
        require(status == 200, 'post-rotation token acquisition')
        verified(acquired['access_token'], 1)
        checks.append({'case': 'rotated-jwks-and-acquisition', 'passed': True})
        for path in ['/issuer/keys', '/issuer/.well-known/openid-configuration', '/issuer/introspect', '/issuer/token']:
            request('/control', control={'outage': [path]})
            form = {'token': token} if path.endswith('/introspect') else {'grant_type': 'client_credentials'} if path.endswith('/token') else None
            status, result = request(path, form=form)
            require(status == 503 and result == {'error': 'synthetic authority outage'}, 'explicit authority outage')
            checks.append({'case': 'outage-' + path.rsplit('/', 1)[-1], 'passed': True})
        request('/control', control={'outage': []})
        status, keys = request('/issuer/keys')
        require(status == 200 and keys == {'keys': [server.state.keys[1]]}, 'authority recovery retains new generation')
        checks.append({'case': 'authority-recovery', 'passed': True})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        require(not thread.is_alive(), 'joined issuer shutdown')
    checks.append({'case': 'joined-issuer-shutdown', 'passed': True})
    report = {'passed': True, 'checks': checks, 'assertion_cases': len(checks),
              'actual_https_requests': len(server.state.events), 'events': server.state.events,
              'sources': {p.name: issuer.digest(p.read_bytes()) for p in [ROOT / 'issuer.py', Path(__file__)]},
              'openssl_version': issuer.command(['openssl', 'version']).decode().strip(),
              'python_tls_version': ssl.OPENSSL_VERSION,
              'public_ca_sha256': issuer.digest((args.scratch / 'ca.der').read_bytes()),
              'scope': 'Synthetic issuer harness qualification only; no production broker/service/client execution or third-party IdP claim.',
              'secret_retention': 'No private keys, client/control credentials or compact live bearers in this receipt.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': True, 'cases': len(checks), 'https_requests': len(server.state.events)}))


if __name__ == '__main__':
    main()
