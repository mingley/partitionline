#!/usr/bin/env python3
"""Finite synthetic HTTPS issuer for independent OIDC acquisition/failure probes.

Private CA, signing keys, test credentials and live bearers stay in an ephemeral
0600 scratch directory or memory. Receipts contain hashes and public claims.
This is a test authority, not an assertion about a third-party IdP.
"""
import argparse
import base64
import hashlib
import http.server
import json
import os
from pathlib import Path
import secrets
import ssl
import subprocess
import threading
import time
import urllib.parse


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def command(args, **kwargs):
    result = subprocess.run(args, capture_output=True, timeout=15, **kwargs)
    if result.returncode:
        raise RuntimeError('test cryptographic command failed: ' + args[1])
    return result.stdout


def prepare(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    previous = os.umask(0o077)
    try:
        ca, leaf = directory / 'ca', directory / 'leaf'
        command(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                 '-keyout', str(ca.with_suffix('.key')), '-out', str(ca.with_suffix('.pem')),
                 '-days', '1', '-subj', '/CN=partitionline synthetic issuer test CA',
                 '-addext', 'basicConstraints=critical,CA:TRUE',
                 '-addext', 'keyUsage=critical,keyCertSign,cRLSign'])
        command(['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes',
                 '-keyout', str(leaf.with_suffix('.key')), '-out', str(leaf.with_suffix('.csr')),
                 '-subj', '/CN=localhost'])
        extensions = directory / 'leaf-extensions'
        extensions.write_text('subjectAltName=DNS:localhost,IP:127.0.0.1\n'
                              'basicConstraints=critical,CA:FALSE\n'
                              'keyUsage=critical,digitalSignature,keyEncipherment\n'
                              'extendedKeyUsage=serverAuth\n')
        command(['openssl', 'x509', '-req', '-in', str(leaf.with_suffix('.csr')),
                 '-CA', str(ca.with_suffix('.pem')), '-CAkey', str(ca.with_suffix('.key')),
                 '-set_serial', str(secrets.randbits(120) + 1), '-days', '1',
                 '-extfile', str(extensions), '-out', str(leaf.with_suffix('.pem'))])
        command(['openssl', 'x509', '-in', str(ca.with_suffix('.pem')), '-outform', 'DER',
                 '-out', str(ca.with_suffix('.der'))])
        keys = []
        for ordinal in range(2):
            stem = directory / ('signing-' + str(ordinal))
            command(['openssl', 'genpkey', '-algorithm', 'RSA', '-pkeyopt',
                     'rsa_keygen_bits:2048', '-out', str(stem.with_suffix('.key'))])
            public = command(['openssl', 'pkey', '-in', str(stem.with_suffix('.key')), '-pubout'])
            stem.with_suffix('.public.pem').write_bytes(public)
            modulus = command(['openssl', 'rsa', '-in', str(stem.with_suffix('.key')),
                               '-noout', '-modulus']).decode('ascii').strip()
            if not modulus.startswith('Modulus='):
                raise RuntimeError('unexpected public RSA modulus encoding')
            keys.append({'kty': 'RSA', 'use': 'sig', 'alg': 'RS256',
                         'kid': 'synthetic-' + str(ordinal),
                         'n': b64(bytes.fromhex(modulus.split('=', 1)[1])),
                         'e': b64((65537).to_bytes(3, 'big'))})
        (directory / 'public-jwks.json').write_text(json.dumps({'keys': keys}) + '\n')
        for name in ['client-secret', 'control-secret']:
            (directory / name).write_text(secrets.token_urlsafe(24))
    finally:
        os.umask(previous)


class State:
    def __init__(self, directory):
        self.directory = directory
        self.keys = json.loads((directory / 'public-jwks.json').read_text())['keys']
        self.lock = threading.Lock()
        self.issuer = None
        self.generation = 0
        self.outage = set()
        self.ttl = 60
        self.revoked = set()
        self.issued = {}
        self.events = []
        self.client_secret = (directory / 'client-secret').read_text()
        self.control_secret = (directory / 'control-secret').read_text()

    def record(self, endpoint, status):
        with self.lock:
            if len(self.events) >= 4096:
                raise RuntimeError('finite issuer event ceiling')
            self.events.append({'ordinal': len(self.events), 'endpoint': endpoint,
                                'status': status, 'epoch': int(time.time())})

    def token(self):
        with self.lock:
            if len(self.issued) >= 256:
                raise RuntimeError('finite issued token ceiling')
            ordinal = self.generation
            now = int(time.time())
            claims = {'iss': self.issuer, 'aud': ['partitionline'], 'sub': 'issuer-probe',
                      'iat': now, 'exp': now + self.ttl, 'jti': secrets.token_hex(16),
                      'scope': 'kafka', 'token_use': 'access'}
            header = {'alg': 'RS256', 'kid': self.keys[ordinal]['kid'], 'typ': 'at+jwt'}
            signing = (b64(json.dumps(header, separators=(',', ':')).encode()) + '.' +
                       b64(json.dumps(claims, separators=(',', ':')).encode())).encode('ascii')
            signature = command(['openssl', 'dgst', '-sha256', '-sign',
                                 str(self.directory / ('signing-' + str(ordinal) + '.key'))],
                                input=signing)
            token = signing.decode('ascii') + '.' + b64(signature)
            self.issued[digest(token.encode())] = claims
            return token, claims


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True

    def __init__(self, directory, port=0):
        self.state = State(directory)
        self.slots = threading.BoundedSemaphore(8)
        super().__init__(('127.0.0.1', port), Handler)
        self.state.issuer = 'https://localhost:' + str(self.server_port) + '/issuer'
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(directory / 'leaf.pem', directory / 'leaf.key')
        self.tls_context = context

    def get_request(self):
        request, address = super().get_request()
        request.settimeout(3)
        try:
            return self.tls_context.wrap_socket(request, server_side=True), address
        except BaseException:
            request.close()
            raise

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.connection.settimeout(3)

    def log_message(self, *_args):
        pass  # Authorization, live bearers and client credentials never enter receipts.

    def reply(self, status, value):
        data = json.dumps(value, separators=(',', ':')).encode()
        if len(data) > 65536:
            raise RuntimeError('finite issuer response ceiling')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True
        self.server.state.record(self.path.split('?', 1)[0], status)

    def authorized(self, username, secret):
        expected = 'Basic ' + base64.b64encode((username + ':' + secret).encode()).decode()
        return secrets.compare_digest(self.headers.get('Authorization', ''), expected)

    def body(self):
        lengths = self.headers.get_all('Content-Length', [])
        if len(lengths) != 1 or self.headers.get('Transfer-Encoding'):
            raise ValueError('one bounded content length required')
        length = int(lengths[0])
        if not 0 <= length <= 16384:
            raise ValueError('request body ceiling')
        data = self.rfile.read(length)
        if len(data) != length:
            raise ValueError('incomplete request body')
        return data

    def do_GET(self):
        state = self.server.state
        with state.lock:
            outage, generation = self.path in state.outage, state.generation
        if outage:
            self.reply(503, {'error': 'synthetic authority outage'})
        elif self.path == '/issuer/.well-known/openid-configuration':
            self.reply(200, {'issuer': state.issuer, 'jwks_uri': state.issuer + '/keys',
                             'token_endpoint': state.issuer + '/token',
                             'introspection_endpoint': state.issuer + '/introspect',
                             'grant_types_supported': ['client_credentials'],
                             'token_endpoint_auth_methods_supported': ['client_secret_basic']})
        elif self.path == '/issuer/keys':
            self.reply(200, {'keys': [state.keys[generation]]})
        else:
            self.reply(404, {'error': 'unknown route'})

    def do_POST(self):
        state = self.server.state
        try:
            data = self.body()
            with state.lock:
                outage = self.path in state.outage
            if outage:
                self.reply(503, {'error': 'synthetic authority outage'})
                return
            if self.path == '/control':
                if not self.authorized('control', state.control_secret):
                    self.reply(401, {'error': 'invalid control credential'})
                    return
                value = json.loads(data)
                if set(value) - {'generation', 'ttl', 'outage', 'revoke_sha256'}:
                    raise ValueError('unknown control')
                with state.lock:
                    if 'generation' in value:
                        if type(value['generation']) is not int or value['generation'] not in [0, 1]:
                            raise ValueError('invalid generation')
                        state.generation = value['generation']
                    if 'ttl' in value:
                        if type(value['ttl']) is not int or not 1 <= value['ttl'] <= 300:
                            raise ValueError('invalid token lifetime')
                        state.ttl = value['ttl']
                    if 'outage' in value:
                        allowed = {'/issuer/keys', '/issuer/.well-known/openid-configuration',
                                   '/issuer/introspect', '/issuer/token'}
                        if not isinstance(value['outage'], list) or not set(value['outage']) <= allowed:
                            raise ValueError('invalid outage route')
                        state.outage = set(value['outage'])
                    if 'revoke_sha256' in value:
                        token_hash = value['revoke_sha256']
                        if not isinstance(token_hash, str) or len(token_hash) != 64:
                            raise ValueError('invalid revocation hash')
                        if token_hash not in state.issued:
                            raise ValueError('unknown issued token hash')
                        state.revoked.add(token_hash)
                self.reply(200, {'changed': True})
                return
            if not self.authorized('partitionline-probe', state.client_secret):
                self.reply(401, {'error': 'invalid_client'})
                return
            if self.headers.get_content_type() != 'application/x-www-form-urlencoded':
                raise ValueError('form encoding required')
            form = urllib.parse.parse_qs(data.decode('utf-8'), keep_blank_values=True,
                                         strict_parsing=True, max_num_fields=8)
            if any(len(v) != 1 for v in form.values()):
                raise ValueError('duplicate form field')
            if self.path == '/issuer/token':
                if form.get('grant_type') != ['client_credentials']:
                    raise ValueError('client credentials grant required')
                token, claims = state.token()
                self.reply(200, {'access_token': token, 'token_type': 'Bearer',
                                 'expires_in': claims['exp'] - claims['iat'], 'scope': 'kafka'})
            elif self.path == '/issuer/introspect':
                if form.get('token_type_hint', ['access_token']) != ['access_token']:
                    raise ValueError('access token hint required')
                token = form.get('token', [''])[0]
                token_hash = digest(token.encode())
                with state.lock:
                    claims = state.issued.get(token_hash)
                    active = claims is not None and token_hash not in state.revoked and claims['exp'] > int(time.time())
                    result = {'active': True, **claims} if active else {'active': False}
                self.reply(200, result)
            else:
                self.reply(404, {'error': 'unknown route'})
        except (ValueError, UnicodeError):
            self.reply(400, {'error': 'invalid_request'})
        except RuntimeError:
            self.reply(503, {'error': 'finite authority budget'})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--port', type=int, default=0)
    args = parser.parse_args()
    prepare(args.scratch)
    server = Server(args.scratch, args.port)
    print(json.dumps({'issuer': server.state.issuer, 'ca_der_file': str(args.scratch / 'ca.der'),
                      'client_id': 'partitionline-probe',
                      'client_secret_file': str(args.scratch / 'client-secret'),
                      'control_secret_file': str(args.scratch / 'control-secret')}), flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
