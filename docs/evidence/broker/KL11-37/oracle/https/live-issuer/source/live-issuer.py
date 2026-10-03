#!/usr/bin/env python3
"""Additive, finite signed-token policies for synthetic live OIDC peers.

The original independent HTTPS issuer is preserved byte for byte beside this
module. Private keys, credentials and live tokens remain in scratch or memory.
This module establishes no broker acceptance/rejection or SDK wire result.
"""
import hashlib
import http.server
import importlib.util
import json
from pathlib import Path
import re
import secrets
import ssl
import sys
import threading
import time

ORIGINAL_SHA256 = '0c3615c44652d6ef9398c39e6c9ffc5e5486055ece62961a3784bbd5b6f4147e'
original_path = Path(__file__).with_name('original-issuer.py')
if hashlib.sha256(original_path.read_bytes()).hexdigest() != ORIGINAL_SHA256:
    raise RuntimeError('original issuer source identity mismatch')
spec = importlib.util.spec_from_file_location('preserved_independent_issuer', original_path)
base = importlib.util.module_from_spec(spec)
sys.dont_write_bytecode = True
spec.loader.exec_module(base)
prepare = base.prepare
command = base.command
b64 = base.b64
digest = base.digest

TOKEN_POLICIES = ('valid', 'wrong_issuer', 'wrong_audience', 'expired',
                  'future_nbf', 'wrong_typ', 'missing_typ', 'missing_subject')
ROUTES = frozenset(('/issuer/keys', '/issuer/.well-known/openid-configuration',
                    '/issuer/introspect', '/issuer/token'))


class State(base.State):
    def __init__(self, directory):
        super().__init__(directory)
        self.token_policy = 'valid'
        self.control_count = 0
        self.issued_metadata = {}
        self.endpoint_counts = {}
        self.policy_counts = {policy: 0 for policy in TOKEN_POLICIES}

    def record(self, endpoint, status):
        # Unrecognized request targets cannot place attacker text in evidence.
        endpoint = endpoint if endpoint in ROUTES or endpoint == '/control' else 'unknown'
        with self.lock:
            if len(self.events) >= 4096:
                raise RuntimeError('finite issuer event ceiling')
            self.events.append({'ordinal': len(self.events), 'endpoint': endpoint,
                                'status': status, 'epoch': int(time.time())})
            counter = endpoint + ':' + str(status)
            self.endpoint_counts[counter] = self.endpoint_counts.get(counter, 0) + 1

    def apply_control(self, value):
        if type(value) is not dict or not value or len(value) > 5:
            raise ValueError('finite control object required')
        if set(value) - {'generation', 'ttl', 'outage', 'revoke_sha256', 'token_policy'}:
            raise ValueError('unknown control')
        if 'generation' in value and (type(value['generation']) is not int
                                      or value['generation'] not in (0, 1)):
            raise ValueError('invalid generation')
        if 'ttl' in value and (type(value['ttl']) is not int or not 1 <= value['ttl'] <= 300):
            raise ValueError('invalid lifetime')
        if 'token_policy' in value and (type(value['token_policy']) is not str
                                        or value['token_policy'] not in TOKEN_POLICIES):
            raise ValueError('invalid token policy')
        if 'outage' in value:
            routes = value['outage']
            if (type(routes) is not list or len(routes) > 4
                    or any(type(route) is not str or route not in ROUTES for route in routes)
                    or len(set(routes)) != len(routes)):
                raise ValueError('invalid outage routes')
        if 'revoke_sha256' in value and (type(value['revoke_sha256']) is not str
                                         or not re.fullmatch('[0-9a-f]{64}', value['revoke_sha256'])):
            raise ValueError('invalid issued hash')
        with self.lock:
            if self.control_count >= 256:
                raise RuntimeError('finite control ceiling')
            if 'revoke_sha256' in value and value['revoke_sha256'] not in self.issued:
                raise ValueError('unknown issued hash')
            # Validate everything before mutation, including issued-hash lookup.
            self.generation = value.get('generation', self.generation)
            self.ttl = value.get('ttl', self.ttl)
            self.token_policy = value.get('token_policy', self.token_policy)
            if 'outage' in value:
                self.outage = set(value['outage'])
            if 'revoke_sha256' in value:
                self.revoked.add(value['revoke_sha256'])
            self.control_count += 1

    def token(self):
        with self.lock:
            if len(self.issued) >= 256:
                raise RuntimeError('finite issued token ceiling')
            generation, policy, now = self.generation, self.token_policy, int(time.time())
            claims = {'iss': self.issuer, 'aud': ['partitionline'], 'sub': 'issuer-probe',
                      'iat': now, 'exp': now + self.ttl, 'jti': secrets.token_hex(16),
                      'scope': 'kafka', 'token_use': 'access'}
            header = {'alg': 'RS256', 'kid': self.keys[generation]['kid'], 'typ': 'at+jwt'}
            if policy == 'wrong_issuer':
                claims['iss'] = self.issuer + '/untrusted'
            elif policy == 'wrong_audience':
                claims['aud'] = ['untrusted-partitionline']
            elif policy == 'expired':
                # Positive OAuth expires_in does not make the JWT unexpired.
                # An SDK may reject the actual JWT locally before SASL.
                claims['iat'], claims['exp'] = now - 120, now - 60
            elif policy == 'future_nbf':
                claims['nbf'] = now + 60
                claims['exp'] = now + max(self.ttl, 120)
            elif policy == 'wrong_typ':
                header['typ'] = 'access+jwt'
            elif policy == 'missing_typ':
                del header['typ']
            elif policy == 'missing_subject':
                del claims['sub']
            signing = (b64(json.dumps(header, separators=(',', ':')).encode()) + '.' +
                       b64(json.dumps(claims, separators=(',', ':')).encode())).encode('ascii')
            signature = command(['openssl', 'dgst', '-sha256', '-sign',
                                 str(self.directory / ('signing-' + str(generation) + '.key'))],
                                input=signing)
            token = signing.decode('ascii') + '.' + b64(signature)
            token_hash = digest(token.encode())
            self.issued[token_hash] = claims
            self.issued_metadata[token_hash] = {'policy': policy, 'generation': generation,
                'issued_epoch': claims['iat'], 'expires_epoch': claims['exp'],
                'subject': claims.get('sub')}
            self.policy_counts[policy] += 1
            return token, claims

    def public_metadata(self):
        with self.lock:
            return {'token_policy': self.token_policy, 'generation': self.generation,
                    'control_count': self.control_count,
                    'endpoint_counts': dict(self.endpoint_counts),
                    'policy_counts': dict(self.policy_counts),
                    'issued': [{'token_sha256': token_hash, **metadata}
                               for token_hash, metadata in self.issued_metadata.items()]}


class Handler(base.Handler):
    def do_POST(self):
        if self.path != '/control':
            return super().do_POST()
        try:
            data = self.body()
            state = self.server.state
            if not self.authorized('control', state.control_secret):
                self.reply(401, {'error': 'invalid control credential'})
                return
            def unique_object(items):
                value = {}
                for key, field in items:
                    if key in value:
                        raise ValueError('duplicate control field')
                    value[key] = field
                return value
            value = json.loads(data, object_pairs_hook=unique_object)
            state.apply_control(value)
            self.reply(200, {'changed': True})
        except (ValueError, UnicodeError, RecursionError):
            self.reply(400, {'error': 'invalid_request'})
        except RuntimeError:
            self.reply(503, {'error': 'finite authority budget'})


class Server(base.Server):
    def __init__(self, directory, port=0):
        self.state = State(directory)
        self.slots = threading.BoundedSemaphore(8)
        http.server.ThreadingHTTPServer.__init__(self, ('127.0.0.1', port), Handler)
        self.state.issuer = 'https://localhost:' + str(self.server_port) + '/issuer'
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(directory / 'leaf.pem', directory / 'leaf.key')
        self.tls_context = context
