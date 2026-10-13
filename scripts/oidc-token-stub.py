#!/usr/bin/env python3
"""OIDC client_credentials stub for ci-auth-smoke.

Default POST /token returns an unsecured Kafka-style JWT (`alg=none`) whose
`sub` matches AUTH_OAUTH_PRINCIPAL. GET /jwks and POST /token/rs256 mint an
RS256 JWT for the cryptographically validating listener. Not for production.
"""

from __future__ import annotations

import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def unsecured_jwt(principal: str, lifetime: int) -> str:
    # Match partitionline::protocol::oauth::unsecured_jwt NumericDate %.3f shape.
    iat = float(int(time.time()))
    exp = iat + float(lifetime)
    header = b64url(b'{"alg":"none"}')
    claims = b64url(
        f'{{"sub":"{principal}","iat":{iat:.3f},"exp":{exp:.3f}}}'.encode()
    )
    return f"{header}.{claims}."


def int_to_b64(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return b64url(raw)


PRINCIPAL = os.environ.get("AUTH_OAUTH_PRINCIPAL", "alice")
ISSUER = os.environ.get("OIDC_STUB_ISSUER", "http://127.0.0.1:18080/")
AUDIENCE = os.environ.get("OIDC_STUB_AUD", "partitionline-smoke")
TOKEN_LIFETIME = int(os.environ.get("OIDC_STUB_EXPIRES_IN", "3600"))
KID = "pl-auth-smoke"

_PRIVATE = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PUBLIC = _PRIVATE.public_key()
_NUMBERS = _PUBLIC.public_numbers()
JWKS = {
    "keys": [
        {
            "kty": "RSA",
            "kid": KID,
            "use": "sig",
            "alg": "RS256",
            "n": int_to_b64(_NUMBERS.n),
            "e": int_to_b64(_NUMBERS.e),
        }
    ]
}


def signed_jwt(principal: str, lifetime: int) -> str:
    now = int(time.time())
    header = b64url(json.dumps({"alg": "RS256", "kid": KID, "typ": "JWT"}).encode())
    claims = b64url(
        json.dumps(
            {
                "sub": principal,
                "iss": ISSUER,
                "aud": AUDIENCE,
                "iat": now,
                "exp": now + lifetime,
            }
        ).encode()
    )
    signing_input = f"{header}.{claims}".encode()
    signature = _PRIVATE.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{claims}.{b64url(signature)}"


UNSECURED = unsecured_jwt(PRINCIPAL, TOKEN_LIFETIME)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        return

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        length = int(self.headers.get("Content-Length", "0"))
        _ = self.rfile.read(length) if length else b""
        if path in ("/token", "/oauth/token", "/"):
            self._json(
                {
                    "access_token": UNSECURED,
                    "token_type": "Bearer",
                    "expires_in": TOKEN_LIFETIME,
                    "token_kind": "unsecured_jwt",
                }
            )
            return
        if path in ("/token/rs256", "/oauth/token/rs256"):
            self._json(
                {
                    "access_token": signed_jwt(PRINCIPAL, TOKEN_LIFETIME),
                    "token_type": "Bearer",
                    "expires_in": TOKEN_LIFETIME,
                    "token_kind": "rs256",
                }
            )
            return
        self.send_error(404)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if path in ("/jwks", "/.well-known/jwks.json"):
            self._json(JWKS)
            return
        self.send_error(404)


def main() -> None:
    host = os.environ.get("OIDC_STUB_HOST", "127.0.0.1")
    port = int(os.environ.get("OIDC_STUB_PORT", "18080"))
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(
        f"oidc-token-stub: http://{host}:{port}/token unsecured + /token/rs256 + /jwks sub={PRINCIPAL}",
        flush=True,
    )
    httpd.serve_forever()


if __name__ == "__main__":
    main()
