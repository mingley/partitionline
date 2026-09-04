#!/usr/bin/env python3
"""Minimal OIDC client_credentials stub for ci-auth-smoke.

Serves POST /token (or /oauth/token) over HTTP and returns an unsecured
Kafka-style JWT (`alg=none`) whose `sub` matches AUTH_OAUTH_PRINCIPAL.
Accepts any Basic-auth client_id/secret. Not for production.
"""

from __future__ import annotations

import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def unsecured_jwt(principal: str) -> str:
    # Match partitionline::protocol::oauth::unsecured_jwt NumericDate %.3f shape.
    iat = float(int(time.time()))
    exp = iat + 3600.0
    header = b64url(b'{"alg":"none"}')
    claims = b64url(
        f'{{"sub":"{principal}","iat":{iat:.3f},"exp":{exp:.3f}}}'.encode()
    )
    return f"{header}.{claims}."


PRINCIPAL = os.environ.get("AUTH_OAUTH_PRINCIPAL", "alice")
TOKEN = unsecured_jwt(PRINCIPAL)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        return

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path not in ("/token", "/oauth/token", "/"):
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        _ = self.rfile.read(length) if length else b""
        body = json.dumps(
            {
                "access_token": TOKEN,
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_error(404)


def main() -> None:
    host = os.environ.get("OIDC_STUB_HOST", "127.0.0.1")
    port = int(os.environ.get("OIDC_STUB_PORT", "18080"))
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"oidc-token-stub: http://{host}:{port}/token sub={PRINCIPAL}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
