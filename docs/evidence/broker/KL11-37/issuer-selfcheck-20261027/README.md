# Synthetic issuer self-check

The independent issuer passed 15 cases across 20 HTTPS requests, covering TLS trust, signed RS256 tokens, token introspection, revocation, signing-key rotation, endpoint outages and clean shutdown. The OpenSSL verifier checked the signatures.

This run checks the test issuer only. It did not exercise the broker or any Kafka client, and does not establish OIDC or production qualification. The receipt records the command, source commit and scope; `result.json` includes hashes of synthetic tokens and public keys. The scratch directory containing private keys and client/control secrets is not part of this archive.

Verify the stored files from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/KL11-37/issuer-selfcheck-20261027
```
