# KL06-06 short-lived secure-broker lifecycle matrix

`REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh` against native Apache Kafka
4.1.0 (`/tmp/kafka_4.1.0`) with disposable local TLS material and a loopback
OIDC stub.

The broker sets `connections.max.reauth.ms=5000`. Admin, manual Consumer, and
classic group RPCs succeed after the 85 percent session window. Idle reconnect
and a bad PLAIN password fail closed. Killing the IdP fails OIDC produce;
restarting it recovers. A JWKS-validating `SASL_SSL_OIDC` listener accepts
RS256 tokens and rejects the unsecured JWT that the 9192 test validator
accepts.

Peer identity: `native:4.1.0 path=/tmp/kafka_4.1.0`.

Producer worker reauthentication remains KL06-11. No production-readiness or
speed ranking is claimed.

## Cases

See `lifecycle-report.json`. Failed attempts from earlier runs and expected
rejection logs are under `failed-attempts/`.

## Checks

See `docs/plan/evidence/KL06-06.json` for commands, the source pin, and limits.
