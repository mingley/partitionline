# KL06-12 RPC SASL reauthentication

Consumer, ConsumerGroup, ShareGroup, and Admin exclusive RPCs honor broker
`SaslAuthenticate` session lifetimes on one shared `BrokerConn::roundtrip`
path. `authenticate` stores the chosen mechanism. A later exclusive
roundtrip reauthenticates with reserved SASL correlation IDs when the 85%
lifetime threshold is due or the session has expired. Renewal sends
SaslHandshake and then SaslAuthenticate, matching live Kafka.

These checks use the in-tree mock broker. They do not qualify a live Kafka
broker, Producer workers (KL06-11), or the short-lived secure-broker matrix
(KL06-06). No production-readiness or speed ranking is claimed.

## Callers

| Caller | RPC after lifetime |
|---|---|
| Admin | `describe_cluster` |
| Manual Consumer | `partitions_for` |
| Classic ConsumerGroup | membership heartbeat, then `poll` |
| KIP-848 ConsumerGroup | membership heartbeat, then `poll` |
| ShareGroup | membership heartbeat, then `poll` |
| Admin + OIDC | `describe_cluster` through the shared token manager |

Failed PLAIN reauthentication returns broker error 58 and does not send the
application request. SaslAuthenticate v0 peers return `Unsupported` and
close the connection.

## Checks

See `docs/plan/evidence/KL06-12.json` for exact commands, counts, and limits.
