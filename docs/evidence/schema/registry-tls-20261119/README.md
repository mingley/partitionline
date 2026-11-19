# Live HTTPS Schema Registry

Default and all features passed on Linux x86_64 with stable Rust 1.100.0.
The source was `908eab877fe8046d16569b5a87d3314ea721170f`. The workflow used Redpanda 26.2.2 from
`docker.redpanda.com/redpandadata/redpanda@sha256:a9dffb84f388a7a2d4ab1aaa541cd0b9a484f0f5c9b72cb54c551953352769ec`.
[Exact run](https://github.com/mingley/partitionline/actions/runs/44635048258)

The fixture has an HTTP listener for the cache and reference checks and a
separate HTTPS listener bound to loopback. It generates a one-day CA and
leaf certificate with `DNS:localhost` in the subject alternative names.
OpenSSL verifies the hostname and chain. Curl registers an Avro schema
over HTTPS. The Rust client then looks up its ID and pinned subject version
using the generated CA, and compares the returned schema and identity.

Both lanes checked formatting, image ID, peer version and removal of the
owned container and volumes. The first attempt failed because the HTTPS
port collided with Redpanda's proxy. The second listener now uses a
separate port.
[Port collision](https://github.com/mingley/partitionline/actions/runs/44633560008)

A combined registry and Kafka round trip reached its 30-second limit after
creating the topic. The HTTPS lookup checks in that run succeeded. Live
credentials and the combined Kafka round trip have separate checks.
[Combined attempt](https://github.com/mingley/partitionline/actions/runs/44634323113)
