# HTTP Basic over HTTPS, 19 November 2026

Both feature profiles used Rust 1.100.0 on Linux x86_64 and checked
the documented HTTP Basic listener in Redpanda 26.2.2. The fixture
generated a fresh credential and CA. Curl registered a schema and
the Rust companion looked up its ID and pinned subject version.
The all-feature lane also passed the earlier Avro Produce and Fetch
round trip. [Live workflow](https://github.com/mingley/partitionline/actions/runs/44640038645)

| Profile | Subject and version | Schema ID |
| --- | --- | ---: |
| Default | `pl-live-basic` v1 | 5 |
| All features | `pl-live-basic` v1 | 7 |

Both jobs verified cleanup of their container, volumes, certificates
and credential file. The pinned image digest, runtime reference and exact
source are in [verification.json](verification.json).

The first attempt failed an import formatting check. The next failed
during startup because the entrypoint split a multiline argument. The
passing fixture used a single argument for the same listener settings.
[Formatting](https://github.com/mingley/partitionline/actions/runs/44639320886) ·
[Startup](https://github.com/mingley/partitionline/actions/runs/44639591865) ·
[Offline checks](https://github.com/mingley/partitionline/actions/runs/44639591897)

Further checks cover bearer tokens, credential rotation and the
Kafka TLS round trip.
