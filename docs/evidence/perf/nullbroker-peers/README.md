# Null-broker peer compatibility

Kafka Java clients 4.3.1, librdkafka 2.15.0 and franz-go 1.22.0 each acknowledge
512 records and validate 512 Fetch records with zero validation failures.
All six qualification processes joined, their groups emptied, and all three
listener ports rebound. The broker checks CRC, record counts and batch structure;
each SDK checks every fetched key, value, timestamp, header count and offset.

| Peer | Produce | Fetch | Metadata | Other observed requests |
|---|---:|---:|---:|---|
| Java 4.3.1 | 12 | 17 | 13 | ApiVersions 4 |
| librdkafka 2.15.0 | 10 | 16 | 13 | FindCoordinator 2; ApiVersions 3 |
| franz-go 1.22.0 | 12 | 17 | 13 | ApiVersions 5 rejected with v0 error 35; retries 4 |

The fixture serves a seeded Fetch stream independently of accepted Produce
batches. Produced keys and values match that stream; Produce uses current-time
SDK timestamps and Fetch uses its synthetic batch clock. These checks establish
protocol compatibility for one partition, uncompressed 100-byte values, zero
headers and non-idempotent Produce. They do not establish persisted readback,
consumer-group support, transaction durability or comparative speed.

The original pinned C client acknowledged no records because Produce v10 was
advertised but unhandled. The broker now advertises implemented ranges, handles
classic coordinator discovery and provides the standard ApiVersions downgrade
response. Socket rejection also shuts down the descriptor immediately. Optional
API-version tracing is explicit and disabled by default.

Source `42edda4ec0a903f4595fad42ebe89c8cfe488a71` supplies the qualified peers and
broker. Thirty Rust tests, formatting and strict Clippy pass on stable Rust 1.99.0.
Their Rust inputs are byte-identical to checked source
`68278185d202d9bf309f0df6f021b4d2a4b429de`; only peer timestamp settings and README
changed afterward. The actual source and binary comparison is retained.

`qualification.json` records limits and outcomes. `study/` retains the actual
old-source failures, intermediate failures, compiler logs, executable bindings,
SDK logs and raw artifacts. SDK binaries, Go module sources and Apache schemas
are retained. The official Go compiler archive remains an external input pinned
by its downloaded metadata and digest. `archive-manifest.json` maps stored and
original hashes. Run `python3 -B verify-evidence.py` and `sha256sum -c SHA256SUMS`
from this directory to verify the archive.
