# Apache broker protocol sources

These archives retain byte-identical Apache Kafka protocol JSON, `ApiKeys.java`,
seven relevant generator sources, and the original `LICENSE` and `NOTICE`. They
are source inputs for the independent broker inventory. They do not add a runtime
dependency or implement a handler.

| Apache tag | Peeled source commit | Retained files | API keys |
| --- | --- | --- | --- |
| 4.1.2 | `c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c` | 207 | 93 |
| 4.2.1 | `18d5ecd939c8d510fdd72d0abb1f7099659dcd58` | 207 | 93 |
| 4.3.1 | `26b251a451ce941d3d7a55e6487bcb7f16b5ad48` | 208 | 93 |

[The matrix](../api-matrix.json) records the immutable original codeload archive
URL and SHA256, the deterministic retained archive SHA256, every retained file
SHA256, and all 93 request/response pairs for each release. The verifier also
anchors the reviewed archive digests independently of the matrix, so rewriting
the matrix's own checksums cannot bless changed source under an unchanged pin.

Run from the repository root:

```sh
python3 -B scripts/check-broker-api-matrix.py
python3 -B -m unittest discover -s tests/ci -p test_broker_api_matrix.py -v
```

To reproduce retention, obtain the exact codeload URLs recorded in the matrix
and save them as `4.1.2.tar.gz`, `4.2.1.tar.gz`, and `4.3.1.tar.gz` in a scratch
directory. The builder checks the full original archive hashes before reading
selected sources. It writes only to the selected output directory:

```sh
python3 -B tests/conformance/broker/upstream/retain-sources.py \
  --original-dir /path/to/original-archives --output-dir /path/to/scratch-output
```

The tar subset uses sorted paths, regular files, mode 0644, empty owner names,
zero UID/GID/mtime, USTAR format, and gzip with an empty filename, mtime zero, and
compression level 9. Source contents, including comments and license notices,
are preserved. The retained gzip bytes were built and reproduced with Python
3.12.14 and zlib 1.3.2; other compression library versions may produce different
gzip bytes and must not silently replace a reviewed digest. The verifier reads
archives in bounded memory without extracting
members, rejects unsafe paths, links, duplicate members, missing required files,
and corrupt archives or per-file hashes. The JSON comment scanner preserves
`//` and escaped quotes inside strings and rejects duplicate properties.

The inventory preserves both directions' valid, flexible, and deprecated
ranges, request listener types, latest-version instability, `clusterAction`,
forwarding, and valid-version header mappings. Keys 4–7 have `validVersions:
none`: the identifiers stay reserved after Apache removed the APIs in 4.0.
They have an explicit removed disposition and no listener or supported version.
Active APIs include broker, controller, and both-listener classifications;
controller APIs are part of the broker program's upstream contract even when
they are absent from the SDK's supported client subset.

The pinned generator returns request header 1 for classic bodies and 2 for
flexible bodies, and response header 0 for classic bodies and 1 for flexible
bodies. ApiVersions responses always use header 0, including flexible body
versions 3–4. Request-header `ClientId` keeps a classic nullable string with a
two-byte length even in a flexible header. Header mappings here enumerate valid
schema versions; they do not imply how a handler accepts unknown body versions.

Apache's `ApiKeys.java` advertises Produce minimum version 0 on the broker
listener as a librdkafka workaround, while both actual Produce schemas support
versions 3–13. The matrix records those facts separately. A future partitionline
broker must advertise only versions it actually implements and tests. This
inventory does not authorize a minimum-0 workaround or any API advertisement.

All active API features remain `implementation: missing` and `qualification:
not_run`. Removed keys are explicitly not applicable upstream. Compatibility,
durability, security, production, and performance qualification require separate
behavioral evidence.
