# Schema Registry HTTP response handling

The Registry HTTP reader now consumes informational responses before the final
response, except that HTTP 101 is returned as an unsupported status. It does
not wait for a body after 101, 204 or 304. The combined informational and final
headers share the existing 64 KiB limit.

The chunked decoder accepts only one `chunked` transfer coding. Mixed,
repeated or unsupported transfer codings are rejected instead of treating the
body as if no other coding were present. When both Transfer-Encoding and
Content-Length are present, the previous chunked precedence remains unchanged;
the client closes the connection. Permissive status parsing and Content-Encoding
handling are unchanged.

## Controls

The tests-only [baseline run](https://github.com/mingley/partitionline/actions/runs/42701370606)
failed all four focused steps: it returned 100 as final, failed a public lookup
after 103, waited for EOF on a bodyless response, and accepted an unsupported
coding. Both public baseline controls failed.

The exact [candidate](https://github.com/mingley/partitionline/commit/8f369a677660cab1478c010757c2c65f292b4d33)
passed [run 42702382087](https://github.com/mingley/partitionline/actions/runs/42702382087)
on x86 Linux with stable Rust 1.99.1. Three informational, one bodyless, one
transfer-coding and three public controls passed. The default, all-feature and
no-default suites passed 108, 149 and 20 tests respectively, including
doctests. Formatting, all-target/all-feature Clippy and strict rustdoc passed.

Earlier candidate [runs](https://github.com/mingley/partitionline/actions/runs/42701725909)
and [42701921642](https://github.com/mingley/partitionline/actions/runs/42701921642) passed
behavioral and strict lint/doc checks but failed formatting; the final run
corrected that failure.

The controls use fragmented in-memory readers and local TCP mock servers.
They do not test a live Schema Registry, a TLS peer, or Kafka. No performance
measurement was made.
