# Exploratory librdkafka client against the RF1 listener

This run exercised the retained `operator.elf` for an uncommitted single-node
RF1 candidate based on commit `64da3d2db9ffa80fb680806dc377e52f4652b38e`.
The build receipt lists broker sources, manifests, fixtures and executable
hashes, checked before and after this run. Included source and binary bytes
correspond to those receipts. The test does not qualify the integrated main
branch or a production cluster.

The retained `ordinary-peer.c` was compiled against librdkafka 2.15.0, source
commit `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`. The source, header, actual
peer executable, shared library, library build manifest, compiler outputs and
run driver are included. The successful build used `gcc -std=c11 -Wall -Wextra
-Werror -pedantic` with the upstream header supplied as a system header. The
first compile failed because the copied header sat beside the source and was
therefore treated as a local header; its compiler errors are retained.

The actual client created a two-partition topic and delivered 24 records using
`acks=1`, `enable.idempotence=false`, no compression and one in-flight request.
Its direct-assignment consumer checked all exact offsets, key/value bytes,
nulls, empty values, timestamps and ordered duplicate/null headers. It also
queried both low/high watermarks and timestamp-based ListOffsets. After clean
SIGTERM and restart on the same data directory it repeated the complete Fetch
and ListOffsets checks. The seed server joined 4/4 accepted connections and
the restarted server joined 2/2, with zero worker failures. Raw producer,
consumer, network-debug and server outputs and final journals are retained.

This is one bounded, plaintext, ordinary, graceful-restart history. It does
not qualify idempotence, transactions, groups, security, replication, crash
recovery, sustained load, performance, or production readiness. Repeat it
against a source-bound committed integrated candidate.

Verify stored and reconstructed files with `python3
benchmarks/runtime/verify-evidence-archive.py <this directory>`.
