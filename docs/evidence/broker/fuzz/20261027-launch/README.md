# Broker fuzz qualification

Five 24-hour campaigns started on October 27, 2026 at 1:41 p.m. Chicago time.
They exercise request headers/voter records, KRaft decoders, controller
responses, journal recovery and epoch-log recovery. The tested source is
`2c0ddfbcc5f2a772c181e96651d920c0dc112511`, built with stable Rust 1.99.1.
The profile uses the broker's default features on Linux x86_64.

The campaigns were running at the retained snapshot. Their earliest completion
is October 28 at 1:41 p.m. Chicago time, if uninterrupted. KL11-47 remains in
progress until each target completes its full interval and passes the audit.

Eight checks passed against actual instrumented executables. They cover
interruption and joined shutdown, resume, changed executables/logs, missing
completion receipts, unjoined workers and false duration labels. The initial
strict harness Clippy run failed on a disallowed mutex; the synchronous target
now uses thread-local state and passes. A pre-existing unused-constant warning
in the broker dependency remains in the build logs.

`capture/launch-snapshot-01/` holds immutable prefixes of live files, with
per-file capture times. `capture/runner-tests/` includes deliberate negative
copies of short-run results. Those copies are test inputs. Neither collection
is a completed 24-hour record. `archive-inventory.json` records exact retained
bytes; `capture/tested-source.tar.gz` contains the tested source.

The live directory is `/workspace/work/qualification-20261027/fuzz-day-01`.
Managed execution session 72458 owns supervisor PID 3428. Worker PIDs were
3431–3435 at launch; use the live `jobs.json` when inspecting the job.

```sh
python3 -B scripts/broker-fuzz-campaign.py audit \
  --output /workspace/work/qualification-20261027/fuzz-day-01
```

Create `STOP` in that directory for joined shutdown. A stopped attempt can
seed a new output directory using `--resume-from`; elapsed time is never
added across attempts. See the [runner instructions](../../../../../fuzz/broker/README.md).

Verify the retained snapshot:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/fuzz/20261027-launch
```

The retained core and full conformance checks both failed. Overall production
readiness still requires the remaining implementation, compatibility, overload
and continuity gates. Performance claims additionally need controlled x86_64
and arm64 comparisons and independent reproduction. These fuzz runs supply
parser/recovery evidence; their execution counts are not Kafka throughput.
