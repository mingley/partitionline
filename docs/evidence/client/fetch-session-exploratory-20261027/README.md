# Exploratory live Fetch session run

This is an exploratory run, not a qualification of a committed source tree. It
used the retained `consumer_fetch_semantics` test executable from an earlier
Cargo matrix that failed in another target. The matrix recorded a baseline
source receipt for `e7f5761a9c6fe13e08efb0759f7924709287165b`, but did not
bind that executable's bytes to that receipt during compilation. The executable
SHA-256, both original matrix logs and source receipts, and an archive of the
corresponding baseline source are retained so this limitation can be checked.
The later candidate client changes were not in this test.

The probe ran on Linux x86_64 against a fresh Apache Kafka 4.1.2 broker using
image digest `sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`.
The Docker inspect and launch commands record the actual image, ports,
configuration, and unique topic. The broker had a one-CPU and 1 GiB limit.
It was removed after the run.

The executable's live test produced and consumed 65 records over 32
partitions. Its observed Fetch v18 history included full session creation,
unchanged incremental fetches, pause and resume, an idle reconnect, and
terminal session retirement. The history validator checked the observations,
and an Apache Java console consumer read back all 65 records with the exact
acknowledged keys, values, partitions, and offsets. The full output and driver
are included. The log records the seven observed session requests.

The run validates only this one exploratory history. It does not qualify the
current candidate, all Fetch behaviors, production readiness, or performance.
A source-bound committed candidate and a fresh independently verified run are
required to change the conformance disposition.

`archive-inventory.json` identifies every stored and reconstructed file. Verify
it with `python3 benchmarks/runtime/verify-evidence-archive.py <this directory>`.
