# CI prerequisites

Two setup failures prevented behavioral checks at `77021f7dc`.

The Java codec workflow requested LZ4 1.10.2 under `org.lz4`, which returned
HTTP 404. Kafka 4.3.1's published POM names `at.yawk.lz4`. The corrected URL
returns the jar matching the existing Java oracle SHA256
`252dabd586d22149a31661a8afc1a50747714eb1a298bd4a6e780c8c91a13866`.
The workflow now checks that hash before invoking Java. The retained POM and
download receipt establish the coordinate and bytes; no Java or Rust codec
tests were executed by this check. Fixture and oracle pins are unchanged.

The broker fuzz supervisor read `/sys/fs/cgroup/cpu.max` unconditionally.
Some Linux runners do not expose that cgroup v2 file. A missing file now records
an unavailable quota, with a null value and source. Other read errors still
fail. This field describes the host; enforced worker limits and campaign
requirements are unchanged.

Three pure metadata controls pass for an available quota, a missing controller
and a permission error. The retained receipt pins both source files and the
complete log. The actual instrumented supervisor suite still needs to run on
the repaired commit.

`files.json` lists the retained files and hashes. The original failures remain
in the [CI snapshot](../main-ci-77021-20261030/README.md).
