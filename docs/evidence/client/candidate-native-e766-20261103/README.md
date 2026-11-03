# Client v10 native rerun

Run 41576616151 tested the isolated v10 client candidate at checkout e7660f6e.
Both library runs failed one required topic-encoding latency test: 2,279 passed
and one failed in each profile. The reported elapsed times were 87.141916 ms
and 80.254912 ms against an 80 ms limit. In the earlier b1 v10 run, both full
library runs passed. Format and strict Clippy passed.

Both Mock Produce binaries failed with 32 passed and 15 failed; both
retained-backing binaries timed out with eight passed, one failed and one
ignored. The same legacy baseline case 07 failed despite its expected pass.
The candidate gate failed.

The manifest and content-addressed archive preserve all 610 members of the
compact report ZIP; all 133 source before/after pairs match the applicable
phase catalog. OOM counters were unavailable. The larger executable/source
artifact remains external. Hosted-runner wall time alone does not establish the
cause of the latency failure.
