# KL09-32 micro-encode

Record encoding now computes each record's body length from varint widths once, then writes those groups. The measured cell is `encode/f03` (500 records, 16-byte keys, 100-byte values, no headers) from `benchmarks/codec`.

Parent commit `48c01f2e88d75322633cd6b3aca30c29f3475b5f`. Five alternating rounds on this host (Intel Xeon, 4 cores, Ubuntu 24.04, rustc 1.99.0), criterion median time per round:

| Round | Parent µs | Candidate µs | Delta |
|---|---:|---:|---:|
| 1 | 37.623 | 28.153 | -25.2% |
| 2 | 37.912 | 28.294 | -25.4% |
| 3 | 37.451 | 28.922 | -22.8% |
| 4 | 37.803 | 31.989 | -15.4% |
| 5 | 37.440 | 28.280 | -24.5% |

Paired mean delta -22.6%, 95% CI [-27.8%, -17.4%]. The allocation budget test still matches its existing constants. This is a local codec measurement, not a client-versus-peer claim.
