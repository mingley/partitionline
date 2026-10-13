# KL09-31 decode cursor

A contiguous-slice record decoder was measured against commit `95caed63034fc49cb6492ab9c053885dc7b81110` and reverted.

`decode/f03` (500 records) over five alternating rounds:

| Round | Parent µs | Candidate µs | Delta |
|---|---:|---:|---:|
| 1 | 48.516 | 47.077 | -3.0% |
| 2 | 51.064 | 47.382 | -7.2% |
| 3 | 51.463 | 47.156 | -8.4% |
| 4 | 48.001 | 54.470 | +13.5% |
| 5 | 49.029 | 46.905 | -4.3% |

Paired mean delta -1.9%, 95% CI [-12.9%, +9.1%]. The interval includes zero, so the change does not meet the acceptance rule. The decoder was removed. Record, fuzz, protocol-oracle, and allocation-budget checks had passed on the candidate before that revert.
