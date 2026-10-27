# Transaction generation fault oracle

The existing seeded history reported 150 discrepancies after transaction
generations began allowing reuse after abort. Its oracle still expected every
old generation to remain fenced. Production transaction code is unchanged by
this repair.

The corrected history checks the generation while its transaction is open,
then checks both outcomes:

| State | Older generation | Observed cases |
| --- | --- | ---: |
| Transaction open | Rejected with `ILLEGAL_GENERATION` | 334 |
| Transaction committed | Rejected with `ILLEGAL_GENERATION` | 184 |
| Transaction aborted | Accepted into the next transaction, still invisible | 150 |

The test checks exact committed offsets and visibility after restart. It keeps
the original seeds, profiles and fault counts. Default and all-feature runs
each pass 3,004 outcomes with zero observer discrepancies. Focused strict
Clippy and formatting pass. Tested oracle source:
`95c5e4cf196e8c9fd5295165760a5b46568f45ce`.

`capture/` retains the original failures, corrected reports, compiled tests and
broader readiness checks. The original failing executable was not copied before
recompilation; its source and actual failure log remain available. Verify
stored and reconstructed bytes with the common evidence verifier.

The whole-candidate suites still fail on Fetch golden responses, a corruption
waiter and a write-prefetch deadline. Those failures remain blockers. This is
a journal-model oracle repair, with no new production or performance claim.
