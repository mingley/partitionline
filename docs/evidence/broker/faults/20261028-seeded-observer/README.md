# Seeded journal observer run

The default-feature broker test executable passed 3,004 seeded fault observations on this x86_64 workspace, with zero observer failures. The three profiles covered the data plane (1,000), transaction markers (1,002), and transactional offsets (1,002). The offsets profile checked 184 committed-generation fences and 150 aborted-generation reuses. The run took 11.6 seconds with the process pinned to one CPU in a shared four-CPU quota.

The retained [report](capture/report.json) and [output](capture/output.log) contain the complete counts. The test models journal close and reopen, lost acknowledgments, fencing, and a follower missing appends. It does not kill a process, inject file-sync errors, or create a real network partition or multi-broker cluster. It does not qualify production readiness or a sustained reliability claim.

The [runner](capture/runner.py) records the command and resource scope in the [receipt](capture/receipt.json). The [compressed executable](build/fault-campaign.elf.gz) reconstructs to SHA-256 `0a21aaaec496e2289e6df9927794a90ddec0455ffdeefe85df6d9c40f2453010`, matching executable 5 in the retained [broker suite receipt](build/broker-suite-receipt.json). The [test source](source/fault_campaign.rs) is retained with SHA-256 `04dd0d91d634543b571bb5915d9e87eb944f61983435fd50cde46c0dcaa8d7af`, which also matches commit `85458239a`. The suite receipt records per-file hashes of the broker source used at build time. Its `source_sha` field is the base commit before uncommitted candidate changes, not an exact source commit; the per-file hashes and executable identify the tested candidate. This archive does not contain a complete source snapshot of that candidate.

Verify stored and reconstructed files with:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/faults/20261028-seeded-observer
```
