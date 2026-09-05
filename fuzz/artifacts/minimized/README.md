# Minimized fuzz artifacts

This directory retains minimized libFuzzer crashes, leaks, OOMs, and timeouts
from `scripts/ci-fuzz-campaign.sh` (`kind=campaign`, `duration_seconds` > 15).
They are kept for triage, not discarded.

Do not commit crashing payloads that break the 15s CI smoke
(`scripts/ci-fuzz-smoke.sh`, `kind=smoke`, `FUZZ_SECONDS=15`).

Campaign metadata: `fuzz/campaign/metadata.example.json` (committed fixture)
and `fuzz/campaign/metadata.json` (runtime stamp, gitignored).
