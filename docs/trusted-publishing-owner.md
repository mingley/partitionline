# Trusted Publishing owner checklist (crates.io UI)

**UNFILLED — not evidence.** This template Does **not** mean Trusted Publishing
is configured and Does **not** lift Suite HOLD. Blank fields mean the owner has
not completed the one-time crates.io UI binding. Workflow shape can be green
while this file stays UNFILLED — crates.io UI still human.

`partitionline` **0.1.0** is already on crates.io (Installable met). Later cuts
should prefer OIDC Trusted Publishing so long-lived `CARGO_REGISTRY_TOKEN` is
not required forever. Agents cannot click the crates.io UI — an owner must.

Probe workflow shape anytime:

```bash
bash scripts/check-trusted-publishing-ready.sh
bash scripts/owner-enable-trusted-publishing.sh   # prints the same UI steps
```

See [RELEASE.md](RELEASE.md) and
[crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing).

## Binding record

| Field | Value |
|---|---|
| crates.io crate | `partitionline` |
| Tip / version when binding was attempted | _UNFILLED_ |
| Publisher source | GitHub |
| Repository | `mingley/partitionline` |
| Workflow file | `release.yml` |
| Environment pin (empty unless used) | _UNFILLED_ |
| Owner who clicked Save | _UNFILLED_ |
| Binding date (UTC) | _UNFILLED_ |
| Binding id / screenshot path | _UNFILLED_ |

## Post-binding verification

| Field | Value |
|---|---|
| Actions secret `CARGO_REGISTRY_TOKEN` kept until first OIDC success? (`yes` / `no`) | _UNFILLED_ |
| First OIDC tag publish (`vX.Y.Z`, not RC) | _UNFILLED_ |
| Release run URL that used OIDC (not token fallback) | _UNFILLED_ |
| Token fallback retired after OIDC success? (`yes` / `no` / `deferred`) | _UNFILLED_ |
| Outcome (`configured` / `blocked` / `aborted`) | _UNFILLED_ |

## Honesty

- Green `check-trusted-publishing-ready.sh` proves **workflow shape**, not UI
  binding. Workflow shape can be green with this checklist still UNFILLED.
- Do not re-cut `0.1.0` to “test” Trusted Publishing.
- Suite HOLD / unsigned Lab A stay open regardless of this checklist.
