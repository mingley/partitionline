# Release readiness checks, 30 October 2026

The local publish-readiness script now requires locked default and all-feature
tests before it can reach packaging. The exact-commit CI gate also requires
the `release-rehearsal` job. The test fixture for a complete CI profile now
includes `schema` and latest stable Rust instead of the obsolete Rust 1.85 cell.
Missing schema or rehearsal jobs remain explicit negative tests.

Before the fixture correction, the profile test class failed 11 of its 17
cases because `schema` was absent. The original log is retained. After the
changes, all 58 release tests passed, including three new executions of the
actual publish-readiness script with a substituted Cargo command. They prove
that default or all-feature test failure stops execution before packaging, and
that both successful suites reach the next stage. Local substitutes cannot
build, upload or authenticate to a registry. The existing exact-commit CI
self-test passed all nine cases.

Commands from the repository root:

```sh
python3 -B tests/release/test_ci_gate.py TestReleaseProfileGate
python3 -B -m unittest discover -s tests/release -p 'test_*.py'
bash scripts/check-main-ci.sh --self-test
bash -n scripts/check-main-ci.sh scripts/ci-publish-ready.sh scripts/audit-civilization-bars.sh
```

The first command failed before the correction; the remaining commands passed
afterward. A local link check also passed 28 links across the revised release
guide, archived guide and TODO. The release guide is shorter and identifies
the unpublished companions, semver migration and current blockers.

Checks ran in the dirty working tree based on `363b60625`; `tested-inputs.json`
records the final relevant file hashes, not complete before/after machine or
source identity. The unchanged baseline profile inputs come from that commit.
These tests do not run the Rust suites or establish publication readiness.
No version, tag, registry upload or release note was created.
