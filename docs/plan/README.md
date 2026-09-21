# One task per session

**Start here to implement the Kafka leadership plan.** Pick one ready task
from [tasks.json](tasks.json), finish its narrow contract, record evidence and
stop. Do not treat a KL package as one assignment.

[Source audit](../audits/2026-09-21.md) explains the verified gaps.
[ROADMAP](../ROADMAP.md) defines profiles and promotion gates.
[TODO](../../TODO.md) is the short launch queue. **Only `tasks.json` owns
task status and dependencies**; the other documents are navigation and context.

## Task contract

Each task has a stable ID, priority, kind, dependencies, starting files,
one deliverable, explicit acceptance criteria and focused checks. All tasks
start `pending`, with no owner or completion evidence. A passing existing
suite or a merged preparatory harness does not complete its dependent task.

| Priority | Meaning |
|---|---|
| P0 | Integrity, trustworthy evidence, bounds or release-safety blocker |
| P1 | Core feature, measurement or usability work needed for the leadership goal |
| P2 | Full-admin/enterprise/ecosystem extension or later qualification |

The prefixes retain the existing roadmap packages:

| Prefix | Lane |
|---|---|
| KL01 | Independent compatibility/conformance evidence |
| KL02 | Resource ownership, deadlines and lifecycle |
| KL03 | Consumer correctness, delivery, transactions and group recovery |
| KL04 | Reproducible benchmarks and measured optimization |
| KL05 | Major feature completion and optional ecosystem support |
| KL06 | Authentication and transport lifecycle |
| KL07 | Documentation, diagnostics and adoption ergonomics |
| KL08 | Releases, support and production qualification |

**Size rule:** one observable behavior, one fixture family, one adapter, one
documentation outcome, or one evidence job per task. Aim for one or two
production files plus focused tests/docs, not a broad subsystem rewrite.
Listed `files` are starting points, not permission to ignore a necessary
caller; `new:` paths do not exist yet. If a task cannot fit that boundary,
split it into dependent child tasks **before** implementation and retain its
acceptance criteria. Do not silently expand the session.

## Find ready work

Run from the repository root. This uses only Python's standard library:

```bash
python3 - <<'PY'
import json
from pathlib import Path
p = json.loads(Path("docs/plan/tasks.json").read_text())
tasks = {t["id"]: t for t in p["tasks"]}
for t in sorted(tasks.values(), key=lambda t: (t["priority"], t["id"])):
    if t["status"] == "pending" and all(
        tasks[d]["status"] == "done" for d in t["depends_on"]
    ):
        print(t["id"], t["priority"], t["title"])
PY
```

Read just one card:

```bash
TASK=KL03-01 python3 - <<'PY'
import json, os
from pathlib import Path
tasks = json.loads(Path("docs/plan/tasks.json").read_text())["tasks"]
print(json.dumps(next(t for t in tasks if t["id"] == os.environ["TASK"]), indent=2))
PY
```

The initial recommended pickups are **KL03-01** (reusable consumer fixture),
**KL07-01** (strict-rustdoc failure), **KL08-01** (release check fail-closed),
**KL01-01** (conformance registry), and **KL04-01** (benchmark contract).
These have disjoint primary code surfaces. Consumer repairs then proceed in
the launch order in TODO; do not run concurrent edits to `src/consumer.rs`.

## Execute and hand off

1. Read applicable repository instructions, this guide, one task card, its
   completed dependency evidence and the relevant audit finding. Verify
   current source; audit line numbers are frozen at `cb7e97d`.
2. Claim the card: set `status=in_progress` and `owner` to the actual owner.
   Check the worktree and coordinate if another session owns the same files.
   Never infer a task is free from an old printed ready list.
3. For a defect, first add the failing behavioral case. Reuse existing
   codecs/helpers and the promoted fixture; do not duplicate a mock per test
   or encode the broken behavior as the expectation.
4. Implement only the card's deliverable, preserving defaults and unrelated
   behavior. An intentional behavior/API change needs its directly affected
   documentation and compatibility tests in the same change.
5. Run the card's checks and the repository's applicable format/lint gate.
   Commands referencing `new:` files are **future acceptance commands**, not
   commands that already pass today. Verify a test selector runs at least
   one intended test; zero selected tests, ignored tests and soft skips are
   not success. Use targeted suites before a full matrix.
6. Record exact source/peer/tool versions, commands, counts, artifacts and
   limitations in `evidence`. Set `done` only when every acceptance criterion
   is met. If blocked, use `blocked`, record the reason/owner/next check in
   `blocked_reason`, and leave dependents unready.
7. Review the diff and hand off the task ID, meaningful change, evidence,
   unresolved limits and next ready ID. Commit/push only within the current
   session's authorization. Stop after the task; do not start a second
   unrelated feature to make the session look more complete.

Example completion evidence (replace placeholders with real values):

```json
{
  "source_sha": "<exact tested SHA>",
  "commands": ["<command and exit status>"],
  "results": "<named assertions/case counts, including skips>",
  "artifacts": ["<durable result or CI URL>"],
  "limits": ["<what was not exercised>"]
}
```

Keep a schema/fixture helper change separate from its large coverage expansion.
Keep infrastructure provisioning separate from campaign execution and report
analysis. Long 24-hour/7-day jobs are resumable evidence work: record job IDs,
artifact paths, stop conditions and the next observation; do not wait in a
busy polling loop or mark a launched job passed.

## Boundaries that never change implicitly

- **Correctness first:** the five reproduced consumer defects stay open until
  their maintained regression tests and fixes land. Current CI is not a waiver.
- **Named scope:** core client, transactional, group/share, secure, full-admin
  and ecosystem profiles have different gates. Broker-internal replication
  APIs, Streams, Connect and a drop-in C ABI are not silently added to scope.
- **Complete means evidenced:** every selected upstream case has a disposition
  and provenance. `unsupported` does not count as passed. Major features such
  as zstd or enterprise auth cannot disappear behind a "demand-led" label when
  claiming the corresponding complete profile.
- **Dependency policy:** no librdkafka client dependency or new native
  compression/SASL default; `unsafe_code` remains forbidden. zstd/GSSAPI
  backend decisions require maintainer approval before policy changes.
  `ring` already includes native compilation; do not claim no C anywhere.
- **Benchmark honesty:** preserve Suite HOLD and historical signoff rules.
  Equal durability/security, all failed cells, raw artifacts and uncertainty
  are mandatory. A universal Kafka performance certification does not exist
  in the inspected evidence; repository targets are proposed, not standards.
- **Operational approval:** no release, merge, deployment, registry upload,
  real traffic, permissions change, external message, paid host or destructive
  topic reset follows automatically from finishing a task. Inspect scripts
  before running them; `owner-*` helpers are not routine validation commands.
- **Sensitive evidence:** publish only sanitized public fixtures/results.
  Keep secrets, customer data and private workload identities out of the repo.

## Completion levels

**Task done** means one card is proven. **Package done** means all applicable
cards plus its roadmap gate are proven. **Profile qualified** additionally
requires the exact release candidate's full case matrix and operational
evidence. **Performance leadership** requires a named reproducible comparison.
**1.0** requires an explicit API/support/maintenance decision.

These are different outcomes. None can be inferred from the existence of this
plan, a harness, a template, a green local test, or a published crate.
