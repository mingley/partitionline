#!/usr/bin/env bash
# Rehearse idempotent partial-release recovery. Does not publish and does not
# re-cut 0.1.0.
#
# --self-test proves the shipped skip gates, not just skip=1 string presence:
# release.yml Authenticate + Publish steps must be gated on
# steps.already.outputs.skip; owner-publish must skip cargo publish when
# crates.io already has the version. Mutations that drop those gates fail.
#
# Usage:
#   bash scripts/rehearse-partial-release.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="${1:-}"
VALIDATE_FILE="${2:-}"
case "$MODE" in
  ""|--self-test|--validate-release|--validate-owner-publish) ;;
  *)
    echo "usage: bash scripts/rehearse-partial-release.sh --self-test" >&2
    echo "rehearse-partial-release: refuses other args (never publishes)" >&2
    exit 1
    ;;
esac

fail() {
  echo "rehearse-partial-release --self-test: FAIL — $*" >&2
  exit 1
}

# 1. Cargo.toml is 0.1.0 — recovery must not bump/re-cut it.
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
[[ "$ver" == "0.1.0" ]] || fail "Cargo.toml version is ${ver}; recovery must not bump/re-cut 0.1.0"

# 2. release-plz is PR-only: command: release-pr, no token-gated publish / command: release.
plz=".github/workflows/release-plz.yml"
[[ -f "$plz" ]] || fail "missing ${plz}"
grep -qF 'command: release-pr' "$plz" \
  || fail "release-plz.yml missing command: release-pr"
if grep -qE 'command:[[:space:]]*"?release"?[[:space:]]*$' "$plz"; then
  fail "release-plz.yml has command: release (would publish)"
fi
if grep -qE 'if:.*CARGO_REGISTRY_TOKEN' "$plz"; then
  fail "release-plz.yml has token-gated job (would publish)"
fi
if grep -qF 'CARGO_REGISTRY_TOKEN' "$plz"; then
  fail "release-plz.yml still mentions CARGO_REGISTRY_TOKEN (would publish)"
fi
if grep -qE 'cargo[[:space:]]+publish' "$plz"; then
  fail "release-plz.yml contains cargo publish (would publish)"
fi

# 3. Canonical publisher: release.yml (cargo publish) + owner-cut-release.
rel=".github/workflows/release.yml"
cut="scripts/owner-cut-release.sh"
ready="scripts/ci-publish-ready.sh"
pub="scripts/owner-publish.sh"
[[ -f "$rel" ]] || fail "missing ${rel}"
[[ -f "$cut" ]] || fail "missing ${cut}"
[[ -f "$ready" ]] || fail "missing ${ready}"
[[ -f "$pub" ]] || fail "missing ${pub}"
grep -qE 'cargo[[:space:]]+publish' "$rel" \
  || fail "release.yml missing cargo publish (canonical publisher)"
grep -qF 'owner-publish.sh' "$cut" \
  || fail "owner-cut-release.sh missing owner-publish.sh (canonical publisher)"

# 4. Exact-SHA CI + crate-consumer required on release.yml / cut / publish-ready.
for f in "$rel" "$cut" "$ready"; do
  grep -qF 'check-main-ci.sh' "$f" \
    || fail "${f} missing check-main-ci.sh (exact-SHA CI required)"
done
grep -qF 'ci-crate-consumer.sh' "$rel" \
  || fail "release.yml missing ci-crate-consumer.sh"
grep -qF 'ci-crate-consumer.sh' "$ready" \
  || fail "ci-publish-ready.sh missing ci-crate-consumer.sh"
grep -qF 'ci-publish-ready.sh' "$cut" \
  || fail "owner-cut-release.sh missing ci-publish-ready.sh (crate-consumer gate)"
grep -qF 'CHECK_SHA=' "$rel" \
  || fail "release.yml missing CHECK_SHA= (exact-SHA CI)"

# 5–6. Shipped skip gates (python: real step ifs + owner-publish else-branch).
command -v python3 >/dev/null 2>&1 || fail "python3 required for skip-gate proof"

pl_kl08_validate_release_yml() {
  python3 - "$1" <<'PY'
import re, sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")

perm = re.search(r"(?ms)^permissions:\n(.*?)(?=^[a-z])", text)
if not perm:
    sys.stderr.write("no workflow permissions block\n")
    sys.exit(1)
if not re.search(r"(?m)^  actions: read\s*$", perm.group(1)):
    sys.stderr.write("permissions missing actions: read (gh run list / exact-SHA CI)\n")
    sys.exit(1)

steps = {}
for m in re.finditer(
    r"(?m)^      - name: ([^\n]+)\n((?:        .*\n)*)",
    text,
):
    steps[m.group(1).strip()] = m.group(2)

already = None
for name, body in steps.items():
    if name.startswith("Soft-skip if version already on crates.io"):
        already = body
        break
if already is None:
    sys.stderr.write("missing Soft-skip if version already on crates.io step\n")
    sys.exit(1)
if "id: already" not in already:
    sys.stderr.write("already-on-crates.io step missing id: already\n")
    sys.exit(1)
if "skip=1" not in already:
    sys.stderr.write("already-on-crates.io step missing skip=1 output\n")
    sys.exit(1)

gate = re.compile(r"if:\s*steps\.already\.outputs\.skip\s*!=\s*'1'")
auth = steps.get("Authenticate to crates.io (OIDC trusted publishing)")
pub = steps.get("Publish")
if not auth:
    sys.stderr.write("missing Authenticate to crates.io step\n")
    sys.exit(1)
if not pub:
    sys.stderr.write("missing Publish step\n")
    sys.exit(1)
if not gate.search(auth):
    sys.stderr.write("Authenticate step is not gated on steps.already.outputs.skip != '1'\n")
    sys.exit(1)
if not gate.search(pub):
    sys.stderr.write("Publish step is not gated on steps.already.outputs.skip != '1'\n")
    sys.exit(1)
if "cargo publish" not in pub:
    sys.stderr.write("Publish step missing cargo publish\n")
    sys.exit(1)
sys.exit(0)
PY
}

pl_kl08_validate_owner_publish() {
  python3 - "$1" <<'PY'
import re, sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8")
if "pl_crates_probe_version" not in text:
    sys.stderr.write("owner-publish missing pl_crates_probe_version\n")
    sys.exit(1)

start = text.find('if [[ "${PL_CRATES_PROBE_STATUS}" == "present" ]]; then')
if start < 0:
    sys.stderr.write("owner-publish missing present-status skip if\n")
    sys.exit(1)

# Walk if/then/else/fi from that if so nested CI ifs do not confuse the else.
i = start
depth = 0
then_end = None
else_end = None
seen_then = False
in_else = False
lines = text[start:].splitlines(keepends=True)
buf = []
then_buf = []
else_buf = []
for line in lines:
    stripped = line.strip()
    if re.match(r"if\s+\[\[", stripped) or stripped.startswith("if "):
        depth += 1
        if depth == 1:
            seen_then = True
        if in_else and depth > 1:
            else_buf.append(line)
        elif seen_then and not in_else and depth > 1:
            then_buf.append(line)
        continue
    if stripped == "else" and depth == 1:
        in_else = True
        continue
    if stripped == "fi":
        if depth == 1:
            break
        depth -= 1
        if in_else:
            else_buf.append(line)
        else:
            then_buf.append(line)
        continue
    if in_else:
        else_buf.append(line)
    elif seen_then:
        then_buf.append(line)

then = "".join(then_buf)
els = "".join(else_buf)
if re.search(r"(?m)^\s*cargo\s+publish\s*$", then):
    sys.stderr.write("owner-publish present-branch still cargo publishes\n")
    sys.exit(1)
if "skipping cargo publish" not in then:
    sys.stderr.write("owner-publish present-branch missing skipping cargo publish\n")
    sys.exit(1)
if not re.search(r"(?m)^\s*cargo\s+publish\s*$", els):
    sys.stderr.write("owner-publish else-branch missing cargo publish\n")
    sys.exit(1)
sys.exit(0)
PY
}

if [[ "$MODE" == "--validate-release" ]]; then
  pl_kl08_validate_release_yml "${VALIDATE_FILE:?release.yml path}"
  exit $?
fi
if [[ "$MODE" == "--validate-owner-publish" ]]; then
  pl_kl08_validate_owner_publish "${VALIDATE_FILE:?owner-publish.sh path}"
  exit $?
fi

pl_kl08_validate_release_yml "$rel" \
  || fail "release.yml skip/auth/publish gates or actions: read missing"
pl_kl08_validate_owner_publish "$pub" \
  || fail "owner-publish does not skip cargo publish when crates.io already has the version"

# Mutations of copies must fail — grep-only skip=1 is not enough.
mut="$(mktemp -d "${TMPDIR:-/tmp}/pl-kl08-rehearse.XXXXXX")"
# shellcheck disable=SC2064
trap 'rm -rf "$mut"' EXIT
cp "$rel" "$mut/release.yml"
cp "$pub" "$mut/owner-publish.sh"

# Drop both skip ifs on Authenticate/Publish.
python3 - "$mut/release.yml" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
text = p.read_text(encoding="utf-8")
text = text.replace("        if: steps.already.outputs.skip != '1'\n", "")
p.write_text(text, encoding="utf-8")
PY
if pl_kl08_validate_release_yml "$mut/release.yml" >/dev/null 2>"$mut/rel-neg.err"; then
  fail "release.yml validator still passed after deleting skip ifs on Authenticate/Publish"
fi
if ! grep -q "not gated on steps.already.outputs.skip" "$mut/rel-neg.err"; then
  fail "release.yml skip-if deletion did not fail the step-gate check (got $(tr '\n' ' ' <"$mut/rel-neg.err"))"
fi

python3 - "$mut/release.yml" "$rel" <<'PY'
from pathlib import Path
import sys
# Restore from original then drop actions: read only.
orig = Path(sys.argv[2]).read_text(encoding="utf-8")
p = Path(sys.argv[1])
p.write_text(orig.replace("  actions: read\n", ""), encoding="utf-8")
PY
if pl_kl08_validate_release_yml "$mut/release.yml" >/dev/null 2>"$mut/perm-neg.err"; then
  fail "release.yml validator still passed after deleting actions: read"
fi
if ! grep -q "actions: read" "$mut/perm-neg.err"; then
  fail "actions: read deletion did not fail the permissions check"
fi

python3 - "$mut/owner-publish.sh" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
text = p.read_text(encoding="utf-8")
text = text.replace(
    'if [[ "${PL_CRATES_PROBE_STATUS}" == "present" ]]; then\n'
    '  echo "owner-publish: ${name} ${ver} already on crates.io — skipping cargo publish (idempotent; not a re-cut)"\n'
    'else\n',
    "if false; then\n  echo unused\nelse\n",
)
p.write_text(text, encoding="utf-8")
PY
if pl_kl08_validate_owner_publish "$mut/owner-publish.sh" >/dev/null 2>"$mut/pub-neg.err"; then
  fail "owner-publish validator still passed after removing the present-status skip"
fi

# 7. Simulated "publish succeeded, day1 pending" → day1/handoff DRY_RUN, not another publish.
day1="scripts/day1-after-publish.sh"
handoff="scripts/owner-post-installable-handoff.sh"
[[ -f "$day1" ]] || fail "missing ${day1}"
[[ -f "$handoff" ]] || fail "missing ${handoff}"
grep -qF 'day1-after-publish.sh' "$cut" \
  || fail "owner-cut-release missing day1-after-publish (recovery would not call day1)"
grep -qF 'owner-post-installable-handoff' "$cut" \
  || fail "owner-cut-release missing handoff (recovery would not call handoff)"
grep -qF 'DRY_RUN' "$day1" \
  || fail "day1-after-publish missing DRY_RUN"
grep -qF 'DRY_RUN' "$handoff" \
  || fail "owner-post-installable-handoff missing DRY_RUN"
if grep -qE 'cargo[[:space:]]+publish' "$day1"; then
  fail "day1-after-publish contains cargo publish (would publish)"
fi
if grep -qE 'cargo[[:space:]]+publish' "$handoff"; then
  fail "handoff contains cargo publish (would publish)"
fi
if ! awk '
  /wait for crates.io/ { wait=NR }
  /day1-after-publish\.sh/ { if (wait && NR > wait) day1=NR }
  /pl_cut_run_handoff/ { if (wait && NR > wait) hand=NR }
  /cargo publish/ { if (wait && NR > wait) pub=NR }
  END { exit (wait && day1 && hand && !pub) ? 0 : 1 }
' "$cut"; then
  fail "publish-succeeded recovery is not day1/handoff (would publish or skip recovery)"
fi

echo "rehearse-partial-release: simulate publish-succeeded, day1 pending"
echo "  would: DRY_RUN=1 bash scripts/day1-after-publish.sh"
echo "  would: DRY_RUN=1 bash scripts/owner-post-installable-handoff.sh"
echo "  would not: cargo publish / version bump / re-cut 0.1.0"

echo "rehearse-partial-release: --self-test OK — 0.1.0 stays; release-plz PR-only; canonical release.yml + owner-cut-release; actions: read; skip-gated Authenticate/Publish; owner-publish skips existing version; day1/handoff DRY_RUN not another publish"
exit 0
