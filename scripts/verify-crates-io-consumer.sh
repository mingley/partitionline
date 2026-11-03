#!/usr/bin/env bash
# Prove Installable/Adoptable the way an adopter experiences it: depend on
# partitionline and cargo-check the operator surface (produce/consume/group/
# share/admin + SASL/TLS config types). Does not publish.
#
# Modes:
#   MODE=registry (default) — require crates.io ${ver}, depend via registry
#   MODE=path               — pre-publish rehearsal against this workspace
#                             (catches API drift before the first cut)
#   MODE=git                — depend on the documented git tag pin (README /
#                             ADOPTION) so pre-crates.io adopters are not lied to
#
# Registry mode is wired into day1-after-publish / owner-finish-installable.
# Path + git modes are wired into cut-path so day1 cannot fail on type drift and
# the public git pin keeps compiling while Installable waits on the token.
#
# Shares the consumer main.rs with scripts/ci-crate-consumer.sh via
# scripts/lib/adopter-consumer-main.sh so the proofs cannot drift.
#
# Usage:
#   bash scripts/verify-crates-io-consumer.sh
#   MODE=path bash scripts/verify-crates-io-consumer.sh
#   MODE=git bash scripts/verify-crates-io-consumer.sh
#   VER=0.1.0 bash scripts/verify-crates-io-consumer.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lib/adopter-consumer-main.sh
source "${ROOT}/scripts/lib/adopter-consumer-main.sh"

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="${VER:-$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)}"
mode="${MODE:-registry}"
repo_url="$(sed -n 's/^repository = "\(.*\)"/\1/p' Cargo.toml | head -1)"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

case "$mode" in
  registry|path|git) ;;
  *)
    echo "verify-crates-io-consumer: MODE must be registry, path, or git (got '$mode')" >&2
    exit 1
    ;;
esac

pl_readme_git_tag() {
  python3 - <<'PY'
import re
text = open("README.md", encoding="utf-8").read()
m = re.search(r'tag\s*=\s*"(v[0-9][^"]*)"', text)
if not m:
    raise SystemExit("verify-crates-io-consumer: no git tag pin in README.md")
print(m.group(1))
PY
}

if [[ "$mode" == "registry" ]]; then
  echo "verify-crates-io-consumer: expecting crates.io ${name} ${ver}"
  if ! bash scripts/check-installable.sh "$ver" >/tmp/pl-verify-installable.log 2>&1; then
    cat /tmp/pl-verify-installable.log >&2 || true
    echo "verify-crates-io-consumer: FAIL — ${name} ${ver} not Installable yet" >&2
    exit 1
  fi
  dep_line="${name} = \"=${ver}\""
elif [[ "$mode" == "path" ]]; then
  echo "verify-crates-io-consumer: MODE=path pre-publish rehearsal against ${ROOT}"
  dep_line="${name} = { path = \"${ROOT}\" }"
else
  # MODE=git — documented pre-crates.io install pin must cargo-check.
  if grep -qE "^${name} = \"[0-9]" README.md; then
    echo "verify-crates-io-consumer: MODE=git SKIP — README already crates.io-shaped"
    echo "verify-crates-io-consumer: ok (git pin retired after Installable)"
    exit 0
  fi
  if [[ -z "$repo_url" ]]; then
    echo "verify-crates-io-consumer: FAIL — Cargo.toml missing repository URL" >&2
    exit 1
  fi
  pin_tag="$(pl_readme_git_tag)"
  # Keep pin honest before compile (tag exists; lag is docs/scripts-only).
  bash scripts/check-adopter-pin.sh
  if ! git rev-parse -q --verify "refs/tags/${pin_tag}^{}" >/dev/null \
    && ! git rev-parse -q --verify "refs/tags/${pin_tag}" >/dev/null; then
    echo "verify-crates-io-consumer: fetching tag ${pin_tag}"
    git fetch -q origin "refs/tags/${pin_tag}:refs/tags/${pin_tag}" || true
  fi
  if ! git rev-parse -q --verify "refs/tags/${pin_tag}^{}" >/dev/null \
    && ! git rev-parse -q --verify "refs/tags/${pin_tag}" >/dev/null; then
    echo "verify-crates-io-consumer: FAIL — tag ${pin_tag} missing locally and on origin" >&2
    exit 1
  fi
  echo "verify-crates-io-consumer: MODE=git adopter pin ${pin_tag} from ${repo_url}"
  dep_line="${name} = { git = \"${repo_url}\", tag = \"${pin_tag}\" }"
fi

cons="$tmpdir/consumer"
mkdir -p "$cons/src"
cat >"$cons/Cargo.toml" <<EOF
[package]
name = "${name}-crates-io-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
${dep_line}
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
EOF

pl_write_adopter_consumer_main "$cons/src/main.rs" "$name" "crates-io-consumer"

echo "verify-crates-io-consumer: cargo check (${mode}) for ${name} ${ver}"
# Sparse-index lag: registry mode may need a few cargo retries after API shows the crate.
# Git mode can also be slow on first fetch — allow a couple retries.
cargo_attempts="${CARGO_CHECK_ATTEMPTS:-12}"
cargo_sleep="${CARGO_CHECK_SLEEP_SECS:-5}"
if [[ "$mode" == "path" ]]; then
  cargo_attempts=1
elif [[ "$mode" == "git" ]]; then
  cargo_attempts="${CARGO_CHECK_ATTEMPTS:-3}"
fi
cargo_ok=0
for cargo_i in $(seq 1 "$cargo_attempts"); do
  if (cd "$cons" && cargo check --quiet); then
    cargo_ok=1
    break
  fi
  if [[ "$cargo_i" -lt "$cargo_attempts" ]]; then
    echo "verify-crates-io-consumer: cargo check not ready (${cargo_i}/${cargo_attempts}); retrying in ${cargo_sleep}s..."
    sleep "$cargo_sleep"
  fi
done
if [[ "$cargo_ok" != "1" ]]; then
  echo "verify-crates-io-consumer: FAIL — cargo check did not succeed for ${name} ${ver} (${mode})" >&2
  exit 1
fi

if [[ "$mode" == "path" ]]; then
  echo "verify-crates-io-consumer: ok (path rehearsal — day1 registry consumer will compile)"
  exit 0
fi

if [[ "$mode" == "git" ]]; then
  # Bind the lock entry to the exact tagged commit and the package version in
  # that commit; the current manifest may have advanced since this docs pin.
  pin_sha="$(git rev-parse --verify "refs/tags/${pin_tag}^{commit}")"
  pin_version="$(git show "${pin_sha}:Cargo.toml" | python3 -c \
    'import sys,tomllib; v=tomllib.loads(sys.stdin.read())["package"]["version"]; assert isinstance(v,str); print(v)')"
  if ! python3 - "$cons/Cargo.lock" "$name" "$pin_version" "$repo_url" "$pin_tag" "$pin_sha" <<'PY'
import sys, tomllib
from urllib.parse import parse_qsl, urlsplit

path, name, version, repo_url, tag, commit = sys.argv[1:]
try:
    with open(path, 'rb') as stream:
        lock = tomllib.load(stream)
    packages = lock.get('package')
    if not isinstance(packages, list) or not all(isinstance(p, dict) for p in packages):
        raise ValueError('invalid package list')
    named = [p for p in packages if p.get('name') == name]
    selected = [p for p in named if p.get('version') == version]
    if len(selected) != 1 or not isinstance(selected[0].get('source'), str):
        raise ValueError('tagged package version is not unique')
    source = selected[0]['source']
    if not source.startswith('git+'):
        raise ValueError('target package is not a git dependency')
    actual, expected = urlsplit(source[4:]), urlsplit(repo_url)
    if ((actual.scheme, actual.netloc, actual.path.rstrip('/')) !=
        (expected.scheme, expected.netloc, expected.path.rstrip('/')) or
        expected.query or expected.fragment or parse_qsl(actual.query, strict_parsing=True) != [('tag', tag)] or
        actual.fragment != commit):
        raise ValueError('git URL, tag or commit does not match the pin')
    root_name = name + '-crates-io-consumer'
    roots = [p for p in packages if p.get('name') == root_name and p.get('version') == '0.0.0'
             and 'source' not in p]
    if len(roots) != 1 or not isinstance(roots[0].get('dependencies'), list):
        raise ValueError('consumer package or dependencies missing')
    dependencies = roots[0]['dependencies']
    if not all(isinstance(dep, str) for dep in dependencies):
        raise ValueError('invalid consumer dependency')
    explicit = {name + ' ' + version, name + ' ' + version + ' (' + source + ')'}
    direct = [dep for dep in dependencies if dep in explicit or (dep == name and len(named) == 1)]
    if len(direct) != 1:
        raise ValueError('tagged package is not the unique direct consumer dependency')
except (OSError, tomllib.TOMLDecodeError, ValueError) as error:
    print('verify-crates-io-consumer: FAIL — invalid git consumer lock: '+str(error), file=sys.stderr)
    raise SystemExit(1)
PY
  then
    exit 1
  fi
  echo "verify-crates-io-consumer: git source confirmed for pin ${pin_tag} at ${pin_sha}"
  echo "verify-crates-io-consumer: ok (adopter can cargo-depend on git tag ${pin_tag})"
  exit 0
fi

if ! python3 - "$cons/Cargo.lock" "$name" "$ver" <<'PY'
import sys, tomllib

path, name, version = sys.argv[1:]
source = 'registry+https://github.com/rust-lang/crates.io-index'
try:
    with open(path, 'rb') as stream:
        lock = tomllib.load(stream)
    packages = lock.get('package')
    if not isinstance(packages, list) or not all(isinstance(p, dict) for p in packages):
        raise ValueError('invalid package list')
    named = [p for p in packages if p.get('name') == name]
    selected = [p for p in named if p.get('version') == version]
    if len(selected) != 1 or selected[0].get('source') != source:
        raise ValueError('target version does not have a unique crates.io source')
    root_name = name + '-crates-io-consumer'
    roots = [p for p in packages if p.get('name') == root_name and p.get('version') == '0.0.0'
             and 'source' not in p]
    if len(roots) != 1 or not isinstance(roots[0].get('dependencies'), list):
        raise ValueError('consumer package or dependencies missing')
    dependencies = roots[0]['dependencies']
    if not all(isinstance(dep, str) for dep in dependencies):
        raise ValueError('invalid consumer dependency')
    explicit = {name + ' ' + version, name + ' ' + version + ' (' + source + ')'}
    direct = [dep for dep in dependencies if dep in explicit or (dep == name and len(named) == 1)]
    if len(direct) != 1:
        raise ValueError('target is not the unique direct consumer dependency')
except (OSError, tomllib.TOMLDecodeError, ValueError) as error:
    print('verify-crates-io-consumer: FAIL — invalid registry consumer lock: '+str(error), file=sys.stderr)
    raise SystemExit(1)
PY
then
  exit 1
fi
echo "verify-crates-io-consumer: registry source confirmed"

echo "verify-crates-io-consumer: ok (adopter can cargo-depend on crates.io ${name} ${ver})"
