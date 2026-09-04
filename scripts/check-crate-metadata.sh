#!/usr/bin/env bash
# Validate crates.io-facing package metadata before the first cut.
# Does not publish. Exit 0 when metadata is publish-shaped.
#
# Usage:
#   bash scripts/check-crate-metadata.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0
ok() { echo "OK  $*"; }
bad() { echo "FAIL  $*"; fail=$((fail + 1)); }

name="$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -1)"
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
echo "check-crate-metadata: ${name} ${ver}"

if [[ "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  ok "version is final ${ver}"
else
  bad "version '${ver}' is not final X.Y.Z (RC tags are git pins only)"
fi

# Python validates the [package] table shape against crates.io constraints.
if python3 - <<'PY'
import pathlib, re, sys

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

text = pathlib.Path("Cargo.toml").read_text(encoding="utf-8")
pkg_lines = []
in_pkg = False
for line in text.splitlines():
    if line.startswith("[package]"):
        in_pkg = True
        continue
    if in_pkg and line.startswith("[") and not line.startswith("[package."):
        break
    if in_pkg:
        pkg_lines.append(line)
pkg = tomllib.loads("[package]\n" + "\n".join(pkg_lines))["package"]

fail = 0

def ok(msg: str) -> None:
    print(f"OK  {msg}")

def bad(msg: str) -> None:
    global fail
    print(f"FAIL  {msg}", file=sys.stderr)
    fail += 1

desc = pkg.get("description") or ""
if 10 <= len(desc) <= 200:
    ok(f"description length {len(desc)}")
else:
    bad(f"description length {len(desc)} (want 10–200)")

for key in ("license", "readme", "repository", "documentation", "homepage"):
    if pkg.get(key):
        ok(f"{key} set")
    else:
        bad(f"missing package.{key}")

dlow = desc.lower()
if ("no c" in dlow or "pure rust" in dlow or "pure-rust" in dlow) and "librdkafka" in dlow:
    ok("description states pure-Rust / no-C / no-librdkafka identity")
elif "no c" in dlow or "pure rust" in dlow:
    ok("description states pure-Rust / no-C identity")
else:
    bad("description should state pure-Rust / no-C identity for crates.io discoverability")

kw = pkg.get("keywords") or []
if 1 <= len(kw) <= 5:
    ok(f"keywords count {len(kw)}")
else:
    bad(f"keywords count {len(kw)} (crates.io allows at most 5)")
for k in kw:
    if not re.fullmatch(r"[a-z0-9_-]{1,20}", k or ""):
        bad(f"keyword '{k}' invalid (lowercase, max 20)")

cats = pkg.get("categories") or []
if 1 <= len(cats) <= 5:
    ok(f"categories count {len(cats)}")
else:
    bad(f"categories count {len(cats)} (crates.io allows at most 5)")
for c in cats:
    ok(f"category '{c}'")

inc = pkg.get("include") or []
if inc:
    ok(f"package.include has {len(inc)} entries")
    joined = " ".join(inc)
    for banned in ("scripts", "fuzz", "target", ".git"):
        # Match path roots / globs, not substrings inside words.
        if any(
            ent == banned
            or ent.startswith(banned + "/")
            or ent.startswith(banned + "/**")
            or ent == banned + "/**"
            for ent in inc
        ):
            bad(f"package.include must not ship {banned}/")
else:
    bad("package.include missing — prefer an allowlist so scripts/fuzz stay out of the crate")

for f in ("LICENSE-MIT", "LICENSE-APACHE", "README.md", "CHANGELOG.md"):
    if pathlib.Path(f).is_file():
        ok(f"{f} present")
    else:
        bad(f"missing {f}")

msrv = pkg.get("rust-version")
if msrv:
    ok(f"rust-version={msrv}")
else:
    bad("missing rust-version (MSRV)")

sys.exit(1 if fail else 0)
PY
then
  : # python checks passed
else
  fail=$((fail + 1))
fi

# Confirm the packed crate does not contain scripts/ or fuzz/.
if cargo package --allow-dirty --no-verify --quiet 2>/dev/null; then
  crate="target/package/${name}-${ver}.crate"
  if [[ -f "$crate" ]]; then
    if tar -tzf "$crate" | grep -E '/(scripts|fuzz)/' >/dev/null; then
      bad "packed crate contains scripts/ or fuzz/"
    else
      ok "packed crate omits scripts/ and fuzz/"
    fi
    size_k=$(( $(wc -c <"$crate") / 1024 ))
    if [[ "$size_k" -lt 5000 ]]; then
      ok "packed crate size ~${size_k}KiB"
    else
      bad "packed crate unexpectedly large (~${size_k}KiB)"
    fi
  else
    bad "expected ${crate} after cargo package"
  fi
else
  bad "cargo package --no-verify failed"
fi

if [[ "$fail" -ne 0 ]]; then
  echo "check-crate-metadata: FAILED" >&2
  exit 1
fi
echo "check-crate-metadata: OK — crates.io metadata looks publish-shaped"
exit 0
