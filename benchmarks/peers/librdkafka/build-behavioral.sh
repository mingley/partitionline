#!/usr/bin/env bash
# KL01-13: reuse the KL04-04 pinned native library; unchanged upstream case.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
BUILD_DIR="${C_PEER_BUILD_DIR:-$ROOT/work/librdkafka-peer}"
CASE_DIR="$ROOT/tests/conformance/librdkafka"
python3 - "$BUILD_DIR" "$CASE_DIR" "$HERE/source-pin.json" <<'PY'
import hashlib,json,pathlib,sys
b,c,p=map(pathlib.Path,sys.argv[1:]);m=json.load(open(b/'build-manifest.json'));pin=json.load(open(p));case=json.load(open(c/'pin.json'))
assert m['commit']==pin['commit']==case['commit']
assert hashlib.sha256((b/'lib/librdkafka.so.1').read_bytes()).hexdigest()==m['library_sha256']
assert hashlib.sha256((c/'upstream/0125-immediate_flush.c').read_bytes()).hexdigest()==case['source_sha256']
PY
"${CC:-cc}" -std=c11 -O2 -Wall -Wextra -Werror -Wno-unused-parameter -ffunction-sections \
  -isystem "$BUILD_DIR/source/src" -I"$CASE_DIR/shim" "$CASE_DIR/upstream/0125-immediate_flush.c" "$CASE_DIR/shim/helpers.c" \
  -L"$BUILD_DIR/lib" -Wl,--gc-sections -Wl,-rpath,'$ORIGIN/lib' -lrdkafka -o "$BUILD_DIR/0125-peer"
printf '%s\n' "$BUILD_DIR/0125-peer"
