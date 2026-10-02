#!/usr/bin/env bash
# Native C only. Source, build products and dependencies never enter Cargo.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
BUILD_DIR="${C_PEER_BUILD_DIR:-$ROOT/work/librdkafka-peer}"
mkdir -p "$BUILD_DIR"
BUILD_DIR="$(cd "$BUILD_DIR" && pwd)"
export BUILD_DIR
PIN="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["commit"])' "$HERE/source-pin.json")"
if [[ ! -d "$BUILD_DIR/source/.git" ]]; then
  git clone --quiet --depth 1 --branch v2.15.0 https://github.com/confluentinc/librdkafka.git "$BUILD_DIR/source"
fi
[[ "$(git -C "$BUILD_DIR/source" rev-parse HEAD)" == "$PIN" ]] || { echo 'C peer: source pin mismatch' >&2; exit 1; }
[[ -z "$(git -C "$BUILD_DIR/source" status --porcelain --untracked-files=no)" ]] || { echo 'C peer: tracked library source is modified' >&2; exit 1; }
(
  cd "$BUILD_DIR/source"
  ./configure --no-download --enable-ssl --enable-zlib --enable-zstd --disable-gssapi --disable-curl --disable-lz4-ext --prefix="$BUILD_DIR/install" > "$BUILD_DIR/configure.log" 2>&1
  make -C src -j"${C_PEER_BUILD_JOBS:-2}" > "$BUILD_DIR/build.log" 2>&1
)
mkdir -p "$BUILD_DIR/lib"
cp -L "$BUILD_DIR/source/src/librdkafka.so.1" "$BUILD_DIR/lib/librdkafka.so.1"
ln -sf librdkafka.so.1 "$BUILD_DIR/lib/librdkafka.so"
"${CC:-cc}" -std=c11 -O3 -Wall -Wextra -Werror -isystem "$BUILD_DIR/source/src" \
  "$HERE/peer.c" -L"$BUILD_DIR/lib" -Wl,-rpath,'$ORIGIN/lib' -lrdkafka -o "$BUILD_DIR/c-peer"
python3 - "$HERE" <<'PY'
import hashlib, json, os, pathlib, subprocess, sys
here = pathlib.Path(sys.argv[1]); build = pathlib.Path(os.environ['BUILD_DIR'])
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def out(args): return subprocess.check_output(args, text=True).strip()
manifest = json.loads((here/'source-pin.json').read_text())
manifest.update(compiler=out([os.environ.get('CC','cc'),'--version']).splitlines()[0],
                build_tool=out(['make','--version']).splitlines()[0],
                peer_source_sha256=sha(here/'peer.c'), binary_sha256=sha(build/'c-peer'),
                library_sha256=sha(build/'lib/librdkafka.so.1'),
                dependencies={x:out(['pkg-config','--modversion',x]) for x in ['openssl','zlib','libzstd']},
                library_tree=out(['git','-C',str(build/'source'),'rev-parse','HEAD^{tree}']))
(build/'build-manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
PY
printf '%s\n' "$BUILD_DIR/c-peer"
