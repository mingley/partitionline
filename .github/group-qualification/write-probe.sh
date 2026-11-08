#!/usr/bin/env bash
set -euo pipefail

name=net::tests::write_timeout_shuts_the_broker_down
for variant in original larger_read; do
  if [ "$variant" = larger_read ]; then
    python3 - <<'PY'
from pathlib import Path

for path in (Path("src/net.rs"), Path("baseline/src/net.rs")):
    text = path.read_text()
    marker = "    async fn write_timeout_shuts_the_broker_down() {"
    start = text.index(marker)
    end = text.index("    #[tokio::test", start)
    test = text[start:end]
    old = "let mut buf = [0u8; 8];"
    assert test.count(old) == 1
    test = test.replace(old, "let mut buf = [0u8; 8192];")
    path.write_text(text[:start] + test + text[end:])
PY
    git diff -- src/net.rs
    git -C baseline diff -- src/net.rs
  fi
  for round in 1 2 3; do
    for impl in baseline candidate; do
      log="$(mktemp)"
      if [ "$impl" = baseline ]; then
        path="$GITHUB_WORKSPACE/baseline"
        target="$GITHUB_WORKSPACE/write-baseline-target"
      else
        path="$GITHUB_WORKSPACE"
        target="$GITHUB_WORKSPACE/write-candidate-target"
      fi
      status=0
      (cd "$path" && CARGO_TARGET_DIR="$target" cargo +stable test --locked --lib "$name" -- --exact --nocapture) >"$log" 2>&1 || status=$?
      printf 'write_timeout_probe variant=%s round=%s implementation=%s status=%s\n' "$variant" "$round" "$impl" "$status"
      cat "$log"
      grep -Fq 'running 1 test' "$log"
      if [ "$status" -eq 0 ]; then
        grep -Fq 'test result: ok. 1 passed; 0 failed;' "$log"
      else
        grep -Fq 'test result: FAILED. 0 passed; 1 failed;' "$log"
        grep -Fq 'a write timeout left the broker blocked in a read' "$log"
      fi
      rm "$log"
    done
  done
done
