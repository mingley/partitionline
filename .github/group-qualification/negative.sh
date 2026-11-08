#!/usr/bin/env bash
set -euo pipefail
test "$(git -C negative rev-parse HEAD)" = "$GITHUB_SHA"
candidate_source="$(sha256sum src/net.rs | cut -d ' ' -f 1)"
negative_source="$(sha256sum negative/src/net.rs | cut -d ' ' -f 1)"
test "$candidate_source" = "$negative_source"

python3 - <<'PY'
from pathlib import Path

path = Path("negative/src/net.rs")
text = path.read_text()
start = text.index("                        IdleRead::Eof => {")
end = text.index("                            arm.notify_one();", start)
section = text[start:end]
old = "drop(poll_fn(|cx| io.poll_terminal_shutdown(cx)).await);"
new = "drop(poll_fn(|cx| Pin::new(&mut *io).poll_shutdown(cx)).await);"
assert section.count(old) == 1
section = section.replace(old, new)
text = text[:start] + section + text[end:]
old_drop = """impl Drop for BrokerConn {
    fn drop(&mut self) {
        // A requested physical retirement keeps the existing watcher alive
        // until a held stream lock is released; no additional task is spawned.
        if !self.physical_retire.load(Ordering::SeqCst) {
            self.idle_watch.abort();
        }
    }
}"""
new_drop = """impl Drop for BrokerConn {
    fn drop(&mut self) {
        self.idle_watch.abort();
    }
}"""
assert text.count(old_drop) == 1
path.write_text(text.replace(old_drop, new_drop))
PY

git -C negative diff -- src/net.rs
names=(
  net::tests::idle_watcher_closes_when_tls_shutdown_stalls_after_eof
  net::tests::owner_drop_keeps_retirement_watcher_until_detached_holder_releases
)
messages=(
  'TLS EOF watcher held the stream while shutdown was pending'
  'the retirement watcher must remain pending while the holder owns the stream'
)
for index in 0 1; do
  log="$(mktemp)"
  status=0
  (
    cd negative
    CARGO_TARGET_DIR="$GITHUB_WORKSPACE/negative-target" cargo +stable test --locked --lib "${names[$index]}" -- --exact --nocapture
  ) >"$log" 2>&1 || status=$?
  cat "$log"
  test "$status" -ne 0
  grep -Fq 'test result: FAILED. 0 passed; 1 failed;' "$log"
  grep -Fq "${messages[$index]}" "$log"
  rm "$log"
done
