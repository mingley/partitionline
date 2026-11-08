#!/usr/bin/env bash
set -euo pipefail

test "$(git -C negative rev-parse HEAD)" = "$GITHUB_SHA"
git -C negative checkout -- src/net.rs
candidate_source="$(sha256sum src/net.rs | cut -d ' ' -f 1)"
negative_source="$(sha256sum negative/src/net.rs | cut -d ' ' -f 1)"
test "$candidate_source" = "$negative_source"

python3 - <<'PY'
from pathlib import Path

path = Path("negative/src/net.rs")
text = path.read_text()
start = text.index("    pub async fn write_all_deadline(")
end = text.index("    pub async fn write_all_timeout(", start)
section = text[start:end]
old = "            self.release_if_closed().await;"
assert section.count(old) == 1
section = section.replace(old, "            // Negative control: deliberately omit the physical shutdown.")
text = text[:start] + section + text[end:]

start = text.index('    async fn write_timeout_shuts_the_broker_down()')
end = text.index('    async fn an_expired_pipeline_lets_the_broker_leave()', start)
section = text[start:end]
old = '''        let mut conn = BrokerConn::connect(&addr, "write-timeout", Duration::from_secs(2))
            .await
            .unwrap();
'''
new = old + '''        conn.idle_watch.abort();
        tokio::task::yield_now().await;
'''
assert section.count(old) == 1
section = section.replace(old, new)
path.write_text(text[:start] + section + text[end:])
PY

git -C negative diff -- src/net.rs
log="$(mktemp)"
status=0
(
  cd negative
  CARGO_TARGET_DIR="$GITHUB_WORKSPACE/negative-write-target" \
    cargo +stable test --locked --lib net::tests::write_timeout_shuts_the_broker_down -- --exact --nocapture
) >"$log" 2>&1 || status=$?
cat "$log"
test "$status" -ne 0
grep -Fq 'test result: FAILED. 0 passed; 1 failed;' "$log"
grep -Fq 'a write timeout left the broker blocked in a read' "$log"
test_bin="$(sed -n 's/.*Running unittests src\/lib.rs (\(.*\))$/\1/p' "$log" | tail -n 1)"
test -n "$test_bin"
test_bin="$(realpath "$test_bin")"
case "$test_bin" in
  "$GITHUB_WORKSPACE"/negative-write-target/*) ;;
  *) printf 'unexpected negative executable: %s\n' "$test_bin" >&2; exit 1 ;;
esac
negative_hash="$(sha256sum "$test_bin" | cut -d ' ' -f 1)"
candidate_hash="$(cat "$GITHUB_WORKSPACE/candidate-test.sha256")"
printf 'write_timeout_negative executable=%s sha256=%s candidate_sha256=%s\n' "$test_bin" "$negative_hash" "$candidate_hash"
test "$negative_hash" != "$candidate_hash"
rm "$log"
