#!/bin/sh
# Build the local operator binary and run install, observe, backup, restore,
# and rejected-downgrade steps. This does not listen or deploy a cluster.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"
data=$(mktemp -d)
trap 'rm -rf "$data"' EXIT
cargo build --locked --offline --manifest-path partitionline-broker/Cargo.toml --bin operator
bin=$root/partitionline-broker/target/debug/operator
"$bin" init --data "$data/live" --cluster cluster-1 --voter 1
"$bin" register-dir --data "$data/live" --log-dir data-a
"$bin" assign --data "$data/live" --topic orders --partition 0 --replicas 1 --leader 1 --log-dir data-a
"$bin" feature --data "$data/live" --name metadata.version --level 2 --version 0
"$bin" feature --data "$data/live" --name metadata.version --level 3 --version 1
if "$bin" feature --data "$data/live" --name metadata.version --level 1 --version 0; then
  echo "downgrade was accepted" >&2
  exit 1
fi
"$bin" backup --data "$data/live" --to "$data/backup/admin.journal"
"$bin" restore --from "$data/backup/admin.journal" --data "$data/restored"
status=$("$bin" status --data "$data/restored")
printf '%s\n' "$status"
printf '%s\n' "$status" | grep -q 'feature metadata.version 3'
printf '%s\n' "$status" | grep -q 'log_dir orders 0 data-a'
mkdir -p docs/evidence/broker/package
sha=$(git rev-parse HEAD)
rustc=$(rustc --version)
platform=$(uname -s -m)
cat > docs/evidence/broker/package/run.json <<EOF
{
  "source_sha": "$sha",
  "rustc": "$rustc",
  "platform": "$platform",
  "binary": "partitionline-broker/target/debug/operator",
  "downgrade_rejected": true,
  "restored_feature": "metadata.version 3",
  "restored_log_dir": "data-a",
  "listens": false
}
EOF
echo "operator package workflow ok on $platform"
