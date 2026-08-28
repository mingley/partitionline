#!/usr/bin/env bash
# Idempotent setup for partitionline development.
#
# Runs after the repository is checked out. Prepares the Rust toolchain,
# compiles dependencies against the committed Cargo.lock, and installs a local
# Apache Kafka broker used by the examples/benchmarks (the test suite itself
# uses an in-tree mock broker and needs no external Kafka).
set -euo pipefail

KAFKA_HOME=/opt/kafka
KAFKA_VERSION=3.9.1
KAFKA_DATA="${KAFKA_HOME}/data"
KAFKA_CFG="${KAFKA_HOME}/config/kraft/pl-server.properties"

echo "==> Rust toolchain (crate MSRV is 1.85; CI builds on stable)"
rustup toolchain install stable --profile minimal -c rustfmt -c clippy
rustup default stable
rustc --version

echo "==> Fetch + build dependencies (locked to Cargo.lock)"
cargo fetch --locked
cargo build --all-targets --locked

echo "==> Apache Kafka ${KAFKA_VERSION} (broker for examples on 127.0.0.1:9092)"
if [ ! -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]; then
  tmp="$(mktemp -d)"
  curl -fsSL -o "${tmp}/kafka.tgz" \
    "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
  sudo mkdir -p "${KAFKA_HOME}"
  sudo tar xzf "${tmp}/kafka.tgz" -C "${KAFKA_HOME}" --strip-components=1
  sudo chown -R "$(id -u):$(id -g)" "${KAFKA_HOME}"
  rm -rf "${tmp}"
fi

mkdir -p "${KAFKA_DATA}" "${KAFKA_HOME}/logs"

# KRaft config that stores its log under the persistent data dir (the stock
# config points at /tmp, which is not durable across boots).
if [ ! -f "${KAFKA_CFG}" ]; then
  sed 's#^log.dirs=.*#log.dirs='"${KAFKA_DATA}"'#' \
    "${KAFKA_HOME}/config/kraft/server.properties" > "${KAFKA_CFG}"
fi

# Format KRaft storage exactly once.
if [ ! -f "${KAFKA_DATA}/meta.properties" ]; then
  cid="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
  "${KAFKA_HOME}/bin/kafka-storage.sh" format -t "${cid}" -c "${KAFKA_CFG}"
fi

echo "==> install complete"
