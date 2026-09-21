#!/usr/bin/env bash
# KL01-03, KL01-04, KL01-05 & KL01-06: Pinned Apache Kafka Java wire-protocol fixture generator.
#
# Generates reproducible reference wire fixtures using pinned Apache Kafka
# message implementations (Message.write). Generation is independent of
# partitionline encoders. Rust tests consume committed fixtures without
# requiring Java or network access.
#
# Usage:
#   bash scripts/generate-protocol-fixtures.sh           # Generate fixtures
#   bash scripts/generate-protocol-fixtures.sh --verify  # Verify committed fixtures match byte-for-byte
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

JAVA_SRC_DIR="$ROOT/tests/conformance/java/src/main/java"
DEFAULT_OUT_DIR="$ROOT/tests/fixtures/protocol_oracles"
OUT_DIR="${DEFAULT_OUT_DIR}"
VERIFY_MODE=0

# Pinned upstream coordinates and checksums
KAFKA_VERSION="3.9.1"
KAFKA_SHA_DIST="25c5e4eb059c35766f645c0e0bd2fe623a1ebdc18250957506b1edbf476d1272"
KAFKA_SHA_CENTRAL="7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9"
SLF4J_VERSION="1.7.36"
SLF4J_SHA="d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"

CACHE_DIR="${CONFORMANCE_CACHE_DIR:-/tmp/partitionline-conformance}"
KAFKA_JAR="$CACHE_DIR/kafka-clients-${KAFKA_VERSION}.jar"
SLF4J_JAR="$CACHE_DIR/slf4j-api-${SLF4J_VERSION}.jar"
CLASSES_DIR="$CACHE_DIR/classes"

DECODE_RUST_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --decode-rust)
      shift
      DECODE_RUST_ARGS=("$@")
      break
      ;;
    --verify|--check)
      VERIFY_MODE=1
      shift
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--verify] [--out-dir <dir>] | [--decode-rust <req|resp|fetch-req|fetch-resp|metadata-req|metadata-resp> <version> <input>]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# Step 1: Check Java toolchain availability
if ! command -v java >/dev/null 2>&1 || ! command -v javac >/dev/null 2>&1; then
  echo "generate-protocol-fixtures: ERROR: java and/or javac not found" >&2
  echo "KL01-03: Pinned Java fixture generation requires javac and java." >&2
  exit 1
fi

JAVA_VER="$(java -version 2>&1 | head -n 1)"
echo "== generate-protocol-fixtures: Java toolchain detected: ${JAVA_VER} =="

# Step 2: Resolve pinned dependency jars
mkdir -p "$CACHE_DIR"

compute_sha256() {
  local file="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    sha256sum "$file" | awk '{print $1}'
  fi
}

verify_jar() {
  local jar="$1"
  local expected1="$2"
  local expected2="${3:-}"
  local actual
  actual="$(compute_sha256 "$jar")"
  if [[ "$actual" == "$expected1" || ( -n "$expected2" && "$actual" == "$expected2" ) ]]; then
    return 0
  else
    echo "Checksum mismatch for $jar: expected $expected1 (or $expected2), got $actual" >&2
    return 1
  fi
}

resolve_dependencies() {
  # Check if already present and verified in CACHE_DIR
  if [[ -f "$KAFKA_JAR" ]] && verify_jar "$KAFKA_JAR" "$KAFKA_SHA_DIST" "$KAFKA_SHA_CENTRAL" && \
     [[ -f "$SLF4J_JAR" ]] && verify_jar "$SLF4J_JAR" "$SLF4J_SHA"; then
    return 0
  fi

  # Check local vendor lib directory
  local local_lib="$ROOT/tests/conformance/java/lib"
  if [[ -f "$local_lib/kafka-clients-${KAFKA_VERSION}.jar" ]]; then
    cp "$local_lib/kafka-clients-${KAFKA_VERSION}.jar" "$KAFKA_JAR"
  fi
  if [[ -f "$local_lib/slf4j-api-${SLF4J_VERSION}.jar" ]]; then
    cp "$local_lib/slf4j-api-${SLF4J_VERSION}.jar" "$SLF4J_JAR"
  fi

  # Try extracting from local Docker image if available (completely offline)
  if command -v docker >/dev/null 2>&1; then
    if docker image inspect "apache/kafka:${KAFKA_VERSION}" >/dev/null 2>&1; then
      echo "generate-protocol-fixtures: extracting jars from local Docker image apache/kafka:${KAFKA_VERSION}..."
      if [[ ! -f "$KAFKA_JAR" ]] || ! verify_jar "$KAFKA_JAR" "$KAFKA_SHA_DIST" "$KAFKA_SHA_CENTRAL"; then
        docker run --rm --entrypoint cat "apache/kafka:${KAFKA_VERSION}" "/opt/kafka/libs/kafka-clients-${KAFKA_VERSION}.jar" > "$KAFKA_JAR"
      fi
      if [[ ! -f "$SLF4J_JAR" ]] || ! verify_jar "$SLF4J_JAR" "$SLF4J_SHA"; then
        docker run --rm --entrypoint cat "apache/kafka:${KAFKA_VERSION}" "/opt/kafka/libs/slf4j-api-${SLF4J_VERSION}.jar" > "$SLF4J_JAR"
      fi
    fi
  fi

  # Fall back to Maven Central download if still missing and verify sha256
  if [[ ! -f "$KAFKA_JAR" ]] || ! verify_jar "$KAFKA_JAR" "$KAFKA_SHA_DIST" "$KAFKA_SHA_CENTRAL"; then
    echo "generate-protocol-fixtures: downloading kafka-clients-${KAFKA_VERSION}.jar from Maven Central..."
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar" -o "$KAFKA_JAR"
    verify_jar "$KAFKA_JAR" "$KAFKA_SHA_DIST" "$KAFKA_SHA_CENTRAL" || { rm -f "$KAFKA_JAR"; exit 1; }
  fi

  if [[ ! -f "$SLF4J_JAR" ]] || ! verify_jar "$SLF4J_JAR" "$SLF4J_SHA"; then
    echo "generate-protocol-fixtures: downloading slf4j-api-${SLF4J_VERSION}.jar from Maven Central..."
    curl -fsSL "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar" -o "$SLF4J_JAR"
    verify_jar "$SLF4J_JAR" "$SLF4J_SHA" || { rm -f "$SLF4J_JAR"; exit 1; }
  fi
}

echo "== generate-protocol-fixtures: resolving pinned dependencies =="
resolve_dependencies
echo "  kafka-clients: ${KAFKA_JAR} ($(compute_sha256 "$KAFKA_JAR"))"
echo "  slf4j-api:     ${SLF4J_JAR} ($(compute_sha256 "$SLF4J_JAR"))"

# Step 3: Compile generator classes
mkdir -p "$CLASSES_DIR"
CP="${KAFKA_JAR}:${SLF4J_JAR}"

echo "== generate-protocol-fixtures: compiling generator =="
javac -cp "$CP" -d "$CLASSES_DIR" \
  "$JAVA_SRC_DIR/org/slf4j/impl/StaticLoggerBinder.java" \
  "$JAVA_SRC_DIR/org/apache/kafka/conformance/FixtureGenerator.java"

# Step 4: Run generator
RUN_CP="${CP}:${CLASSES_DIR}"

if [[ ${#DECODE_RUST_ARGS[@]} -gt 0 ]]; then
  exec java -cp "$RUN_CP" org.apache.kafka.conformance.FixtureGenerator --decode-rust "${DECODE_RUST_ARGS[@]}"
fi

GEN_ARGS=(--out-dir "$OUT_DIR")
if [[ "$VERIFY_MODE" -eq 1 ]]; then
  GEN_ARGS+=(--verify)
  echo "== generate-protocol-fixtures: verifying fixtures against ${OUT_DIR} =="
else
  echo "== generate-protocol-fixtures: writing fixtures to ${OUT_DIR} =="
fi

java -cp "$RUN_CP" org.apache.kafka.conformance.FixtureGenerator "${GEN_ARGS[@]}"

if [[ "$VERIFY_MODE" -eq 0 ]]; then
  echo "== generate-protocol-fixtures: committed fixture hashes =="
  for f in "$OUT_DIR"/*_request.bin "$OUT_DIR"/*_response.bin "$OUT_DIR"/*.json; do
    if [[ -f "$f" && "$(basename "$f")" != "matrix.json" ]]; then
      echo "  $(basename "$f"): $(compute_sha256 "$f")"
    fi
  done
fi

echo "generate-protocol-fixtures: ok"
