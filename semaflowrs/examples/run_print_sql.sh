#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_print_sql.sh <models_dir> <request_json> [--release]

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <models_dir> <request_json> [--release]"
  exit 1
fi

MODELS_DIR="$1"
REQUEST_JSON="$2"
PROFILE="debug"
if [ "${3-}" = "--release" ]; then
  PROFILE="release"
fi

# Build the example binary once
if [ "$PROFILE" = "release" ]; then
  cargo build --example print_sql --release >/dev/null
  BIN="target/release/examples/print_sql"
else
  cargo build --example print_sql >/dev/null
  BIN="target/debug/examples/print_sql"
fi

"$BIN" "$MODELS_DIR" "$REQUEST_JSON"
