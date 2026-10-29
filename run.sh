#!/usr/bin/env bash
# Runs the service with only a JDK (17+) installed — no Maven needed.
# Usage: ./run.sh [input.csv] [options]     e.g.  ./run.sh data/sample-transactions.csv --fail-before-commit TXN-2005:1
set -euo pipefail
cd "$(dirname "$0")"
OUT=target/classes
mkdir -p "$OUT"
javac --release 17 -d "$OUT" $(find src/main/java -name "*.java")
exec java -cp "$OUT" com.fde.txnprocessor.App "$@"
