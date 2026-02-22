#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

run() {
    echo ""
    echo ">>> $*"
    "$@"
}

lint_and_test() {
    run go vet ./...
    run staticcheck ./...
    run go test ./...
}

# ── simtest (root module) ────────────────────────────────────────────────────

cd "$SCRIPT_DIR"
lint_and_test

# ── simtest/shared ───────────────────────────────────────────────────────────

cd "$SCRIPT_DIR/shared"
lint_and_test

# ── simtest/proxy (separate module) ─────────────────────────────────────────

cd "$SCRIPT_DIR/proxy"
lint_and_test

# ── integration run ──────────────────────────────────────────────────────────

cd "$SCRIPT_DIR"

run go run . -verbose -stopframe=2 -partition1=archive-1,archive-2,archive-3 -partition2=archive-4
