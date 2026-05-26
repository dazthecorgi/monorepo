#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SHORT=false
for arg in "$@"; do
    if [[ "$arg" == "-short" ]]; then
        SHORT=true
    fi
done

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

# ── simtest/rankpartitions ───────────────────────────────────────────────────────────

cd "$SCRIPT_DIR/rankpartitions"
lint_and_test

# ── simtest/shared ───────────────────────────────────────────────────────────

cd "$SCRIPT_DIR/shared"
lint_and_test

# ── simtest/proxy (separate module) ─────────────────────────────────────────

cd "$SCRIPT_DIR/proxy"
lint_and_test

# ── integration run ──────────────────────────────────────────────────────────

if [[ "$SHORT" == false ]]; then
    cd "$SCRIPT_DIR"
    run go run . single --verbose --stopframe=5 --rank-partitions='[{"rank":0,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]}]'
fi
