#!/usr/bin/env bash
set -euo pipefail

# CI for the devnet harness.
#
# Runs formatting, lints, and unit tests for the `devnet` crate, then (unless
# -short is passed) a single end-to-end integration run against the compose
# stack. The integration run requires Docker and the proxy image.

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

# ── lint + unit tests ────────────────────────────────────────────────────────
# clippy is run with --no-deps: some transitive workspace crates set their own
# `#![deny(clippy::pedantic)]`, which fails under newer clippy and is out of
# scope here. We only gate on devnet's own lints.

run cargo fmt -p devnet -p devnet-proxy -- --check
run cargo clippy -p devnet -p devnet-proxy --no-deps --all-targets -- -D warnings
run cargo nextest run -p devnet -p devnet-proxy

# ── integration run ──────────────────────────────────────────────────────────

if [[ "$SHORT" == false ]]; then
    cd "$SCRIPT_DIR"
    run cargo run -p devnet -- single --verbose --stopframe=5 \
        --view-partitions='[{"view":2,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]}]'

    # Empty-store forward-fill regression: isolate archive-4 for views 1 AND 2
    # — the first frames — so its archive poller is still at last_frame=0 when
    # frames 1-2 finalize without it. Catching up after the heal then REQUIRES
    # the forward-fill pass to fetch from frame 1 out of an empty store, the
    # exact path a `last_frame > 0` guard in quil-rpc's poller used to skip:
    # the poller latched onto the polled head, the hole below it was permanent
    # (the record-gap scan runs only at startup), and the materializer wedged
    # before the frames carrying the client registrations — failing enrollment.
    # The view-2-only run above races this bug (the poller may have polled head
    # 1 before the partition applied); this one cannot.
    run cargo run -p devnet -- single --verbose --stopframe=5 \
        --view-partitions='[{"view":1,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]},{"view":2,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]}]'

    # App-shard run: the 4 clients form a 4-member simplex committee on the
    # genesis-deployed devnet token app's single shard; terminal success
    # additionally requires that shard to reach --app-stop-frame. The shard
    # starts finalizing only after the allocations turn Active at an epoch
    # boundary and re-register fresh leaf roots (~2 epochs — ~30 s at the
    # compose file's 1 s cadence and 15-frame epochs; a full run measures
    # ~150 s). The global timeout is left at its default so the orchestrator's
    # app-run auto-bump (600 s) applies.
    run cargo run -p devnet -- single --verbose --stopframe=30 \
        --app-stop-frame=3

    # App-shard committee under partition: fully isolate client-1 (from all 7
    # other nodes — the partitioner only blocks listed cross-group pairs) for
    # global views 22-23, mid-epoch-2, before any app frame can finalize. The
    # remaining 3-of-4 committee keeps quorum; client-1 must heal its global
    # frame gap (client-side forward-fill) and rejoin shard CW in time to vote
    # at/after the app target view, or the participation check fails the run.
    run cargo run -p devnet -- single --verbose --stopframe=30 \
        --app-stop-frame=3 \
        --view-partitions='[{"view":22,"partition1":["archive-1","archive-2","archive-3","archive-4","client-2","client-3","client-4"],"partition2":["client-1"]},{"view":23,"partition1":["archive-1","archive-2","archive-3","archive-4","client-2","client-3","client-4"],"partition2":["client-1"]}]'
fi
