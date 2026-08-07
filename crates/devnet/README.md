# Quilibrium local test environment

A Docker-based development network harness that runs multiple Quilibrium nodes
locally and helps test consensus under controlled network partitions. 

It spins up 4 archive nodes + 4 client nodes via `docker compose`, fronted by a
proxy that intercepts gossip and gRPC traffic, applies network partitions at
specified consensus views, and reports the run's outcome (frame liveness,
safety, client enrollment, and optionally app-shard consensus progress) back to
the orchestrator. If the test fails, logs from each node are saved to disk.

## Prerequisites

- Rust toolchain (see `rust-toolchain.toml`)
- Docker (with `docker compose`)

## Quick start

```
# single run with one partition at view 1, stopping at frame 5
cargo run -p devnet -- single --verbose --stopframe=5 \
  --view-partitions='[{"view":1,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]}]'

# additionally require 3 frames of app-shard consensus on the devnet token app
cargo run -p devnet -- single --verbose --stopframe=30 --app-stop-frame=3
```

Run `cargo run -p devnet -- --help` (and `single --help` / `exhaustive --help`)
for the full flag list.

### App-shard consensus (`--app-stop-frame`)

With `--app-stop-frame=N` (default 0 = off), terminal success additionally
requires N frames of **app shard consensus** on the *devnet token app* — a
QUIL-functionality token clone deployed deterministically at testnet genesis
(env `QUIL_SEED_APP_TOKEN=1`, set on every node service in
`docker-compose.yml`; `quil-engine`'s `genesis::devnet_token_domain()` is the
single source of truth for its address). The app has exactly ONE shard, so its
consensus filter is the bare 32-byte app address.

How the pieces fit:

- Each of the 4 clients pins its single worker to that address via
  `engine.dataWorkerFilters` and runs `appConsensusCw: true` — together they
  form a 4-member commonware-simplex committee (quorum 3-of-4, so shard
  *liveness* tolerates one stalled client — but the run's verdict does not:
  terminal verification requires all four clients to converge on the app stop
  frame and to have voted, so a stalled client still fails the run).
- Config pinning alone never joins a shard (pinned workers are
  `manually_managed`, which the auto-join lifecycle skips), so the proxy's
  *join driver* issues `NodeService.RequestJoin` for each client and re-issues
  it if the allocation doesn't land.
- Shard consensus is gossip-only, and the clients' only peer is the proxy: the
  proxy bulk-subscribes the per-shard bitmasks (as archives do) to relay the
  committee's traffic, and tracks the shard's frame number from the full
  `AppShardFrame` each member publishes on finalization (with equivocation
  detection on the frame output).
- The terminal verification fires only when BOTH the global stop frame and the
  app target are met (one-shot latches on each side); the single global timeout
  remains the failure backstop for both chains.
- App-shard consensus is then verified with the same structure as global
  consensus: a *convergence* check polls every committee client's
  `AppShardService` until all of them serve the app stop frame; a *safety*
  check runs the same chain-linearity verification over the polled shard
  frames (plus gossip-level equivocation detection — an observed fork
  fast-fails the run instead of waiting out the timeout); and a
  *participation* check requires every committee member to have originated a
  shard consensus vote at or after the view that produced the app stop frame
  (the app analogue of the global rejoin check — a member that merely ingested
  finalized frames without voting fails it). On a timeout, the notification
  carries real polled per-node counts for both chains, so the verdict names
  the side that actually lagged.

Timing: the compose file runs a deliberately fast devnet cadence — global
frames land every ~1 s (`QUIL_IDEAL_FRAME_TIME_MS=1000` paces the proposer and
sets the ASERT target; `QUIL_MIN_DIFFICULTY=5000` shrinks the per-frame VDF
solve to ~0.25 s so it fits the interval) with 15-frame epochs
(`QUIL_EPOCH_LENGTH_FRAMES=15` ≈ 15 s per epoch — kept above the 10-frame
confirm window it must contain). A join confirms after the testnet confirm
window and the allocation turns Active at the next epoch boundary; the shard
only starts finalizing once the Active allocations re-register fresh per-epoch
storage leaf roots (the `ReconfirmEpoch` lifecycle action — proposals before
that fail storage-attestation verification by design). That activation is ~2
epoch boundaries; app frames themselves are unpaced under commonware-simplex
and finalize in seconds. A full app run measures ~150 s wall clock; when
`--app-stop-frame` is set and `--global-timeout` is left at its default, the
orchestrator raises the timeout to 600 s to cover slower hosts and stalled
views (~30 s each, `consensusLeaderTimeoutSecs`).

`QUIL_SEED_APP_TOKEN`, `QUIL_EPOCH_LENGTH_FRAMES`, `QUIL_IDEAL_FRAME_TIME_MS`
and `QUIL_MIN_DIFFICULTY` are genesis/consensus parameters hardcoded on
**every** node service in the compose file — a node missing the first two
forks at genesis, and a divergent cadence value skews the difficulty target
and header timestamps. The client containers use dedicated 2-CPU cpusets
(`0-1` … `6-7`), so the stack wants a host with ~10+ logical cores.

### The partition schedule

The schedule is keyed on the **simplex consensus view**, and is a point trigger
with an implicit heal: an entry applies when its view is observed, and the first
observed view with *no* entry clears every partition.

A view is not a frame. Views advance on every consensus round, including the
nullified rounds a partition induces — which produce no frame at all. The proxy
reads views off the simplex vote and certificate channels (every `Notarize`,
`Nullify` and `Finalize` is broadcast to all peers) as well as off proposed
frames, so it sees a view as soon as the first vote for it crosses the wire,
rather than waiting for that round to produce a block.

Three consequences worth planning around:

- **A partition that leaves no quorum anywhere freezes the view.** Simplex
  advances a view on a notarization or a nullification *certificate*, and both
  need a quorum. Split 4 archives 2-and-2 and neither side can form one: every
  node re-broadcasts `Nullify` for the same view indefinitely, the view never
  moves, and the schedule never reaches its healing view. Such a run ends at
  `--global-timeout`. Keep a quorum on one side of the split (e.g. 3-and-1) if
  the schedule is meant to heal itself.
- **A stalled view costs ~30s of wall clock** (`consensusLeaderTimeoutSecs`,
  default 30). Budget `--global-timeout` accordingly for a schedule that stalls
  consensus for several views.
- **To hold a partition open, repeat the entry on consecutive views** — a single
  entry lasts exactly one view:

  ```
  --view-partitions='[
    {"view":3,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]},
    {"view":4,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]},
    {"view":5,"partition1":["archive-1","archive-2","archive-3"],"partition2":["archive-4"]}
  ]'
  ```

The schedule must heal before the stop frame, or the rejoin check can never
observe the isolated archive voting for the last frame. The orchestrator rejects
a schedule that heals too late up front rather than reporting a spurious failure.

## Development

Run `./test.sh` (or `./test.sh -short` to skip the Docker integration run) to test
changes.

## Architecture

Two binaries make up the harness:

- **`devnet`** — the host-side orchestrator: CLI, Docker compose orchestration,
  notification server, and log capture.
- **`devnet-proxy`** (`./proxy`) — the in-container gossip/gRPC proxy that 
  enforces a predefined partition schedule and verifies invariants:
  - All archive nodes reach a predefined stop frame.
  - The archives' committed chain is a single linear chain (safety).
  - All archive nodes participate in consensus after the network is healed.
  - Every client node can successfully join as a prover.
  - With `--app-stop-frame`, the same three checks (convergence, safety,
    participation) for the app-shard committee's chain.

## Common issues

If you get:

```
Error response from daemon: all predefined address pools have been fully subnetted
```

decrease the capacity of each bridge network so Docker can allocate more
networks, by adding to `/etc/docker/daemon.json`:

```json
{
  "default-address-pools" : [
    { "base" : "172.17.0.0/12", "size" : 20 },
    { "base" : "192.168.0.0/16", "size" : 24 }
  ]
}
```

then `sudo systemctl restart docker`. See
[this article](https://straz.to/2021-09-08-docker-address-pools/) for details.
