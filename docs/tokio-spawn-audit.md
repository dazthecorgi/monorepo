# `tokio::spawn` Error & Panic Propagation Audit

Audit date: 2026-05-23
Scope: all `tokio::spawn`, `tokio::task::spawn`, and `tokio::task::spawn_blocking` call sites in this monorepo (112 sites across ~20 files).

## TL;DR

Errors inside spawned task bodies are usually logged with `warn!`/`error!`, but **panics are not propagated** at roughly 70% of spawn sites. The `JoinHandle` is dropped, so a panicking task disappears silently and the surrounding system continues as if the work succeeded. The most exposed subsystems are the consensus event loop, frame/bundle publishers, lifecycle action submitters, and the P2P command shims.

The codebase already contains a good reference pattern — `crates/quil-p2p/src/node.rs:736` spawns a watcher task that awaits the swarm `JoinHandle`, downcasts the panic payload, and logs at `error!`. This pattern should be generalized into a small helper and applied to the high-risk spawns.

---

## Methodology

For each call site we recorded:

1. Whether the future returns a `Result` and how `Err` is handled inside the task.
2. What happens to the returned `JoinHandle` — awaited, stored, or dropped.
3. Whether a panic would reach a `JoinError` consumer or vanish.

We then bucketed sites into four categories.

---

## Category 1 — Fire-and-forget (panics swallowed)

`JoinHandle` is dropped at the end of the statement. Internal errors are logged; panics are gone.

Representative sites:

- `crates/quil-engine/src/consensus_glue.rs:447` — spawned `sleep_until(...) + publish_consensus(bytes)`. No error handling at all inside the task. A panic during the sleep window or inside `publish_consensus` stalls consensus with no signal.
- `crates/quil-engine/src/app_engine.rs:1238` — shard consensus event loop. `event_loop.run().await` returns `Result`; `Err` is logged, but a panic inside `run` exits the task and the shard remains "running" in the parent state machine.
- `crates/quil-engine/src/prover_pipeline.rs:80, 90, 98, 106, 114, 122, 130` — seven lifecycle actions (`ProposeJoin`, `ConfirmJoins`, `RejectJoins`, `ProposeLeave`, `ConfirmLeaves`, `RejectLeaves`, `ProposeSeniorityMerge`). Each logs `Err` via `warn!`, but a panic mid-submission leaves the lifecycle state machine believing the action ran.
  - Note: `ProposeJoin` (line 80) also sets `set_proof_in_progress(true)` before spawning and resets it inside the task. A panic before the reset would pin the flag permanently — this is a concrete livelock risk.
- `crates/quil-node/src/main.rs:1625, 1652, 1673, 1693, 1730, 1749` — P2P publish tasks for shard frames, bundles, and global prover messages. Logged-but-not-watched.
- `crates/quil-node/src/main.rs:5033, 5037, 5051, 5057` — `subscribe`, `unsubscribe`, `set_peer_score`, `add_peer_score` shims. **Zero error handling inside the task body** — the call's `Result` is even dropped at `subscribe`/`unsubscribe` sites where the method returns `()`. A panic in the P2P command channel is invisible.
- `crates/quil-rpc/src/proxy_pubsub.rs:139` — proxy message stream reader. Channels errors out but a panic in the read loop kills the proxy without surfacing.

## Category 2 — Handle stored but only sometimes awaited

The handle is kept but consumption is conditional, so panic visibility depends on the happy path.

- `crates/quil-rpc/src/frame_sync.rs:228, 516, 541` — archive poller and frame-sync waiters. Caller owns the `JoinHandle<()>` but lifetime semantics are unclear; if the parent drops without awaiting, panics disappear.

## Category 3 — Handle awaited (panics surface as `JoinError`)

Good citizens. Panics turn into a `JoinError` that the consumer sees.

- `crates/quil-node/src/prover_message_transport_prod.rs:159` — fan-out spawns stored in `handles`, then `h.await` in a loop with both `Err` and `JoinError` logged.
- `crates/quil-engine/src/prover_pipeline.rs:195` — `spawn_blocking` tasks awaited with `.await?`, propagating both task errors and join errors via `?`.
- `crates/quil-engine/src/event_distributor.rs:239` — 10 worker tasks awaited with `.unwrap()` (line 253). A panic in any worker re-panics the awaiter, which is at least loud, but the awaiter itself is then a fire-and-forget task — see Category 1.
- `crates/quil-engine/src/thread_worker.rs:568` — engine handle joined inside a `select!` with an abort path on cancellation.

## Category 4 — Explicit panic watcher (the reference pattern)

- `crates/quil-p2p/src/node.rs:736` — spawns a watcher task that does `swarm_task.await`, branches on `join_err.is_panic()`, downcasts the payload to `&str`/`String`, and logs at `error!`. **This is what every high-risk spawn should look like.**

---

## Critical concerns (ranked)

1. **`consensus_glue.rs:447` — consensus proposal publication.** Stalls global consensus silently on panic. Highest blast radius.
2. **`app_engine.rs:1238` — shard consensus event loop.** Shard appears alive but is dead; queued messages back up forever.
3. **`prover_pipeline.rs:80` — `ProposeJoin` proof-in-progress flag.** A panic between `set_proof_in_progress(true)` and the reset on line 82 pins the flag and prevents future evaluations. Concrete livelock path.
4. **`main.rs:1625, 1652, 1673, 1693, 1730, 1749` — frame/bundle publish loops.** Partial publishes can poison downstream consensus state; lack of a watcher hides the cause.
5. **`main.rs:5033, 5037, 5051, 5057` — P2P sub/unsub/score shims.** No internal error handling at all. A panic leaves subscriptions and peer scores inconsistent across nodes.
6. **`prover_pipeline.rs:88–135` — lifecycle action submitters (the remaining six).** Lifecycle state machine drifts out of sync with chain reality on panic.

---

# Plan

The fixes split into three phases. Each phase is independently shippable.

## Phase 1 — Introduce a supervised-spawn helper (1 PR)

Add a small utility crate-local helper that wraps `tokio::spawn` and guarantees panic visibility. Model it on the watcher in `quil-p2p/src/node.rs:736`.

Proposed signature, somewhere like `crates/quil-util/src/task.rs` (or wherever shared utilities already live):

```rust
/// Spawn a task whose panic will be logged at `error!` with the supplied
/// `name` as context. Returns the JoinHandle so callers can also await
/// if they want, but dropping it no longer means a panic is silent.
pub fn spawn_supervised<F>(name: &'static str, fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = tokio::spawn(fut);
    let watcher_handle = ...; // clone-or-share strategy TBD; see note below
    tokio::spawn(async move {
        match watcher_handle.await {
            Ok(_) => {}
            Err(e) if e.is_panic() => {
                let msg = panic_payload_to_string(e.into_panic());
                tracing::error!(task = name, panic = %msg, "supervised task panicked");
            }
            Err(e) => tracing::error!(task = name, error = %e, "supervised task join error"),
        }
    });
    handle
}
```

Implementation note: `JoinHandle` is not `Clone`, so the helper either has to (a) return a wrapper that owns the join channel and forwards a panic-aware watcher in `Drop`, or (b) consume the handle entirely and return `()` for fire-and-forget callers. Option (b) is simpler and matches the actual usage at every Category 1 site. Suggest shipping both: `spawn_supervised_detached(name, fut)` for fire-and-forget, and a `SupervisedJoinHandle` wrapper for callers who still want to await.

Tasks:

- [ ] Decide on home crate (existing `quil-util` or a new shared `task` module).
- [ ] Implement `spawn_supervised_detached` and `SupervisedJoinHandle`.
- [ ] Unit tests: panic propagates to `tracing` test subscriber; clean exit is silent.
- [ ] Add doc comment pointing to `quil-p2p/src/node.rs:736` as the inspiration.

## Phase 2 — Apply to the critical sites (1 PR per crate, 3–4 total)

Convert Category 1 sites to `spawn_supervised_detached`. Order by blast radius:

- [ ] `crates/quil-engine/src/consensus_glue.rs:447` — name `"consensus_proposal_publish"`.
- [ ] `crates/quil-engine/src/app_engine.rs:1238` — name `"shard_consensus_event_loop"`. **Additionally:** when this task exits (cleanly or with a panic), the parent should learn — consider a oneshot/notify so the shard can be torn down or restarted rather than silently dead.
- [ ] `crates/quil-engine/src/prover_pipeline.rs:80` — wrap the `ProposeJoin` body in a guard that calls `set_proof_in_progress(false)` on `Drop`, not just at the end of the happy path. This is necessary independently of the supervised-spawn change.
- [ ] `crates/quil-engine/src/prover_pipeline.rs:88–135` — convert all seven lifecycle submitters.
- [ ] `crates/quil-node/src/main.rs:1625, 1652, 1673, 1693, 1730, 1749` — convert frame/bundle publish loops.
- [ ] `crates/quil-node/src/main.rs:5025–5057` — convert all P2P command shims; also add internal error logging where it's currently absent (subscribe/unsubscribe ignore the call's return entirely).

For each conversion: keep existing internal `warn!` logging, add the wrapper, and pick a stable `task =` name so panics are greppable in logs.

## Phase 3 — Tighten the remaining sites (1 PR)

- [ ] `crates/quil-rpc/src/frame_sync.rs:228, 516, 541` — document the handle ownership story; if the parent doesn't await, switch to `spawn_supervised_detached`.
- [ ] `crates/quil-engine/src/event_distributor.rs:239` — the outer awaiter task that uses `.unwrap()` is itself fire-and-forget. Either move it to a `JoinSet` whose drop logs, or convert the outer spawn to supervised.
- [ ] `crates/quil-rpc/src/proxy_pubsub.rs:139` — supervise the proxy read loop.
- [ ] Audit any new `tokio::spawn` introduced after this audit (add a CI grep or a clippy custom lint that flags raw `tokio::spawn` outside the helper module).

## Phase 4 — Optional structural follow-up

Consider whether long-lived event loops (shard consensus, swarm, prover pipeline) should be supervised by a `JoinSet` owned by the parent component, so a panicking task can trigger a coordinated shutdown or restart instead of leaving the parent in a degraded state. This is a larger change and should be scoped after Phase 2 ships and we see the supervised-spawn logs in practice.

---

## Out of scope

- Replacing `tokio::spawn` with structured-concurrency primitives wholesale.
- Converting `spawn_blocking` sites (they already propagate via `.await?` at every call site we saw).
- Test code (`crates/quil-lifecycle/src/supervisor.rs:435, 486, 562`) — uses `tokio::time::timeout` and is fine.
