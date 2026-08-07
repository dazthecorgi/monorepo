//! `devnet-proxy` — in-container gossip/gRPC partition proxy for the devnet
//! harness.
//!
//! Runs a BlossomSub host that meshes with every node and applies bipartite
//! partitions, snoops global consensus on `:8340` to drive per-view partition
//! timing, and (once a proposal past the stop frame is seen) polls the archives
//! for frame convergence, checks chain safety, verifies rejoin participation
//! and client enrollment, and POSTs the result back to the orchestrator.
//!
//! With `APP_STOP_FRAME` set, app-shard consensus is verified with the exact
//! same structure: a snoop (the gossip-fed `AppShardTracker`, since shard
//! consensus is gossip-only) latches the app stop condition and tallies shard
//! votes, a second `FrameMonitor` polls every committee client's
//! `AppShardService` for the target frame, the same chain-safety check runs
//! over the polled shard frames, and the vote tally answers the participation
//! question the global rejoin check answers for archives. The single global
//! timeout backstops both chains, and its notification carries real polled
//! per-node counts for each.
//!
//! The two participation checks both have to prove activity *after* the
//! partition healed. The global one gets that from the schedule statically
//! (`validate_partition_views` forces the heal before the stop frame); shard
//! views are a different numbering, so the app one keys on the runtime heal
//! latch the schedule publishes instead — see `view_schedule::HealSignal`.

mod app_shard_monitor;
mod blossomsub_proxy;
mod consensus_events;
mod enrollment_monitor;
mod frame;
mod frame_monitor;
mod grpc_proxy;
mod grpc_serve;
mod join_driver;
mod netutil;
mod participation;
mod partitioner;
mod safety;
mod simplex_view;
mod view_schedule;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use devnet::shared::{FrameNotification, NodeInfo, NotificationType};
use devnet::viewpartitions::{self, ViewPartitionEntry};

use crate::app_shard_monitor::AppShardTracker;
use crate::blossomsub_proxy::BlossomSubProxy;
use crate::consensus_events::ConsensusEvent;
use crate::enrollment_monitor::{ArchiveTarget, EnrollmentMonitor, EnrollmentTarget};
use crate::frame_monitor::{FrameMonitor, FrameTarget};
use crate::join_driver::{JoinDriver, JoinTarget};
use crate::participation::VoteLedger;
use crate::partitioner::NetworkPartitioner;
use crate::safety::{check_safety, FrameFields};
use crate::view_schedule::ViewSchedule;

const GRPC_BASE_PORT: u16 = 9000;

/// How long after both stop conditions are met to let the shard participation
/// check settle before proceeding to the terminal verification anyway.
///
/// Rejoining after a heal takes a few shard views, and the app stop latch can
/// trip in the very next global frame, so deciding the instant both latches
/// are met would fail clients that are mid-rejoin. The wait also covers the
/// unanswerable states (target view not yet observed, no post-heal vote yet),
/// which normally resolve within a few frames. It is bounded: an unbounded
/// wait on a client that never rejoins would burn the whole global timeout and
/// report nothing but the timeout — no safety, rejoin, or enrollment verdicts.
/// Past the deadline the run proceeds with whatever the check says then, so
/// the terminal notification always carries the full verdict set.
const PARTICIPATION_SETTLE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Parser)]
#[command(name = "devnet-proxy")]
struct Cli {
    /// Configuration directory.
    #[arg(long = "config", default_value = ".config")]
    config: String,
    /// Active network (mainnet = 0, primary testnet = 1).
    #[arg(long = "network", default_value_t = 0)]
    network: u8,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    init_logging();
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "devnet-proxy failed");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let run_id = env_required("RUN_ID")?;
    let runner_address = env_required("RUNNER_ADDRESS")?;
    let runner_auth = std::env::var("RUNNER_AUTH").unwrap_or_default();
    let stop_frame: u64 = env_required("STOP_FRAME")?
        .parse()
        .context("parse STOP_FRAME")?;
    let node_infos_json = env_required("NODE_INFOS")?;
    let min_nodes: usize = env_required("MIN_NODES")?
        .parse()
        .context("parse MIN_NODES")?;
    if min_nodes == 0 {
        bail!("MIN_NODES must be > 0");
    }

    let global_timeout = Duration::from_secs(env_required("GLOBAL_TIMEOUT")?.parse()?);
    let node_catchup_timeout = Duration::from_secs(env_required("NODE_CATCHUP_TIMEOUT")?.parse()?);
    let poll_interval = Duration::from_secs(5);

    // Optional (absent = 0 = disabled) so a pre-app-shard compose file keeps
    // working unchanged.
    let app_stop_frame: u64 = std::env::var("APP_STOP_FRAME")
        .ok()
        .filter(|v| !v.is_empty())
        .map(|v| v.parse().context("parse APP_STOP_FRAME"))
        .transpose()?
        .unwrap_or(0);

    let mut config =
        quil_config::load_config(std::path::Path::new(&cli.config)).context("load config")?;
    config.p2p.network = cli.network;

    let nodes: Vec<NodeInfo> =
        serde_json::from_str(&node_infos_json).context("parse NODE_INFOS")?;
    for n in &nodes {
        if n.peer_id.is_empty() {
            bail!("node info {} missing peer ID", n.name);
        }
    }
    let archive_count = nodes.iter().filter(|n| n.is_archive).count();

    // The app-shard filter set: the union of the clients' pinned filters
    // (their `engine.dataWorkerFilters`, carried through NODE_INFOS). When
    // app-shard verification is on, every filter must be the devnet token
    // app's genesis-deployed domain — anything else means the fixtures and the
    // genesis deploy have drifted apart, and the shard being tracked (or
    // joined) would not exist. Fail fast rather than time out blind.
    let pinned_filters = decode_pinned_filters(&nodes)?;
    if app_stop_frame > 0 {
        if pinned_filters.is_empty() {
            bail!("APP_STOP_FRAME is {app_stop_frame} but no client carries pinned filters");
        }
        let expected = quil_engine::genesis::devnet_token_domain().to_vec();
        for f in &pinned_filters {
            if *f != expected {
                bail!(
                    "pinned filter {} does not match the devnet token app domain {} — fixtures \
                     out of sync with genesis (QUIL_SEED_APP_TOKEN)",
                    hex::encode(f),
                    hex::encode(&expected)
                );
            }
        }
    }
    // Shared partition table consulted by both the gossip and gRPC paths.
    let partitioner = Arc::new(NetworkPartitioner::new());

    // The partition schedule. It owns partition timing and is applied inline by
    // the gRPC snoop rather than from the event loop, so a view takes effect
    // before the message that revealed it reaches the partition gate. It also
    // publishes the heal latch the shard participation check keys on, so it is
    // built before the tracker that consumes it.
    let schedule = Arc::new(ViewSchedule::new(
        parse_view_partitions_env()?,
        Arc::clone(&partitioner),
    ));
    schedule.apply_initial();

    // Committee-client roster for the shard participation tally (the app
    // analogue of the archive rejoin roster). Attribution is by libp2p peer-id
    // bytes, which is what a gossip message's signature-verified source
    // carries.
    let client_voters = required_client_voters(&nodes);
    let tracker = Arc::new(AppShardTracker::new(
        app_stop_frame,
        pinned_filters.clone(),
        client_voters,
        schedule.heal_signal(),
        app_shard_monitor::HEAL_VOTE_GRACE,
    ));

    // Identity the proxy dials archives with for frame polling / enrollment
    // checks. `:8340` is PQNoise-authenticated by a Falcon `q-prover-key`, and
    // the proxy is not a prover so it has no such key of its own — it borrows
    // the first archive's, which it already holds to relay that archive's
    // traffic. Impersonating a genesis archive also guarantees the dial clears
    // whatever caller policy the archives apply to each other.
    let monitor_dial_key = nodes
        .iter()
        .find(|n| n.is_archive)
        .context("no archive node to borrow a dial identity from")
        .and_then(falcon_key_of)?;

    // Start the BlossomSub host (swarm on the supervisor). The binding must
    // outlive the run: dropping it shuts the swarm down.
    let mut sup = quil_lifecycle::Supervisor::<anyhow::Error>::new();
    // Sized so the loop's tally cannot be truncated by a burst at a view
    // boundary: a dropped event now fails the run.
    let (consensus_tx, consensus_rx) = mpsc::channel::<ConsensusEvent>(8192);
    // App-shard observations the loop reacts to (head advances, forks). Only
    // head advances and a single fork ever ride it, so the capacity is ample;
    // a drop is still counted on the tracker and fails the run as a harness
    // error, mirroring the consensus channel above.
    let (app_tx, app_rx) = mpsc::channel::<app_shard_monitor::AppShardEvent>(1024);
    let _blossom = BlossomSubProxy::start(
        &mut sup,
        &config.p2p,
        Arc::clone(&partitioner),
        Arc::clone(&tracker),
        app_tx,
    )
    .await
    .context("start blossomsub proxy")?;

    // Join driver: the clients' pinned workers are `manually_managed`, so the
    // node's own lifecycle never joins them — the proxy issues the joins.
    // Runs whenever any client carries pinned filters, NOT only when
    // APP_STOP_FRAME is set: the enrollment check requires the clients'
    // prover registrations to land in the archives' registries, and with
    // every worker pinned there is no auto-join left to produce them.
    let targets = join_targets(&nodes)?;
    let join_driver = if targets.is_empty() {
        None
    } else {
        let driver = Arc::new(JoinDriver::new(targets));
        let jd = Arc::clone(&driver);
        sup.spawn("join-driver", move |token| async move {
            jd.run(&token, Duration::from_secs(5)).await;
            Ok(())
        });
        Some(driver)
    };

    // Build the gRPC backend specs (backend + per-caller Falcon identities) and
    // start one transparent-h2 partition proxy listener per archive. The event
    // loop keeps a handle on the snoop context to read its dropped-event count
    // when it forms the verdict.
    let mut snoop_ctx: Option<Arc<grpc_serve::SnoopContext>> = None;
    match build_grpc_backends(&nodes) {
        Ok(specs) => {
            let specs: Vec<Arc<grpc_proxy::BackendSpec>> =
                specs.into_iter().map(Arc::new).collect();
            tracing::info!(backends = specs.len(), "starting gRPC proxy");
            let part = Arc::clone(&partitioner);
            // The gRPC proxy also snoops SubmitGlobalConsensus requests for the
            // stop-frame/view signals that moved off gossip in v2.1.0.25. Events
            // are attributed to the calling node's prover address, so the snoop
            // needs the peer-ID → prover-address mapping up front.
            let snoop = Arc::new(grpc_serve::SnoopContext {
                prover_addresses: prover_addresses_by_peer(&nodes)?,
                cursor: Default::default(),
                schedule: Arc::clone(&schedule),
                dropped: AtomicU64::new(0),
            });
            snoop_ctx = Some(Arc::clone(&snoop));
            let grpc_consensus_tx = consensus_tx.clone();
            sup.spawn("grpc-proxy", move |token| async move {
                tokio::select! {
                    _ = token.cancelled() => Ok(()),
                    r = grpc_serve::serve_all(specs, part, grpc_consensus_tx, snoop) => r,
                }
            });
        }
        Err(e) => tracing::error!(error = %e, "failed to build gRPC backend specs"),
    }

    // Frame monitor: archives only (a stuck client must not mask a stuck archive).
    let frame_targets: Vec<FrameTarget> = nodes
        .iter()
        .filter(|n| n.is_archive)
        .map(|n| FrameTarget {
            address: n.stream_address(),
        })
        .collect();
    let mut frame_monitor = FrameMonitor::new(
        monitor_dial_key.clone(),
        stop_frame,
        frame_targets,
        poll_interval,
        min_nodes,
        node_catchup_timeout,
    );

    // App frame monitor: the app-side convergence check, polling every
    // committee client's `AppShardService` for the tracked shard's head — the
    // exact mirror of the global monitor above (same borrowed dial identity;
    // the `:8340` interceptor accepts any authenticated Falcon peer). ALL
    // committee clients must converge, mirroring the global default of
    // min_nodes == archive count.
    let mut app_frame_monitor = if app_stop_frame > 0 {
        let app_targets: Vec<FrameTarget> = nodes
            .iter()
            .filter(|n| !n.is_archive && !n.pinned_filters.is_empty())
            .map(|n| FrameTarget {
                address: n.stream_address(),
            })
            .collect();
        let client_count = app_targets.len();
        Some(FrameMonitor::new_app_shard(
            monitor_dial_key,
            app_stop_frame,
            pinned_filters[0].clone(),
            app_targets,
            poll_interval,
            client_count,
            node_catchup_timeout,
        ))
    } else {
        None
    };

    let cancel = CancellationToken::new();
    install_signal_handler(cancel.clone(), sup.token());

    tracing::info!(
        stop_frame,
        min_nodes,
        archive_count,
        app_stop_frame,
        "proxy running"
    );

    // POSTs notifications (progress + terminal) to the orchestrator.
    let notifier = Notifier {
        runner_address,
        runner_auth,
        run_id,
    };

    // Run the consensus event loop; it tracks frame progress and consensus
    // participation, and on reaching the stop frame runs the frame/enrollment
    // verification and emits the terminal notification. Partition timing lives
    // in `schedule`, applied by the snoop.
    let outcome = consensus_event_loop(
        consensus_rx,
        app_rx,
        &cancel,
        global_timeout,
        stop_frame,
        archive_count,
        &schedule,
        snoop_ctx.as_deref(),
        &mut frame_monitor,
        app_frame_monitor.as_mut(),
        &nodes,
        min_nodes,
        poll_interval,
        node_catchup_timeout,
        &notifier,
        app_stop_frame,
        &tracker,
        join_driver.as_deref(),
    )
    .await;

    // Emit the run-completion notification to the orchestrator.
    if let Some(notification) = outcome {
        notifier.send(notification).await;
    }

    cancel.cancel();
    Ok(())
}

/// The proxy's core loop. Returns the notification to send, or `None` if the
/// loop ended without a verdict (e.g. external cancellation).
#[allow(clippy::too_many_arguments)]
async fn consensus_event_loop(
    mut consensus_rx: mpsc::Receiver<ConsensusEvent>,
    mut app_rx: mpsc::Receiver<app_shard_monitor::AppShardEvent>,
    cancel: &CancellationToken,
    global_timeout: Duration,
    stop_frame: u64,
    archive_count: usize,
    schedule: &ViewSchedule,
    snoop: Option<&grpc_serve::SnoopContext>,
    frame_monitor: &mut FrameMonitor,
    mut app_frame_monitor: Option<&mut FrameMonitor>,
    nodes: &[NodeInfo],
    min_nodes: usize,
    poll_interval: Duration,
    node_catchup_timeout: Duration,
    notifier: &Notifier,
    app_stop_frame: u64,
    tracker: &AppShardTracker,
    join_driver: Option<&JoinDriver>,
) -> Option<FrameNotification> {
    // Archives that must each originate a consensus message for the last frame
    // to prove they rejoined consensus rather than passively syncing frames.
    let required_voters = required_archive_voters(nodes);
    // Active participation per view. A vote names a view, never a frame, so
    // attributing it to a frame means correlating through the view that
    // *produced* that frame — which only the block channel can establish.
    let mut active_votes = VoteLedger::default();
    // The view that produced `stop_frame`, learned from that frame's block.
    let mut stop_frame_view: Option<u64> = None;
    // Monotone rejoin latch, mirroring the app side's `satisfied_voters`: an
    // archive counts as rejoined the moment a vote of its lands at/after the
    // stop frame's view, and STAYS counted. The raw `VoteLedger` prunes to a
    // 64-view tail, and the terminal verification is delayed by app-shard
    // catch-up plus the participation settle gate — evaluating `missing_since`
    // only at that delayed terminal let an archive that voted near the stop
    // frame and then wedged be falsely reported as never rejoining once its
    // votes aged out of the window.
    let mut global_satisfied: HashSet<Vec<u8>> = HashSet::new();
    // Highest frame observed in a consensus message so far — used only to log
    // frame progress once per new frame (events repeat per view and per backend).
    let mut max_frame_seen: u64 = 0;
    // Set once a proposal past the global stop frame is observed. The terminal
    // verification is additionally gated on the app-shard condition, so the
    // run keeps going (and consensus keeps producing frames) until the tracked
    // shard reaches its target too. The app side keeps its own one-shot latch
    // on the tracker (`stop_seen`), the same semantics off the gossip snoop.
    let mut global_stop_seen = false;
    let mut app_wait_logged = false;
    let mut app_participation_wait_logged = false;
    // Started the first time both stop conditions hold; bounds how long the
    // shard participation check may hold the run back from the terminal
    // verification (see PARTICIPATION_SETTLE_TIMEOUT).
    let mut participation_settle_deadline: Option<tokio::time::Instant> = None;
    // Set on an observed app-shard fork: the run has already failed safety, so
    // fall through to the terminal verification immediately instead of waiting
    // out the app target (which a forked shard may never cleanly meet).
    let mut fast_fail_fork = false;
    // The app event channel closes when the swarm shuts down; stop selecting
    // on it then rather than spinning on `None`.
    let mut app_rx_open = true;
    let global_timer = tokio::time::sleep(global_timeout);
    tokio::pin!(global_timer);
    // Wakes the loop so the terminal recheck below doesn't depend on the
    // cadence of consensus events (gossip frames arrive outside this channel).
    let mut recheck = tokio::time::interval(Duration::from_secs(5));
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return None,
            _ = &mut global_timer => {
                let app_status = tracker.status();
                tracing::warn!(?global_timeout, stop_frame, "global timeout expired without meeting the stop conditions");
                // Compose the app diagnostics: sticky tracker error, per-filter
                // and per-client lag, and any clients whose joins never landed.
                let app_shard_error = if app_stop_frame == 0 {
                    String::new()
                } else {
                    compose_app_diagnostics(&app_status.error, tracker, join_driver)
                };
                // One bounded poll pass per monitor so the notification carries
                // the REAL per-node counts on both chains instead of hardcoded
                // zeros — the timed-out run's shortfall is then attributed to
                // the side that actually lagged.
                let (reached, total) = frame_monitor.snapshot().await;
                let (app_reached, app_total) = match app_frame_monitor.as_deref_mut() {
                    Some(m) => m.snapshot().await,
                    None => (0, 0),
                };
                return Some(FrameNotification {
                    run_id: String::new(),
                    stop_frame,
                    notification_type: NotificationType::GlobalTimeout,
                    safety_error: String::new(),
                    nodes_reached_stop_frame: reached as i32,
                    total_nodes: total as i32,
                    enrollment_error: String::new(),
                    rejoin_error: String::new(),
                    // A timed-out run is already a failure; the harness check
                    // still runs so the report says whether the scenario it was
                    // asked to run actually happened.
                    harness_error: compute_harness_error(
                        schedule,
                        snoop,
                        None,
                        false,
                        tracker.dropped(),
                        false,
                        false,
                    ),
                    app_shard_error,
                    app_shards_reached: app_status.reached,
                    app_shards_total: app_status.total,
                    app_stop_frame,
                    app_nodes_reached: app_reached as i32,
                    app_total_nodes: app_total as i32,
                    // The sticky equivocation is snoop-derived and already
                    // known, unlike the polled chain checks (meaningless
                    // mid-run) — a fork observed before the global side ever
                    // met its stop condition still gets reported as what it
                    // is.
                    app_safety_error: tracker.equivocation_error(),
                    // Also snoop-derived, so it is meaningful even mid-run: a
                    // run that times out waiting on the post-heal
                    // participation gate must say which clients it was
                    // waiting for.
                    app_participation_error: if app_stop_frame > 0 {
                        tracker.participation_error()
                    } else {
                        String::new()
                    },
                    app_frame_number: 0,
                });
            }
            _ = recheck.tick() => {}
            maybe_app = app_rx.recv(), if app_rx_open => {
                match maybe_app {
                    Some(app_shard_monitor::AppShardEvent::NewHead { filter, frame }) => {
                        tracing::debug!(
                            filter = %hex::encode(&filter),
                            frame,
                            app_stop_frame,
                            "app shard head advanced"
                        );
                        // Mirror of the global frame-advance progress below:
                        // report app liveness to the orchestrator, spawned so
                        // the loop never stalls on network I/O.
                        let notifier = notifier.clone();
                        let total = archive_count as i32;
                        let global_frame = max_frame_seen;
                        tokio::spawn(async move {
                            notifier
                                .progress(global_frame, total, frame, app_stop_frame)
                                .await
                        });
                    }
                    Some(app_shard_monitor::AppShardEvent::Fork) => {
                        tracing::error!(
                            "app shard fork observed — fast-failing to terminal verification"
                        );
                        fast_fail_fork = true;
                    }
                    None => app_rx_open = false,
                }
            }
            maybe_event = consensus_rx.recv() => {
                let event = maybe_event?;

                // Record active participation against the view it names. Only
                // views at or after the stop frame's can matter, but that view
                // isn't known until its block arrives, so keep a bounded tail.
                if event.source.is_active() && !event.sender_address.is_empty() {
                    active_votes.record(event.view, &event.sender_address);
                    // Latch rejoin the moment it is provable, immune to the
                    // ledger's pruning between now and the delayed terminal.
                    if stop_frame_view.is_some_and(|v| event.view >= v) {
                        global_satisfied.insert(event.sender_address.clone());
                    }
                }

                // Everything below needs a frame. A view-only observation (a
                // nullification, or any vote before the first block) carries
                // none, and is exactly the case the view schedule exists to
                // keep tracking.
                let Some(frame_number) = event.frame_number else {
                    continue;
                };

                // The block for `stop_frame` is what ties that frame to a view.
                // A vote must never establish this: it names its own view but
                // inherits its frame, and since the cursor is updated separately
                // from the channel send, a vote for a *later* view can reach the
                // loop before the block it inherited the frame from — pairing
                // `stop_frame` with a view that did not produce it and shifting
                // the rejoin window off the votes it is supposed to count.
                if frame_number == stop_frame
                    && event.source.states_own_frame()
                    && stop_frame_view.is_none()
                {
                    stop_frame_view = Some(event.view);
                    // The window's start is now known — fold the speculatively
                    // buffered ledger into the latch before any of it prunes.
                    let missing: HashSet<&str> = active_votes
                        .missing_since(event.view, &required_voters)
                        .into_iter()
                        .collect();
                    for (name, key) in &required_voters {
                        if !missing.contains(name.as_str()) {
                            global_satisfied.insert(key.clone());
                        }
                    }
                }

                if frame_number > max_frame_seen {
                    max_frame_seen = frame_number;
                    tracing::debug!(
                        frame = frame_number,
                        view = event.view,
                        stop_frame,
                        "global consensus frame advanced"
                    );
                    // Report liveness to the orchestrator so it can show progress
                    // during the run (it otherwise only hears the terminal frame).
                    // Spawned so the loop never stalls on network I/O while votes
                    // are arriving — a dropped event would invalidate the run.
                    let notifier = notifier.clone();
                    let total = archive_count as i32;
                    let app_frame = tracker.head();
                    tokio::spawn(async move {
                        notifier
                            .progress(frame_number, total, app_frame, app_stop_frame)
                            .await
                    });
                }

                if frame_number > stop_frame && !global_stop_seen {
                    global_stop_seen = true;
                    tracing::info!(
                        event_frame = frame_number,
                        stop_frame,
                        "observed proposal past stop frame"
                    );
                }
            }
        }

        // Terminal verification fires once BOTH stop conditions hold: the
        // global chain passed the stop frame, and (when enabled) the tracked
        // app shard reached its target — one-shot latches on both sides.
        // Global consensus keeps producing frames while the app shard catches
        // up; the global timeout above is the backstop if it never does. An
        // observed app fork skips the app wait (the shard has already failed
        // safety; there is nothing left to wait for), but never the global
        // one — the global checks below are only meaningful once the global
        // chain actually passed its stop frame.
        if !global_stop_seen {
            continue;
        }
        if app_stop_frame > 0 && !tracker.stop_seen() && !fast_fail_fork {
            if !app_wait_logged {
                app_wait_logged = true;
                let app_status = tracker.status();
                tracing::info!(
                    app_stop_frame,
                    reached = app_status.reached,
                    total = app_status.total,
                    "global stop frame reached; waiting for the app shard to reach its target"
                );
            }
            continue;
        }
        // Let the shard participation window settle before deciding on it —
        // rejoining after a heal takes a few shard views, so checking the
        // instant both latches are met would fail clients that are mid-rejoin,
        // and an unanswerable check (target view or first post-heal vote not
        // yet observed) usually answers itself a few frames later. The wait is
        // bounded: past the settle deadline the run proceeds to the terminal
        // verification with whatever the check says then, so a client that
        // never rejoins still yields the full verdict set instead of burning
        // the global timeout. The 5 s recheck tick re-enters this test.
        if app_stop_frame > 0 && !fast_fail_fork {
            let now = tokio::time::Instant::now();
            let deadline =
                *participation_settle_deadline.get_or_insert(now + PARTICIPATION_SETTLE_TIMEOUT);
            let unanswerable = tracker.target_view().is_none() || !tracker.has_post_heal_vote();
            let pending = tracker.participation_error();
            if unanswerable || !pending.is_empty() {
                if now < deadline {
                    if !app_participation_wait_logged {
                        app_participation_wait_logged = true;
                        tracing::info!(
                            pending = %pending,
                            unanswerable,
                            "stop conditions met; waiting for every committee client to vote \
                             after the heal"
                        );
                    }
                    continue;
                }
                tracing::warn!(
                    pending = %pending,
                    unanswerable,
                    "shard participation did not settle within the deadline; proceeding to \
                     terminal verification"
                );
            }
        }

        tracing::info!("stop conditions met, monitoring all nodes");
        let (reached, total) = frame_monitor.start_monitoring(cancel).await;
        tracing::info!(reached, total, "frame monitoring complete");

        let (frames, fetch_misses) = frame_monitor.fetch_committed().await;
        // With nobody at the stop frame there is nothing to adjudicate: the
        // shortfall is a convergence failure the reached/total counts already
        // carry. Running the safety check on the empty fetch would report
        // "empty frame sequence" — a safety violation, which outranks
        // convergence in the runner and buries the real story.
        //
        // A PARTIAL fetch from ready nodes is a harness failure, not a pass:
        // `check_safety` sees no frame numbers, so a sequence truncated at
        // either end (frame 1 or the target missing) still looks like one
        // linear chain. A node that just proved convergence must serve its
        // whole committed range or the run cannot claim the chain was checked.
        let safety_error = if reached == 0 {
            String::new()
        } else if !fetch_misses.is_empty() {
            format!(
                "safety fetch incomplete — ready nodes could not serve their committed range: {}",
                fetch_misses.join("; ")
            )
        } else {
            compute_safety_error(&frames)
        };

        // Rejoin is only answerable once the stop frame's view is known;
        // without it the run can't say either way, which is a harness failure
        // rather than a rejoin failure. The verdict reads the MONOTONE
        // `global_satisfied` latch, not the live ledger: the terminal runs
        // long after the stop frame (app catch-up + settle gate), and by then
        // the ledger's 64-view tail may have pruned the very votes that
        // proved an archive rejoined before it wedged.
        let rejoin_error = match stop_frame_view {
            Some(view) => {
                // Final fold: stragglers whose votes are still inside the
                // window count too (normally already latched; harmless).
                let in_window_missing: HashSet<&str> = active_votes
                    .missing_since(view, &required_voters)
                    .into_iter()
                    .collect();
                for (name, key) in &required_voters {
                    if !in_window_missing.contains(name.as_str()) {
                        global_satisfied.insert(key.clone());
                    }
                }
                let missing: Vec<&str> = required_voters
                    .iter()
                    .filter(|(_, key)| !global_satisfied.contains(key))
                    .map(|(name, _)| name.as_str())
                    .collect();
                let err = compute_rejoin_error(&missing, stop_frame);
                if err.is_empty() {
                    tracing::info!(
                        archives = required_voters.len(),
                        stop_frame,
                        stop_frame_view = view,
                        "all archives voted in the last frame's view (rejoined consensus)"
                    );
                } else {
                    tracing::error!(error = %err, "rejoin verification failed");
                }
                err
            }
            None => String::new(),
        };

        let enrollment_error = run_enrollment(
            nodes,
            min_nodes,
            poll_interval,
            node_catchup_timeout,
            cancel,
        )
        .await;

        // App-side verification, mirroring the global sequence above:
        // convergence poll over every committee client, then chain safety over
        // the polled frames (joined with any snooped equivocation), then the
        // participation check (the app analogue of rejoin).
        let mut app_nodes_reached = 0usize;
        let mut app_total_nodes = 0usize;
        let mut app_safety_error = String::new();
        let mut app_participation_error = String::new();
        if app_stop_frame > 0 {
            let equivocation = tracker.equivocation_error();
            if let Some(monitor) = app_frame_monitor.as_deref_mut() {
                let (ar, at) = if fast_fail_fork {
                    // One bounded pass: a forked shard may never converge, and
                    // the point of the fast-fail is not to wait around.
                    monitor.snapshot().await
                } else {
                    monitor.start_monitoring(cancel).await
                };
                app_nodes_reached = ar;
                app_total_nodes = at;
                tracing::info!(
                    reached = ar,
                    total = at,
                    "app shard frame monitoring complete"
                );
                if ar == 0 {
                    // Convergence shortfall, not a safety verdict (see the
                    // global fetch above); the snooped equivocation still
                    // stands on its own.
                    app_safety_error = equivocation;
                } else {
                    let (app_frames, app_misses) = monitor.fetch_committed().await;
                    app_safety_error = if !app_misses.is_empty() {
                        // Same rule as the global fetch: partial evidence from
                        // ready clients is a harness failure, not a pass.
                        let incomplete = format!(
                            "app safety fetch incomplete — ready clients could not serve their committed range: {}",
                            app_misses.join("; ")
                        );
                        if equivocation.is_empty() {
                            incomplete
                        } else {
                            format!("{equivocation}; {incomplete}")
                        }
                    } else {
                        compute_app_safety_error(&app_frames, &equivocation)
                    };
                }
            } else {
                app_safety_error = equivocation;
            }
            // Safe to re-derive this long after the settle gate: the verdict
            // is monotone per client (see `AppShardTracker`), so a client that
            // satisfied the gate cannot flip back to missing here — it can
            // only improve if stragglers voted during the monitoring above.
            app_participation_error = tracker.participation_error();
            if !app_participation_error.is_empty() {
                tracing::error!(error = %app_participation_error, "shard participation verification failed");
            }
        }
        // Whether the shard participation check was left unanswerable: the app
        // latch tripped without the target frame itself ever being observed,
        // so its view is unknown — the app mirror of the `stop_frame_view`
        // harness clause. Irrelevant on the fork path (the safety verdict is
        // the finding; participation is moot).
        let app_target_view_missing =
            app_stop_frame > 0 && !fast_fail_fork && tracker.target_view().is_none();
        // Its post-heal counterpart: no committee client voted after the
        // partition healed, so "voted in the post-heal window" has no window to
        // be true in.
        let app_post_heal_vote_missing =
            app_stop_frame > 0 && !fast_fail_fork && !tracker.has_post_heal_vote();

        let app_status = tracker.status();
        // Terminal app diagnostics: on a clean terminal both latches were met,
        // so lag/join detail is noise — only the fork fast-fail path attaches
        // it, to say where the shard stood when the fork was caught.
        let app_shard_error = if fast_fail_fork {
            // No sticky-error prefix here: the terminal path already surfaces
            // the tracker error via `app_safety_error`; repeating it would
            // double-report the same fork.
            compose_app_diagnostics("", tracker, join_driver)
        } else {
            String::new()
        };

        return Some(FrameNotification {
            run_id: String::new(),
            stop_frame,
            notification_type: NotificationType::TerminalFrame,
            safety_error,
            nodes_reached_stop_frame: reached as i32,
            total_nodes: total as i32,
            enrollment_error,
            rejoin_error,
            harness_error: compute_harness_error(
                schedule,
                snoop,
                stop_frame_view,
                true,
                tracker.dropped(),
                app_target_view_missing,
                app_post_heal_vote_missing,
            ),
            app_shard_error,
            app_shards_reached: app_status.reached,
            app_shards_total: app_status.total,
            app_stop_frame,
            app_nodes_reached: app_nodes_reached as i32,
            app_total_nodes: app_total_nodes as i32,
            app_safety_error,
            app_participation_error,
            app_frame_number: 0,
        });
    }
}

/// Describes any way the harness itself failed to run the scenario, rather than
/// the network under test failing.
///
/// A run that never applied its partitions, or that lost consensus events, is
/// not evidence of anything — reporting it as a pass is worse than reporting a
/// real failure, because it looks like the scenario was exercised.
fn compute_harness_error(
    schedule: &ViewSchedule,
    snoop: Option<&grpc_serve::SnoopContext>,
    stop_frame_view: Option<u64>,
    reached_terminal: bool,
    app_events_dropped: u64,
    app_target_view_missing: bool,
    app_post_heal_vote_missing: bool,
) -> String {
    let mut problems = Vec::new();

    let missed = schedule.missed_views();
    if !missed.is_empty() {
        problems.push(format!(
            "scheduled partition views were never observed and so never applied: {missed:?}"
        ));
    }

    let dropped = snoop.map_or(0, |s| s.dropped.load(AtomicOrdering::Relaxed));
    if dropped > 0 {
        problems.push(format!(
            "{dropped} consensus event(s) dropped; participation tallies are incomplete"
        ));
    }

    if app_events_dropped > 0 {
        problems.push(format!(
            "{app_events_dropped} app gossip event(s) dropped; app shard progress reporting is \
             incomplete"
        ));
    }

    // Three checks can end up unanswerable because the window they measure was
    // never established. All three are only meaningful once the run reached its
    // terminal frame: on the timeout path, not reaching the stop frame is
    // itself the reported failure.
    //
    // 1. the global rejoin window — the stop frame's view;
    // 2. the app participation window's start — the app stop latch tripped
    //    without the target frame itself being observed;
    // 3. the app participation window's post-heal bound — no shard vote was
    //    seen after the partition healed.
    let unanswerable = [
        (
            stop_frame_view.is_none(),
            "the stop frame's view was never established",
            "rejoin",
        ),
        (
            app_target_view_missing,
            "the app stop frame's view was never established",
            "shard participation",
        ),
        (
            app_post_heal_vote_missing,
            "no shard vote was observed after the partition healed",
            "shard participation",
        ),
    ];
    if reached_terminal {
        for (missing, cause, check) in unanswerable {
            if missing {
                problems.push(format!("{cause}, so {check} could not be verified"));
            }
        }
    }

    problems.join("; ")
}

/// The archive prover addresses that must each vote for the last frame, paired
/// with the node name for diagnostics. Skips (with a warning) any archive whose
/// prover address is missing or malformed so a setup gap can't masquerade as a
/// rejoin failure.
fn required_archive_voters(nodes: &[NodeInfo]) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for n in nodes.iter().filter(|n| n.is_archive) {
        if n.prover_address.is_empty() {
            tracing::warn!(name = %n.name, "archive missing prover address; excluded from rejoin check");
            continue;
        }
        match hex::decode(&n.prover_address) {
            Ok(b) if b.len() == 32 => out.push((n.name.clone(), b)),
            Ok(b) => {
                tracing::warn!(name = %n.name, len = b.len(), "archive prover address wrong length; excluded from rejoin check")
            }
            Err(e) => {
                tracing::warn!(name = %n.name, error = %e, "archive prover address decode failed; excluded from rejoin check")
            }
        }
    }
    out
}

/// The committee clients that must each originate a shard consensus vote for
/// the app stop frame's view, as `(name, libp2p peer-id bytes)` — the app
/// analogue of [`required_archive_voters`], keyed by peer id rather than
/// prover address because gossip attribution carries the signature-verified
/// source peer. Skips (with a warning) any client whose peer id fails to
/// parse, so a setup gap can't masquerade as a participation failure.
fn required_client_voters(nodes: &[NodeInfo]) -> Vec<(String, Vec<u8>)> {
    use std::str::FromStr;
    let mut out = Vec::new();
    for n in nodes
        .iter()
        .filter(|n| !n.is_archive && !n.pinned_filters.is_empty())
    {
        match quil_p2p::PeerId::from_str(&n.peer_id) {
            Ok(p) => out.push((n.name.clone(), p.to_bytes())),
            Err(e) => tracing::warn!(
                name = %n.name,
                error = %e,
                "client peer id malformed; excluded from shard participation check"
            ),
        }
    }
    out
}

/// Compose the app-shard diagnostics attached to a failing run: the sticky
/// tracker error (empty when the caller surfaces it elsewhere), per-filter and
/// per-client lag, and any clients whose joins never landed — one
/// implementation shared by the global-timeout and fork fast-fail arms so the
/// two answers to "where did the shard stand when the run failed" cannot
/// drift apart.
fn compose_app_diagnostics(
    sticky_error: &str,
    tracker: &AppShardTracker,
    join_driver: Option<&JoinDriver>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !sticky_error.is_empty() {
        parts.push(sticky_error.to_string());
    }
    let lag = tracker.lag_report();
    if !lag.is_empty() {
        parts.push(lag);
    }
    if let Some(pending) = join_driver.map(|d| d.pending_summary()) {
        if !pending.is_empty() {
            parts.push(pending);
        }
    }
    parts.join("; ")
}

/// Build the rejoin-error string: empty when every required archive originated a
/// consensus message for the last frame, otherwise names the archives that
/// didn't (they never rejoined consensus, only passively synced frames).
fn compute_rejoin_error(missing: &[&str], stop_frame: u64) -> String {
    if missing.is_empty() {
        String::new()
    } else {
        format!(
            "{} did not vote for the last frame (stop_frame={stop_frame}) — did not rejoin consensus",
            missing.join(", ")
        )
    }
}

/// Compute the safety-violation string for the fetched frames (empty = safe).
fn compute_safety_error(frames: &[Box<dyn FrameFields>]) -> String {
    match check_safety(frames) {
        Ok(()) => String::new(),
        Err(e) => {
            tracing::error!(error = %e, "safety violation detected");
            e.to_string()
        }
    }
}

/// The app-side safety verdict: the same chain-linearity check the global path
/// runs, over the polled app-shard frames, joined with any snooped
/// equivocation (either alone is a safety failure).
fn compute_app_safety_error(frames: &[Box<dyn FrameFields>], equivocation: &str) -> String {
    let chain = match check_safety(frames) {
        Ok(()) => String::new(),
        Err(e) => {
            tracing::error!(error = %e, "app shard safety violation detected");
            format!("app shard {e}")
        }
    };
    [chain, equivocation.to_string()]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Run the enrollment monitor; returns the error string (empty when confirmed
/// or when there are no clients).
async fn run_enrollment(
    nodes: &[NodeInfo],
    min_nodes: usize,
    poll_interval: Duration,
    timeout: Duration,
    cancel: &CancellationToken,
) -> String {
    let archives: Vec<ArchiveTarget> = nodes
        .iter()
        .filter(|n| n.is_archive)
        .map(|n| ArchiveTarget {
            name: n.name.clone(),
            address: format!("{}:{}", n.hostname, node_port(n)),
        })
        .collect();

    let mut clients = Vec::new();
    for n in nodes.iter().filter(|n| !n.is_archive) {
        if n.prover_address.is_empty() {
            tracing::warn!(name = %n.name, "client missing prover address, skipping");
            continue;
        }
        let prover_address = match hex::decode(&n.prover_address) {
            Ok(b) if b.len() == 32 => b,
            Ok(b) => return format!("client {} prover address wrong length: {}", n.name, b.len()),
            Err(e) => return format!("client {} prover address decode: {e}", n.name),
        };
        clients.push(EnrollmentTarget {
            name: n.name.clone(),
            prover_address,
            // Expected bound workers = the client's pinned filter count (one
            // worker per pinned app-shard filter). Legacy fixtures without
            // pinned filters keep the Go default of 2 (client pinned to 3
            // cores → available_parallelism-1 = 2 workers). Use the client's
            // own NodeService for the supplementary worker check.
            node_address: format!("{}:{}", n.hostname, node_port(n)),
            expected_cores: if n.pinned_filters.is_empty() {
                2
            } else {
                n.pinned_filters.len() as i32
            },
        });
    }

    let mut monitor = EnrollmentMonitor::new(archives, clients, poll_interval, min_nodes, timeout);
    match monitor.wait_for_enrollment(cancel).await {
        Ok(()) => String::new(),
        Err(e) => {
            tracing::error!(error = %e, "enrollment verification failed");
            e
        }
    }
}

fn node_port(n: &NodeInfo) -> i32 {
    if n.node_port == 0 {
        8337
    } else {
        n.node_port
    }
}

/// Build per-archive gRPC backend specs (server TLS impersonating the backend +
/// per-caller client TLS). Backends are archives only, but EVERY node (archives
/// and clients) is a potential caller — a client frame-syncs from archives
/// through the proxy, so the proxy must hold its caller identity too.
fn build_grpc_backends(nodes: &[NodeInfo]) -> Result<Vec<grpc_proxy::BackendSpec>> {
    use std::str::FromStr;
    let mut callers = Vec::new();
    let mut backends = Vec::new();
    for n in nodes {
        let wiring = grpc_proxy::NodeWiring {
            peer_id: quil_p2p::PeerId::from_str(&n.peer_id)
                .map_err(|e| anyhow::anyhow!("peer id for {}: {e}", n.name))?,
            falcon_signing_key: falcon_key_of(n)?,
            backend_addr: n.stream_address(),
            listen_port: 0,
        };
        callers.push(wiring.clone());
        if n.is_archive {
            let ordinal = n
                .ordinal()
                .map_err(|e| anyhow::anyhow!("ordinal for {}: {e}", n.name))?;
            backends.push(grpc_proxy::NodeWiring {
                listen_port: GRPC_BASE_PORT + ordinal as u16,
                ..wiring
            });
        }
    }
    grpc_proxy::build_backend_specs(&backends, &callers)
}

// ---- helpers ----------------------------------------------------------------

/// Decode and dedupe the union of every node's pinned filters (hex →
/// bytes). NODE_INFOS is produced by the orchestrator, which already
/// validated the hex, but the proxy re-validates since it is a separate
/// process trusting an env var.
fn decode_pinned_filters(nodes: &[NodeInfo]) -> Result<Vec<Vec<u8>>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for n in nodes {
        for f in &n.pinned_filters {
            let b =
                hex::decode(f).map_err(|e| anyhow::anyhow!("pinned filter for {}: {e}", n.name))?;
            if !out.contains(&b) {
                out.push(b);
            }
        }
    }
    Ok(out)
}

/// Build the join driver's targets: every non-archive node with pinned
/// filters, addressed via its plaintext NodeService.
fn join_targets(nodes: &[NodeInfo]) -> Result<Vec<JoinTarget>> {
    let mut out = Vec::new();
    for n in nodes.iter().filter(|n| !n.is_archive) {
        if n.pinned_filters.is_empty() {
            tracing::warn!(name = %n.name, "client has no pinned filters; join driver will skip it");
            continue;
        }
        let filters = n
            .pinned_filters
            .iter()
            .map(|f| {
                hex::decode(f).map_err(|e| anyhow::anyhow!("pinned filter for {}: {e}", n.name))
            })
            .collect::<Result<Vec<_>>>()?;
        out.push(JoinTarget {
            name: n.name.clone(),
            node_address: format!("{}:{}", n.hostname, node_port(n)),
            filters,
        });
    }
    Ok(out)
}

fn parse_view_partitions_env() -> Result<HashMap<u64, ViewPartitionEntry>> {
    let raw = std::env::var("VIEW_PARTITIONS").unwrap_or_default();
    if raw.is_empty() {
        return Ok(HashMap::new());
    }
    let parsed = viewpartitions::parse_view_partitions(&raw).context("parse VIEW_PARTITIONS")?;
    // Validate every peer ID decodes.
    use std::str::FromStr;
    for e in parsed.values() {
        for p in e.partition1.iter().chain(e.partition2.iter()) {
            quil_p2p::PeerId::from_str(p.trim()).map_err(|err| {
                anyhow::anyhow!("invalid peer ID {p:?} in VIEW_PARTITIONS: {err}")
            })?;
        }
    }
    tracing::info!(entries = parsed.len(), "loaded view partition schedule");
    Ok(parsed.into_iter().collect())
}

/// Map each node's peer ID to its prover address, so the gRPC snoop can name the
/// node behind a handshake-verified caller. Nodes with no prover address are
/// skipped — the event loop only ever looks up archives.
fn prover_addresses_by_peer(nodes: &[NodeInfo]) -> Result<HashMap<quil_p2p::PeerId, Vec<u8>>> {
    use std::str::FromStr;
    let mut out = HashMap::new();
    for n in nodes {
        if n.prover_address.is_empty() {
            continue;
        }
        let peer = quil_p2p::PeerId::from_str(&n.peer_id)
            .map_err(|e| anyhow::anyhow!("peer id for {}: {e}", n.name))?;
        let address = hex::decode(&n.prover_address)
            .map_err(|e| anyhow::anyhow!("prover address for {}: {e}", n.name))?;
        out.insert(peer, address);
    }
    Ok(out)
}

/// Decode a node's Falcon `q-prover-key` signing key from its [`NodeInfo`].
fn falcon_key_of(n: &NodeInfo) -> Result<Vec<u8>> {
    if n.falcon_signing_key.is_empty() {
        anyhow::bail!("node info {} missing falcon signing key", n.name);
    }
    hex::decode(&n.falcon_signing_key)
        .map_err(|e| anyhow::anyhow!("falcon signing key for {}: {e}", n.name))
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow::anyhow!("{key} environment variable is required"))
}

/// Sends notifications (progress + terminal) to the orchestrator, stamping each
/// with the run ID.
#[derive(Clone)]
struct Notifier {
    runner_address: String,
    runner_auth: String,
    run_id: String,
}

impl Notifier {
    /// POST `notification` (run_id stamped) to the runner, logging on failure.
    async fn send(&self, notification: FrameNotification) {
        let notification = FrameNotification {
            run_id: self.run_id.clone(),
            ..notification
        };
        if let Err(e) =
            post_notification(&self.runner_address, &self.runner_auth, &notification).await
        {
            tracing::error!(error = %e, "failed to notify runner");
        }
    }

    /// POST an intermediate frame-progress update — a best-effort liveness signal
    /// the orchestrator logs. The reached global frame rides
    /// `stop_frame`/`frame_number`; the app shard's current head rides
    /// `app_frame_number` (both 0 when app checking is disabled). One
    /// notification shape for both chains, so an orchestrator predating the
    /// app fields still decodes it.
    async fn progress(&self, frame: u64, total_nodes: i32, app_frame: u64, app_stop_frame: u64) {
        self.send(FrameNotification {
            run_id: String::new(),
            stop_frame: frame,
            notification_type: NotificationType::Progress,
            safety_error: String::new(),
            nodes_reached_stop_frame: 0,
            total_nodes,
            enrollment_error: String::new(),
            rejoin_error: String::new(),
            harness_error: String::new(),
            app_shard_error: String::new(),
            app_shards_reached: 0,
            app_shards_total: 0,
            app_stop_frame,
            app_nodes_reached: 0,
            app_total_nodes: 0,
            app_safety_error: String::new(),
            app_participation_error: String::new(),
            app_frame_number: app_frame,
        })
        .await;
    }
}

/// POST the notification JSON to the orchestrator over plain HTTP/1.1 (the
/// runner endpoint is plaintext). Avoids pulling a full HTTP client.
async fn post_notification(
    runner_address: &str,
    auth_token: &str,
    notification: &FrameNotification,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = serde_json::to_vec(notification).context("serialize notification")?;
    let host = runner_address;
    let request = format!(
        "POST /run-notification HTTP/1.1\r\n\
         Host: {host}\r\n\
         Authorization: Bearer {auth_token}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );

    let mut stream = tokio::net::TcpStream::connect(runner_address)
        .await
        .with_context(|| format!("connect to runner {runner_address}"))?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok();
    let head = String::from_utf8_lossy(&response);
    let status_ok = head
        .lines()
        .next()
        .map(|l| l.contains(" 200") || l.contains(" 2"))
        .unwrap_or(false);
    if status_ok {
        tracing::info!("notified runner");
        Ok(())
    } else {
        bail!(
            "runner returned non-success: {}",
            head.lines().next().unwrap_or("")
        );
    }
}

fn install_signal_handler(cancel: CancellationToken, sup_token: CancellationToken) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received interrupt signal");
        cancel.cancel();
        sup_token.cancel();
    });
}

fn init_logging() {
    // Cap the HTTP/2 + gRPC stack (h2 codec, hyper, tonic, tower) at warn even
    // under full debug (RUST_LOG=debug): their per-frame send/received logs
    // otherwise drown the proxy's own output. Errors/warnings are kept.
    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    for directive in ["h2=warn", "hyper=warn", "tonic=warn", "tower=warn"] {
        filter = filter.add_directive(directive.parse().expect("static directive is valid"));
    }
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(name: &str, prover_address: &str) -> NodeInfo {
        NodeInfo {
            name: name.into(),
            hostname: name.into(),
            stream_port: 8340,
            node_port: 8337,
            peer_id: "QmTest".into(),
            peer_priv_key: String::new(),
            falcon_signing_key: String::new(),
            is_archive: true,
            prover_address: prover_address.into(),
            pinned_filters: Vec::new(),
        }
    }

    fn addr(b: u8) -> Vec<u8> {
        vec![b; 32]
    }

    // ---- rejoin correlation ------------------------------------------------

    fn ledger(pairs: &[(u64, &[u8])]) -> VoteLedger {
        let mut l = VoteLedger::default();
        for (view, addr) in pairs {
            l.record(*view, addr);
        }
        l
    }

    /// The defect this guards: a vote inherits the newest block's frame number,
    /// so without the view window, activity in an *earlier* view could satisfy
    /// the rejoin gate for the stop frame.
    #[test]
    fn rejoin_fails_when_an_archive_only_participated_before_the_stop_view() {
        let required = required_archive_voters(&[
            archive("archive-1", &hex32(1)),
            archive("archive-2", &hex32(2)),
        ]);
        // archive-2 voted only in view 5; the stop frame was decided in view 6.
        let l = ledger(&[(6, &addr(1)), (5, &addr(2))]);
        let err = compute_rejoin_error(&l.missing_since(6, &required), 6);
        assert!(err.contains("archive-2"), "unexpected: {err}");
        assert!(!err.contains("archive-1"), "unexpected: {err}");
    }

    fn hex32(b: u8) -> String {
        hex::encode(addr(b))
    }

    #[test]
    fn required_voters_includes_only_well_formed_archives() {
        let client = NodeInfo {
            is_archive: false,
            ..archive("client-1", &hex32(0x09))
        };
        let nodes = vec![
            archive("archive-1", &hex32(0x01)),
            archive("archive-2", &hex32(0x02)),
            archive("archive-3", ""),     // missing → excluded
            archive("archive-4", "zz"),   // malformed hex → excluded
            archive("archive-5", "00ff"), // wrong length → excluded
            client,                       // not an archive → excluded
        ];
        let req = required_archive_voters(&nodes);
        let names: Vec<&str> = req.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["archive-1", "archive-2"]);
        assert_eq!(req[0].1, addr(0x01));
    }

    #[test]
    fn rejoin_error_empty_when_all_archives_voted() {
        let required = vec![
            ("archive-1".to_string(), addr(0x01)),
            ("archive-2".to_string(), addr(0x02)),
        ];
        // An archive outside the roster voting too is not a problem.
        let l = ledger(&[(5, &addr(0x01)), (5, &addr(0x02)), (5, &addr(0x03))]);
        assert_eq!(compute_rejoin_error(&l.missing_since(5, &required), 5), "");
    }

    #[test]
    fn rejoin_error_names_archives_that_did_not_vote() {
        let required = vec![
            ("archive-1".to_string(), addr(0x01)),
            ("archive-4".to_string(), addr(0x04)),
        ];
        // Only archive-1 voted for the last frame.
        let l = ledger(&[(5, &addr(0x01))]);
        let err = compute_rejoin_error(&l.missing_since(5, &required), 5);
        assert!(
            err.contains("archive-4"),
            "error should name archive-4: {err}"
        );
        assert!(!err.contains("archive-1"), "archive-1 voted: {err}");
        assert!(err.contains("stop_frame=5"));
    }

    // ---- shard participation roster ----------------------------------------

    /// A base58 peer id that parses (identity multihash of 4 bytes).
    const VALID_PEER: &str = "12D3KooWBhV8jEv8sMknhNhkkGKQdBy4XjXQZ1sGwzvBkbNhfLoB";

    fn client(name: &str, peer_id: &str, pinned: bool) -> NodeInfo {
        NodeInfo {
            is_archive: false,
            peer_id: peer_id.into(),
            pinned_filters: if pinned {
                vec![hex32(0xAA)]
            } else {
                Vec::new()
            },
            ..archive(name, &hex32(0x09))
        }
    }

    #[test]
    fn required_client_voters_includes_only_pinned_clients_with_valid_peer_ids() {
        let nodes = vec![
            archive("archive-1", &hex32(0x01)),     // archive → excluded
            client("client-1", VALID_PEER, true),   // included
            client("client-2", "not-a-peer", true), // malformed → excluded
            client("client-3", VALID_PEER, false),  // no pinned filter → excluded
        ];
        let req = required_client_voters(&nodes);
        let names: Vec<&str> = req.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["client-1"]);
        use std::str::FromStr;
        assert_eq!(
            req[0].1,
            quil_p2p::PeerId::from_str(VALID_PEER).unwrap().to_bytes()
        );
    }

    // ---- harness error, app clauses ----------------------------------------

    #[test]
    fn harness_error_reports_app_drops_and_missing_app_target_view() {
        let schedule = ViewSchedule::new(HashMap::new(), Arc::new(NetworkPartitioner::new()));
        assert_eq!(
            compute_harness_error(&schedule, None, Some(1), true, 0, false, false),
            ""
        );
        let err = compute_harness_error(&schedule, None, Some(1), true, 2, true, false);
        assert!(err.contains("2 app gossip event(s) dropped"), "{err}");
        assert!(
            err.contains("app stop frame's view was never established"),
            "{err}"
        );
        // The view clause is suppressed on the timeout path (reached_terminal
        // = false), exactly like the global stop_frame_view clause; the drop
        // clause is not.
        let err = compute_harness_error(&schedule, None, None, false, 1, true, false);
        assert!(err.contains("app gossip event(s) dropped"), "{err}");
        assert!(!err.contains("view was never established"), "{err}");
    }

    /// An unanswerable post-heal participation check must read as a harness
    /// failure, not as a pass — the same standard the two view clauses hold.
    #[test]
    fn harness_error_reports_a_missing_post_heal_window() {
        let schedule = ViewSchedule::new(HashMap::new(), Arc::new(NetworkPartitioner::new()));
        let err = compute_harness_error(&schedule, None, Some(1), true, 0, false, true);
        assert!(
            err.contains("no shard vote was observed after the partition healed"),
            "{err}"
        );
        // Suppressed on the timeout path, like its siblings.
        let err = compute_harness_error(&schedule, None, None, false, 0, false, true);
        assert_eq!(err, "");
    }
}
