//! Polls nodes until enough of them reach the target frame, then fetches the
//! committed frame chain for the safety check.
//!
//! Mirrors the Go `FrameMonitor`. Dials each node's `:8340` gRPC server
//! directly (the proxy shares every node's network) via
//! `quil_rpc::ArchiveClient`, reusing the production PQNoise connector. The
//! dial identity is a Falcon `q-prover-key` signing key borrowed from an
//! archive, since that port authenticates peers by Falcon identity (any
//! authenticated Falcon peer clears the interceptor, so the borrowed key works
//! against clients too).
//!
//! Two poll kinds share the machinery: [`PollKind::Global`] queries archives'
//! `GlobalService` for the global chain, and [`PollKind::AppShard`] queries
//! clients' `AppShardService` for the tracked shard's chain — the app-side
//! convergence check that makes app-shard verification structurally identical
//! to global verification.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use quil_rpc::ArchiveClient;

use crate::frame::{AppShardFrameWrapper, GlobalFrameWrapper};
use crate::safety::FrameFields;

/// A node to poll: a `host:port` gRPC (`:8340`) address.
#[derive(Debug, Clone)]
pub struct FrameTarget {
    pub address: String,
}

/// Which chain the monitor polls.
#[derive(Debug, Clone)]
pub enum PollKind {
    /// Archives' `GlobalService::get_global_frame`.
    Global,
    /// Clients' `AppShardService::get_app_shard_frame` for one shard filter,
    /// asked for the target frame by number (not frame 0 = "your latest"), so
    /// "reached" means the node actually serves the frame the safety check will
    /// then re-fetch.
    AppShard { filter: Vec<u8> },
}

/// Every dial and RPC the monitor issues is bounded by this, so no single
/// black-holed target can stall a poll pass (or the timeout-path snapshot).
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Consecutive failed polls after which a node that held the target frame is
/// treated as really gone rather than transiently erroring. The leniency
/// applies only to the failed count in `should_stop` — `poll_all` re-polls
/// converged nodes every pass, so one blip near the end of the catch-up budget
/// must not retire such a node and abort the run — and never to [`reached`],
/// where crediting a cached head through an outage would let a run pass on
/// stale evidence.
///
/// [`reached`]: FrameMonitor::reached
const TRANSIENT_ERROR_TOLERANCE: u32 = 2;

struct NodeStatus {
    /// Lazily (re)connected client; `None` until first connect / after error.
    client: Option<ArchiveClient>,
    last_head_frame: u64,
    first_polled_at: Option<Instant>,
    consecutive_errors: u32,
}

impl NodeStatus {
    fn new() -> Self {
        Self {
            client: None,
            last_head_frame: 0,
            first_polled_at: None,
            consecutive_errors: 0,
        }
    }

    /// A successful poll: the head is recomputed from what the node serves
    /// *now*, so a node that stops serving the target frame (`None`) falls
    /// back out of "reached" instead of staying converged on stale evidence.
    ///
    /// `None` here means the SERVER answered an authoritative "I don't have
    /// that frame" — `AppShardRpcServer` maps a store read error to
    /// `Status::internal` (the `on_error` path, covered by
    /// `TRANSIENT_ERROR_TOLERANCE`), never to `frame: None`. That server-side
    /// contract is what keeps a one-off RocksDB blip on a converged node from
    /// zeroing the head here and aborting the run as a convergence failure.
    fn on_success(&mut self, head: Option<u64>) {
        self.last_head_frame = head.unwrap_or(0);
        self.consecutive_errors = 0;
    }

    /// A failed connect or poll: extend the error streak and force a
    /// reconnect on the next pass.
    fn on_error(&mut self) {
        self.consecutive_errors += 1;
        self.client = None;
    }
}

/// Polls nodes for frame convergence.
pub struct FrameMonitor {
    /// Falcon signing key the proxy dials nodes with (borrowed from an archive).
    dial_key: Vec<u8>,
    stop_frame: u64,
    kind: PollKind,
    targets: Vec<FrameTarget>,
    poll_interval: Duration,
    timeout: Duration,
    min_nodes: usize,
    statuses: HashMap<String, NodeStatus>,
}

impl FrameMonitor {
    pub fn new(
        dial_key: Vec<u8>,
        stop_frame: u64,
        targets: Vec<FrameTarget>,
        poll_interval: Duration,
        min_nodes: usize,
        timeout: Duration,
    ) -> Self {
        Self::with_kind(
            dial_key,
            stop_frame,
            PollKind::Global,
            targets,
            poll_interval,
            min_nodes,
            timeout,
        )
    }

    /// An app-shard monitor: polls each client's `AppShardService` for
    /// `filter`'s chain instead of the archives' global chain.
    pub fn new_app_shard(
        dial_key: Vec<u8>,
        app_stop_frame: u64,
        filter: Vec<u8>,
        targets: Vec<FrameTarget>,
        poll_interval: Duration,
        min_nodes: usize,
        timeout: Duration,
    ) -> Self {
        Self::with_kind(
            dial_key,
            app_stop_frame,
            PollKind::AppShard { filter },
            targets,
            poll_interval,
            min_nodes,
            timeout,
        )
    }

    fn with_kind(
        dial_key: Vec<u8>,
        stop_frame: u64,
        kind: PollKind,
        targets: Vec<FrameTarget>,
        poll_interval: Duration,
        min_nodes: usize,
        timeout: Duration,
    ) -> Self {
        let statuses = targets
            .iter()
            .map(|t| (t.address.clone(), NodeStatus::new()))
            .collect();
        Self {
            dial_key,
            stop_frame,
            kind,
            targets,
            poll_interval,
            timeout,
            min_nodes,
            statuses,
        }
    }

    /// Connect with a bounded dial so a black-holed target cannot stall the
    /// caller (the timeout snapshot runs on the global-timeout path, where an
    /// unbounded dial would delay the failure notification indefinitely).
    async fn connect(address: &str, dial_key: &[u8]) -> Option<ArchiveClient> {
        crate::netutil::bounded_connect(
            "frame monitor",
            address,
            RPC_TIMEOUT,
            ArchiveClient::connect_mtls(address, dial_key),
        )
        .await
    }

    /// Ask one node for one frame — the single question both the convergence
    /// poll and the committed-frame fetch ask, shared so the two paths cannot
    /// drift apart (they once asked different questions — the poll asked for
    /// frame 0 "your latest" while the fetch asked by number — which let a
    /// node pass convergence on a frame the safety check then couldn't get).
    ///
    /// `Ok(Some((number, frame)))` is the served frame and its header number,
    /// `Ok(None)` is a healthy "I do not have that frame" (only the app
    /// service can say so; the global one answers NOT_FOUND, an `Err`), and
    /// `Err` carries the RPC error string, or `None` for a timeout.
    async fn fetch_frame(
        kind: &PollKind,
        client: &mut ArchiveClient,
        frame_num: u64,
    ) -> Result<Option<(u64, Box<dyn FrameFields>)>, Option<String>> {
        match kind {
            PollKind::Global => {
                match tokio::time::timeout(RPC_TIMEOUT, client.get_global_frame(frame_num)).await {
                    Ok(Ok(frame)) => {
                        let number = frame.header.as_ref().map(|h| h.frame_number).unwrap_or(0);
                        Ok(Some((number, Box::new(GlobalFrameWrapper::new(frame)))))
                    }
                    Ok(Err(e)) => Err(Some(e.to_string())),
                    Err(_) => Err(None),
                }
            }
            PollKind::AppShard { filter } => {
                match tokio::time::timeout(
                    RPC_TIMEOUT,
                    client.get_app_shard_frame(filter.clone(), frame_num),
                )
                .await
                {
                    Ok(Ok(Some(frame))) => {
                        let number = frame.header.as_ref().map(|h| h.frame_number).unwrap_or(0);
                        Ok(Some((number, Box::new(AppShardFrameWrapper::new(frame)))))
                    }
                    Ok(Ok(None)) => Ok(None),
                    Ok(Err(e)) => Err(Some(e.to_string())),
                    Err(_) => Err(None),
                }
            }
        }
    }

    /// Poll one node: connect if needed, query the stop frame, update status.
    async fn poll_node(&mut self, address: &str) {
        let dial_key = self.dial_key.clone();
        let stop_frame = self.stop_frame;
        let kind = self.kind.clone();
        let status = self.statuses.get_mut(address).expect("status exists");
        if status.first_polled_at.is_none() {
            status.first_polled_at = Some(Instant::now());
        }

        // (Re)connect if we don't have a live client.
        if status.client.is_none() {
            match Self::connect(address, &dial_key).await {
                Some(c) => status.client = Some(c),
                None => {
                    status.on_error();
                    return;
                }
            }
        }

        let client = status.client.as_mut().unwrap();
        // The target frame is asked for by number on both chains: serving it
        // is what "reached" has to mean, since the safety check re-fetches
        // that very frame. `Ok(None)` — the node is healthy but lacks the
        // frame — clears the head, and the catch-up budget in `should_stop`
        // retires the node if it never catches up.
        match Self::fetch_frame(&kind, client, stop_frame).await {
            Ok(served) => status.on_success(served.map(|(number, _)| number)),
            Err(Some(e)) => {
                status.on_error();
                tracing::debug!(address, stop_frame, error = %e, "frame monitor: poll failed");
            }
            Err(None) => {
                status.on_error();
                tracing::debug!(address, stop_frame, "frame monitor: poll timed out");
            }
        }
    }

    async fn poll_all(&mut self) {
        let addresses: Vec<String> = self.targets.iter().map(|t| t.address.clone()).collect();
        for address in addresses {
            self.poll_node(&address).await;
        }
    }

    /// Whether this node's last poll showed it currently holding the target
    /// frame. Strict on errors: the convergence verdict and the fetch set are
    /// built from this, so it must reflect the node's present state — a head
    /// cached from before an outage is not evidence the node still serves the
    /// frame the safety check will re-fetch.
    fn reached(&self, status: &NodeStatus) -> bool {
        status.last_head_frame >= self.stop_frame && status.consecutive_errors == 0
    }

    fn count_reached(&self) -> usize {
        self.statuses.values().filter(|s| self.reached(s)).count()
    }

    /// Returns true when monitoring should stop: either enough nodes reached the
    /// stop frame, or too many have failed to ever reach the minimum.
    fn should_stop(&self) -> bool {
        let ready = self.count_reached();
        if ready >= self.min_nodes {
            return true;
        }

        // A node has failed once it has burned its whole catch-up budget
        // without reaching the target. The shortfall itself is what counts, not
        // how it was expressed: an RPC error, a timeout, and a healthy "I do not
        // have that frame" all leave the node short. Gating on poll errors
        // instead is what let a responsive-but-stuck app client keep the run
        // alive forever — it answered every poll, so it never looked failed, and
        // with `min_nodes == client_count` the ready branch above could never
        // fire either.
        let now = Instant::now();
        let mut failed = 0;
        for status in self.statuses.values() {
            if self.reached(status) {
                continue;
            }
            // A converged node inside a short error streak is indeterminate,
            // not failed: the streak is likely a transient blip, and the next
            // successful poll restores it to reached. Only a persistent streak
            // (see TRANSIENT_ERROR_TOLERANCE) — or a node that never held the
            // frame at all — can count toward the abort.
            if status.last_head_frame >= self.stop_frame
                && status.consecutive_errors < TRANSIENT_ERROR_TOLERANCE
            {
                continue;
            }
            let elapsed = status
                .first_polled_at
                .map(|t| now.duration_since(t))
                .unwrap_or_default();
            if elapsed > self.timeout {
                failed += 1;
            }
        }

        tracing::info!(
            ready,
            failed,
            min_nodes = self.min_nodes,
            "frame convergence check"
        );

        // Impossible to reach the minimum even if all non-failed nodes succeed.
        if self.targets.len().saturating_sub(failed) < self.min_nodes {
            tracing::error!(
                min_nodes = self.min_nodes,
                failed,
                total = self.targets.len(),
                "insufficient nodes available to reach minimum, aborting"
            );
            return true;
        }
        false
    }

    /// Poll until enough nodes reach the stop frame (or it becomes impossible).
    /// Returns `(nodes_reached, total_nodes)`.
    pub async fn start_monitoring(
        &mut self,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> (usize, usize) {
        let total = self.targets.len();
        tracing::info!(
            node_count = total,
            stop_frame = self.stop_frame,
            "starting frame monitoring"
        );

        // Hard ceiling on the whole call. `should_stop` already bounds the wait
        // through each node's catch-up budget, but that is a property of the
        // per-node bookkeeping; this makes "monitoring cannot outlive the
        // catch-up budget" true locally and by construction. The extra poll
        // interval is slack so a node that would have been retired exactly at
        // the budget still gets its final poll. Every poll pass races the
        // deadline too: targets are polled sequentially at up to ~10 s per
        // unresponsive one (bounded dial + bounded RPC), so a pass straddling
        // the deadline would otherwise overshoot it by that much per target.
        // A cut-off pass is safe: each node polled so far has already absorbed
        // its answer, the rest keep their previous state, and the expiry path
        // below reports whatever the bookkeeping honestly holds.
        let deadline = tokio::time::Instant::now() + self.timeout + self.poll_interval;

        if tokio::time::timeout_at(deadline, self.poll_all())
            .await
            .is_ok()
            && self.should_stop()
        {
            return (self.count_reached(), total);
        }

        let mut ticker = tokio::time::interval(self.poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick
        loop {
            // Checked here as well as in the select arm below so expiry is
            // deterministic: with the deadline already past, `select!` would
            // otherwise be free to pick the ready ticker arm over the sleep.
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("frame monitoring cancelled");
                    return (self.count_reached(), total);
                }
                _ = tokio::time::sleep_until(deadline) => break,
                _ = ticker.tick() => {
                    if tokio::time::timeout_at(deadline, self.poll_all()).await.is_err() {
                        break;
                    }
                    if self.should_stop() {
                        return (self.count_reached(), total);
                    }
                }
            }
        }

        let reached = self.count_reached();
        tracing::warn!(
            reached,
            total,
            timeout = ?self.timeout,
            "frame monitoring deadline expired"
        );
        (reached, total)
    }

    /// One poll pass over every target, returning `(reached, total)` without
    /// waiting for convergence. Used on the global-timeout path so the failure
    /// notification carries real per-node counts instead of hardcoded zeros;
    /// every dial and RPC is individually bounded at 5 s, so the worst case
    /// across a handful of targets stays in the tens of seconds.
    pub async fn snapshot(&mut self) -> (usize, usize) {
        self.poll_all().await;
        (self.count_reached(), self.targets.len())
    }

    /// Addresses of nodes whose last poll reached the stop frame.
    fn ready_addresses(&self) -> Vec<String> {
        self.statuses
            .iter()
            .filter(|(_, s)| self.reached(s))
            .map(|(a, _)| a.clone())
            .collect()
    }

    /// Fetch frames `1..=stop_frame` from every node that reached the target,
    /// on whichever chain this monitor polls. Call after
    /// [`Self::start_monitoring`].
    ///
    /// Duplicates across nodes are kept (the safety check tolerates the same
    /// frame reported by more than one node). Every fetch miss from a READY
    /// node — connect failure, "not found", error, timeout — is recorded in
    /// the returned miss list: a node that just proved it converged must be
    /// able to serve its full committed range, and `check_safety` alone
    /// cannot catch a truncated fetch (it sees no frame numbers, so a chain
    /// missing frame 1 or the target frame still looks linear). Callers
    /// treat a non-empty miss list as a harness failure instead of running
    /// the safety check on partial evidence.
    ///
    /// A pass with misses is RETRIED for a bounded window before the misses
    /// stand: a node partitioned during the earliest views converges on the
    /// stop frame via gossip while its poller is still RPC-backfilling the
    /// frames it missed, so the terminal fetch can race a catch-up that lands
    /// seconds later. The retries tell "still catching up" apart from
    /// "permanently cannot serve" — a real hole (no peer ever had the frame)
    /// stays missing through every retry and still fails the run.
    ///
    /// The frames are erased to `dyn FrameFields` so one loop serves both
    /// chains; `check_safety` accepts the boxes directly through the blanket
    /// impl in [`crate::frame`].
    pub async fn fetch_committed(&mut self) -> (Vec<Box<dyn FrameFields>>, Vec<String>) {
        const FETCH_ATTEMPTS: u32 = 6;
        const FETCH_RETRY_BACKOFF: Duration = Duration::from_secs(5);
        let mut last = (Vec::new(), Vec::new());
        for attempt in 1..=FETCH_ATTEMPTS {
            last = self.fetch_committed_once().await;
            if last.1.is_empty() || attempt == FETCH_ATTEMPTS {
                break;
            }
            tracing::info!(
                attempt,
                misses = last.1.len(),
                "committed-range fetch incomplete — retrying (converged nodes may still be backfilling)"
            );
            tokio::time::sleep(FETCH_RETRY_BACKOFF).await;
        }
        last
    }

    async fn fetch_committed_once(&mut self) -> (Vec<Box<dyn FrameFields>>, Vec<String>) {
        let kind = self.kind.clone();
        let chain = match kind {
            PollKind::Global => "global",
            PollKind::AppShard { .. } => "app-shard",
        };
        let ready_addrs = self.ready_addresses();
        let mut frames: Vec<Box<dyn FrameFields>> = Vec::new();
        let mut misses: Vec<String> = Vec::new();
        for address in &ready_addrs {
            let Some(mut client) = Self::connect(address, &self.dial_key).await else {
                tracing::warn!(address, chain, "fetch frames: connect failed");
                misses.push(format!("{address}: connect failed"));
                continue;
            };
            for frame_num in 1..=self.stop_frame {
                match Self::fetch_frame(&kind, &mut client, frame_num).await {
                    Ok(Some((_, frame))) => frames.push(frame),
                    Ok(None) => {
                        tracing::warn!(address, chain, frame_num, "fetch frame: not found");
                        misses.push(format!("{address}: frame {frame_num} not found"));
                    }
                    Err(Some(e)) => {
                        tracing::warn!(address, chain, frame_num, error = %e, "fetch frame failed");
                        misses.push(format!("{address}: frame {frame_num} fetch failed: {e}"));
                    }
                    Err(None) => {
                        tracing::warn!(address, chain, frame_num, "fetch frame timed out");
                        misses.push(format!("{address}: frame {frame_num} fetch timed out"));
                    }
                }
            }
        }

        tracing::info!(
            chain,
            node_count = ready_addrs.len(),
            frame_count = frames.len(),
            miss_count = misses.len(),
            "fetched committed frames"
        );
        (frames, misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30);

    fn targets(names: &[&str]) -> Vec<FrameTarget> {
        names
            .iter()
            .map(|a| FrameTarget {
                address: (*a).to_string(),
            })
            .collect()
    }

    fn app_monitor(addresses: &[&str], min_nodes: usize) -> FrameMonitor {
        FrameMonitor::new_app_shard(
            vec![0u8; 4],
            3,
            vec![0xAA; 32],
            targets(addresses),
            Duration::from_secs(5),
            min_nodes,
            TIMEOUT,
        )
    }

    /// Mark a node as having reached the target, first polled `age` ago.
    fn set_reached(m: &mut FrameMonitor, address: &str, age: Duration) {
        let target = m.stop_frame;
        let s = m.statuses.get_mut(address).unwrap();
        s.last_head_frame = target;
        s.consecutive_errors = 0;
        s.first_polled_at = Some(Instant::now() - age);
    }

    /// Mark a node as answering polls happily while still short of the target —
    /// the app service's `Ok(None)` case. First polled `age` ago.
    fn set_healthy_but_short(m: &mut FrameMonitor, address: &str, age: Duration) {
        let s = m.statuses.get_mut(address).unwrap();
        s.last_head_frame = 0;
        s.consecutive_errors = 0;
        s.first_polled_at = Some(Instant::now() - age);
    }

    #[test]
    fn count_reached_requires_head_at_target_and_no_error() {
        let mut m = app_monitor(&["a:8340", "b:8340", "c:8340"], 3);
        m.statuses.get_mut("a:8340").unwrap().last_head_frame = 3;
        m.statuses.get_mut("b:8340").unwrap().last_head_frame = 2;
        let c = m.statuses.get_mut("c:8340").unwrap();
        c.last_head_frame = 5;
        c.consecutive_errors = 1;
        assert_eq!(
            m.count_reached(),
            1,
            "a cached head through an outage is not current convergence"
        );
        assert_eq!(m.ready_addresses(), vec!["a:8340".to_string()]);
    }

    /// One transient poll failure leaves a converged node indeterminate: it
    /// must not count as currently reached (a stale head must not pass the
    /// run) and must not count as failed either (with `min_nodes == target
    /// count`, a blip past the budget would otherwise abort the run as a
    /// convergence failure). The next poll settles it either way.
    #[test]
    fn a_transient_error_leaves_a_converged_node_indeterminate_not_failed() {
        let mut m = app_monitor(&["a:8340", "b:8340", "c:8340"], 3);
        for a in ["a:8340", "b:8340", "c:8340"] {
            set_reached(&mut m, a, TIMEOUT * 2);
        }
        m.statuses.get_mut("c:8340").unwrap().on_error();
        assert_eq!(m.count_reached(), 2, "an erroring node is not reached now");
        assert!(
            !m.should_stop(),
            "but one blip past the budget is not failure"
        );

        // A successful poll restores it.
        m.statuses.get_mut("c:8340").unwrap().on_success(Some(3));
        assert_eq!(m.count_reached(), 3);
        assert!(m.should_stop());

        // A persistent streak is a real outage: past the budget the node
        // counts failed, making the minimum impossible.
        let c = m.statuses.get_mut("c:8340").unwrap();
        c.on_error();
        c.on_error();
        assert_eq!(m.count_reached(), 2);
        assert!(m.should_stop(), "a persistent outage aborts the run");
    }

    /// "Reached" must not be sticky: a node that served the target frame once
    /// and then stops (store reset, or a read error the service collapses
    /// into `None`) has to fall back out of the converged set, or the
    /// convergence verdict and the frames actually fetched for the safety
    /// check diverge.
    #[test]
    fn a_node_that_stops_serving_the_target_frame_regresses() {
        let mut m = app_monitor(&["a:8340"], 1);
        set_reached(&mut m, "a:8340", Duration::from_secs(1));
        assert_eq!(m.count_reached(), 1);
        m.statuses.get_mut("a:8340").unwrap().on_success(None);
        assert_eq!(m.count_reached(), 0);
    }

    /// The hang this change exists for: a client that answers every poll but
    /// never serves the target frame. It answers every poll without erroring,
    /// so the old error-gated rule never retired it, and with `min_nodes == client_count`
    /// the ready branch could never fire either — the run waited forever.
    #[test]
    fn responsive_but_stale_app_client_is_retired_by_its_catchup_budget() {
        let mut m = app_monitor(&["a:8340", "b:8340", "c:8340"], 3);
        set_reached(&mut m, "a:8340", TIMEOUT * 2);
        set_reached(&mut m, "b:8340", TIMEOUT * 2);
        set_healthy_but_short(&mut m, "c:8340", TIMEOUT * 2);

        assert!(m.should_stop(), "the stalled client must end monitoring");
        assert!(
            m.count_reached() < 3,
            "and it must end short of the minimum, so the run fails on convergence"
        );
    }

    #[test]
    fn a_node_inside_its_catchup_budget_does_not_trip_the_rule() {
        let mut m = app_monitor(&["a:8340", "b:8340", "c:8340"], 3);
        set_reached(&mut m, "a:8340", TIMEOUT * 2);
        set_reached(&mut m, "b:8340", TIMEOUT * 2);
        set_healthy_but_short(&mut m, "c:8340", Duration::from_secs(1));

        assert!(!m.should_stop(), "the client is still catching up");
    }

    /// Age alone must not retire a node: the rule keys on the shortfall, and a
    /// node holding the target frame has none however long it has been polled.
    #[test]
    fn a_node_that_reached_the_target_is_never_counted_as_failed() {
        let mut m = app_monitor(&["a:8340", "b:8340", "c:8340"], 3);
        set_reached(&mut m, "a:8340", TIMEOUT * 4);
        set_healthy_but_short(&mut m, "b:8340", Duration::from_secs(1));
        set_healthy_but_short(&mut m, "c:8340", Duration::from_secs(1));

        assert!(!m.should_stop());
    }

    /// Global parity: an erroring archive past its budget still retires the
    /// same way it did under the old error-gated rule.
    #[test]
    fn an_erroring_global_node_past_its_budget_still_trips() {
        let mut m = FrameMonitor::new(
            vec![0u8; 4],
            3,
            targets(&["a:8340", "b:8340", "c:8340", "d:8340"]),
            Duration::from_secs(5),
            4,
            TIMEOUT,
        );
        for a in ["a:8340", "b:8340", "c:8340"] {
            set_reached(&mut m, a, TIMEOUT * 2);
        }
        let d = m.statuses.get_mut("d:8340").unwrap();
        d.consecutive_errors = 1;
        d.first_polled_at = Some(Instant::now() - TIMEOUT * 2);

        assert!(m.should_stop());
        assert_eq!(m.count_reached(), 3);
    }
}
