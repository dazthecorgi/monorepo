//! Drives the clients' prover joins onto their pinned app-shard filters.
//!
//! Config pinning (`engine.dataWorkerFilters`) marks workers
//! `manually_managed`, which EXCLUDES them from the node's auto-join lifecycle
//! (`ProverLifecycle::evaluate` only proposes joins for `free_auto()` workers)
//! — pinning binds a worker to a filter but nothing on the node ever *joins*
//! that filter. The proxy fills the operator's role, exactly like
//! `qclient node prover join`: `NodeService.RequestJoin{filters, worker_ids}`
//! pre-pins each filter to its worker and submits the join through the prover
//! pipeline. The node's manual-bucket lifecycle then self-confirms at window
//! maturity, and the worker allocator starts the shard consensus engine when
//! the allocation goes Active.
//!
//! Join confirmation is read from `GetNodeInfo().shard_allocations` — the
//! registry-derived allocation view — NOT from `GetWorkerInfo`, whose `filter`
//! field is already non-empty from config pinning alone. A join that never
//! lands (an archive dropped the bundle; the allocator swept the pending pin
//! after `PROPOSAL_TIMEOUT_FRAMES`) is re-issued after a grace period.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use quil_types::proto::node::node_service_client::NodeServiceClient;
use quil_types::proto::node::{GetNodeInfoRequest, RequestJoinRequest};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

/// Re-issue the join if this many global frames pass without the allocation
/// appearing. Comfortably beyond the node's `PROPOSAL_TIMEOUT_FRAMES` (10),
/// after which the allocator sweeps the pending pin and a re-issue is safe.
const REISSUE_AFTER_FRAMES: u64 = 15;

/// One client to drive.
pub struct JoinTarget {
    pub name: String,
    /// Plaintext NodeService address, `host:8337`.
    pub node_address: String,
    /// Decoded pinned filters (the app addresses to join), in worker order:
    /// filter `i` is pre-pinned to worker `i + 1`.
    pub filters: Vec<Vec<u8>>,
}

#[derive(Default)]
struct TargetState {
    client: Option<NodeServiceClient<Channel>>,
    confirmed: bool,
    /// Global head frame at the last RequestJoin issue (0 = never issued).
    issued_at_frame: u64,
    attempts: u32,
}

/// Issues and re-issues RequestJoin until every target's pinned filters have
/// registry allocations. Shared so the terminal path can read
/// [`JoinDriver::pending_summary`] for diagnostics.
pub struct JoinDriver {
    targets: Vec<JoinTarget>,
    state: Mutex<HashMap<String, TargetState>>,
}

impl JoinDriver {
    pub fn new(targets: Vec<JoinTarget>) -> Self {
        let state = targets
            .iter()
            .map(|t| (t.name.clone(), TargetState::default()))
            .collect();
        Self {
            targets,
            state: Mutex::new(state),
        }
    }

    /// Poll loop: steps every unconfirmed target each interval until all are
    /// confirmed or the run is cancelled. Targets step CONCURRENTLY — joins
    /// sit on the committee-formation critical path, and stepping serially
    /// let one hung client delay every other client's RequestJoin by its full
    /// dial/RPC bound each tick.
    pub async fn run(&self, cancel: &CancellationToken, poll_interval: Duration) {
        loop {
            let unconfirmed: Vec<&JoinTarget> = {
                let state = self.state.lock().unwrap();
                self.targets
                    .iter()
                    .filter(|t| !state[&t.name].confirmed)
                    .collect()
            };
            if unconfirmed.is_empty() {
                tracing::info!(
                    clients = self.targets.len(),
                    "join driver: all clients have allocations for their pinned filters"
                );
                return;
            }
            futures::future::join_all(unconfirmed.into_iter().map(|t| self.step_target(t))).await;
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }
    }

    /// Names the clients whose pinned filters still have no allocation, with
    /// attempt counts — folded into the run's diagnostics when the shard never
    /// advances.
    pub fn pending_summary(&self) -> String {
        let state = self.state.lock().unwrap();
        let pending: Vec<String> = self
            .targets
            .iter()
            .filter(|t| !state[&t.name].confirmed)
            .map(|t| format!("{} (join attempts: {})", t.name, state[&t.name].attempts))
            .collect();
        if pending.is_empty() {
            String::new()
        } else {
            format!(
                "clients without a registry allocation for their pinned filters: {}",
                pending.join(", ")
            )
        }
    }

    async fn step_target(&self, t: &JoinTarget) {
        // Connect lazily, with a bounded dial (a black-holed client must not
        // stall the tick); a failed connect just retries next tick.
        let client = {
            let cached = self.state.lock().unwrap()[&t.name].client.clone();
            match cached {
                Some(c) => c,
                None => {
                    match crate::netutil::connect_node_service("join driver", &t.node_address).await
                    {
                        Some(c) => {
                            self.state.lock().unwrap().get_mut(&t.name).unwrap().client =
                                Some(c.clone());
                            c
                        }
                        None => return,
                    }
                }
            }
        };

        let mut client = client;
        let info = match tokio::time::timeout(
            Duration::from_secs(5),
            client.get_node_info(GetNodeInfoRequest {}),
        )
        .await
        {
            Ok(Ok(resp)) => resp.into_inner(),
            _ => {
                // Drop the channel so the next tick reconnects.
                self.state.lock().unwrap().get_mut(&t.name).unwrap().client = None;
                return;
            }
        };

        // Confirmed once every pinned filter has a registry allocation (any
        // status — Joining self-confirms via the node's manual bucket).
        let all_allocated = t
            .filters
            .iter()
            .all(|f| info.shard_allocations.iter().any(|a| a.filter == *f));
        if all_allocated {
            let mut state = self.state.lock().unwrap();
            let st = state.get_mut(&t.name).unwrap();
            st.confirmed = true;
            tracing::info!(
                name = %t.name,
                filters = t.filters.len(),
                attempts = st.attempts,
                "join driver: allocations landed"
            );
            return;
        }

        // RequestJoin errors out before the node has observed any global frame.
        let head = info.last_global_head_frame;
        if head == 0 {
            return;
        }

        let should_issue = {
            let state = self.state.lock().unwrap();
            let st = &state[&t.name];
            st.issued_at_frame == 0 || head >= st.issued_at_frame + REISSUE_AFTER_FRAMES
        };
        if !should_issue {
            return;
        }

        let req = RequestJoinRequest {
            filters: t.filters.clone(),
            delegate: Vec::new(),
            worker_ids: (1..=t.filters.len() as u32).collect(),
        };
        match tokio::time::timeout(Duration::from_secs(5), client.request_join(req)).await {
            Ok(Ok(_)) => {
                let mut state = self.state.lock().unwrap();
                let st = state.get_mut(&t.name).unwrap();
                st.issued_at_frame = head;
                st.attempts += 1;
                if st.attempts > 1 {
                    tracing::warn!(
                        name = %t.name,
                        attempts = st.attempts,
                        head_frame = head,
                        "join driver: re-issued RequestJoin (previous join never landed)"
                    );
                } else {
                    tracing::info!(name = %t.name, head_frame = head, "join driver: issued RequestJoin");
                }
            }
            Ok(Err(e)) => {
                tracing::debug!(name = %t.name, error = %e, "join driver: RequestJoin rejected; will retry");
            }
            Err(_) => {
                self.state.lock().unwrap().get_mut(&t.name).unwrap().client = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn driver_with(names: &[&str], confirmed: &[bool]) -> JoinDriver {
        let targets = names
            .iter()
            .map(|n| JoinTarget {
                name: n.to_string(),
                node_address: format!("{n}:8337"),
                filters: vec![vec![0xAA; 32]],
            })
            .collect();
        let d = JoinDriver::new(targets);
        {
            let mut state = d.state.lock().unwrap();
            for (n, c) in names.iter().zip(confirmed) {
                state.get_mut(*n).unwrap().confirmed = *c;
                state.get_mut(*n).unwrap().attempts = 2;
            }
        }
        d
    }

    #[test]
    fn pending_summary_names_unconfirmed_targets() {
        let d = driver_with(&["client-1", "client-2"], &[true, false]);
        let s = d.pending_summary();
        assert!(s.contains("client-2"), "{s}");
        assert!(!s.contains("client-1 "), "{s}");
        assert!(s.contains("attempts: 2"), "{s}");
    }

    #[test]
    fn pending_summary_empty_when_all_confirmed() {
        let d = driver_with(&["client-1"], &[true]);
        assert!(d.pending_summary().is_empty());
    }
}
