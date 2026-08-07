//! Wire types shared between the orchestrator and the proxy.
//!
//! The JSON field names here are a contract with the proxy: `NodeInfo` is
//! serialized into the `NODE_INFOS` docker env var and decoded by the proxy,
//! and `FrameNotification` is the JSON body the proxy POSTs back to the
//! orchestrator's notification server. The names mirror the Go `shared` package.

use serde::{Deserialize, Serialize};

/// The kind of notification emitted by the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationType {
    #[serde(rename = "terminal_frame_reached")]
    TerminalFrame,
    #[serde(rename = "global_timeout")]
    GlobalTimeout,
    /// Intermediate liveness update: the global-consensus frame advanced. The
    /// reached frame is carried in `frame_number` (`FrameNotification::stop_frame`).
    /// The run continues; the orchestrator just logs it as progress.
    #[serde(rename = "frame_progress")]
    Progress,
}

/// Per-node address and identity information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NodeInfo {
    /// Service name, e.g. "archive-1" or "client-1".
    pub name: String,
    /// Hostname, e.g. "archive-1".
    pub hostname: String,
    /// TCP stream port, e.g. 8340.
    pub stream_port: i32,
    /// Plaintext NodeService gRPC port, e.g. 8337.
    pub node_port: i32,
    /// base58-encoded peer ID, empty if unknown. Derived from the node's Falcon
    /// `q-prover-key` — since the Falcon migration that key, not the Ed448 peer
    /// key, IS the libp2p network identity, so this is what the proxy matches
    /// handshake-verified peers and partition entries against.
    pub peer_id: String,
    /// hex-encoded Ed448 private key. Retained as the node's seniority-root
    /// identity; it is no longer the network identity.
    pub peer_priv_key: String,
    /// Hex-encoded Falcon `q-prover-key` SIGNING key (1281 B), read out of the
    /// node's `keys.yml`. This is the node's `:8340` PQNoise identity, so the
    /// proxy needs it to answer callers as a backend and to re-originate their
    /// calls to that backend as them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub falcon_signing_key: String,
    /// true for archive nodes; false for client (non-archive) nodes.
    pub is_archive: bool,
    /// Hex-encoded 32-byte Poseidon(BLS pubkey) derived from the node's
    /// q-prover-key. Used by the enrollment monitor to assert that a client's
    /// prover registration landed in the archives' registry. Pre-computed at
    /// startup so the proxy doesn't need a Poseidon dependency.
    pub prover_address: String,
    /// Hex-encoded app-shard consensus filters from the node's
    /// `engine.dataWorkerFilters` (empty for archives). The proxy derives the
    /// app-shard poll set, the per-client `RequestJoin` filter list, and the
    /// enrollment monitor's expected worker count from this — the client
    /// configs are the single source of truth. `#[serde(default)]` keeps old
    /// NODE_INFOS payloads decodable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pinned_filters: Vec<String>,
}

impl NodeInfo {
    /// Returns `hostname:stream_port`.
    pub fn stream_address(&self) -> String {
        format!("{}:{}", self.hostname, self.stream_port)
    }

    /// Returns the numeric suffix of the service name (e.g. "archive-3" → 3).
    pub fn ordinal(&self) -> Result<i64, std::num::ParseIntError> {
        let suffix = self.name.rsplit('-').next().unwrap_or(&self.name);
        suffix.parse()
    }
}

/// Run-completion notification posted by the proxy to the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FrameNotification {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// The configured stop frame for the run (JSON key `frame_number`).
    #[serde(rename = "frame_number")]
    pub stop_frame: u64,
    #[serde(rename = "type")]
    pub notification_type: NotificationType,
    /// Set when the proxy detected a consensus safety violation.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub safety_error: String,
    pub nodes_reached_stop_frame: i32,
    pub total_nodes: i32,
    /// Set when one or more client nodes failed to land their prover_address in
    /// at least `minimum_nodes` archives' prover registries. Empty when there
    /// are no client nodes or when all clients are confirmed enrolled.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub enrollment_error: String,
    /// Set when one or more archives failed to vote for the last frame
    /// (`stop_frame`) — i.e. they never rejoined consensus after a partition and
    /// merely passively synced frames. Empty when every archive originated a
    /// consensus message for the last frame.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rejoin_error: String,
    /// Set when the harness itself failed to run the scenario — a scheduled
    /// partition view was never observed and so never applied, a consensus event
    /// was dropped, or the stop frame's view was never established. Unlike the
    /// other errors, which report the network under test failing, this says the
    /// run is not evidence of anything and its other results cannot be trusted.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub harness_error: String,
    /// Set when app-shard verification failed: the tracked shard never reached
    /// `app_stop_frame`, an equivocation was observed, or the tracker could not
    /// classify traffic. Empty when app-shard checking is disabled or passed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub app_shard_error: String,
    /// Number of pinned app-shard filters whose observed frame number reached
    /// `app_stop_frame`. 0 when app-shard checking is disabled.
    #[serde(default)]
    pub app_shards_reached: i32,
    /// Total number of pinned app-shard filters under observation. 0 when
    /// app-shard checking is disabled.
    #[serde(default)]
    pub app_shards_total: i32,
    /// The configured app-shard stop frame (0 = app-shard checking disabled).
    #[serde(default)]
    pub app_stop_frame: u64,
    /// Number of client nodes whose polled app-shard head reached
    /// `app_stop_frame` — the app analogue of `nodes_reached_stop_frame`,
    /// measured out-of-band over each client's `AppShardService`.
    #[serde(default)]
    pub app_nodes_reached: i32,
    /// Total client nodes polled for app-shard convergence. 0 when app-shard
    /// checking is disabled (and on bodies from proxies predating the field).
    #[serde(default)]
    pub app_total_nodes: i32,
    /// Set when the app shard violated safety: the polled chain
    /// `1..=app_stop_frame` is not a single linear chain, or two frames with
    /// the same number carried different outputs on gossip (equivocation).
    /// The app analogue of `safety_error`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub app_safety_error: String,
    /// Set when one or more committee clients never originated a shard
    /// consensus vote at or after the app stop frame's view — i.e. they
    /// passively ingested frames without participating. The app analogue of
    /// `rejoin_error`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub app_participation_error: String,
    /// On `Progress` notifications, the app shard's current head frame
    /// (0 when app-shard checking is disabled or no frame was observed yet).
    #[serde(default)]
    pub app_frame_number: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_info_json_roundtrip_keys() {
        let n = NodeInfo {
            name: "archive-1".into(),
            hostname: "archive-1".into(),
            stream_port: 8340,
            node_port: 8337,
            peer_id: "QmAbc".into(),
            peer_priv_key: "deadbeef".into(),
            falcon_signing_key: "c0ffee".into(),
            is_archive: true,
            prover_address: String::new(),
            pinned_filters: Vec::new(),
        };
        let v: serde_json::Value = serde_json::to_value(&n).unwrap();
        // Exact wire keys consumed by the proxy.
        for key in [
            "name",
            "hostname",
            "stream_port",
            "node_port",
            "peer_id",
            "peer_priv_key",
            "falcon_signing_key",
            "is_archive",
            "prover_address",
        ] {
            assert!(v.get(key).is_some(), "missing wire key {key}");
        }
    }

    #[test]
    fn frame_notification_decodes_proxy_body() {
        let body = r#"{
            "run_id": "r1",
            "frame_number": 5,
            "type": "terminal_frame_reached",
            "nodes_reached_stop_frame": 3,
            "total_nodes": 4
        }"#;
        let n: FrameNotification = serde_json::from_str(body).unwrap();
        assert_eq!(n.run_id, "r1");
        assert_eq!(n.stop_frame, 5);
        assert_eq!(n.notification_type, NotificationType::TerminalFrame);
        assert_eq!(n.nodes_reached_stop_frame, 3);
        assert!(n.safety_error.is_empty());
        // Bodies from a proxy predating the app-shard fields still decode,
        // with app checking reported as disabled.
        assert!(n.app_shard_error.is_empty());
        assert_eq!(n.app_shards_reached, 0);
        assert_eq!(n.app_shards_total, 0);
        assert_eq!(n.app_stop_frame, 0);
        assert_eq!(n.app_nodes_reached, 0);
        assert_eq!(n.app_total_nodes, 0);
        assert!(n.app_safety_error.is_empty());
        assert!(n.app_participation_error.is_empty());
        assert_eq!(n.app_frame_number, 0);
    }

    #[test]
    fn frame_notification_app_shard_fields_roundtrip() {
        let n = FrameNotification {
            run_id: "r1".into(),
            stop_frame: 30,
            notification_type: NotificationType::TerminalFrame,
            safety_error: String::new(),
            nodes_reached_stop_frame: 4,
            total_nodes: 4,
            enrollment_error: String::new(),
            rejoin_error: String::new(),
            harness_error: String::new(),
            app_shard_error: "shard 17685a…8643 at frame 1 < 3".into(),
            app_shards_reached: 0,
            app_shards_total: 1,
            app_stop_frame: 3,
            app_nodes_reached: 2,
            app_total_nodes: 4,
            app_safety_error: "app shard fork: frame 2 has two outputs".into(),
            app_participation_error: "client-3 did not vote".into(),
            app_frame_number: 0,
        };
        let v: serde_json::Value = serde_json::to_value(&n).unwrap();
        assert_eq!(v.get("app_shards_total").unwrap(), 1);
        assert_eq!(v.get("app_stop_frame").unwrap(), 3);
        assert_eq!(v.get("app_nodes_reached").unwrap(), 2);
        assert_eq!(v.get("app_total_nodes").unwrap(), 4);
        let back: FrameNotification = serde_json::from_value(v).unwrap();
        assert_eq!(back.app_shard_error, n.app_shard_error);
        assert_eq!(back.app_shards_reached, 0);
        assert_eq!(back.app_shards_total, 1);
        assert_eq!(back.app_stop_frame, 3);
        assert_eq!(back.app_nodes_reached, 2);
        assert_eq!(back.app_total_nodes, 4);
        assert_eq!(back.app_safety_error, n.app_safety_error);
        assert_eq!(back.app_participation_error, n.app_participation_error);
    }

    #[test]
    fn node_info_pinned_filters_default_and_roundtrip() {
        // Old payload without the key decodes to empty.
        let old = r#"{
            "name": "client-1", "hostname": "client-1", "stream_port": 8340,
            "node_port": 8337, "peer_id": "", "peer_priv_key": "",
            "is_archive": false, "prover_address": ""
        }"#;
        let n: NodeInfo = serde_json::from_str(old).unwrap();
        assert!(n.pinned_filters.is_empty());

        let with = NodeInfo {
            pinned_filters: vec![
                "17685a7f980797b781da4120062d2c167a51fabadc745302b4972328f9718643".into(),
            ],
            ..n
        };
        let v: serde_json::Value = serde_json::to_value(&with).unwrap();
        let back: NodeInfo = serde_json::from_value(v).unwrap();
        assert_eq!(back.pinned_filters, with.pinned_filters);
    }

    #[test]
    fn progress_notification_roundtrips() {
        let n = FrameNotification {
            run_id: "r1".into(),
            stop_frame: 3,
            notification_type: NotificationType::Progress,
            safety_error: String::new(),
            nodes_reached_stop_frame: 0,
            total_nodes: 4,
            enrollment_error: String::new(),
            rejoin_error: String::new(),
            harness_error: String::new(),
            app_shard_error: String::new(),
            app_shards_reached: 0,
            app_shards_total: 0,
            app_stop_frame: 3,
            app_nodes_reached: 0,
            app_total_nodes: 0,
            app_safety_error: String::new(),
            app_participation_error: String::new(),
            app_frame_number: 2,
        };
        let v: serde_json::Value = serde_json::to_value(&n).unwrap();
        assert_eq!(v.get("type").unwrap(), "frame_progress");
        assert_eq!(v.get("frame_number").unwrap(), 3);
        assert_eq!(v.get("app_frame_number").unwrap(), 2);
        let back: FrameNotification = serde_json::from_value(v).unwrap();
        assert_eq!(back.notification_type, NotificationType::Progress);
        assert_eq!(back.stop_frame, 3);
        assert_eq!(back.app_frame_number, 2);
        assert_eq!(back.app_stop_frame, 3);
    }

    #[test]
    fn ordinal_parses_suffix() {
        let n = NodeInfo {
            name: "archive-3".into(),
            hostname: "archive-3".into(),
            stream_port: 0,
            node_port: 0,
            peer_id: String::new(),
            peer_priv_key: String::new(),
            falcon_signing_key: String::new(),
            is_archive: true,
            prover_address: String::new(),
            pinned_filters: Vec::new(),
        };
        assert_eq!(n.ordinal().unwrap(), 3);
    }
}
