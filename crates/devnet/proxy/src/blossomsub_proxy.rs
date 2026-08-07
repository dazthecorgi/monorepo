//! BlossomSub side of the proxy: a libp2p host that meshes with every node,
//! relays gossip, and applies bipartite network partitions via the per-(src,dst)
//! forward filter.
//!
//! GLOBAL consensus moved to point-to-point gRPC on `:8340` in v2.1.0.25 (the
//! proxy snoops it there — see `crate::consensus_events`), but gossip still
//! carries APP-SHARD consensus signal: shard CW votes and finalized shard
//! frames ride the per-shard bitmasks, and this module both relays them
//! (behind the partition filter) and feeds them to the `AppShardTracker` for
//! participation/fork verification.
//!
//! Built on `quil-p2p`'s `P2PNode`/`P2PHandle` (reusing the production
//! BlossomSub) plus the `set_forward_filter` hook added for devnet (enforced
//! by the vendored gossipsub's send-path gate). Mirrors the Go proxy's
//! `BlossomSubProxy`.

use std::sync::Arc;

use anyhow::Context;
use quil_config::P2PConfig;
use quil_engine::bitmasks;
use quil_lifecycle::Supervisor;
use quil_p2p::node::P2PNode;
use quil_p2p::P2PHandle;
use tokio::sync::mpsc;

use crate::app_shard_monitor::{AppShardEvent, AppShardTracker};
use crate::partitioner::NetworkPartitioner;

/// Default QUIC listen address when the config doesn't specify one.
const DEFAULT_LISTEN: &str = "/ip4/0.0.0.0/udp/8336/quic-v1";

/// The proxy's gossip layer.
///
/// Holding this value is what keeps the swarm alive — dropping the last
/// `P2PHandle` makes the swarm command loop see `None` and shut down. The proxy
/// only relays (via the forward filter) and never publishes, so there is nothing
/// to call on it; partitions are driven through the shared
/// [`NetworkPartitioner`] the forward filter closes over.
pub struct BlossomSubProxy {
    #[allow(dead_code)]
    handle: P2PHandle,
}

impl BlossomSubProxy {
    /// Build the host, install the partition forward filter, and subscribe to
    /// the five global bitmasks plus the exact per-shard bitmasks of every
    /// pinned filter. The swarm task is registered on `sup`; received shard
    /// traffic is fed to `tracker`, and observations the event loop must react
    /// to (head advances, forks) ride `app_tx` into it — the app-side analogue
    /// of the gRPC snoop's consensus-event channel.
    pub async fn start(
        sup: &mut Supervisor<anyhow::Error>,
        p2p_config: &P2PConfig,
        partitioner: Arc<NetworkPartitioner>,
        tracker: Arc<AppShardTracker>,
        app_tx: mpsc::Sender<AppShardEvent>,
    ) -> anyhow::Result<Self> {
        let node = P2PNode::new(p2p_config).context("construct P2PNode")?;
        let listen_addr = if p2p_config.listen_multiaddr.is_empty() {
            DEFAULT_LISTEN.to_string()
        } else {
            p2p_config.listen_multiaddr.clone()
        };
        let (handle, mut msg_rx) = node
            .start(sup, &listen_addr)
            .await
            .context("start P2P swarm")?;

        // Install the partition forward filter. The closure captures the shared
        // partitioner, so later `apply_partition` calls take effect live without
        // reinstalling the filter.
        {
            let p = Arc::clone(&partitioner);
            handle
                .set_forward_filter(move |src, dst| p.forward_filter(src, dst))
                .await;
        }

        // Subscribe before relaying — BlossomSub only forwards on subscribed
        // bitmasks, and rejects publishes to unsubscribed ones.
        handle.subscribe(bitmasks::GLOBAL_CONSENSUS.to_vec()).await;
        handle.subscribe(bitmasks::GLOBAL_PROVER.to_vec()).await;
        handle.subscribe(bitmasks::GLOBAL_PEER_INFO.to_vec()).await;
        handle.subscribe(bitmasks::GLOBAL_ALERT.to_vec()).await;
        // GLOBAL_FRAME: the CW global proposer gossip-publishes every finalized
        // global frame and ALL nodes (archives + clients) subscribe to it
        // (`master_node/networking.rs`). Publishers count only exact-topic
        // subscribers, and the proxy is every node's sole peer — without this
        // subscription the proposer's publish fails with
        // NoPeersSubscribedToTopic and clients fall back to RPC-polling
        // archives for the head.
        handle.subscribe(bitmasks::GLOBAL_FRAME.to_vec()).await;

        // EXACT per-shard subscriptions for every pinned filter — the only
        // mechanism that works on the stock-gossipsub bridge, where every
        // bitmask is an exact-match topic (hex string). All-ones "covering
        // masks" from the fork era subscribe to a literal topic of 0xFF bytes
        // that nothing publishes on; covering-mask semantics exist only in the
        // orphaned fork crate. A publisher only counts peers subscribed to the
        // exact topic, so a client publishing shard-CW votes to a proxy
        // without the exact subscription fails with NoPeersSubscribedToTopic —
        // and since the clients sit on isolated networks with the proxy as
        // their only peer, the 4-member committee can never exchange a single
        // vote (observed live: every `shard cw publish failed` while the
        // engines timed out view after view). Any filter OUTSIDE the pinned
        // set (e.g. a post-split confirmation filter) has no proxy
        // subscription and would wedge its committee the same way.
        for filter in tracker.filters() {
            handle
                .subscribe(quil_engine::bitmasks::shard_frame_bitmask(filter))
                .await;
            handle
                .subscribe(quil_engine::bitmasks::shard_consensus_bitmask(filter))
                .await;
            handle
                .subscribe(quil_engine::bitmasks::shard_dispatch_bitmask(filter))
                .await;
            handle
                .subscribe(quil_engine::bitmasks::shard_prover_bitmask(filter))
                .await;
            handle
                .subscribe(quil_engine::bitmasks::shard_cw_bitmask(filter))
                .await;
        }
        tracing::info!(
            pinned_filters = tracker.filters().len(),
            "proxy subscribed to global and pinned exact per-shard bitmasks"
        );

        // Drain received gossip, feeding shard traffic (finalized frames AND
        // shard CW votes) to the app-shard tracker, attributed by the
        // signature-verified source peer. Relaying itself happens inside the
        // swarm, behind the forward filter — but the receiver must keep being
        // emptied or the swarm backs up behind a full channel, so the observe
        // call stays cheap (length gate + prefix decode of tiny devnet
        // payloads). Events the loop must react to are forwarded non-blocking;
        // a full channel is counted on the tracker and reported as a harness
        // error, mirroring the gRPC snoop's dropped-event accounting.
        sup.run_until_cancelled("blossomsub-consumer", move |_token| async move {
            while let Some(m) = msg_rx.recv().await {
                if let Some(ev) = tracker.observe(&m.bitmask, &m.data, &m.from) {
                    if app_tx.try_send(ev).is_err() {
                        tracker.count_dropped();
                    }
                }
            }
            Ok(())
        });

        Ok(Self { handle })
    }
}
