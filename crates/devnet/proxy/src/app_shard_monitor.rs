//! App-shard consensus snoop: the gossip half of app-shard verification.
//!
//! Plays the role `consensus_events` plays for global consensus. Global
//! consensus rides point-to-point gRPC the proxy snoops; app-shard consensus is
//! gossip-only, and the clients' only gossip peer is the proxy, so every
//! finalized frame and consensus vote passes through here. Two topics are
//! decoded per pinned filter:
//!
//! - `shard_frame_bitmask(filter)` (32 bytes): the full `AppShardFrame` each
//!   committee member publishes when a frame finalizes
//!   (`master_node/worker_manager.rs` `FullFrameProduced`). Drives the app
//!   stop-condition latch ([`Self::stop_seen`]), per-filter and per-publisher
//!   heads, equivocation detection, and records the simplex view that produced
//!   the app stop frame — a finalized app frame's `header.rank` IS its view, so
//!   this is the app analogue of the global loop's `stop_frame_view` (learned
//!   there from the block channel).
//! - `shard_cw_bitmask(filter)` (33 bytes, `0x01 || appFilter`): the committee's
//!   commonware-simplex traffic, channel id in the first payload byte. Vote-
//!   channel `Notarize`/`Finalize` messages are tallied per publisher — the app
//!   analogue of the global rejoin check's active-voter tally — but only once
//!   the partition schedule has healed, since a vote cast from inside a
//!   partition says nothing about rejoining. Attribution uses the gossip
//!   message's source peer (`ReceivedMessage::from`), which BlossomSub
//!   signature-verifies (`MessageAuthenticity::Signed` +
//!   `ValidationMode::Strict`), so it survives relaying and cannot be spoofed.
//!
//! Frames can ALSO be polled out-of-band since the master began mirroring
//! worker-finalized frames into the store `AppShardService` reads — that
//! convergence check lives in `frame_monitor`; this module is the in-band
//! snoop half, exactly the snoop/poll split the global path uses.
//!
//! Equivocation compares `header.output` — the consensus-determined
//! deterministic frame output — and NOT the raw payload bytes, because every
//! committee member publishes its own copy of a finalized frame and the
//! embedded CWCT finalization cert can legitimately aggregate different vote
//! subsets per member.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quil_engine::bitmasks;
use quil_types::proto::global::AppShardFrame;

use crate::participation::VoteLedger;
use crate::simplex_view::{self, CW_VOTE_CHANNEL};
use crate::view_schedule::HealSignal;

/// How many frames of `output` history to retain per filter for equivocation
/// detection. Re-publishes of old frames beyond this window are ignored rather
/// than compared; devnet targets are single digits, so the window is generous.
const OUTPUT_HISTORY_FRAMES: u64 = 256;

/// How long after the heal latch is first seen before shard votes count.
///
/// The latch flips on the gRPC snoop task while gossip drains from a deep
/// receive queue on another task, so the first votes dequeued after the flip
/// can still be partition-era backlog — recording them would credit a client
/// that never voted after the network was whole, a false pass of the exact
/// property the participation check exists to prove. Views cannot fence this
/// (post-heal shard views can run *behind* an isolated member's), so a grace
/// window covers the backlog drain instead. Dropping a genuine early vote only
/// delays a clean verdict (the settle gate waits for later votes); crediting a
/// stale one would silently fake a rejoin.
pub const HEAL_VOTE_GRACE: Duration = Duration::from_secs(2);

/// An observation worth waking the event loop for, returned by
/// [`AppShardTracker::observe`]. Mirrors the global path, where every snooped
/// consensus event rides a channel into the loop; the app snoop only surfaces
/// the two events the loop reacts to and keeps the rest as internal state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppShardEvent {
    /// A pinned filter's observed head advanced to `frame`.
    NewHead { filter: Vec<u8>, frame: u64 },
    /// First equivocation observed — the app analogue of a safety violation.
    /// Emitted exactly once; the error string stays sticky on the tracker.
    Fork,
}

/// Snapshot of frame-tracking state, evaluated against the configured target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppShardStatus {
    /// Pinned filters whose observed head reached the target.
    pub reached: i32,
    /// Total pinned filters under observation.
    pub total: i32,
    /// Sticky error (equivocation); empty when clean.
    pub error: String,
}

/// Tracks app-shard consensus for a fixed set of pinned filters and a fixed
/// committee-client roster.
pub struct AppShardTracker {
    /// The app-shard stop frame; 0 = disabled.
    target: u64,
    /// The pinned 32-byte app addresses (== consensus filters) to track.
    filters: Vec<Vec<u8>>,
    /// Precomputed `shard_cw_bitmask` (33 bytes, `0x01 || appFilter` bloom) per
    /// pinned filter — the exact gossip topics whose votes are tallied.
    cw_topics: Vec<Vec<u8>>,
    /// Committee clients as `(name, libp2p peer-id bytes)`. Peer-id bytes are
    /// what `ReceivedMessage::from` carries, so this is the attribution key.
    clients: Vec<(String, Vec<u8>)>,
    /// Set once the partition schedule has healed for good. Shard votes are
    /// only tallied while this is set, which is what makes the participation
    /// check a proof of *rejoining* rather than of merely having voted at some
    /// point (see [`HealSignal`]).
    heal: Arc<HealSignal>,
    /// How long after the latch is first seen before votes count (see
    /// [`HEAL_VOTE_GRACE`]; zero in most tests).
    heal_grace: Duration,
    /// Events dropped on the way to the event loop (channel full). Non-zero
    /// means the tallies are incomplete — surfaced as a harness error, exactly
    /// like the global snoop's dropped-event counter.
    dropped: AtomicU64,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Max observed `frame_number` per pinned filter.
    max_frame: HashMap<Vec<u8>, u64>,
    /// Max observed `frame_number` per publisher peer (any publisher; read
    /// back per committee client for the lag report).
    max_frame_by_peer: HashMap<Vec<u8>, u64>,
    /// `(filter, frame_number)` → `header.output`, bounded per filter by
    /// [`OUTPUT_HISTORY_FRAMES`].
    outputs: HashMap<(Vec<u8>, u64), Vec<u8>>,
    /// First observed equivocation, kept sticky so a later clean observation
    /// cannot erase it.
    equivocation: Option<String>,
    /// Whether [`AppShardEvent::Fork`] was already emitted.
    fork_reported: bool,
    /// Shard vote-channel Notarize/Finalize publishers per view. Only votes
    /// observed after the heal (plus grace) are recorded here.
    votes: VoteLedger,
    /// Whether any post-heal (post-grace) vote has been recorded. `false`
    /// means no committee member has voted since the partition healed, which
    /// leaves the participation check unanswerable rather than failed.
    has_post_heal_vote: bool,
    /// When the heal latch was first seen set from this side — the drain
    /// position where the flip became visible. Anchors the grace window
    /// ([`HEAL_VOTE_GRACE`]): everything dequeued before it was received while
    /// still partitioned, and votes shortly after it may be too.
    healed_seen_at: Option<Instant>,
    /// Committee clients already seen voting inside the participation window.
    /// Participation is monotone — once a client has voted post-heal at or
    /// after the window start, that fact is permanent — so each client latches
    /// here the first time it is seen, keeping the verdict immune to the vote
    /// ledger's rolling tail pruning the evidence between the settle gate and
    /// the terminal check.
    satisfied_voters: HashSet<Vec<u8>>,
    /// The view (`header.rank`) of the first observed finalized frame whose
    /// number equals the target — the app analogue of `stop_frame_view`.
    target_view: Option<u64>,
    /// One-shot latch: a finalized frame at or past the target was observed.
    stop_seen: bool,
}

impl Inner {
    /// Anchor the grace window at the first message processed after the heal
    /// latch is seen set. The latch flips on the gRPC snoop task while gossip
    /// drains from a queue on this task, so this marks the drain position
    /// where the flip became visible.
    fn note_healed(&mut self, healed: bool) {
        if healed && self.healed_seen_at.is_none() {
            self.healed_seen_at = Some(Instant::now());
        }
    }
}

impl AppShardTracker {
    pub fn new(
        target: u64,
        filters: Vec<Vec<u8>>,
        clients: Vec<(String, Vec<u8>)>,
        heal: Arc<HealSignal>,
        heal_grace: Duration,
    ) -> Self {
        let cw_topics = filters
            .iter()
            .map(|f| bitmasks::shard_cw_bitmask(f))
            .collect();
        // A run with nothing to heal from starts its grace window immediately,
        // during startup, so it is spent before consensus casts any votes.
        let inner = Inner {
            healed_seen_at: heal.is_healed().then(Instant::now),
            ..Default::default()
        };
        Self {
            target,
            filters,
            cw_topics,
            clients,
            heal,
            heal_grace,
            dropped: AtomicU64::new(0),
            inner: Mutex::new(inner),
        }
    }

    /// The pinned filters under observation (the gossip layer subscribes the
    /// proxy to each filter's exact per-shard topics).
    pub fn filters(&self) -> &[Vec<u8>] {
        &self.filters
    }

    /// Feed one received gossip message; `from` is the message's
    /// signature-verified source peer (`ReceivedMessage::from`). Cheap
    /// non-matches (unrelated bitmask, undecodable payload, address not
    /// pinned) are ignored silently: this sits on the proxy's full gossip
    /// drain. Returns an event when the loop should react.
    pub fn observe(&self, bitmask: &[u8], data: &[u8], from: &[u8]) -> Option<AppShardEvent> {
        match bitmask.len() {
            // `shard_frame_bitmask` is the only 32-byte topic in the system
            // (global topics are 1/2/3/4/16 bytes, the other shard topics
            // 33-35), so the length check alone scopes frame decoding.
            32 => self.observe_frame(data, from),
            33 => self.observe_cw(bitmask, data, from),
            _ => None,
        }
    }

    fn observe_frame(&self, data: &[u8], from: &[u8]) -> Option<AppShardEvent> {
        let Ok(frame) = <AppShardFrame as prost::Message>::decode(data) else {
            return None;
        };
        let header = frame.header?;
        if !self.filters.contains(&header.address) {
            return None;
        }
        let filter = header.address;
        let frame_number = header.frame_number;

        // Frames are denser than votes, so probing the latch here too anchors
        // the grace window as close to the actual flip as this task can see.
        let healed = self.heal.is_healed();
        let mut inner = self.inner.lock().unwrap();
        inner.note_healed(healed);
        let mut event = None;

        // Enforce the documented ignore window BEFORE touching the output
        // history: a re-publish of a frame older than the tracked head by more
        // than the window is neither inserted (the trim below only runs when
        // the head ADVANCES, so late old frames would otherwise accumulate
        // without bound) nor compared (its original output was already pruned,
        // so a differing-but-honest historical copy would fabricate an
        // equivocation against nothing).
        let tracked_head = inner.max_frame.get(&filter).copied().unwrap_or(0);
        if frame_number.saturating_add(OUTPUT_HISTORY_FRAMES) < tracked_head {
            return None;
        }

        // Equivocation: same (filter, frame) must always carry the same
        // consensus-determined output.
        let key = (filter.clone(), frame_number);
        match inner.outputs.get(&key) {
            Some(prev) if *prev != header.output => {
                let msg = format!(
                    "equivocation on app shard {}: frame {frame_number} observed with two \
                     different outputs",
                    hex::encode(&filter),
                );
                tracing::error!(error = %msg, "app shard equivocation");
                inner.equivocation.get_or_insert(msg);
                if !inner.fork_reported {
                    inner.fork_reported = true;
                    event = Some(AppShardEvent::Fork);
                }
            }
            Some(_) => {}
            None => {
                inner.outputs.insert(key, header.output);
            }
        }

        // Per-publisher head: every committee member publishes its own copy of
        // each finalized frame, so this is a per-client convergence signal.
        if !from.is_empty() {
            let head = inner.max_frame_by_peer.entry(from.to_vec()).or_insert(0);
            if frame_number > *head {
                *head = frame_number;
            }
        }

        if self.target > 0 {
            // A finalized frame states its own view (`header.rank`), so unlike
            // the global path there is no vote/block inheritance hazard here.
            if frame_number == self.target && inner.target_view.is_none() {
                inner.target_view = Some(header.rank);
            }
            // Stop latch at `>=` rather than the global loop's `>`: the global
            // snoop sees pre-finality proposals (stop+1 proposed proves stop
            // notarized), while a full-frame publish only happens at finality,
            // so observing the target itself is the same evidentiary strength.
            if frame_number >= self.target && !inner.stop_seen {
                inner.stop_seen = true;
                tracing::info!(
                    filter = %hex::encode(&filter),
                    frame = frame_number,
                    app_stop_frame = self.target,
                    "observed finalized frame at app stop frame"
                );
            }
        }

        let head = inner.max_frame.entry(filter.clone()).or_insert(0);
        if frame_number > *head {
            *head = frame_number;
            tracing::debug!(
                filter = %hex::encode(&filter),
                frame = frame_number,
                target = self.target,
                "app shard frame observed"
            );
            if event.is_none() {
                event = Some(AppShardEvent::NewHead {
                    filter: filter.clone(),
                    frame: frame_number,
                });
            }
            // Trim output history that has fallen out of the window.
            let cutoff = frame_number.saturating_sub(OUTPUT_HISTORY_FRAMES);
            inner
                .outputs
                .retain(|(f, n), _| *n >= cutoff || *f != filter);
        }
        event
    }

    fn observe_cw(&self, bitmask: &[u8], data: &[u8], from: &[u8]) -> Option<AppShardEvent> {
        if !self.cw_topics.iter().any(|t| t == bitmask) {
            return None;
        }
        let (channel, cw_bytes) = bitmasks::shard_cw_split_payload(data)?;
        if channel != CW_VOTE_CHANNEL {
            return None;
        }
        let view = simplex_view::decode_view(channel, cw_bytes)?;
        // Notarize/Finalize on the vote channel are active participation;
        // Nullify is what a stalled member emits and proves nothing. Shared
        // with the global loop as `VoteKind::is_active_vote`. No epoch filter,
        // and none would help: the shard CW engine is constructed with epoch 0
        // hardcoded (`activate_app_consensus_cw` is passed `0` in
        // `app_engine.rs`), so shard votes carry epoch 0 exactly like global's
        // — filtering on a nonzero epoch here would drop every real vote.
        if !view.kind.is_active_vote() {
            return None;
        }
        if from.is_empty() {
            return None;
        }
        let healed = self.heal.is_healed();
        let mut inner = self.inner.lock().unwrap();
        inner.note_healed(healed);
        // Votes cast while a partition is still applied prove nothing about
        // rejoining, so they are not recorded at all. Filtering by view alone
        // would not be enough: the proxy is the gossip hub and receives both
        // sides' traffic throughout a partition, and an isolated member still
        // notarizes within its own side — at shard views that can run ahead of
        // the views seen after the heal. Dropping them at record time is what
        // makes "voted in the window" mean "voted after the heal".
        if !healed {
            return None;
        }
        // The latch flips on another task while this one drains the gossip
        // queue, so a vote dequeued just after the flip may still have been
        // received inside the partition. Votes only count once the grace
        // window has passed (see [`HEAL_VOTE_GRACE`]).
        if inner
            .healed_seen_at
            .is_none_or(|seen| seen.elapsed() < self.heal_grace)
        {
            return None;
        }
        inner.votes.record(view.view, from);
        inner.has_post_heal_vote = true;
        // Latch satisfaction at record time when the window is already known,
        // so it does not depend on when `participation_error` is next called.
        if inner.target_view.is_some_and(|tv| view.view >= tv) {
            inner.satisfied_voters.insert(from.to_vec());
        }
        None
    }

    /// Whether a finalized frame at or past the target was observed — the app
    /// side's one-shot stop latch, mirroring the global loop's
    /// `global_stop_seen`. Always false when tracking is disabled (target 0);
    /// the caller gates on `app_stop_frame > 0` first.
    pub fn stop_seen(&self) -> bool {
        self.inner.lock().unwrap().stop_seen
    }

    /// The view that produced the app stop frame, once observed.
    pub fn target_view(&self) -> Option<u64> {
        self.inner.lock().unwrap().target_view
    }

    /// Whether any committee member has voted since the partition healed (and
    /// the grace window passed). `false` leaves
    /// [`Self::participation_error`] unanswerable, which the caller reports
    /// as a harness error if it never resolves.
    pub fn has_post_heal_vote(&self) -> bool {
        self.inner.lock().unwrap().has_post_heal_vote
    }

    /// The sticky equivocation error (empty when clean).
    pub fn equivocation_error(&self) -> String {
        self.inner
            .lock()
            .unwrap()
            .equivocation
            .clone()
            .unwrap_or_default()
    }

    /// The highest observed head across all pinned filters (for progress
    /// reporting; devnet pins a single filter).
    pub fn head(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        self.filters
            .iter()
            .map(|f| inner.max_frame.get(f).copied().unwrap_or(0))
            .max()
            .unwrap_or(0)
    }

    /// Count one event dropped between the gossip drain and the event loop.
    pub fn count_dropped(&self) {
        self.dropped.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Events dropped so far (non-zero = tallies incomplete = harness error).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(AtomicOrdering::Relaxed)
    }

    /// Evaluate the tracked frame state against the configured target.
    pub fn status(&self) -> AppShardStatus {
        let inner = self.inner.lock().unwrap();
        let total = self.filters.len() as i32;
        let reached = self
            .filters
            .iter()
            .filter(|f| inner.max_frame.get(*f).copied().unwrap_or(0) >= self.target)
            .count() as i32;
        let error = inner.equivocation.clone().unwrap_or_default();
        AppShardStatus {
            reached,
            total,
            error,
        }
    }

    /// The app analogue of the global rejoin check: every committee client must
    /// have originated a shard Notarize/Finalize *after the partition healed*,
    /// at or after the view that produced the app stop frame — otherwise it
    /// merely ingested finalized frames without rejoining consensus.
    ///
    /// The heal half is what the global check gets for free: its window opens
    /// at the global stop frame's view, and `validate_partition_views` proves
    /// the schedule healed before then. Shard views are a different numbering
    /// entirely, so the app side has to establish it at runtime — votes are
    /// only recorded post-heal (see [`Self::observe`]), so the window is
    /// simply the target frame's view onward.
    ///
    /// The verdict is monotone per client: once a client is seen voting
    /// inside the window it stays satisfied, even after the vote ledger's
    /// rolling tail prunes the vote itself — the settle gate and the terminal
    /// check re-ask this question minutes apart and must not disagree.
    ///
    /// Returns empty when clean, and also when the check cannot answer: the
    /// target's view was never observed, or nothing has voted since the heal.
    /// Both are harness errors the caller reports, not passes.
    pub fn participation_error(&self) -> String {
        let mut inner = self.inner.lock().unwrap();
        let Some(target_view) = inner.target_view else {
            return String::new();
        };
        if !inner.has_post_heal_vote {
            return String::new();
        }
        // Fold the ledger into the satisfied set: votes recorded before the
        // target frame (and thus the window start) was known are latched here.
        let inner = &mut *inner;
        let missing_now: HashSet<&str> = inner
            .votes
            .missing_since(target_view, &self.clients)
            .into_iter()
            .collect();
        for (name, key) in &self.clients {
            if !missing_now.contains(name.as_str()) {
                inner.satisfied_voters.insert(key.clone());
            }
        }
        let missing: Vec<&str> = self
            .clients
            .iter()
            .filter(|(_, key)| !inner.satisfied_voters.contains(key))
            .map(|(name, _)| name.as_str())
            .collect();
        if missing.is_empty() {
            String::new()
        } else {
            format!(
                "{} did not vote after the partition healed at or after the app stop frame's \
                 view (app_stop_frame={}, window opens at shard view {target_view}) — did not \
                 rejoin shard consensus",
                missing.join(", "),
                self.target
            )
        }
    }

    /// Human-readable lag report for the timeout path: names each pinned
    /// filter that has not reached the target and its current head, plus each
    /// committee client whose own published head lags the target.
    pub fn lag_report(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let mut lagging: Vec<String> = self
            .filters
            .iter()
            .filter_map(|f| {
                let head = inner.max_frame.get(f).copied().unwrap_or(0);
                (head < self.target).then(|| {
                    format!(
                        "app shard {} at frame {head} < target {}",
                        hex::encode(f),
                        self.target
                    )
                })
            })
            .collect();
        for (name, peer) in &self.clients {
            let head = inner.max_frame_by_peer.get(peer).copied().unwrap_or(0);
            if head < self.target {
                lagging.push(format!(
                    "{name} last published shard frame {head} < target {}",
                    self.target
                ));
            }
        }
        lagging.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_codec::varint::UInt;
    use commonware_codec::Encode;
    use quil_types::proto::global::FrameHeader;

    fn addr(b: u8) -> Vec<u8> {
        vec![b; 32]
    }

    fn peer(b: u8) -> Vec<u8> {
        vec![b; 38]
    }

    /// A tracker for an unpartitioned run: the heal signal is born set, so
    /// votes count from the first one, exactly as before the signal existed.
    fn tracker(target: u64) -> AppShardTracker {
        partitioned_tracker(target, Arc::new(HealSignal::healed())).0
    }

    /// A tracker for a partitioned run, plus the latch the caller heals with.
    /// Grace-free so votes count the instant the latch is set; the grace
    /// window has its own test.
    fn partitioned_tracker(
        target: u64,
        heal: Arc<HealSignal>,
    ) -> (AppShardTracker, Arc<HealSignal>) {
        graced_tracker(target, heal, Duration::ZERO)
    }

    fn graced_tracker(
        target: u64,
        heal: Arc<HealSignal>,
        grace: Duration,
    ) -> (AppShardTracker, Arc<HealSignal>) {
        let t = AppShardTracker::new(
            target,
            vec![addr(0xAA)],
            vec![("client-1".into(), peer(1)), ("client-2".into(), peer(2))],
            Arc::clone(&heal),
            grace,
        );
        (t, heal)
    }

    fn frame_bytes(address: &[u8], frame_number: u64, output: &[u8]) -> Vec<u8> {
        frame_bytes_with_rank(address, frame_number, 0, output)
    }

    fn frame_bytes_with_rank(
        address: &[u8],
        frame_number: u64,
        rank: u64,
        output: &[u8],
    ) -> Vec<u8> {
        let frame = AppShardFrame {
            header: Some(FrameHeader {
                address: address.to_vec(),
                frame_number,
                rank,
                output: output.to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        };
        prost::Message::encode_to_vec(&frame)
    }

    /// A shard CW vote payload encoded with the production varint codec:
    /// `[channel_u8] || tag || epoch || view || filler`.
    fn cw_vote_payload(channel: u64, tag: u8, epoch: u64, view: u64) -> Vec<u8> {
        let mut out = vec![channel as u8, tag];
        out.extend_from_slice(&UInt(epoch).encode());
        out.extend_from_slice(&UInt(view).encode());
        out.extend_from_slice(&[0xAB; 666]);
        out
    }

    fn cw_topic(address: &[u8]) -> Vec<u8> {
        bitmasks::shard_cw_bitmask(address)
    }

    #[test]
    fn tracks_max_frame_for_pinned_filter_only() {
        let t = tracker(3);
        // Pinned filter advances (frame topic = any 32-byte bitmask).
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"o1"), &peer(1));
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 2, b"o2"), &peer(1));
        // Foreign app's frames are ignored.
        t.observe(&addr(0x01), &frame_bytes(&addr(0xBB), 9, b"x"), &peer(1));
        let s = t.status();
        assert_eq!((s.reached, s.total), (0, 1));
        assert!(!t.stop_seen());
        // Reaching the target flips the latch.
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 3, b"o3"), &peer(1));
        let s = t.status();
        assert_eq!((s.reached, s.total), (1, 1));
        assert!(s.error.is_empty());
        assert!(t.stop_seen());
    }

    /// Frames older than the tracked head by more than
    /// [`OUTPUT_HISTORY_FRAMES`] are ignored outright: their original outputs
    /// were already pruned, so inserting/comparing a late re-publish could
    /// only grow the map without bound or fabricate an equivocation against a
    /// differing-but-honest historical copy.
    #[test]
    fn ancient_republishes_are_ignored_not_compared() {
        let t = tracker(0);
        // Advance the head far past the window.
        let head = OUTPUT_HISTORY_FRAMES + 50;
        t.observe(
            &addr(0x01),
            &frame_bytes(&addr(0xAA), 10, b"orig"),
            &peer(1),
        );
        t.observe(
            &addr(0x01),
            &frame_bytes(&addr(0xAA), head, b"head"),
            &peer(1),
        );
        // Frame 10's output has been pruned; a re-publish with a DIFFERENT
        // output must not trip the fork detector.
        let ev = t.observe(
            &addr(0x01),
            &frame_bytes(&addr(0xAA), 10, b"other"),
            &peer(2),
        );
        assert_eq!(ev, None);
        assert!(
            t.equivocation_error().is_empty(),
            "an out-of-window re-publish must not fabricate an equivocation"
        );
        // A frame just inside the window is still compared (and still forks).
        let recent = head - OUTPUT_HISTORY_FRAMES;
        t.observe(
            &addr(0x01),
            &frame_bytes(&addr(0xAA), recent, b"a"),
            &peer(1),
        );
        let ev = t.observe(
            &addr(0x01),
            &frame_bytes(&addr(0xAA), recent, b"b"),
            &peer(2),
        );
        assert_eq!(ev, Some(AppShardEvent::Fork));
    }

    #[test]
    fn new_head_events_fire_once_per_advance() {
        let t = tracker(3);
        let ev = t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"o1"), &peer(1));
        assert_eq!(
            ev,
            Some(AppShardEvent::NewHead {
                filter: addr(0xAA),
                frame: 1
            })
        );
        // A re-publish of the same frame by another member: no event.
        let ev = t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"o1"), &peer(2));
        assert_eq!(ev, None);
    }

    #[test]
    fn stop_latch_is_at_least_and_one_shot() {
        let t = tracker(3);
        // Skipping straight past the target still latches (>=, not ==).
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 4, b"o4"), &peer(1));
        assert!(t.stop_seen());
        // But the target's own view is unknown: frame 3 was never observed.
        assert_eq!(t.target_view(), None);
    }

    #[test]
    fn target_view_set_only_by_the_frame_stating_the_target() {
        let t = tracker(3);
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 7, b"o2"),
            &peer(1),
        );
        assert_eq!(t.target_view(), None);
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 3, 9, b"o3"),
            &peer(1),
        );
        assert_eq!(t.target_view(), Some(9));
        // First observation wins; a re-publish cannot move it.
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 3, 12, b"o3"),
            &peer(2),
        );
        assert_eq!(t.target_view(), Some(9));
    }

    #[test]
    fn ignores_unrelated_bitmask_lengths_and_garbage() {
        let t = tracker(1);
        // 34-byte bitmask: not decoded even with a valid payload.
        t.observe(&[0u8; 34], &frame_bytes(&addr(0xAA), 5, b"o"), &peer(1));
        // Garbage payload on a 32-byte bitmask: ignored.
        t.observe(&addr(0x01), b"not a proto frame", &peer(1));
        assert_eq!(t.status().reached, 0);
    }

    #[test]
    fn equivocation_is_detected_sticky_and_forks_once() {
        let t = tracker(2);
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"one"), &peer(1));
        // Same frame, same output — a re-publish from another member: fine.
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"one"), &peer(2));
        assert!(t.equivocation_error().is_empty());
        // Same frame, different output: fork, emitted exactly once.
        let ev = t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"two"), &peer(2));
        assert_eq!(ev, Some(AppShardEvent::Fork));
        let ev = t.observe(
            &addr(0x01),
            &frame_bytes(&addr(0xAA), 1, b"three"),
            &peer(2),
        );
        assert_eq!(ev, None);
        // Sticky: reaching the target does not clear it.
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 2, b"o2"), &peer(1));
        assert!(t.equivocation_error().contains("equivocation"));
        assert_eq!(t.status().reached, 1);
    }

    #[test]
    fn disabled_tracker_never_latches() {
        let t = tracker(0);
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 5, b"o"), &peer(1));
        assert!(!t.stop_seen());
        assert_eq!(t.target_view(), None);
        // A target of 0 is trivially reached by every filter; the runner
        // ignores these fields entirely when app checking is disabled.
        let s = t.status();
        assert_eq!(s.reached, s.total);
        assert!(s.error.is_empty());
    }

    #[test]
    fn lag_report_names_lagging_filters_and_clients() {
        let t = tracker(3);
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 1, b"o1"), &peer(1));
        let report = t.lag_report();
        assert!(report.contains("at frame 1 < target 3"), "{report}");
        // client-1 published frame 1; client-2 never published anything.
        assert!(
            report.contains("client-1 last published shard frame 1 < target 3"),
            "{report}"
        );
        assert!(
            report.contains("client-2 last published shard frame 0 < target 3"),
            "{report}"
        );
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 3, b"o3"), &peer(1));
        t.observe(&addr(0x01), &frame_bytes(&addr(0xAA), 3, b"o3"), &peer(2));
        assert!(t.lag_report().is_empty());
    }

    #[test]
    fn vote_tally_counts_notarize_and_finalize_only() {
        let t = tracker(2);
        let topic = cw_topic(&addr(0xAA));
        // Frame 2 finalized in view 9 establishes the participation window.
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        assert_eq!(t.target_view(), Some(9));
        // client-1 notarizes in the target view; a nonzero epoch is accepted
        // too (real shard engines hardcode epoch 0, and the monitor applies
        // no epoch filter either way).
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9), &peer(1));
        // client-2 only nullifies (passive) — does not count.
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 1, 4, 9), &peer(2));
        let err = t.participation_error();
        assert!(err.contains("client-2"), "{err}");
        assert!(!err.contains("client-1"), "{err}");
        // A later Finalize from client-2 clears it (at-or-after window).
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 2, 4, 11),
            &peer(2),
        );
        assert!(t.participation_error().is_empty());
    }

    #[test]
    fn votes_before_the_target_view_do_not_count() {
        let t = tracker(2);
        let topic = cw_topic(&addr(0xAA));
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 5), &peer(2));
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9), &peer(1));
        let err = t.participation_error();
        assert!(err.contains("client-2"), "{err}");
    }

    #[test]
    fn participation_unanswerable_without_target_view() {
        let t = tracker(2);
        // Votes observed, but the target frame itself never was.
        t.observe(
            &cw_topic(&addr(0xAA)),
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9),
            &peer(1),
        );
        assert_eq!(t.participation_error(), String::new());
    }

    #[test]
    fn cert_channel_and_foreign_topics_are_ignored() {
        let t = tracker(2);
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        // Cert channel (1) is passive relay traffic, not an origination proof.
        t.observe(
            &cw_topic(&addr(0xAA)),
            &cw_vote_payload(1, 0, 4, 9),
            &peer(1),
        );
        // A vote on a foreign shard's CW topic is not tallied either.
        t.observe(
            &cw_topic(&addr(0xBB)),
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9),
            &peer(1),
        );
        // client-2 votes for real, so the window is established and the check
        // can answer; client-1's two ignored messages leave it missing.
        t.observe(
            &cw_topic(&addr(0xAA)),
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9),
            &peer(2),
        );
        let err = t.participation_error();
        assert!(err.contains("client-1"), "{err}");
        assert!(!err.contains("client-2"), "{err}");
    }

    // ---- heal-aware participation -------------------------------------------

    /// The gap this closes: a client can be voting the whole time it is
    /// partitioned away, at shard views at or past the target's, and that says
    /// nothing about whether it rejoined once the network was whole again.
    #[test]
    fn votes_cast_before_the_heal_do_not_satisfy_participation() {
        let (t, heal) = partitioned_tracker(2, Arc::new(HealSignal::default()));
        let topic = cw_topic(&addr(0xAA));

        // Frame 2 finalizes in view 9 while the partition is still applied.
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        assert_eq!(t.target_view(), Some(9));
        // client-1 notarizes from inside its side of the partition, in a view
        // at (and then past) the target's.
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9), &peer(1));
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 12),
            &peer(1),
        );
        assert!(!t.has_post_heal_vote(), "nothing counts before the heal");

        heal.set();
        // Only client-2 votes once the network is whole.
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 2, 4, 13),
            &peer(2),
        );

        let err = t.participation_error();
        assert!(
            err.contains("client-1"),
            "pre-heal votes must not count: {err}"
        );
        assert!(!err.contains("client-2"), "{err}");
    }

    #[test]
    fn a_post_heal_vote_clears_the_check() {
        let (t, heal) = partitioned_tracker(2, Arc::new(HealSignal::default()));
        let topic = cw_topic(&addr(0xAA));
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        heal.set();
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 10),
            &peer(1),
        );
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 2, 4, 11),
            &peer(2),
        );
        assert!(t.has_post_heal_vote());
        assert!(t.participation_error().is_empty());
    }

    /// Healed, target frame observed, but nobody has voted since — the check
    /// cannot answer, so it must not report a pass. The caller turns the
    /// missing post-heal vote into a harness error.
    #[test]
    fn heal_without_any_later_vote_is_unanswerable() {
        let (t, heal) = partitioned_tracker(2, Arc::new(HealSignal::default()));
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        heal.set();
        assert!(!t.has_post_heal_vote());
        assert_eq!(t.participation_error(), String::new());
    }

    /// The window opens at the target frame's view: a post-heal vote in an
    /// earlier view proves the client votes, but not that it participated in
    /// (or after) the consensus that produced the stop frame.
    #[test]
    fn a_post_heal_vote_before_the_target_view_does_not_count() {
        let (t, heal) = partitioned_tracker(2, Arc::new(HealSignal::default()));
        let topic = cw_topic(&addr(0xAA));
        heal.set();
        // Both clients vote in view 5, then frame 2 finalizes in view 9.
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 5), &peer(1));
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 5), &peer(2));
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        assert!(t.has_post_heal_vote());
        let err = t.participation_error();
        assert!(err.contains("client-1"), "{err}");
        assert!(err.contains("client-2"), "{err}");
        assert!(err.contains("window opens at shard view 9"), "{err}");
    }

    /// The latch flips on another task while votes drain from a queue, so a
    /// vote dequeued right after the flip may still be partition-era backlog:
    /// votes must not count until the grace window has passed.
    #[test]
    fn votes_inside_the_heal_grace_window_do_not_count() {
        let grace = Duration::from_millis(40);
        let (t, heal) = graced_tracker(2, Arc::new(HealSignal::default()), grace);
        let topic = cw_topic(&addr(0xAA));
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        heal.set();
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9), &peer(1));
        assert!(!t.has_post_heal_vote(), "backlog-era vote must not count");

        std::thread::sleep(grace * 2);
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 10),
            &peer(1),
        );
        assert!(t.has_post_heal_vote());
        let err = t.participation_error();
        assert!(!err.contains("client-1"), "{err}");
    }

    /// Participation is monotone: a client seen voting in the window stays
    /// satisfied even after the ledger's rolling tail prunes the vote, so the
    /// settle gate and the terminal check cannot disagree.
    #[test]
    fn a_satisfied_client_survives_vote_pruning() {
        use crate::participation::VOTE_HISTORY_VIEWS;
        let t = tracker(2);
        let topic = cw_topic(&addr(0xAA));
        t.observe(
            &addr(0x01),
            &frame_bytes_with_rank(&addr(0xAA), 2, 9, b"o2"),
            &peer(1),
        );
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9), &peer(1));
        t.observe(&topic, &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9), &peer(2));
        assert!(t.participation_error().is_empty());
        // client-2 goes quiet while client-1 keeps voting far past the tail,
        // pruning client-2's only vote out of the ledger.
        t.observe(
            &topic,
            &cw_vote_payload(CW_VOTE_CHANNEL, 0, 4, 9 + VOTE_HISTORY_VIEWS + 10),
            &peer(1),
        );
        assert!(
            t.participation_error().is_empty(),
            "the pruned vote was already latched"
        );
    }

    #[test]
    fn dropped_counter_accumulates() {
        let t = tracker(1);
        assert_eq!(t.dropped(), 0);
        t.count_dropped();
        t.count_dropped();
        assert_eq!(t.dropped(), 2);
    }
}
