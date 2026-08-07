//! The vote ledger both participation checks are built on.
//!
//! The global rejoin check and the app-shard participation check ask the same
//! question of different chains: did every member that is supposed to be
//! driving consensus actually originate an active vote inside some window,
//! rather than merely ingesting finalized frames? Both therefore need the same
//! structure — active voters keyed by view, pruned to a bounded tail, diffed
//! against a required roster.
//!
//! What the two do *not* share is attribution or wording: global votes are
//! snooped off point-to-point gRPC and keyed by 32-byte prover address, shard
//! votes are snooped off gossip and keyed by libp2p peer id, and the two error
//! messages name different things. So this is the shared mechanism, with a thin
//! adapter on each side, rather than one shared function.

use std::collections::{BTreeMap, HashSet};

/// How many views of vote history to retain.
///
/// A window's start view is not known when its votes arrive — the global side
/// learns the stop frame's view only when that frame's block arrives, and the
/// app side only when the target frame finalizes — so votes have to be buffered
/// speculatively. This tail is far more than the gap between a vote and the
/// frame it helped finalize, and it keeps a long run from growing the map
/// without bound.
pub const VOTE_HISTORY_VIEWS: u64 = 64;

/// Active voters per view, pruned to the last [`VOTE_HISTORY_VIEWS`] views.
///
/// Plain data with no interior locking: the global loop owns one directly, and
/// the app tracker keeps one inside the mutex it already holds.
#[derive(Debug, Default)]
pub struct VoteLedger {
    by_view: BTreeMap<u64, HashSet<Vec<u8>>>,
}

impl VoteLedger {
    /// Record that `voter` actively participated in `view`, and drop history
    /// that has fallen out of the window.
    ///
    /// Pruning keys off the view just recorded rather than the highest ever
    /// seen, so an out-of-order straggler cannot drag the window backwards and
    /// discard newer history.
    pub fn record(&mut self, view: u64, voter: &[u8]) {
        self.by_view.entry(view).or_default().insert(voter.to_vec());
        if let Some(prune_below) = view.checked_sub(VOTE_HISTORY_VIEWS) {
            self.by_view.retain(|&v, _| v >= prune_below);
        }
    }

    /// The names in `required` that did not vote in `since` or any later view.
    ///
    /// The window INCLUDES `since` itself (`range(since..)` is inclusive at
    /// the start — see `the_window_includes_its_own_view_and_everything_after`)
    /// and everything after it: a vote in the very view being decided counts,
    /// and a member whose first post-rejoin vote lands in a later view counts
    /// too.
    pub fn missing_since<'a>(&self, since: u64, required: &'a [(String, Vec<u8>)]) -> Vec<&'a str> {
        let voters: HashSet<&Vec<u8>> = self
            .by_view
            .range(since..)
            .flat_map(|(_, voters)| voters.iter())
            .collect();
        required
            .iter()
            .filter(|(_, key)| !voters.contains(key))
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8) -> Vec<u8> {
        vec![b; 32]
    }

    fn roster(names: &[(&str, u8)]) -> Vec<(String, Vec<u8>)> {
        names
            .iter()
            .map(|(n, b)| ((*n).to_string(), key(*b)))
            .collect()
    }

    #[test]
    fn the_window_includes_its_own_view_and_everything_after() {
        let mut l = VoteLedger::default();
        l.record(6, &key(1));
        l.record(7, &key(2));
        l.record(8, &key(3));
        let required = roster(&[("a", 1), ("b", 2), ("c", 3)]);

        let missing = l.missing_since(7, &required);
        assert_eq!(
            missing,
            vec!["a"],
            "view 6 is before the window; 7 and 8 are inside it"
        );
    }

    /// The defect the view-keyed ledger exists to prevent: activity in an
    /// earlier view must not satisfy a window that opens later.
    #[test]
    fn earlier_views_never_satisfy_the_window() {
        let mut l = VoteLedger::default();
        l.record(1, &key(1));
        l.record(2, &key(1));
        let required = roster(&[("a", 1)]);
        assert_eq!(l.missing_since(3, &required), vec!["a"]);
    }

    #[test]
    fn a_full_roster_leaves_nothing_missing() {
        let mut l = VoteLedger::default();
        l.record(5, &key(1));
        l.record(5, &key(2));
        // An unrequired voter is simply ignored.
        l.record(5, &key(9));
        let required = roster(&[("a", 1), ("b", 2)]);
        assert!(l.missing_since(5, &required).is_empty());
    }

    #[test]
    fn history_is_pruned_to_the_window() {
        let mut l = VoteLedger::default();
        l.record(1, &key(1));
        l.record(VOTE_HISTORY_VIEWS + 2, &key(2));
        let required = roster(&[("a", 1), ("b", 2)]);
        // The view-1 vote is gone, so it cannot answer even its own view.
        assert_eq!(l.missing_since(1, &required), vec!["a"]);
    }

    /// Pruning follows the view being recorded, so a late straggler must not
    /// discard the newer history around it.
    #[test]
    fn an_out_of_order_straggler_does_not_prune_newer_views() {
        let mut l = VoteLedger::default();
        l.record(100, &key(1));
        l.record(99, &key(2));
        let required = roster(&[("a", 1), ("b", 2)]);
        assert!(l.missing_since(99, &required).is_empty());
    }
}
