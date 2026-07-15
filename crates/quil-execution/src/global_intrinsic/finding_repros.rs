//! Failing-test repros for the prover-join logic bugs found by the TLA+
//! specification in `formal/tla/` (see the README findings section there;
//! each test names the finding it reproduces).
//!
//! EVERY TEST IN THIS MODULE ASSERTS THE *INTENDED* BEHAVIOR AND
//! CURRENTLY FAILS.  Each failure is a live bug repro, not a broken
//! test: when the corresponding fix lands, the test goes green and the
//! matching intended-design property in `formal/tla/ProverJoin.tla`
//! should be promoted into `models/MC_Safety.cfg`
//! (`formal/tla/check.sh findings` will report it as "NOT refuted").
//!
//! All scenarios use mainnet epoch length (720 frames):
//!   E0 = frames 0..719, E1 = 720..1439, E2 = 1440..2159,
//!   E3 = 2160..2879, E4 = 2880..3599.

use quil_types::consensus::{EffectiveStatus, ProverAllocationInfo, ProverStatus};

use crate::global_schema::read_field;

use super::materialize::{
    create_allocation_vertex_tree, materialize_prover_confirm, materialize_prover_leave,
    materialize_prover_reject,
};
use super::prover_join::ProverJoin;
use super::verify::verify_prover_join_allocations_expired;

const CLS: &str = "allocation:ProverAllocation";

/// Decode a materialized allocation vertex the way the registry does
/// (`prover_registry.rs::decode_allocation`), so `effective_status` sees
/// exactly what a running node would.
fn info_from_tree(tree: &quil_tries::VectorCommitmentTree) -> ProverAllocationInfo {
    let status_byte = read_field(tree, CLS, "Status")
        .and_then(|b| b.first().copied())
        .expect("allocation has a Status byte");
    let status = match status_byte {
        0 => ProverStatus::Joining,
        1 => ProverStatus::Active,
        2 => ProverStatus::Paused,
        3 => ProverStatus::Leaving,
        4 => ProverStatus::Rejected,
        5 => ProverStatus::Kicked,
        _ => ProverStatus::Unknown,
    };
    let u64f = |name: &str| {
        read_field(tree, CLS, name)
            .filter(|b| b.len() == 8)
            .map(|b| u64::from_be_bytes(b.try_into().unwrap()))
            .unwrap_or(0)
    };
    ProverAllocationInfo {
        status,
        confirmation_filter: read_field(tree, CLS, "ConfirmationFilter").unwrap_or_default(),
        rejection_filter: read_field(tree, CLS, "RejectionFilter").unwrap_or_default(),
        join_frame_number: u64f("JoinFrameNumber"),
        leave_frame_number: u64f("LeaveFrameNumber"),
        pause_frame_number: u64f("PauseFrameNumber"),
        resume_frame_number: u64f("ResumeFrameNumber"),
        kick_frame_number: u64f("KickFrameNumber"),
        join_confirm_frame_number: u64f("JoinConfirmFrameNumber"),
        join_reject_frame_number: u64f("JoinRejectFrameNumber"),
        leave_confirm_frame_number: u64f("LeaveConfirmFrameNumber"),
        leave_reject_frame_number: u64f("LeaveRejectFrameNumber"),
        last_active_frame_number: u64f("LastActiveFrameNumber"),
        epoch: u64f("Epoch"),
        vertex_address: Vec::new(),
    }
}

/// A data-shard allocation that joined at frame 100 (E0) and was
/// confirmed at frame 800 (E1) — an ordinary active member.
fn active_member_tree() -> quil_tries::VectorCommitmentTree {
    let prover_addr = [0x11u8; 32];
    let filter = vec![0x22u8; 32];
    let mut tree = create_allocation_vertex_tree(&prover_addr, &filter, 100).unwrap();
    materialize_prover_confirm(&mut tree, 800).unwrap();
    tree
}

/// FINDING 1 — `materialize_prover_leave` does not clear
/// `LeaveConfirmFrameNumber`, so a later leave inherits the previous
/// cycle's confirm frame and `effective_status` reads the allocation as
/// already-departed the moment the new leave lands.
///
/// On-chain-legal sequence: leave(E2) → confirmLeave(E3, its exact
/// confirm slot) → rejectLeave(E3, no timing gate) → leave again (E4).
/// The second leave, proposed in E4, should await its confirm in E5 and
/// serve notice as `Leaving`; instead the stale confirm frame from the
/// FIRST cycle (deactivation epoch = epoch(2200)+1 = E4) makes it read
/// `ExpiredLeaving` instantly — a mid-epoch committee drop.
#[test]
fn finding1_releave_after_reject_must_not_inherit_stale_leave_confirm() {
    let mut tree = active_member_tree();
    materialize_prover_leave(&mut tree, 1500).unwrap(); // E2: leave #1
    materialize_prover_confirm(&mut tree, 2200).unwrap(); // E3: confirm leave #1
    materialize_prover_reject(&mut tree, 2300).unwrap(); // E3: reject leave #1 -> Active
    materialize_prover_leave(&mut tree, 2900).unwrap(); // E4: leave #2

    let info = info_from_tree(&tree);
    assert_eq!(info.status, ProverStatus::Leaving);
    assert_eq!(info.leave_frame_number, 2900);
    // The root cause, stated as the intended field invariant: a leave
    // proposed at frame F carries no confirm from an earlier cycle.
    // (Currently 2200 — the stale value from leave #1.)
    assert_eq!(
        info.leave_confirm_frame_number, 0,
        "FINDING 1: materialize_prover_leave left the previous cycle's \
         LeaveConfirmFrameNumber ({}) on the vertex",
        info.leave_confirm_frame_number,
    );
    // And its observable effect: one frame after the new leave, the
    // member must still be serving notice (Leaving), not departed.
    assert_eq!(
        info.effective_status(2901),
        EffectiveStatus::Leaving,
        "FINDING 1: the fresh leave at frame 2900 (E4) reads the stale \
         LeaveConfirmFrameNumber=2200 (deactivation epoch E4) and drops \
         the member from the committee instantly, mid-epoch",
    );
}

/// FINDING 2 — no reject timing gate anywhere on the ProverReject path
/// (`verify_prover_reject` is signature-only; compare Go's 720-frame
/// window in `global_prover_reject.go`).  A reject landing after the
/// leave has already implicitly completed (`ExpiredLeaving` — the
/// prover has departed the committee) resurrects the allocation to
/// Active without a fresh join/confirm cycle.
#[test]
fn finding2_late_leave_reject_must_not_resurrect_departed_allocation() {
    let mut tree = active_member_tree();
    materialize_prover_leave(&mut tree, 1500).unwrap(); // E2: leave, never confirmed

    // Sanity (passes): by E4 the unconfirmed leave has implicitly
    // completed — the prover departed at the E4 boundary.
    let departed = info_from_tree(&tree);
    assert_eq!(departed.effective_status(2900), EffectiveStatus::ExpiredLeaving);

    // INTENDED: a reject 1400 frames after the leave (far past the
    // confirm window, past the implicit departure) is refused, at the
    // latest at the state transition itself.
    let result = materialize_prover_reject(&mut tree, 2900);
    assert!(
        result.is_err(),
        "FINDING 2: leave-reject has no timing gate — a ProverReject at \
         frame 2900 resurrected an allocation whose leave (frame 1500) \
         already implicitly completed; the vertex is Active again: {:?}",
        info_from_tree(&tree).status,
    );
}

/// FINDING 3 — the rejoin gate
/// (`verify_prover_join_allocations_expired`) only checks that the
/// existing allocation's `JoinFrameNumber` is at least 720 frames old,
/// regardless of its status byte.  Go additionally requires the
/// existing allocation to be left or expired
/// (`global_prover_join.go:1023-1066`).  Consequence: a healthy ACTIVE
/// allocation older than 720 frames can be overwritten by a fresh
/// self-join, resetting the member to Joining mid-epoch.
#[test]
fn finding3_rejoin_gate_must_refuse_live_active_allocation() {
    // Active member since E0/E1 (join frame 100, confirmed frame 800).
    let tree = active_member_tree();
    assert_eq!(info_from_tree(&tree).status, ProverStatus::Active);

    let pubkey = vec![0x33u8; 64];
    let filter = vec![0x22u8; 32];
    let op = ProverJoin {
        filters: vec![filter],
        frame_number: 2000,
        public_key_signature_bls48581: None,
        delegate_address: Vec::new(),
        merge_targets: Vec::new(),
        proof: Vec::new(),
    };

    // The gate is handed the prover's existing ACTIVE allocation for
    // that filter (frame 2000 > join frame 100 + 720).
    let mut existing = Some(tree);
    let result =
        verify_prover_join_allocations_expired(&op, &pubkey, 2000, |_addr| Ok(existing.take()));
    assert!(
        result.is_err(),
        "FINDING 3: the rejoin gate accepted a join over a LIVE Active \
         allocation just because its JoinFrameNumber is more than 720 \
         frames old — the fresh join would reset an active member to \
         Joining (Go refuses this unless the allocation is left/expired)",
    );
}
