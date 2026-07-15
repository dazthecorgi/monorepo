------------------------------ MODULE ProverJoin ------------------------------
(***************************************************************************)
(* Concrete specification of the prover join / lifecycle protocol, as     *)
(* implemented by the Rust node (source of truth):                        *)
(*                                                                         *)
(*   crates/quil-execution/src/global_intrinsic/materialize.rs  (writes)   *)
(*   crates/quil-execution/src/global_intrinsic/verify.rs       (guards)   *)
(*   crates/quil-execution/src/global_intrinsic/intrinsic.rs    (dispatch) *)
(*   crates/quil-types/src/consensus.rs                (effective_status)  *)
(*                                                                         *)
(* See formal/tla/README.md for the full action <-> Rust correspondence   *)
(* table and the modeling decisions.  Highlights:                          *)
(*                                                                         *)
(*  - One allocation per prover, on one abstract data shard.               *)
(*  - Global consensus is abstracted to a monotone finalized-frame counter *)
(*    `frame` that can advance or stall (stalling = stuttering; the safety *)
(*    spec assumes no fairness, so frames may stop forever).  Finalized    *)
(*    frames never fork and never skip -- both inherent in a counter.      *)
(*  - Prover ops travel through a one-slot-per-prover mempool with         *)
(*    nondeterministic inclusion timing (message delay) and DropOp         *)
(*    (gossip loss / rank-buffer retention expiry).                        *)
(*  - `joinsBlocked` collapses the coverage halt (halt_state.rs) and the   *)
(*    shard split/merge freeze (intrinsic.rs) into one environment bit     *)
(*    that blocks join admission independent of consensus progress.        *)
(*  - Expiry is a DERIVED read-side predicate (EffOf), never an action --  *)
(*    exactly like Rust's EffectiveStatus overlay.                         *)
(*  - Status byte values map to Rust materialize.rs constants:             *)
(*      "none"    -> no allocation vertex                                  *)
(*      "joining" -> STATUS_JOINING (0)                                    *)
(*      "active"  -> STATUS_ACTIVE  (1)                                    *)
(*      "paused"  -> STATUS_PAUSED  (2)                                    *)
(*      "leaving" -> STATUS_LEAVING (3)                                    *)
(*      "left"    -> STATUS_KICKED  (4, "left"); the `kicked` flag         *)
(*                   distinguishes an actual kick (prover-level            *)
(*                   KickFrameNumber != 0, rejoin barred) from a reject/   *)
(*                   ordinary departure.                                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
  Provers,          \* model provers
  EPOCH_LENGTH,     \* frames per epoch     (Rust: EPOCH_LENGTH_FRAMES = 720)
  FRESHNESS,        \* join freshness window (Rust: JOIN_FRESHNESS_WINDOW = 10)
  REJOIN_WINDOW,    \* rejoin-over-existing-alloc window (Rust: 720)
  MAX_FRAME,        \* exploration bound on the frame counter
  BLOCKING_ENABLED, \* whether the joinsBlocked environment bit can be set
  HONEST_ISSUE      \* TRUE: provers only issue joins an honest lifecycle
                    \* would (no rejoin over a live allocation).  FALSE:
                    \* provers may issue any join the CHAIN would accept,
                    \* exercising the permissive rejoin gate.

ASSUME EPOCH_LENGTH >= 2 /\ FRESHNESS >= 1 /\ MAX_FRAME >= 2 * EPOCH_LENGTH

VARIABLES
  frame,        \* latest finalized global frame number
  alloc,        \* [Provers -> allocation record] (the on-trie state)
  mempool,      \* [Provers -> pending op record] (one slot per prover)
  joinsBlocked  \* coverage halt / shard freeze bit

vars == <<frame, alloc, mempool, joinsBlocked>>

StatusBytes == {"none", "joining", "active", "paused", "leaving", "left"}

OpTypes == {"join", "confirmJoin", "reject", "leave", "confirmLeave",
            "reconfirm", "pause", "resume"}

MaxEpoch == MAX_FRAME \div EPOCH_LENGTH

NoAlloc == [status |-> "none", joinF |-> 0, joinConfF |-> 0,
            leaveF |-> 0, leaveConfF |-> 0, epoch |-> 0, kicked |-> FALSE]

NoOp == [type |-> "none", stamp |-> 0]

--------------------------------------------------------------------------
(* Derived state -- the epoch clock and Rust's EffectiveStatus overlay.   *)

\* consensus.rs:235  epoch_for_frame
EpochOf(f) == f \div EPOCH_LENGTH

CurEpoch == EpochOf(frame)

(* Transcription of ProverAllocationInfo::effective_status
   (consensus.rs:345-430) as a pure function of an allocation record and
   a frame, so it can be evaluated on both unprimed and primed state.
   We model a data shard (non-empty filter), so the empty-filter
   exemptions do not apply.  Frame-number fields are never 0 for a live
   allocation in this model (frames start at 1), so the genesis
   sentinels (join_frame_number == 0 etc.) are intentionally out of
   scope. *)
EffOf(a, f) ==
  LET e == EpochOf(f) IN
  CASE a.status = "none" -> "None"
    [] a.status = "joining" ->
         \* consensus.rs:348-363: confirm slot is exactly join_epoch+1;
         \* a Joining byte past it is implicitly rejected.
         IF e > EpochOf(a.joinF) + 1
           THEN "ExpiredJoining"
           ELSE "Joining"
    [] a.status = "active" ->
         \* consensus.rs:371-386: deferred activation -- a confirmed join
         \* reads as Joining until the E+2 boundary.
         IF a.joinConfF > 0 /\ e < EpochOf(a.joinConfF) + 1
           THEN "Joining"
           \* consensus.rs:387-398: epoch re-confirm obligation.
           ELSE IF a.epoch >= e THEN "Active" ELSE "ExpiredEpoch"
    [] a.status = "paused" -> "Paused"
    [] a.status = "leaving" ->
         \* consensus.rs:401-424: confirmed leave departs at the
         \* deactivation boundary; unconfirmed leave expires past its
         \* confirm slot.
         IF a.leaveConfF > 0
           THEN IF e < EpochOf(a.leaveConfF) + 1
                  THEN "Leaving"
                  ELSE "ExpiredLeaving"
           ELSE IF e > EpochOf(a.leaveF) + 1
                  THEN "ExpiredLeaving"
                  ELSE "Leaving"
    [] a.status = "left" ->
         IF a.kicked THEN "Kicked" ELSE "Left"

EffStatus(p) == EffOf(alloc[p], frame)

\* Committee membership: effective Active plus Leaving (a departing
\* member serves notice and stays counted -- consensus.rs:404-409).
CommitteeStates == {"Active", "Leaving"}

Committee == {p \in Provers : EffStatus(p) \in CommitteeStates}

--------------------------------------------------------------------------

Init ==
  /\ frame = 1
  /\ alloc = [p \in Provers |-> NoAlloc]
  /\ mempool = [p \in Provers |-> NoOp]
  /\ joinsBlocked = FALSE

--------------------------------------------------------------------------
(* Prover-side: issue an op into the mempool (BlossomSub gossip publish,  *)
(* prover_pipeline.rs::submit_join and provers/actions.rs).  Confirm and  *)
(* reject are SELF-SIGNED by the joining prover -- there is no external   *)
(* vote (provers/lifecycle.rs decides off-chain).                          *)

\* Would the chain's rejoin gate accept a join for p right now?
\* verify.rs:614-659 (verify_prover_join_allocations_expired): an existing
\* allocation must be left (status 4) or have JoinFrameNumber at least
\* REJOIN_WINDOW frames old -- REGARDLESS of its current status byte.
RejoinGateOpen(p) ==
  \/ alloc[p].status \in {"none", "left"}
  \/ frame >= alloc[p].joinF + REJOIN_WINDOW

\* What an honest lifecycle would issue: only (re)join when it does not
\* own a live slot (EffectiveStatus::is_live -- consensus.rs:302-307).
HonestJoinIssue(p) ==
  \/ alloc[p].status \in {"none", "left"}
  \/ EffStatus(p) \in {"ExpiredJoining", "ExpiredLeaving"}

\* Under HONEST_ISSUE, confirms are only emitted while the proposal is
\* effectively pending (the lifecycle engine reads effective status, not
\* the raw byte); permissive mode issues anything byte-plausible.
Plausible(p, t) ==
  CASE t = "join"         -> IF HONEST_ISSUE THEN HonestJoinIssue(p)
                                             ELSE RejoinGateOpen(p)
    [] t = "confirmJoin"  -> /\ alloc[p].status = "joining"
                             /\ HONEST_ISSUE => EffStatus(p) = "Joining"
    [] t = "reject"       -> alloc[p].status \in {"joining", "leaving"}
    [] t = "leave"        -> alloc[p].status \in {"active", "paused"}
    [] t = "confirmLeave" -> /\ alloc[p].status = "leaving"
                             /\ HONEST_ISSUE => EffStatus(p) = "Leaving"
    [] t = "reconfirm"    -> alloc[p].status = "active"
    [] t = "pause"        -> alloc[p].status = "active"
    [] t = "resume"       -> alloc[p].status = "paused"

Issue(p, t) ==
  /\ mempool[p].type = "none"
  /\ Plausible(p, t)
  /\ mempool' = [mempool EXCEPT ![p] = [type |-> t, stamp |-> frame]]
  /\ UNCHANGED <<frame, alloc, joinsBlocked>>

\* Gossip loss, or the op aging out of the rank-buffer retention window
\* (message_collector.rs RETENTION_WINDOW).
DropOp(p) ==
  /\ mempool[p].type # "none"
  /\ mempool' = [mempool EXCEPT ![p] = NoOp]
  /\ UNCHANGED <<frame, alloc, joinsBlocked>>

--------------------------------------------------------------------------
(* Chain-side: the elected leader includes the op in the next finalized   *)
(* frame; guards are the verify.rs validation run at inclusion time and   *)
(* the effects are the materialize.rs writes.  Inclusion timing is        *)
(* nondeterministic = message/ordering delay.                              *)

ClearOp(p) == mempool' = [mempool EXCEPT ![p] = NoOp]

(* ProverJoin.  Guards:
     freshness   verify.rs:276   op.frame_number + 10 < current => reject
     halt/freeze intrinsic.rs:1109-1129 + halt_state.rs (joinsBlocked)
     not kicked  verify.rs:581-602
     rejoin gate verify.rs:614-659
   Effect: materialize.rs:499 creates a FRESH allocation vertex --
   status Joining, JoinFrameNumber = frame, everything else reset. *)
IncludeJoin(p) ==
  /\ mempool[p].type = "join"
  /\ mempool[p].stamp + FRESHNESS >= frame
  /\ ~joinsBlocked
  /\ ~alloc[p].kicked
  /\ RejoinGateOpen(p)
  /\ alloc' = [alloc EXCEPT ![p] =
       [status |-> "joining", joinF |-> frame, joinConfF |-> 0,
        leaveF |-> 0, leaveConfF |-> 0, epoch |-> 0, kicked |-> FALSE]]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

(* ProverConfirm on a Joining allocation.  Guard: exact-epoch confirm slot
   (verify.rs:777-797).  Effect: materialize.rs:228-234 -- Active byte,
   JoinConfirmFrameNumber, Epoch registered one ahead.  Committee entry is
   still deferred to the E+2 boundary by EffOf. *)
IncludeConfirmJoin(p) ==
  /\ mempool[p].type = "confirmJoin"
  /\ alloc[p].status = "joining"
  /\ CurEpoch = EpochOf(alloc[p].joinF) + 1
  /\ alloc' = [alloc EXCEPT ![p].status = "active",
                            ![p].joinConfF = frame,
                            ![p].epoch = CurEpoch + 1]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

(* ProverConfirm on an Active allocation: the per-epoch re-confirm.
   Guard: verify.rs:820-843 (not already registered ahead).  Effect:
   materialize.rs:236-245 (Epoch renewed one ahead; JoinConfirmFrameNumber
   untouched). *)
IncludeReconfirm(p) ==
  /\ mempool[p].type = "reconfirm"
  /\ alloc[p].status = "active"
  /\ alloc[p].epoch <= CurEpoch
  /\ alloc' = [alloc EXCEPT ![p].epoch = CurEpoch + 1]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

(* ProverConfirm on a Leaving allocation.  Guard: exact-epoch confirm slot
   for the leave (verify.rs:799-818).  Effect: materialize.rs:247-251 --
   only LeaveConfirmFrameNumber is written; the byte stays Leaving and the
   departure happens at the E+2 boundary via EffOf. *)
IncludeConfirmLeave(p) ==
  /\ mempool[p].type = "confirmLeave"
  /\ alloc[p].status = "leaving"
  /\ CurEpoch = EpochOf(alloc[p].leaveF) + 1
  /\ alloc' = [alloc EXCEPT ![p].leaveConfF = frame]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

(* ProverReject -- dual purpose (materialize.rs:266-301).  NOTE: Rust has
   no reject timing gate (verify.rs:852-883 checks only the signature);
   Go's 720-frame reject window has no Rust equivalent.  Divergence
   documented in the README. *)
IncludeReject(p) ==
  /\ mempool[p].type = "reject"
  /\ \/ /\ alloc[p].status = "joining"     \* reject join -> left
        /\ alloc' = [alloc EXCEPT ![p].status = "left"]
     \/ /\ alloc[p].status = "leaving"     \* reject leave -> back to Active
        /\ alloc' = [alloc EXCEPT ![p].status = "active"]
        \* materialize.rs:284-294: Epoch deliberately NOT bumped.
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

(* ProverLeave: from Active or Paused (materialize.rs:103-129).  NOTE:
   LeaveConfirmFrameNumber from a previous leave/confirm cycle is NOT
   cleared -- faithful to materialize_prover_leave, which writes only
   Status and LeaveFrameNumber. *)
IncludeLeave(p) ==
  /\ mempool[p].type = "leave"
  /\ alloc[p].status \in {"active", "paused"}
  /\ alloc' = [alloc EXCEPT ![p].status = "leaving", ![p].leaveF = frame]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

\* ProverPause: Active only (materialize.rs:41-70, verify.rs:60-110).
IncludePause(p) ==
  /\ mempool[p].type = "pause"
  /\ alloc[p].status = "active"
  /\ alloc' = [alloc EXCEPT ![p].status = "paused"]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

\* ProverResume: Paused only (materialize.rs:74-99).  NOTE: Rust has no
\* pause timeout (Go's 360-frame limit in global_prover_resume.go:429-436
\* has no Rust equivalent).  Divergence documented in the README.
IncludeResume(p) ==
  /\ mempool[p].type = "resume"
  /\ alloc[p].status = "paused"
  /\ alloc' = [alloc EXCEPT ![p].status = "active"]
  /\ ClearOp(p)
  /\ UNCHANGED <<frame, joinsBlocked>>

(* ProverKick / storage-audit eviction, collapsed into one action: any
   live allocation -> left, prover-level KickFrameNumber set (modeled as
   the `kicked` flag), which permanently bars rejoin via IncludeJoin's
   ~kicked guard (verify.rs:581-602).  Equivocation evidence itself is
   out of scope (no byzantine modeling). *)
Kick(p) ==
  /\ alloc[p].status \in {"joining", "active", "paused", "leaving"}
  /\ alloc' = [alloc EXCEPT ![p].status = "left", ![p].kicked = TRUE]
  /\ UNCHANGED <<frame, mempool, joinsBlocked>>

--------------------------------------------------------------------------
(* Environment: the abstract global consensus.                             *)

(* One more frame finalizes.  Contiguity and no-fork are inherent in the
   counter.  A stalled chain = this action not firing (stuttering).
   Ops older than FRESHNESS frames are garbage-collected: for joins this
   is the leader validating and dropping stale joins before proposing
   (leader_provider.rs prove_next_state, step 3); for every op type it is
   the rank-buffer retention window (message_collector.rs
   RETENTION_WINDOW = 10 ranks -- same magnitude as the join freshness
   window, so the model reuses FRESHNESS). *)
AdvanceFrame ==
  /\ frame < MAX_FRAME
  /\ frame' = frame + 1
  /\ mempool' = [q \in Provers |->
       IF mempool[q].type # "none" /\ mempool[q].stamp + FRESHNESS < frame + 1
         THEN NoOp
         ELSE mempool[q]]
  /\ UNCHANGED <<alloc, joinsBlocked>>

\* Coverage halt begins / shard split-merge freeze proposed
\* (halt_state.rs, intrinsic.rs:1109-1129).
SetJoinsBlocked ==
  /\ BLOCKING_ENABLED
  /\ ~joinsBlocked
  /\ joinsBlocked' = TRUE
  /\ UNCHANGED <<frame, alloc, mempool>>

\* Halt resumes / freeze lifts.
ClearJoinsBlocked ==
  /\ joinsBlocked
  /\ joinsBlocked' = FALSE
  /\ UNCHANGED <<frame, alloc, mempool>>

--------------------------------------------------------------------------

Next ==
  \/ AdvanceFrame
  \/ SetJoinsBlocked
  \/ ClearJoinsBlocked
  \/ \E p \in Provers :
       \/ \E t \in OpTypes : Issue(p, t)
       \/ DropOp(p)
       \/ IncludeJoin(p)
       \/ IncludeConfirmJoin(p)
       \/ IncludeReconfirm(p)
       \/ IncludeConfirmLeave(p)
       \/ IncludeReject(p)
       \/ IncludeLeave(p)
       \/ IncludePause(p)
       \/ IncludeResume(p)
       \/ Kick(p)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------
(* Safety invariants.                                                      *)

AllocRecs ==
  [status: StatusBytes, joinF: 0..MAX_FRAME, joinConfF: 0..MAX_FRAME,
   leaveF: 0..MAX_FRAME, leaveConfF: 0..MAX_FRAME, epoch: 0..(MaxEpoch + 1),
   kicked: BOOLEAN]

OpRecs == [type: OpTypes \union {"none"}, stamp: 0..MAX_FRAME]

TypeOK ==
  /\ frame \in 1..MAX_FRAME
  /\ alloc \in [Provers -> AllocRecs]
  /\ mempool \in [Provers -> OpRecs]
  /\ joinsBlocked \in BOOLEAN

\* verify.rs:766-850: a recorded confirm always sits in the epoch exactly
\* after its proposal.  The leave clause uses <= because a leave-reject /
\* re-leave cycle legitimately leaves a STALE LeaveConfirmFrameNumber from
\* an earlier cycle on the vertex (materialize_prover_leave does not clear
\* it) -- see the README findings section.
ConfirmTimingOK ==
  \A p \in Provers :
    /\ alloc[p].joinConfF > 0 =>
         EpochOf(alloc[p].joinConfF) = EpochOf(alloc[p].joinF) + 1
    /\ alloc[p].leaveConfF > 0 =>
         EpochOf(alloc[p].leaveConfF) <= EpochOf(alloc[p].leaveF) + 1

\* Deferred activation honored: an effective-Active prover was confirmed,
\* and its activation boundary has passed.
NoActiveWithoutConfirmedJoin ==
  \A p \in Provers :
    EffStatus(p) = "Active" =>
      /\ alloc[p].joinConfF > 0
      /\ CurEpoch >= EpochOf(alloc[p].joinConfF) + 1

\* The Active byte always carries a confirm record (status flips to
\* active only via IncludeConfirmJoin or a leave-reject on a previously
\* confirmed allocation).
ActiveByteHasConfirm ==
  \A p \in Provers :
    alloc[p].status = "active" => alloc[p].joinConfF > 0

\* A kicked prover is terminal-left and outside the committee.
KickedStaysOut ==
  \A p \in Provers :
    alloc[p].kicked => /\ alloc[p].status = "left"
                       /\ p \notin Committee

\* An allocation never registers more than one epoch ahead
\* (verify.rs:820-843 gate + materialize one-ahead writes).
ReconfirmSound ==
  \A p \in Provers : alloc[p].epoch <= CurEpoch + 1

--------------------------------------------------------------------------
(* Action properties (checked as PROPERTY -- safety over steps).           *)

\* The status-byte machine only moves along materialize.rs edges (plus
\* the whole-vertex rejoin reset, which can start from any non-kicked
\* status whose JoinFrameNumber has aged past the rejoin window).
LegalEdges ==
  { <<"none", "joining">>, <<"left", "joining">>,     \* join / rejoin
    <<"active", "joining">>, <<"paused", "joining">>, \* stale-rejoin reset
    <<"leaving", "joining">>,                          \* stale-rejoin reset
    <<"joining", "active">>,                           \* confirm join
    <<"joining", "left">>,                             \* reject join / kick
    <<"active", "paused">>,                            \* pause
    <<"paused", "active">>,                            \* resume
    <<"active", "leaving">>, <<"paused", "leaving">>,  \* leave
    <<"leaving", "active">>,                           \* reject leave
    <<"active", "left">>, <<"paused", "left">>,        \* kick
    <<"leaving", "left">> }                            \* kick

TransitionsLegal ==
  [][\A p \in Provers :
       alloc[p].status # alloc'[p].status =>
         <<alloc[p].status, alloc'[p].status>> \in LegalEdges
    ]_vars

\* Kick is irreversible.
KickMonotone ==
  [][\A p \in Provers : alloc[p].kicked => alloc'[p].kicked]_vars

(* THE committee-freeze theorem, full-lifecycle form: while the epoch
   clock does not tick, nobody ENTERS the committee from outside the
   live allocation set.  Mid-epoch entries can only come from states
   where the prover already owns a live slot: Paused (resume, or leave-
   from-paused), ExpiredEpoch (re-confirm recovery, or leave), Joining
   (leave of a confirmed-but-not-activated allocation), or
   ExpiredLeaving (see FINDING 1 below).  A prover that is
   None/Left/Kicked/ExpiredJoining can never appear in the committee
   without an epoch boundary.

   FINDING 1 (stale LeaveConfirmFrameNumber): materialize_prover_leave
   (materialize.rs:103-129) does not clear LeaveConfirmFrameNumber, and
   Rust has no reject timing gate.  After a
   leave -> confirmLeave -> rejectLeave -> leave cycle, the fresh
   Leaving allocation reads the STALE LeaveConfirmFrameNumber and is
   instantly ExpiredLeaving (wrongly departed); the new confirmLeave
   (or a late rejectLeave) then flips it BACK into the committee
   mid-epoch.  Removing "ExpiredLeaving" from the allowed set below
   reproduces the TLC counterexample. *)
CommitteeFreeze ==
  [][ EpochOf(frame') = EpochOf(frame) =>
        \A p \in Provers :
          ( /\ EffOf(alloc'[p], frame') \in CommitteeStates
            /\ EffOf(alloc[p], frame) \notin CommitteeStates )
          => EffOf(alloc[p], frame)
               \in {"Joining", "Paused", "ExpiredEpoch", "ExpiredLeaving"}
    ]_vars

--------------------------------------------------------------------------
(* INTENDED-DESIGN properties.  These state what the epoch-aligned        *)
(* lifecycle is DOCUMENTED to guarantee (materialize.rs doc comments,     *)
(* consensus.rs doc comments).  TLC REFUTES each of them against the      *)
(* faithful model -- every counterexample is a candidate logic bug in     *)
(* the implementation, catalogued in the README findings section.         *)
(* `./check.sh findings` runs them and EXPECTS violations.                *)

\* INTENDED: a recorded leave-confirm sits exactly one epoch after its
\* leave.  REFUTED by the stale LeaveConfirmFrameNumber left behind by a
\* leave -> confirm -> reject -> re-leave cycle (FINDING 1).
StrongLeaveConfirmTiming ==
  \A p \in Provers :
    alloc[p].leaveConfF > 0 =>
      EpochOf(alloc[p].leaveConfF) = EpochOf(alloc[p].leaveF) + 1

\* INTENDED: mid-epoch committee entry only from live non-committee
\* states (resume, re-confirm recovery, leave-of-pending).  REFUTED:
\* an ExpiredLeaving (departed) allocation re-enters mid-epoch via a
\* fresh confirmLeave over a stale expiry, or a late rejectLeave
\* (FINDING 1 + DIVERGENCE D2).
StrongCommitteeFreeze ==
  [][ EpochOf(frame') = EpochOf(frame) =>
        \A p \in Provers :
          ( /\ EffOf(alloc'[p], frame') \in CommitteeStates
            /\ EffOf(alloc[p], frame) \notin CommitteeStates )
          => EffOf(alloc[p], frame) \in {"Joining", "Paused", "ExpiredEpoch"}
    ]_vars

\* INTENDED: a departing member serves notice through the epoch -- a
\* committee member never flips to ExpiredLeaving without an epoch
\* boundary.  REFUTED: IncludeLeave over a stale LeaveConfirmFrameNumber
\* departs the member INSTANTLY, mid-epoch (FINDING 1, removal side).
NoInstantDeparture ==
  [][ EpochOf(frame') = EpochOf(frame) =>
        \A p \in Provers :
          ~( /\ EffOf(alloc[p], frame) \in CommitteeStates
             /\ EffOf(alloc'[p], frame') = "ExpiredLeaving" )
    ]_vars

\* INTENDED (Go semantics, global_prover_join.go:1023-1066): a rejoin
\* may only overwrite an allocation that is left or EXPIRED.  REFUTED:
\* Rust's rejoin gate (verify.rs:614-659) only checks JoinFrameNumber
\* age, so a live Active/Paused/Leaving allocation older than
\* REJOIN_WINDOW is silently overwritten by a fresh join
\* (DIVERGENCE D3).
NoRejoinOverLiveAllocation ==
  [][ \A p \in Provers :
        ( /\ alloc'[p].status = "joining"
          /\ alloc'[p].joinF = frame
          /\ alloc[p].status # alloc'[p].status )
        => \/ alloc[p].status \in {"none", "left"}
           \/ EffOf(alloc[p], frame) \in {"ExpiredJoining", "ExpiredLeaving"}
    ]_vars

\* INTENDED: once a leave has implicitly completed (ExpiredLeaving),
\* the departure is final -- the allocation can only be re-entered by a
\* fresh join.  REFUTED: Rust has no reject timing gate
\* (verify.rs:852-883), so an arbitrarily late ProverReject on the
\* long-departed Leaving byte resurrects it to Active (DIVERGENCE D2).
NoLateLeaveReject ==
  [][ \A p \in Provers :
        ( /\ alloc[p].status = "leaving"
          /\ EffOf(alloc[p], frame) = "ExpiredLeaving" )
        => alloc'[p].status \in {"leaving", "left", "joining"}
    ]_vars

--------------------------------------------------------------------------
(* Refinement: this spec implements MembershipAbstract under the mapping  *)
(* below.  The abstract state is a pure function of the concrete state,   *)
(* so TLC checks Abs!Spec directly as a temporal property                 *)
(* (spec-as-property; the abstract spec is fairness-free).                *)

AbsMem(p) ==
  LET e == EffStatus(p) IN
  CASE e = "None"           -> "out"
    [] e = "Left"           -> "out"
    [] e = "ExpiredJoining" -> "out"
    [] e = "ExpiredLeaving" -> "out"
    [] e = "Joining"        -> "pending"   \* incl. confirmed-not-activated
    [] e = "Active"         -> "member"
    [] e = "Paused"         -> "suspended"
    [] e = "ExpiredEpoch"   -> "degraded"
    [] e = "Leaving"        -> "departing"
    [] e = "Kicked"         -> "barred"

Abs == INSTANCE MembershipAbstract
         WITH epoch <- CurEpoch,
              mem   <- [p \in Provers |-> AbsMem(p)]

RefinesAbstract == Abs!Spec

==========================================================================
