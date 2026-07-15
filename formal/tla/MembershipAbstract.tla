-------------------------- MODULE MembershipAbstract --------------------------
(***************************************************************************)
(* Abstract specification of prover committee membership.                 *)
(*                                                                         *)
(* The single load-bearing idea of the epoch-aligned prover lifecycle     *)
(* (crates/quil-types/src/consensus.rs): committee membership is (mostly) *)
(* a step function of the epoch clock.  A prover's abstract state is one  *)
(* of:                                                                     *)
(*                                                                         *)
(*   out        -- no live allocation (never joined / rejected / expired / *)
(*                 departed)                                               *)
(*   pending    -- join in flight: Joining byte, or confirmed but not yet  *)
(*                 activated (deferred activation, consensus.rs:371-386)   *)
(*   member     -- effective Active: in the committee                      *)
(*   suspended  -- Paused                                                  *)
(*   degraded   -- ExpiredEpoch: Active byte, missed the epoch re-confirm; *)
(*                 out of the committee but recoverable (consensus.rs:387) *)
(*   departing  -- Leaving: serving notice, still in the committee         *)
(*                 (consensus.rs:401-424)                                  *)
(*   barred     -- kicked: terminal, can never rejoin                      *)
(*                                                                         *)
(* Committee == member + departing.                                        *)
(*                                                                         *)
(* Mid-epoch (MidEpoch action), membership can change only in restricted  *)
(* ways -- crucially, a "pending" join NEVER becomes a member mid-epoch:  *)
(* new joiners are activated only when the epoch clock ticks (Tick        *)
(* action), which models a finalized global frame crossing an epoch       *)
(* boundary.  Tick simultaneously applies the read-side overlay           *)
(* flips of consensus.rs::effective_status: pending activates or expires, *)
(* members that missed their re-confirm degrade, departing members leave. *)
(*                                                                         *)
(* This spec is deliberately fairness-free: it is a pure safety-level      *)
(* description, and the concrete spec (ProverJoin.tla) is checked to       *)
(* refine it via TLC's spec-as-property mechanism.                         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANT Provers

VARIABLES
  epoch,  \* the epoch clock (Nat)
  mem     \* [Provers -> abstract membership state]

vars == <<epoch, mem>>

States == {"out", "pending", "member", "suspended", "degraded",
           "departing", "barred"}

CommitteeStates == {"member", "departing"}

Committee == {p \in Provers : mem[p] \in CommitteeStates}

TypeOK ==
  /\ epoch \in Nat
  /\ mem \in [Provers -> States]

Init ==
  /\ epoch = 0
  /\ mem = [p \in Provers |-> "out"]

(***************************************************************************)
(* Mid-epoch transitions.  Each pair <<from, to>> is justified by a        *)
(* concrete protocol action (see ProverJoin.tla and the README             *)
(* correspondence table):                                                  *)
(*                                                                         *)
(*   X -> pending      a (re)join is included on chain.  X ranges over     *)
(*                     every non-barred state because Rust's rejoin gate   *)
(*                     (verify.rs::verify_prover_join_allocations_expired) *)
(*                     only checks JoinFrameNumber age, so even an Active  *)
(*                     or Paused allocation older than the rejoin window   *)
(*                     can be overwritten by a fresh join.                 *)
(*   pending -> out    join rejected (self-signed ProverReject)            *)
(*   pending -> suspended  pause of a confirmed-but-not-activated alloc    *)
(*   pending -> departing  leave of a confirmed-but-not-activated alloc    *)
(*   member/suspended/degraded -> departing   ProverLeave                  *)
(*   member/degraded -> suspended             ProverPause                  *)
(*   suspended -> member/degraded             ProverResume (degraded when  *)
(*                                            the epoch registration is    *)
(*                                            stale)                       *)
(*   degraded -> member                       epoch re-confirm recovery    *)
(*   departing -> member/degraded/pending     leave-reject (back to the    *)
(*                                            Active byte; pending if the  *)
(*                                            alloc never activated)       *)
(*   any non-barred -> barred                 ProverKick / audit eviction  *)
(***************************************************************************)
MidEpochTrans ==
  { <<"out", "pending">>,
    <<"member", "pending">>, <<"suspended", "pending">>,
    <<"degraded", "pending">>, <<"departing", "pending">>,
    \* out -> departing/member/degraded: an ExpiredLeaving allocation
    \* (abstractly "out" -- the prover has departed) re-enters via a
    \* late confirmLeave or a late rejectLeave -- the stale-
    \* LeaveConfirmFrameNumber quirk (README FINDING 1) plus the absence
    \* of a reject timing gate in Rust (README FINDING 2 / D2).  The
    \* rejectLeave resurrection lands on member when the epoch
    \* registration is still fresh, else on degraded.
    <<"out", "departing">>, <<"out", "member">>, <<"out", "degraded">>,
    \* member/suspended/degraded -> out: INSTANT departure -- a re-leave
    \* over a stale LeaveConfirmFrameNumber reads as already-expired the
    \* moment it lands, dropping the prover mid-epoch instead of letting
    \* it serve notice (README FINDING 1, removal side).
    <<"member", "out">>, <<"suspended", "out">>, <<"degraded", "out">>,
    <<"pending", "out">>,
    <<"pending", "suspended">>,
    <<"pending", "departing">>,
    <<"member", "departing">>, <<"suspended", "departing">>,
    <<"degraded", "departing">>,
    <<"member", "suspended">>, <<"degraded", "suspended">>,
    <<"suspended", "member">>, <<"suspended", "degraded">>,
    <<"degraded", "member">>,
    <<"departing", "member">>, <<"departing", "degraded">> }

MidEpoch(p) ==
  /\ UNCHANGED epoch
  /\ \E t \in States :
       /\ \/ <<mem[p], t>> \in MidEpochTrans
          \/ /\ mem[p] # "barred"   \* kick
             /\ t = "barred"
       /\ mem' = [mem EXCEPT ![p] = t]

(***************************************************************************)
(* Epoch-boundary overlay flips (consensus.rs::effective_status).  These   *)
(* are pure read-side re-interpretations of unchanged on-chain state:      *)
(*                                                                         *)
(*   pending -> member     deferred activation at E+2                      *)
(*   pending -> out        ExpiredJoining (confirm epoch missed)           *)
(*   member -> degraded    ExpiredEpoch (missed epoch re-confirm)          *)
(*   departing -> out      departure at E+2 / ExpiredLeaving               *)
(***************************************************************************)
TickTrans ==
  { <<"pending", "member">>,
    <<"pending", "out">>,
    <<"member", "degraded">>,
    <<"departing", "out">> }

Tick ==
  /\ epoch' = epoch + 1
  /\ mem' \in [Provers -> States]
  /\ \A p \in Provers :
       \/ mem'[p] = mem[p]
       \/ <<mem[p], mem'[p]>> \in TickTrans

Next ==
  \/ Tick
  \/ \E p \in Provers : MidEpoch(p)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------
(* Properties of the abstract spec itself (checked by MC_Abstract).       *)

\* A kicked prover can never come back.
BarredTerminal ==
  [][\A p \in Provers : mem[p] = "barred" => mem'[p] = "barred"]_vars

\* THE deferred-activation theorem: a pending join NEVER becomes a
\* member without an epoch tick (activation happens only at the E+2
\* boundary).  Note the theorem is deliberately about "pending", not
\* about all non-members: an "out" prover CAN flicker back to
\* member/departing mid-epoch through the stale-LeaveConfirmFrameNumber
\* quirk + missing reject timing gate (README FINDING 1 / D2).
NoEarlyActivationMidEpoch ==
  [][ epoch' = epoch =>
        \A p \in Provers :
          ~(mem[p] = "pending" /\ mem'[p] = "member")
    ]_vars

\* "pending" never counts toward the committee.
PendingNotInCommittee ==
  \A p \in Provers : mem[p] = "pending" => p \notin Committee

==========================================================================
