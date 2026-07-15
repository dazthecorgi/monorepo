---- MODULE MC_Liveness ----
(* Liveness under fairness.  The safety spec deliberately allows frames
   to stall forever (stuttering); here we add the fairness needed to
   state "things resolve":

     WF(AdvanceFrame)          -- frames stall only FINITELY (HotStuff
                                  pacemaker liveness after GST)
     SF(Issue join/confirm)    -- the lifecycle engine retries every
                                  frame tick (SF: the one-slot mempool
                                  makes the guard intermittent)
     SF(Include join/confirm/  -- an op that is repeatedly includable is
        reject)                  eventually included (SF because the
                                  freshness/epoch windows make the guard
                                  intermittent)
     WF(DropOp)                -- rank-buffer retention guarantees every
                                  unincluded op eventually ages out; the
                                  MAX_FRAME cap disables the AdvanceFrame
                                  GC stand-in, so DropOp carries that
                                  guarantee at the horizon

   HONEST_ISSUE = TRUE (the prover only issues joins its lifecycle
   would); BLOCKING_ENABLED = FALSE (a permanently-halted shard trivially
   blocks all admission liveness).

   Horizon caveat: MAX_FRAME bounds the frame counter, so leads-to
   properties are guarded by "enough frames remain before the cap"
   antecedents; without the guard they would fail spuriously at the
   bound. *)
EXTENDS ProverJoin

Fairness ==
  /\ WF_vars(AdvanceFrame)
  /\ \A p \in Provers :
       /\ SF_vars(Issue(p, "join"))
       /\ SF_vars(Issue(p, "confirmJoin"))
       /\ SF_vars(IncludeJoin(p))
       /\ SF_vars(IncludeConfirmJoin(p))
       /\ SF_vars(IncludeReject(p))
       /\ WF_vars(DropOp(p))

LiveSpec == Spec /\ Fairness

(* Every join resolves: an effective-Joining allocation (pending join,
   confirmed or not) always leaves the Joining state -- it activates at
   the E+2 boundary, expires (stalled confirm), or is superseded
   (reject/pause/leave/kick).  Needs only WF(AdvanceFrame): expiry is
   the escape hatch, no cooperation required.  This is the "stalled
   frames cause clean expiry, not wedging" theorem. *)
JoinResolves ==
  \A p \in Provers :
    ( /\ EffStatus(p) = "Joining"
      /\ frame + 2 * EPOCH_LENGTH <= MAX_FRAME )
    ~> (EffStatus(p) # "Joining")

(* An expired join is not a dead end: unless the prover is kicked or
   the join is superseded, the prover eventually has a fresh join
   recorded (the rejoin window is not a deadlock). *)
RejoinPossible ==
  \A p \in Provers :
    ( /\ EffStatus(p) = "ExpiredJoining"
      /\ frame + 2 <= MAX_FRAME )
    ~> (EffStatus(p) \in {"Joining", "Kicked", "Left"})
====
