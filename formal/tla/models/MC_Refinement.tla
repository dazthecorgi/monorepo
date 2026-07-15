---- MODULE MC_Refinement ----
(* Refinement check: ProverJoin refines MembershipAbstract under the
   AbsMem mapping (spec-as-property).  One prover suffices: the mapping
   and every lifecycle transition are per-prover, and provers do not
   interact (joinsBlocked is global but symmetric), so a single prover
   exercises every abstract transition.  Chain-permissive issuing
   (HONEST_ISSUE FALSE) so the stale-rejoin and finding paths are
   covered by the refinement too. *)
EXTENDS ProverJoin
====
