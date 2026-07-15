---- MODULE MC_Abstract ----
(* Sanity model for the abstract membership spec.  Epoch is unbounded in
   the spec; the CONSTRAINT in MC_Abstract.cfg bounds exploration (safety
   properties only, so a state constraint is sound here). *)
EXTENDS MembershipAbstract

EpochBound == epoch <= 5
====
