---- MODULE MC_Safety ----
(* Safety model: chain-permissive issuing (HONEST_ISSUE FALSE) so the
   permissive rejoin gate and every chain-side guard is exercised.
   Constants are the mainnet values scaled ~240x down; the frame bound
   is enforced in AdvanceFrame's guard (not a CONSTRAINT), so the model
   is also sound for the action properties.

   ONE prover: the model has no cross-prover coupling -- every action
   reads and writes a single prover's allocation and mempool slot, and
   the shared state (frame, joinsBlocked) affects all provers
   symmetrically -- and every checked property is a per-prover \A.  A
   1-prover model therefore checks exactly the same properties as an
   N-prover one, whose state space is just the N-fold product of
   independent components.  Dropping the second prover buys the full
   MAX_FRAME = 13 horizon (4.3 epochs: join -> confirm -> activate ->
   leave -> confirm-leave -> departure, plus every expiry/rejoin path,
   with frames to spare). *)
EXTENDS ProverJoin
====
