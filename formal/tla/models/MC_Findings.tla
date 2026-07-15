---- MODULE MC_Findings ----
(* Harness for the intended-design properties.  check.sh generates
   MC_Findings.cfg per property and EXPECTS TLC to refute it; each
   counterexample demonstrates a catalogued implementation quirk (see
   the README findings section).  MC_Findings.cfg is generated -- do
   not edit it by hand. *)
EXTENDS ProverJoin
====
