#!/usr/bin/env bash
# TLC model-check runner for the prover join TLA+ specs.
#
# Usage: ./check.sh [abstract|safety|refinement|liveness|findings|all]
#        (default: all).  `findings` checks the INTENDED-DESIGN properties
#        and EXPECTS TLC to refute each one -- every counterexample
#        demonstrates a catalogued implementation quirk/bug (see README
#        findings section).  `all` includes findings.
#
# Downloads and caches tla2tools.jar on first run (pinned release + sha256).
# Requires Java 11+. Docker fallback if Java is unavailable:
#   docker run --rm -v "$PWD":/spec -w /spec eclipse-temurin:21-jre \
#     bash -c "./check.sh all"
set -euo pipefail

cd "$(dirname "$0")"

TOOLS_DIR=".tools"
JAR="$TOOLS_DIR/tla2tools.jar"
JAR_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"
JAR_SHA256="58d44845a37a8d776deaf8cf3a623213b59d311bc0ec287bcdfbe148dd11bb3d"

fetch_jar() {
  mkdir -p "$TOOLS_DIR"
  if [[ ! -f "$JAR" ]] || ! echo "$JAR_SHA256  $JAR" | sha256sum -c --quiet - 2>/dev/null; then
    echo "Fetching tla2tools.jar (TLA+ tools v1.8.0)..."
    curl -sL -o "$JAR" "$JAR_URL"
    echo "$JAR_SHA256  $JAR" | sha256sum -c --quiet -
  fi
}

run_tlc() {
  local name="$1"
  echo "=== TLC: $name ==="
  # -workers auto: use all cores. Deadlock checking is disabled in the
  # .cfg files (CHECK_DEADLOCK FALSE) because frame/epoch bounds make
  # terminal stuttering expected.
  java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
    -workers auto -cleanup \
    -config "models/MC_${name}.cfg" "models/MC_${name}.tla"
  echo ""
}

fetch_jar

# Intended-design properties: NAME:KIND where KIND is INVARIANT or PROPERTY.
FINDINGS=(
  "StrongLeaveConfirmTiming:INVARIANT"
  "StrongCommitteeFreeze:PROPERTY"
  "NoInstantDeparture:PROPERTY"
  "NoRejoinOverLiveAllocation:PROPERTY"
  "NoLateLeaveReject:PROPERTY"
)

run_findings() {
  local failed=0
  for entry in "${FINDINGS[@]}"; do
    local prop="${entry%%:*}" kind="${entry##*:}"
    echo "=== TLC (expect refutation): $prop ==="
    cat > models/MC_Findings.cfg <<EOF
SPECIFICATION Spec
CONSTANTS
  Provers = {p1}
  EPOCH_LENGTH = 3
  FRESHNESS = 2
  REJOIN_WINDOW = 3
  MAX_FRAME = 13
  BLOCKING_ENABLED = TRUE
  HONEST_ISSUE = FALSE
${kind} ${prop}
CHECK_DEADLOCK FALSE
EOF
    if java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
         -workers auto -cleanup \
         -config models/MC_Findings.cfg models/MC_Findings.tla \
         > ".findings_${prop}.out" 2>&1; then
      echo "UNEXPECTED: $prop was NOT refuted (the underlying quirk may be fixed" \
           "-- update the README findings section and promote it to MC_Safety)"
      failed=1
    else
      grep -m1 -E "Error: (Action property|Invariant|Temporal propert)" \
        ".findings_${prop}.out" || true
      echo "refuted as expected (trace in .findings_${prop}.out)"
    fi
    echo ""
  done
  return "$failed"
}

TARGET="${1:-all}"
case "$TARGET" in
  abstract)   run_tlc Abstract ;;
  safety)     run_tlc Safety ;;
  refinement) run_tlc Refinement ;;
  liveness)   run_tlc Liveness ;;
  findings)   run_findings ;;
  all)
    run_tlc Abstract
    run_tlc Safety
    run_tlc Refinement
    run_tlc Liveness
    run_findings
    ;;
  *)
    echo "unknown target: $TARGET (expected abstract|safety|refinement|liveness|findings|all)" >&2
    exit 1
    ;;
esac
echo "All requested model checks passed."
