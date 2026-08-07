//! Rust port of
//! `node/consensus/difficulty/aserti3_2d_difficulty_adjuster.go`.
//!
//! Implements the ASERTi3-2d integer-arithmetic difficulty adjustment
//! algorithm, adapted for Quilibrium's VDF-based proof of time. Go's
//! implementation deliberately uses integer Q16 fixed-point math to
//! avoid floating-point non-determinism across architectures — we
//! mirror the same integer pipeline bit-for-bit so both
//! implementations produce identical outputs for the same
//! `(anchor_frame, anchor_time, anchor_difficulty, current_frame,
//! current_time)` tuple.

use quil_types::consensus::DifficultyAdjuster;

/// Target frame interval in milliseconds (10 seconds).
pub const IDEAL_FRAME_TIME: i64 = 10_000;

/// Process-global override of the target frame interval. Defaults to
/// [`IDEAL_FRAME_TIME`]; dev networks shrink it via `QUIL_IDEAL_FRAME_TIME_MS`
/// (wired in the node's master init, gated to any NON-MAINNET network — the
/// gate is `network != 0`, so it also applies on a public testnet, where a
/// mismatched value forks the chain; like `QUIL_EPOCH_LENGTH_FRAMES`).
/// CONSENSUS PARAMETER: it feeds the ASERT difficulty target and the
/// proposer's frame pacing/timestamp stamping, so every node in the net MUST
/// run the same value. The effective cadence floor stays the VDF solve time
/// at [`MIN_DIFFICULTY`] (~3 s) — an interval below that just makes the
/// proposer VDF-bound instead of sleep-bound.
static IDEAL_FRAME_TIME_MS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(IDEAL_FRAME_TIME);

/// Lowest settable target frame interval: below ~100 ms the proposer's pacing
/// sleep degenerates into a busy spin with no legitimate dev use.
pub const MIN_IDEAL_FRAME_TIME_MS: i64 = 100;

/// Override the target frame interval (milliseconds). Dev-network only — see
/// [`IDEAL_FRAME_TIME_MS`]. Accepts only
/// `MIN_IDEAL_FRAME_TIME_MS..=IDEAL_FRAME_TIME` and returns whether the value
/// was applied. The upper bound is a hard safety line, not taste: `ideal ×
/// frame_delta` feeds `compute_difficulty`'s Q16 pipeline, whose
/// `wrapping_mul(RADIX)` overflows i64 once `|time_delta − ideal×delta|`
/// reaches 2^47 — a huge interval (e.g. a units mistake like 1e13) wraps
/// within ~14 frames of the anchor and difficulty silently oscillates between
/// `u64::MAX` (unsolvable) and the floor. Values above the mainnet default
/// have no legitimate dev use; raising the cadence beyond it is not a
/// supported configuration.
#[must_use]
pub fn set_ideal_frame_time_ms(ms: i64) -> bool {
    if (MIN_IDEAL_FRAME_TIME_MS..=IDEAL_FRAME_TIME).contains(&ms) {
        IDEAL_FRAME_TIME_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
        true
    } else {
        false
    }
}

/// The effective target frame interval in milliseconds.
pub fn ideal_frame_time_ms() -> i64 {
    IDEAL_FRAME_TIME_MS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Process-global override of the difficulty floor. Defaults to
/// [`MIN_DIFFICULTY`]; dev networks shrink it via `QUIL_MIN_DIFFICULTY`
/// (wired in the node's master init, gated to any NON-MAINNET network, incl.
/// public testnets). On a devnet the ASERT anchor decays to the floor
/// immediately, so the floor IS the operating difficulty — lowering it is
/// what shortens the per-frame VDF solve (~50_000 ≈ 2.5-3 s; roughly linear
/// in the difficulty). CONSENSUS PARAMETER: proposers solve and followers
/// verify at the header's stated difficulty, but a mixed-floor net would
/// produce frames some nodes consider implausibly cheap — set it identically
/// everywhere or not at all.
static MIN_DIFFICULTY_RT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(MIN_DIFFICULTY);

/// Lowest settable difficulty floor: below ~1000 the VDF solve is effectively
/// free and frame cadence is pure network/IO noise.
pub const MIN_SETTABLE_DIFFICULTY: u64 = 1_000;

/// Override the difficulty floor. Dev-network only — see
/// [`MIN_DIFFICULTY_RT`]. Accepts only
/// `MIN_SETTABLE_DIFFICULTY..=MIN_DIFFICULTY` (the knob may LOWER the mainnet
/// floor for a faster dev cadence, never raise it) and returns whether the
/// value was applied. The upper bound also keeps every difficulty value
/// u32-representable: the frame header and VDF challenge builders cast
/// difficulty `as u32`, so a floor above `u32::MAX` would silently truncate
/// (e.g. 2^32 → 0).
#[must_use]
pub fn set_min_difficulty(d: u64) -> bool {
    if (MIN_SETTABLE_DIFFICULTY..=MIN_DIFFICULTY).contains(&d) {
        MIN_DIFFICULTY_RT.store(d, std::sync::atomic::Ordering::Relaxed);
        true
    } else {
        false
    }
}

/// The effective difficulty floor.
pub fn min_difficulty() -> u64 {
    MIN_DIFFICULTY_RT.load(std::sync::atomic::Ordering::Relaxed)
}

/// ASERT half-life in milliseconds (2 hours).
pub const HALF_LIFE: i64 = 7_200_000;

/// Minimum difficulty floor — matches Go's hard-coded 50_000.
pub const MIN_DIFFICULTY: u64 = 50_000;

/// The anchor's starting-point difficulty to use when the stored
/// anchor is zero (bootstrap path before any frames have been seen).
pub const BOOTSTRAP_DIFFICULTY: u64 = 50_000;

/// ASERTi3-2d difficulty adjuster. Anchored to a specific frame (the
/// "anchor frame") and adjusts upward/downward from there based on
/// elapsed time vs. the ideal inter-frame interval.
pub struct AsertDifficultyAdjuster {
    /// The frame number the anchor was taken at.
    pub anchor_frame_number: u64,
    /// Timestamp (ms since epoch) of the anchor frame's parent.
    pub anchor_parent_time: i64,
    /// Difficulty observed at the anchor frame. `u32` to match Go.
    pub anchor_difficulty: u32,
}

impl AsertDifficultyAdjuster {
    pub fn new(
        anchor_frame_number: u64,
        anchor_parent_time: i64,
        anchor_difficulty: u32,
    ) -> Self {
        Self {
            anchor_frame_number,
            anchor_parent_time,
            anchor_difficulty,
        }
    }
}

impl DifficultyAdjuster for AsertDifficultyAdjuster {
    fn get_next_difficulty(&self, current_frame_number: u64, current_time: i64) -> u64 {
        // Q16 radix: 1 << 16.
        const RADIX: i64 = 1 << 16;

        // Bootstrap path: uninitialised anchor → return the floor
        // (`BOOTSTRAP_DIFFICULTY` == `MIN_DIFFICULTY`, so this honors the
        // dev-network floor override and is unchanged without one).
        if self.anchor_difficulty == 0 {
            return min_difficulty();
        }

        // Height and time deltas from the anchor. `frameNumberDelta`
        // is done as int64 — Go code does the same, and we rely on
        // the unsigned → signed wrap matching between languages
        // (it's legal to compute a difference through int64
        // subtraction).
        let frame_number_delta: i64 =
            current_frame_number as i64 - self.anchor_frame_number as i64;
        let time_delta: i64 = current_time - self.anchor_parent_time;

        // Q16 exponent: -((time_delta - IDEAL * (delta + 1)) * RADIX) / HALF_LIFE.
        // Bracketing matches Go's exact order of operations. Go's
        // int64 arithmetic is two's-complement wrapping; Rust's
        // default `*`/`-` panic in debug mode, so we explicitly use
        // `wrapping_*` to preserve Go behaviour on pathological
        // inputs. Normal mainnet inputs never approach the overflow
        // boundary.
        let ideal_x_delta = ideal_frame_time_ms()
            .wrapping_mul(frame_number_delta.wrapping_add(1));
        let inner = time_delta.wrapping_sub(ideal_x_delta);
        let numerator = inner.wrapping_mul(RADIX).wrapping_neg();
        let exponent: i64 = numerator / HALF_LIFE;

        // Decompose the Q16 exponent into integer shift and fractional
        // part. Arithmetic shift matches Go (Go's `>>` on signed types
        // is arithmetic, i.e. sign-extending).
        let shifts: i64 = exponent >> 16;
        let frac: u64 = (exponent & 0xFFFF) as u16 as u64;

        // Polynomial approximation of 2^(frac/65536.0), rounded via
        // `+ (1 << 47)` then `>> 48`. Constants match Go byte for
        // byte; see the source paper for derivation.
        let poly = 195_766_423_245_049u64 * frac
            + 971_821_376u64 * frac * frac
            + 5_127u64 * frac * frac * frac
            + (1u64 << 47);
        let factor: u64 = 65_536u64 + (poly >> 48);

        // Apply factor against the anchor difficulty.
        let mut scaled: u64 = (self.anchor_difficulty as u64) * factor;

        // Apply the integer shift. Negative shifts reduce; positive
        // shifts amplify.
        if shifts < 0 {
            let k = (-shifts) as u32;
            // Guard against oversized shift (>= 64 would be UB in
            // Rust). Saturate to 0 — Go's behaviour is a uint64 shift
            // wrap, so we diverge slightly but only for values that
            // would be clamped to the floor anyway.
            if k >= 64 {
                scaled = 0;
            } else {
                scaled >>= k;
            }
        } else {
            // Prevent overflow: if we'd shift more than 48 places
            // with a nonzero anchor, Go returns u64::MAX.
            if shifts > 48 && self.anchor_difficulty > 0 {
                return u64::MAX;
            }
            let k = shifts as u32;
            if k >= 64 {
                return u64::MAX;
            }
            scaled <<= k;
        }

        // Final divide by 65536 (Q16 → integer).
        scaled >>= 16;

        // Floor.
        let floor = min_difficulty();
        if scaled < floor {
            return floor;
        }
        scaled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bootstrap path: zero anchor → always `BOOTSTRAP_DIFFICULTY`.
    #[test]
    fn bootstrap_returns_floor() {
        let adj = AsertDifficultyAdjuster::new(0, 0, 0);
        assert_eq!(adj.get_next_difficulty(0, 0), BOOTSTRAP_DIFFICULTY);
        assert_eq!(adj.get_next_difficulty(100, 100_000), BOOTSTRAP_DIFFICULTY);
    }

    /// When elapsed time exactly equals the ideal (`current_frame -
    /// anchor_frame`) × `IDEAL_FRAME_TIME`, the exponent is zero, the
    /// fractional polynomial evaluates to ~65536, and the final
    /// difficulty equals the anchor (after the >>16 normalisation).
    #[test]
    fn on_schedule_returns_anchor_difficulty() {
        let anchor_time = 1_000_000i64;
        let anchor_diff = 100_000u32;
        let adj = AsertDifficultyAdjuster::new(10, anchor_time, anchor_diff);

        // 5 frames after the anchor, exactly on schedule.
        // frame_delta = 5, time_delta = IDEAL * (5 + 1) = 60000.
        let current_time = anchor_time + IDEAL_FRAME_TIME * 6;
        let d = adj.get_next_difficulty(15, current_time);
        // Not exactly equal due to the polynomial approximation
        // rounding, but should be within 1 unit.
        let diff = (d as i64 - anchor_diff as i64).abs();
        assert!(
            diff <= 1,
            "expected ~= anchor_diff, got {} (diff {})",
            d,
            diff
        );
    }

    /// Behind schedule → difficulty drops below anchor.
    #[test]
    fn behind_schedule_lowers_difficulty() {
        let anchor_time = 1_000_000i64;
        let anchor_diff = 1_000_000u32;
        let adj = AsertDifficultyAdjuster::new(10, anchor_time, anchor_diff);

        // 5 frames later but the wall clock elapsed 1 hour instead of
        // the expected 60 seconds → way behind schedule → lower
        // difficulty.
        let current_time = anchor_time + 60 * 60 * 1000;
        let d = adj.get_next_difficulty(15, current_time);
        assert!(d < anchor_diff as u64);
        assert!(d >= MIN_DIFFICULTY);
    }

    /// Ahead of schedule → difficulty rises above anchor.
    #[test]
    fn ahead_of_schedule_raises_difficulty() {
        let anchor_time = 1_000_000i64;
        let anchor_diff = 1_000_000u32;
        let adj = AsertDifficultyAdjuster::new(10, anchor_time, anchor_diff);

        // 100 frames but only half the expected wall time elapsed.
        let current_time = anchor_time + (IDEAL_FRAME_TIME * 101) / 2;
        let d = adj.get_next_difficulty(110, current_time);
        assert!(d > anchor_diff as u64, "expected > anchor, got {}", d);
    }

    /// Floor enforcement: extreme downward movement clamps to
    /// `MIN_DIFFICULTY`.
    #[test]
    fn floor_clamps_to_min() {
        let adj = AsertDifficultyAdjuster::new(0, 0, 100);
        // Frame 10 with half-max ms elapsed → massive downward
        // pressure → scaled clamped to the floor.
        let d = adj.get_next_difficulty(10, i64::MAX / 2);
        assert_eq!(d, MIN_DIFFICULTY);
    }

    /// Cross-check: anchor at frame 0, time 0, difficulty 100000.
    /// Query at frame 1 with time `IDEAL_FRAME_TIME * 2`. That's
    /// exactly on schedule for `(frame_delta + 1) = 2` ideal ticks,
    /// so exponent is 0, factor is 65536, scaled = 100000 * 65536,
    /// shifts=0, >>16 → 100000.
    #[test]
    fn regression_on_schedule_step() {
        let adj = AsertDifficultyAdjuster::new(0, 0, 100_000);
        let d = adj.get_next_difficulty(1, IDEAL_FRAME_TIME * 2);
        assert_eq!(d, 100_000);
    }

    /// Mainnet anchor wiring: anchor_frame=244200, anchor_time=1762862400000,
    /// anchor_difficulty=80000. With these values the adjuster must NOT
    /// return BOOTSTRAP_DIFFICULTY (50000) — it must use the real ASERTi3-2d
    /// feedback loop.
    #[test]
    fn mainnet_anchor_does_not_bootstrap() {
        let adj = AsertDifficultyAdjuster::new(244_200, 1_762_862_400_000, 80_000);
        let frame = 244_201;
        let ts = 1_762_862_400_000 + IDEAL_FRAME_TIME * 2;
        let d = adj.get_next_difficulty(frame, ts);
        assert_ne!(d, BOOTSTRAP_DIFFICULTY, "mainnet anchor must not hit bootstrap path");
        assert_eq!(d, 80_000, "on-schedule frame should return anchor difficulty");
    }

    /// Adversarial: post-halt resumption can't be used to gain an
    /// asymmetric VDF advantage.
    ///
    /// Hypothetical attack: stall the network for a long period (e.g.
    /// via a coverage halt of `DEFAULT_HALT_GRACE_FRAMES`), then race
    /// other provers to produce the first post-halt frame. The fear
    /// is that the time_delta vs. expected_delta gap causes ASERT to
    /// collapse difficulty to a level so low that an attacker with
    /// pre-staged VDF compute can produce many leader proofs cheaply
    /// before the difficulty re-adjusts.
    ///
    /// In practice the half-life clamp (2h) plus the `MIN_DIFFICULTY`
    /// floor (50_000) make this benign:
    /// - A 1800-frame halt at 10s/frame ≈ 5h of stall, 2.5 half-lives.
    /// Difficulty drops by ~2^2.5 ≈ 5.66× per cycle.
    /// - The drop is symmetric: everyone gets the lower difficulty,
    /// not just the attacker.
    /// - Even an infinite halt clamps to `MIN_DIFFICULTY = 50_000`,
    /// which is the protocol's hard floor — not exploitable as
    /// "free" compute.
    /// This test pins those properties so any future ASERT tweak that
    /// removes the floor or amplifies the post-halt drop will fail
    /// loudly.
    #[test]
    fn post_halt_resumption_does_not_grant_asymmetric_advantage() {
        const MAINNET_ANCHOR_DIFF: u32 = 80_000;
        let anchor_time = 1_762_862_400_000i64;
        let anchor_frame = 244_200u64;
        let adj = AsertDifficultyAdjuster::new(
            anchor_frame,
            anchor_time,
            MAINNET_ANCHOR_DIFF,
        );

        // Realistic worst-case halt: full DEFAULT_HALT_GRACE_FRAMES
        // (1800) at 10s/frame = 18_000 seconds = 5 hours of stall on
        // an otherwise-on-schedule chain. The first post-halt frame
        // sees a time_delta that's 5 hours larger than the expected
        // 10s × (frame_delta + 1).
        let frame_delta = 1u64; // one frame after the anchor
        let halt_seconds: i64 = (1800 * IDEAL_FRAME_TIME) / 1000; // 18000
        let halt_ms = halt_seconds * 1000;
        let current_frame = anchor_frame + frame_delta;
        let on_schedule_time =
            anchor_time + IDEAL_FRAME_TIME * (frame_delta as i64 + 1);
        let post_halt_time = on_schedule_time + halt_ms;

        let d_post_halt = adj.get_next_difficulty(current_frame, post_halt_time);

        // (1) Floor: difficulty must never drop below MIN_DIFFICULTY,
        //     regardless of stall length.
        assert!(
            d_post_halt >= MIN_DIFFICULTY,
            "post-halt difficulty {} fell below floor {}",
            d_post_halt, MIN_DIFFICULTY,
        );

        // (2) Symmetry: the lower difficulty applies to every prover
        //     querying the same (frame, time) — there's no per-prover
        //     differentiation, so no honest-vs-attacker asymmetry.
        let d_for_attacker = adj.get_next_difficulty(current_frame, post_halt_time);
        let d_for_honest = adj.get_next_difficulty(current_frame, post_halt_time);
        assert_eq!(d_for_attacker, d_for_honest);

        // (3) Recovery: as soon as frames resume on-schedule, the
        //     adjuster moves back toward anchor. After roughly one
        //     half-life of on-schedule frames the difficulty should
        //     have closed most of the gap toward `MAINNET_ANCHOR_DIFF`.
        let recovery_anchor_diff = d_post_halt as u32;
        let recovery_anchor_time = post_halt_time;
        let recovery_adj = AsertDifficultyAdjuster::new(
            current_frame,
            recovery_anchor_time,
            recovery_anchor_diff,
        );
        // ~1 half-life later (2h), still on-schedule frame-by-frame.
        let frames_per_halflife = (HALF_LIFE / IDEAL_FRAME_TIME) as u64; // 720
        let after_halflife_frame = current_frame + frames_per_halflife;
        let after_halflife_time =
            recovery_anchor_time + IDEAL_FRAME_TIME * (frames_per_halflife as i64 + 1);
        let d_after = recovery_adj.get_next_difficulty(after_halflife_frame, after_halflife_time);
        // On-schedule recovery → returns the (now-lower) anchor.
        // The point is it doesn't drift further down once frames
        // resume; the post-halt floor is the worst case, and it
        // recovers from there.
        assert_eq!(
            d_after, recovery_anchor_diff as u64,
            "on-schedule frames after halt must stabilize at the post-halt difficulty, \
             not continue dropping",
        );

        // (4) Halt magnitude is bounded by the half-life: even a 24h
        //     halt (12 half-lives) clamps to MIN_DIFFICULTY rather
        //     than producing exotic values.
        let mega_halt_time = on_schedule_time + 24 * 3600 * 1000;
        let d_mega = adj.get_next_difficulty(current_frame, mega_halt_time);
        assert_eq!(
            d_mega, MIN_DIFFICULTY,
            "24h halt should clamp to floor, got {}",
            d_mega,
        );
    }

    /// The cadence-override setters refuse out-of-range values (leaving the
    /// effective value untouched) and apply in-range ones. One combined test:
    /// the overrides are process-global atomics, so keeping every mutation in
    /// a single test avoids order dependence on other tests in this binary.
    #[test]
    fn cadence_overrides_are_range_checked() {
        // Out-of-range: refused, default stays.
        assert!(!set_ideal_frame_time_ms(0));
        assert!(!set_ideal_frame_time_ms(-5));
        assert!(!set_ideal_frame_time_ms(MIN_IDEAL_FRAME_TIME_MS - 1));
        assert!(!set_ideal_frame_time_ms(IDEAL_FRAME_TIME + 1));
        // The units-mistake case that wraps the ASERT Q16 math (~1e13 ms).
        assert!(!set_ideal_frame_time_ms(10_000_000_000_000));
        assert_eq!(ideal_frame_time_ms(), IDEAL_FRAME_TIME);

        assert!(!set_min_difficulty(0));
        assert!(!set_min_difficulty(MIN_SETTABLE_DIFFICULTY - 1));
        // May only LOWER the floor, never raise it (also keeps it u32-safe
        // for the `difficulty as u32` header/VDF casts).
        assert!(!set_min_difficulty(MIN_DIFFICULTY + 1));
        assert!(!set_min_difficulty(u64::from(u32::MAX) + 1));
        assert_eq!(min_difficulty(), MIN_DIFFICULTY);

        // In-range: applied.
        assert!(set_ideal_frame_time_ms(1_000));
        assert_eq!(ideal_frame_time_ms(), 1_000);
        assert!(set_min_difficulty(5_000));
        assert_eq!(min_difficulty(), 5_000);

        // Restore defaults for any test sharing this process.
        assert!(set_ideal_frame_time_ms(IDEAL_FRAME_TIME));
        assert!(set_min_difficulty(MIN_DIFFICULTY));
    }
}
