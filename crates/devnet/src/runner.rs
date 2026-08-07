//! Single-test execution: start the compose stack, await the proxy's
//! notification (or cancellation), adjudicate the result, capture artifacts on
//! failure, and always tear the stack down.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use devnet::shared::{FrameNotification, NodeInfo, NotificationType};
use devnet::viewpartitions::ViewPartitionEntry;

use crate::artifacts::{self, TestConfig};
use crate::docker::{self, ExecuteTest};
use crate::notification::NotificationRouter;
use crate::registry::ProjectRegistry;

#[derive(Debug, Clone, Default)]
pub struct TestResult {
    pub run_id: String,
    pub success: bool,
    pub error_message: String,
    pub duration: Duration,
    pub artifact_dir: String,
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub exec_dir: String,
    pub bearer_token: String,
    pub listen_port: String,
    pub verbose: bool,
    pub stop_frame: i32,
    pub nodes: Vec<NodeInfo>,
    pub minimum_nodes: i32,
    pub view_partitions_resolved: String,
    pub view_partitions_original: Vec<ViewPartitionEntry>,
    pub out_dir: String,
    pub save_logs_on_success: bool,
    pub parallel: i32,
    pub global_timeout: Duration,
    pub node_catchup_timeout: Duration,
    /// App-shard frame target for the pinned token-app shard (0 = disabled).
    pub app_stop_frame: u64,
}

/// Runs a single simulation for `run_id` and returns its result.
pub async fn run_single_test(
    cancel: &CancellationToken,
    run_id: &str,
    cfg: &RunConfig,
    router: &NotificationRouter,
    registry: &ProjectRegistry,
) -> TestResult {
    let mut notif_rx = router.register(run_id);
    let project_name = format!("devnet_run_{run_id}");

    // Register before startup, not after. `docker compose up --wait` creates the
    // network and containers before it blocks on healthchecks, so a failure (or
    // an interrupt) part-way through startup can leave a partial stack behind.
    // Registering now means both this function's teardown and the interrupt-time
    // safety net (`cleanup_active_projects`) can reap whatever was created.
    registry.register(&project_name);

    tracing::info!(run_id, view_partitions = ?cfg.view_partitions_original, "Starting test");

    if let Err(e) = docker::execute_test(ExecuteTest {
        run_id,
        exec_dir: &cfg.exec_dir,
        bearer_token: &cfg.bearer_token,
        listen_port: &cfg.listen_port,
        project_name: &project_name,
        stop_frame: cfg.stop_frame,
        verbose: cfg.verbose,
        parallel: cfg.parallel,
        nodes: &cfg.nodes,
        minimum_nodes: cfg.minimum_nodes,
        resolved_view_partitions: &cfg.view_partitions_resolved,
        global_timeout: cfg.global_timeout,
        node_catchup_timeout: cfg.node_catchup_timeout,
        app_stop_frame: cfg.app_stop_frame,
    })
    .await
    {
        tracing::error!(error = %e, run_id, "Failed to start compose stack");
        // `up --wait` may have created containers/networks before failing on an
        // unhealthy service, so explicitly tear down with a bounded budget — the
        // interrupt-time safety net only runs on cancellation, not on a normal
        // startup failure. A `down` on a never-created project is a harmless
        // no-op, so this is safe even when `execute_test` failed before `up`.
        let down =
            docker::docker_compose_down(&cfg.exec_dir, &project_name, cfg.verbose, cfg.parallel);
        match tokio::time::timeout(Duration::from_secs(30), down).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::error!(error = %err, run_id, project = %project_name, "Failed to clean up partial compose stack")
            }
            Err(_) => {
                tracing::error!(run_id, project = %project_name, "Timed out tearing down partial compose stack")
            }
        }
        registry.unregister(&project_name);
        router.unregister(run_id);
        return TestResult {
            run_id: run_id.to_string(),
            success: false,
            error_message: format!("failed to start: {e}"),
            ..Default::default()
        };
    }

    // Wait for the proxy's terminal notification or context cancellation, logging
    // intermediate frame-progress updates as they arrive so a live run shows
    // progress instead of going silent until the verdict.
    let mut result = loop {
        tokio::select! {
            maybe_n = notif_rx.recv() => match maybe_n {
                Some(n) if n.notification_type == NotificationType::Progress => {
                    if cfg.verbose {
                        if n.app_stop_frame > 0 {
                            tracing::debug!(
                                run_id,
                                frame = n.stop_frame,
                                stop_frame = cfg.stop_frame,
                                app_frame = n.app_frame_number,
                                app_stop_frame = n.app_stop_frame,
                                "Frame progress"
                            );
                        } else {
                            tracing::debug!(
                                run_id,
                                frame = n.stop_frame,
                                stop_frame = cfg.stop_frame,
                                "Frame progress"
                            );
                        }
                    }
                    continue;
                }
                Some(n) => break handle_terminal_notification(run_id, cfg, n),
                None => break TestResult {
                    run_id: run_id.to_string(),
                    success: false,
                    error_message: "notification channel closed".to_string(),
                    ..Default::default()
                },
            },
            _ = cancel.cancelled() => {
                tracing::debug!(run_id, "Test run cancelled");
                break TestResult {
                    run_id: run_id.to_string(),
                    success: false,
                    error_message: "test run cancelled".to_string(),
                    ..Default::default()
                };
            }
        }
    };

    // Save artifacts for failing tests (and successes when requested), while the
    // stack is still up so service logs are available.
    if (!result.success || cfg.save_logs_on_success) && !cfg.out_dir.is_empty() {
        let tcfg = TestConfig {
            run_id: run_id.to_string(),
            stop_frame: cfg.stop_frame,
            nodes: cfg.nodes.clone(),
            minimum_nodes: cfg.minimum_nodes,
            view_partitions: cfg.view_partitions_original.clone(),
            app_stop_frame: cfg.app_stop_frame,
        };
        result.artifact_dir = artifacts::save_failure_artifacts(
            &cfg.out_dir,
            run_id,
            &project_name,
            &cfg.exec_dir,
            &result,
            &tcfg,
        )
        .await;
    }

    // Always tear the stack down — even on cancellation — with a bounded budget
    // independent of the (possibly cancelled) run context.
    let down = docker::docker_compose_down(&cfg.exec_dir, &project_name, cfg.verbose, cfg.parallel);
    match tokio::time::timeout(Duration::from_secs(30), down).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::error!(error = %e, run_id, project = %project_name, "Failed to cleanup compose stack")
        }
        Err(_) => {
            tracing::error!(run_id, project = %project_name, "Timed out tearing down compose stack")
        }
    }
    registry.unregister(&project_name);
    router.unregister(run_id);

    result
}

fn handle_terminal_notification(run_id: &str, cfg: &RunConfig, n: FrameNotification) -> TestResult {
    tracing::debug!(
        run_id,
        notification_type = ?n.notification_type,
        stop_frame = n.stop_frame,
        nodes_reached_stop_frame = n.nodes_reached_stop_frame,
        total_nodes = n.total_nodes,
        "Terminal notification received"
    );

    let base = || TestResult {
        run_id: run_id.to_string(),
        ..Default::default()
    };

    let app_enabled = cfg.app_stop_frame > 0;
    let fail = |message: String| TestResult {
        success: false,
        error_message: message,
        run_id: run_id.to_string(),
        ..Default::default()
    };

    // Precedence: harness > safety (global, then app) > convergence (global,
    // then app) > enrollment > rejoin > shard participation > app diagnostics.
    // The two chains are adjudicated with the same structure; within each
    // tier the global verdict comes first.
    //
    // Checked first: if the harness did not actually run the scenario, every
    // other signal in this notification is meaningless, and a pass would falsely
    // read as "the scenario was exercised and the network held up".
    if !n.harness_error.is_empty() {
        return fail(format!("harness verification failed: {}", n.harness_error));
    }
    if !n.safety_error.is_empty() {
        return fail(n.safety_error);
    }
    // The app safety verdict sits in the safety tier so a shard fork (chain
    // non-linearity or an equivocation) can never be masked by a convergence
    // shortfall it may itself have caused.
    if app_enabled && !n.app_safety_error.is_empty() {
        return fail(format!(
            "app shard safety violation: {}",
            n.app_safety_error
        ));
    }
    // `minimum_nodes` is a lower bound, not an exact count: the frame monitor
    // stops as soon as `>= min_nodes` reach the stop frame, and a single poll
    // cycle can carry several nodes across at once, so `nodes_reached_stop_frame`
    // may legitimately exceed the threshold. Only fewer than required is a
    // failure.
    if n.nodes_reached_stop_frame < cfg.minimum_nodes {
        return fail(format!(
            "expected at least {} nodes to reach stop frame, but got {}",
            cfg.minimum_nodes, n.nodes_reached_stop_frame
        ));
    }
    // App convergence, mirroring the global check above: every polled
    // committee client must serve the app stop frame. An app-enabled TERMINAL
    // notification that reports ZERO app targets never ran this tier at all —
    // the proxy is built from the same tree as the runner, so zero is always
    // a bug (a proxy that skipped the app monitor), never version skew, and
    // letting it pass would clear the whole per-client convergence verdict
    // silently. (A timed-out run may carry 0/0 from a missing snapshot; it
    // fails on the timeout check below with the accurate message.)
    if app_enabled
        && n.notification_type == NotificationType::TerminalFrame
        && n.app_total_nodes == 0
    {
        return fail(
            "proxy reported no app-shard clients — app convergence verification did not run"
                .to_string(),
        );
    }
    if app_enabled && n.app_nodes_reached < n.app_total_nodes {
        return fail(format!(
            "expected all {} clients to reach app stop frame {}, but got {}",
            n.app_total_nodes, cfg.app_stop_frame, n.app_nodes_reached
        ));
    }
    if !n.enrollment_error.is_empty() {
        return fail(format!(
            "enrollment verification failed: {}",
            n.enrollment_error
        ));
    }
    if !n.rejoin_error.is_empty() {
        return fail(format!(
            "consensus rejoin verification failed: {}",
            n.rejoin_error
        ));
    }
    // The app analogue of the rejoin verdict: a committee member that never
    // originated a shard vote merely ingested frames.
    if app_enabled && !n.app_participation_error.is_empty() {
        return fail(format!(
            "shard participation verification failed: {}",
            n.app_participation_error
        ));
    }
    // App diagnostics tier: the tracker's own error/lag reporting (snoop
    // granularity — filters, not nodes).
    if app_enabled {
        if !n.app_shard_error.is_empty() {
            return fail(format!(
                "app shard verification failed: {}",
                n.app_shard_error
            ));
        }
        if n.app_shards_reached < n.app_shards_total {
            return fail(format!(
                "app shard verification failed: {}/{} tracked shards reached app stop \
                 frame {}",
                n.app_shards_reached, n.app_shards_total, cfg.app_stop_frame
            ));
        }
    }
    // A timed-out run must never fall through to success: reaching here means
    // every populated field looked clean (e.g. both chains converged in the
    // timeout snapshot but the stop conditions raced the timer), yet the stop
    // conditions were not met in time.
    if n.notification_type == NotificationType::GlobalTimeout {
        return fail("run timed out before meeting its stop conditions".to_string());
    }
    TestResult {
        success: true,
        ..base()
    }
}

pub fn print_summary(results: &[TestResult], interrupted: bool) {
    let passed = results.iter().filter(|r| r.success).count();
    let failed = results.len() - passed;

    let status = if interrupted {
        "INTERRUPTED"
    } else if failed > 0 {
        "FAILED"
    } else {
        "PASSED"
    };

    tracing::info!(
        status,
        total = results.len(),
        passed,
        failed,
        "Test Summary"
    );

    if failed > 0 {
        tracing::info!("Failed test runs:");
        for r in results.iter().filter(|r| !r.success) {
            tracing::info!(run_id = %r.run_id, error = %r.error_message, duration = ?r.duration, "  Run failed");
        }
        for r in results.iter().filter(|r| !r.artifact_dir.is_empty()) {
            tracing::info!(dir = %r.artifact_dir, run_id = %r.run_id, "Saved failure artifacts");
        }
    }
}

pub fn has_failures(results: &[TestResult]) -> bool {
    results.iter().any(|r| !r.success)
}

#[cfg(test)]
mod evaluate_notification_tests {
    use super::*;
    use devnet::shared::NotificationType;

    /// A RunConfig that only sets the field `evaluate_notification` reads
    /// (`minimum_nodes`); everything else is irrelevant to the success check.
    fn cfg(minimum_nodes: i32) -> RunConfig {
        RunConfig {
            exec_dir: String::new(),
            bearer_token: String::new(),
            listen_port: String::new(),
            verbose: false,
            stop_frame: 30,
            nodes: Vec::new(),
            minimum_nodes,
            view_partitions_resolved: String::new(),
            view_partitions_original: Vec::new(),
            out_dir: String::new(),
            save_logs_on_success: false,
            parallel: 1,
            global_timeout: Duration::from_secs(120),
            node_catchup_timeout: Duration::from_secs(60),
            app_stop_frame: 0,
        }
    }

    /// A clean terminal-frame notification with `reached` nodes and no errors.
    fn ok_notification(reached: i32) -> FrameNotification {
        FrameNotification {
            run_id: String::new(),
            stop_frame: 30,
            notification_type: NotificationType::TerminalFrame,
            safety_error: String::new(),
            nodes_reached_stop_frame: reached,
            total_nodes: reached,
            enrollment_error: String::new(),
            rejoin_error: String::new(),
            harness_error: String::new(),
            app_shard_error: String::new(),
            app_shards_reached: 0,
            app_shards_total: 0,
            app_stop_frame: 0,
            app_nodes_reached: 0,
            app_total_nodes: 0,
            app_safety_error: String::new(),
            app_participation_error: String::new(),
            app_frame_number: 0,
        }
    }

    /// A run that did not execute its scenario must never report success, even
    /// though every other signal looks clean — that is exactly the false pass
    /// this check exists to prevent.
    #[test]
    fn harness_error_fails_an_otherwise_clean_run() {
        let n = FrameNotification {
            harness_error: "scheduled partition views were never observed: [1]".into(),
            ..ok_notification(3)
        };
        let r = handle_terminal_notification("run", &cfg(3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("harness verification failed"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// The harness check outranks the others: if the scenario did not run, the
    /// other verdicts are not evidence of anything.
    #[test]
    fn harness_error_takes_precedence_over_safety_error() {
        let n = FrameNotification {
            harness_error: "consensus event dropped".into(),
            safety_error: "fork detected".into(),
            ..ok_notification(3)
        };
        let r = handle_terminal_notification("run", &cfg(3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("harness verification failed"),
            "unexpected: {}",
            r.error_message
        );
    }

    #[test]
    fn exact_match_succeeds() {
        let r = handle_terminal_notification("run", &cfg(3), ok_notification(3));
        assert!(r.success, "{}", r.error_message);
    }

    /// The regression this guards: `minimum_nodes` is a lower bound, so more
    /// nodes than required reaching the stop frame is a pass, not a failure.
    /// With `--minnodes=3` and 4 healthy archives, the frame monitor can report
    /// 4 reached, which previously failed the run via a strict `!=` check.
    #[test]
    fn more_than_minimum_succeeds() {
        let r = handle_terminal_notification("run", &cfg(3), ok_notification(4));
        assert!(r.success, "{}", r.error_message);
    }

    #[test]
    fn fewer_than_minimum_fails() {
        let r = handle_terminal_notification("run", &cfg(3), ok_notification(2));
        assert!(!r.success);
        assert!(
            r.error_message.contains("at least 3"),
            "unexpected message: {}",
            r.error_message
        );
    }

    /// A RunConfig with app-shard checking enabled at the given target.
    fn cfg_app(minimum_nodes: i32, app_stop_frame: u64) -> RunConfig {
        RunConfig {
            app_stop_frame,
            ..cfg(minimum_nodes)
        }
    }

    /// A clean notification whose app-shard fields report success.
    fn ok_app_notification(reached: i32) -> FrameNotification {
        FrameNotification {
            app_shards_reached: 1,
            app_shards_total: 1,
            app_stop_frame: 3,
            app_nodes_reached: 4,
            app_total_nodes: 4,
            ..ok_notification(reached)
        }
    }

    #[test]
    fn app_shard_error_fails_run_when_enabled() {
        let n = FrameNotification {
            app_shard_error: "equivocation on filter 1768…8643 frame 2".into(),
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("app shard verification failed"),
            "unexpected: {}",
            r.error_message
        );
    }

    #[test]
    fn app_shard_shortfall_fails_run_when_enabled() {
        let n = FrameNotification {
            app_shards_reached: 0,
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("0/1 tracked shards"),
            "unexpected: {}",
            r.error_message
        );
    }

    #[test]
    fn app_shard_fields_ignored_when_disabled() {
        // App checking disabled (app_stop_frame == 0): even a notification
        // carrying app errors must not fail the run — preserves the legacy
        // global-only behavior byte-for-byte.
        let n = FrameNotification {
            app_shard_error: "stale field".into(),
            app_shards_reached: 0,
            app_shards_total: 1,
            ..ok_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg(3), n);
        assert!(r.success, "{}", r.error_message);
    }

    #[test]
    fn app_shard_clean_run_succeeds() {
        let r = handle_terminal_notification("run", &cfg_app(3, 3), ok_app_notification(4));
        assert!(r.success, "{}", r.error_message);
    }

    /// Global verdicts outrank the app verdict: a min-nodes shortfall is
    /// reported as the failure even when the app fields also look bad.
    #[test]
    fn global_shortfall_takes_precedence_over_app_error() {
        let n = FrameNotification {
            app_shard_error: "shard lagging".into(),
            ..ok_app_notification(2)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("at least 3"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// The app safety verdict sits in the safety tier: it outranks the global
    /// convergence shortfall (which a fork may itself cause) …
    #[test]
    fn app_safety_outranks_global_shortfall() {
        let n = FrameNotification {
            app_safety_error: "equivocation on app shard 1768…8643: frame 2".into(),
            ..ok_app_notification(2)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("app shard safety violation"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// … but not the global safety or harness verdicts.
    #[test]
    fn global_safety_and_harness_outrank_app_safety() {
        let n = FrameNotification {
            safety_error: "fork detected".into(),
            app_safety_error: "app fork".into(),
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert_eq!(r.error_message, "fork detected");

        let n = FrameNotification {
            harness_error: "consensus event dropped".into(),
            app_safety_error: "app fork".into(),
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(
            r.error_message.contains("harness verification failed"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// The app convergence verdict mirrors the global min-nodes check: every
    /// polled client must serve the app stop frame.
    #[test]
    fn app_convergence_shortfall_fails_run() {
        let n = FrameNotification {
            app_nodes_reached: 2,
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message
                .contains("expected all 4 clients to reach app stop frame 3, but got 2"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// An app-enabled terminal notification with ZERO app targets means the
    /// proxy never ran the app convergence tier — a hard failure, not a pass.
    /// (The old compat guard for "proxies predating the field" let exactly
    /// this clear the whole per-client verdict silently; proxy and runner are
    /// built from the same tree, so zero is always a bug.)
    #[test]
    fn app_convergence_with_zero_total_is_a_failure() {
        let n = FrameNotification {
            app_nodes_reached: 0,
            app_total_nodes: 0,
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("no app-shard clients"),
            "unexpected: {}",
            r.error_message
        );
    }

    #[test]
    fn app_participation_error_fails_run() {
        let n = FrameNotification {
            app_participation_error: "client-3 did not vote at or after the app stop frame's \
                                      view"
                .into(),
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message
                .contains("shard participation verification failed"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// A timeout notification whose global snapshot shows the archives DID
    /// converge no longer hides the app failure behind a bogus "0 nodes"
    /// shortfall — the app branches are reachable and name the real problem.
    #[test]
    fn timeout_with_global_converged_reports_the_app_shortfall() {
        let n = FrameNotification {
            notification_type: NotificationType::GlobalTimeout,
            app_nodes_reached: 1,
            app_shards_reached: 0,
            app_shard_error: "app shard 1768…8643 at frame 1 < target 3".into(),
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message
                .contains("expected all 4 clients to reach app stop frame"),
            "unexpected: {}",
            r.error_message
        );
    }

    /// A timed-out run must never fall through to success, even when every
    /// populated field looks clean.
    #[test]
    fn clean_timeout_still_fails() {
        let n = FrameNotification {
            notification_type: NotificationType::GlobalTimeout,
            ..ok_app_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg_app(3, 3), n);
        assert!(!r.success);
        assert!(
            r.error_message.contains("timed out"),
            "unexpected: {}",
            r.error_message
        );

        // Same for a global-only run.
        let n = FrameNotification {
            notification_type: NotificationType::GlobalTimeout,
            ..ok_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg(3), n);
        assert!(!r.success, "a clean global-only timeout must fail");
    }

    /// With app checking disabled, every new app field is ignored — preserves
    /// the legacy global-only behavior byte-for-byte.
    #[test]
    fn new_app_fields_ignored_when_disabled() {
        let n = FrameNotification {
            app_safety_error: "stale".into(),
            app_participation_error: "stale".into(),
            app_nodes_reached: 0,
            app_total_nodes: 4,
            ..ok_notification(4)
        };
        let r = handle_terminal_notification("run", &cfg(3), n);
        assert!(r.success, "{}", r.error_message);
    }
}
