//! Exhaustive mode: run every (resumable) symmetry-unique partition schedule in
//! a bounded worker pool, persisting progress so interrupted runs can resume.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use devnet::rankpartitions::RankPartitionEntry;
use devnet::shared::NodeInfo;

use crate::notification::NotificationRouter;
use crate::registry::ProjectRegistry;
use crate::runner::{run_single_test, RunConfig, TestResult};
use crate::util::new_run_id;

const SCHEMA_VERSION: i32 = 2;

/// A schedule that ran to a failing verdict, retained so a resumed sweep can
/// still surface failures inherited from earlier runs in its summary and exit
/// code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedSchedule {
    pub index: usize,
    pub error_message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressState {
    pub schema_version: i32,
    pub seed: i64,
    pub nodes: Vec<String>,
    pub stop_rank: u64,
    pub schedules: Vec<Vec<RankPartitionEntry>>,
    #[serde(default)]
    pub completed: Vec<usize>,
    /// Indices (with their error messages) of schedules that completed with a
    /// failure — a subset of `completed`. Kept so a failure survives a resume:
    /// completed schedules are skipped on restart, so without this a resume that
    /// finishes cleanly would report success and hide an earlier failure.
    #[serde(default)]
    pub failures: Vec<FailedSchedule>,
}

/// Reads and validates an existing progress file. Returns `Ok(None)` when `path`
/// is empty or the file does not exist (the caller should start a fresh sweep);
/// returns an error if the file exists but its `nodes`/`stop_rank` disagree with
/// the current config. Loading is deliberately separate from generation so a
/// resume reuses the persisted schedules and seed rather than regenerating the
/// (exponential) schedule set — the file is authoritative for execution order.
pub fn read_progress_file(
    path: &str,
    nodes: &[String],
    stop_rank: u64,
) -> Result<Option<ProgressState>> {
    if path.is_empty() {
        return Ok(None);
    }

    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("failed to read progress file"),
    };

    let state: ProgressState =
        serde_json::from_str(&data).context("failed to parse progress file")?;

    if state.stop_rank != stop_rank {
        bail!(
            "progress file stop_rank {} does not match --partition-stop-rank {}; delete the file or use a different --progress-file path",
            state.stop_rank, stop_rank
        );
    }
    if state.nodes.len() != nodes.len() {
        bail!(
            "progress file has {} nodes {:?} but current config has {} nodes {:?}; delete the file or use a different --progress-file path",
            state.nodes.len(), state.nodes, nodes.len(), nodes
        );
    }
    let node_set: HashSet<&str> = nodes.iter().map(String::as_str).collect();
    for n in &state.nodes {
        if !node_set.contains(n.as_str()) {
            bail!(
                "progress file node {:?} not found in current config nodes {:?}; delete the file or use a different --progress-file path",
                n, nodes
            );
        }
    }

    Ok(Some(state))
}

/// Builds a fresh progress state (nothing completed yet) and, unless `path` is
/// empty, persists it so the sweep is resumable from the first schedule.
pub fn create_progress_file(
    path: &str,
    seed: i64,
    nodes: &[String],
    stop_rank: u64,
    schedules: Vec<Vec<RankPartitionEntry>>,
) -> Result<ProgressState> {
    let state = ProgressState {
        schema_version: SCHEMA_VERSION,
        seed,
        nodes: nodes.to_vec(),
        stop_rank,
        schedules,
        completed: Vec::new(),
        failures: Vec::new(),
    };
    if !path.is_empty() {
        write_progress_file(path, &state).context("failed to write initial progress file")?;
    }
    Ok(state)
}

/// Atomically writes `state` to `path` via a temp file + rename.
pub fn write_progress_file(path: &str, state: &ProgressState) -> Result<()> {
    let data = serde_json::to_string_pretty(state)?;
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Records `idx` as completed — retaining its error message when `success` is
/// false — then atomically persists the progress file (no-op persistence when
/// `path` is empty).
fn mark_schedule_complete(
    path: &str,
    state: &Mutex<ProgressState>,
    idx: usize,
    success: bool,
    error_message: &str,
) -> Result<()> {
    let mut guard = state.lock().unwrap();
    guard.completed.push(idx);
    if !success {
        guard.failures.push(FailedSchedule {
            index: idx,
            error_message: error_message.to_string(),
        });
    }
    if path.is_empty() {
        return Ok(());
    }
    write_progress_file(path, &guard)
}

/// Converts a service-name schedule into peer-ID JSON for the `RANK_PARTITIONS`
/// env var. Returns an empty string for an empty schedule.
pub fn resolve_schedule(
    schedule: &[RankPartitionEntry],
    node_info_map: &HashMap<String, NodeInfo>,
) -> Result<String> {
    if schedule.is_empty() {
        return Ok(String::new());
    }
    let resolve_names = |names: &[String]| -> Result<Vec<String>> {
        names
            .iter()
            .map(|name| {
                node_info_map
                    .get(name)
                    .map(|info| info.peer_id.clone())
                    .with_context(|| format!("node {name:?} not found in nodeInfoMap"))
            })
            .collect()
    };
    let resolved: Vec<RankPartitionEntry> = schedule
        .iter()
        .map(|e| {
            Ok(RankPartitionEntry {
                rank: e.rank,
                partition1: resolve_names(&e.partition1)?,
                partition2: resolve_names(&e.partition2)?,
            })
        })
        .collect::<Result<_>>()?;
    serde_json::to_string(&resolved).context("failed to serialize resolved schedule")
}

pub struct ExhaustiveConfig {
    pub run: RunConfig,
    pub node_info_map: HashMap<String, NodeInfo>,
    pub fail_fast: bool,
    pub progress_path: String,
    pub progress: Arc<Mutex<ProgressState>>,
}

/// Runs all not-yet-completed schedules in a worker pool. Returns the collected
/// results and whether execution was interrupted (cancelled).
pub async fn run_exhaustive(
    cancel: &CancellationToken,
    cfg: ExhaustiveConfig,
    router: &NotificationRouter,
    registry: &ProjectRegistry,
) -> (Vec<TestResult>, bool) {
    let (schedules, completed, prior_failures): (
        Vec<Vec<RankPartitionEntry>>,
        HashSet<usize>,
        Vec<FailedSchedule>,
    ) = {
        let st = cfg.progress.lock().unwrap();
        (
            st.schedules.clone(),
            st.completed.iter().copied().collect(),
            st.failures.clone(),
        )
    };

    // Pending work as (original index, schedule), skipping completed indices.
    let pending: Arc<Vec<(usize, Vec<RankPartitionEntry>)>> = Arc::new(
        schedules
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| !completed.contains(idx))
            .collect(),
    );

    let cursor = Arc::new(AtomicUsize::new(0));
    // Distinguishes "we cancelled the run because a test failed under fail-fast"
    // from "an external signal interrupted us". Both cancel the token, but only
    // the latter should be reported as an interrupt (exit 130); a fail-fast stop
    // is a test failure (exit 2).
    let fail_fast_triggered = Arc::new(AtomicBool::new(false));
    let results = Arc::new(Mutex::new(Vec::new()));
    let node_info_map = Arc::new(cfg.node_info_map);
    let progress_path = Arc::new(cfg.progress_path);
    let parallel = cfg.run.parallel.max(1);
    let base_run = Arc::new(cfg.run);

    let mut handles = Vec::new();
    for _ in 0..parallel {
        let pending = Arc::clone(&pending);
        let cursor = Arc::clone(&cursor);
        let results = Arc::clone(&results);
        let node_info_map = Arc::clone(&node_info_map);
        let progress_path = Arc::clone(&progress_path);
        let base_run = Arc::clone(&base_run);
        let progress = Arc::clone(&cfg.progress);
        let fail_fast_triggered = Arc::clone(&fail_fast_triggered);
        let cancel = cancel.clone();
        let router = router.clone();
        let registry = registry.clone();
        let fail_fast = cfg.fail_fast;

        handles.push(tokio::spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    return;
                }
                let i = cursor.fetch_add(1, Ordering::SeqCst);
                if i >= pending.len() {
                    return;
                }
                let (idx, schedule) = &pending[i];

                let resolved = match resolve_schedule(schedule, &node_info_map) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!(error = %e, idx, "Failed to resolve schedule");
                        let error_message = format!("failed to resolve schedule: {e}");
                        if let Err(merr) = mark_schedule_complete(
                            &progress_path,
                            &progress,
                            *idx,
                            false,
                            &error_message,
                        ) {
                            tracing::error!(error = %merr, idx, "Failed to mark schedule complete");
                        }
                        if fail_fast {
                            fail_fast_triggered.store(true, Ordering::SeqCst);
                            cancel.cancel();
                        }
                        results.lock().unwrap().push(TestResult {
                            run_id: new_run_id(),
                            success: false,
                            error_message,
                            ..Default::default()
                        });
                        continue;
                    }
                };

                let mut run_cfg = (*base_run).clone();
                run_cfg.rank_partitions_original = schedule.clone();
                run_cfg.rank_partitions_resolved = resolved;

                let run_id = new_run_id();
                let start = Instant::now();
                let mut result =
                    run_single_test(&cancel, &run_id, &run_cfg, &router, &registry).await;
                result.duration = start.elapsed();

                // Only persist schedules that ran to a verdict. A cancelled run
                // (signal, or another worker's fail-fast) never finished, so
                // marking it complete would silently skip it on resume.
                if !result.cancelled {
                    if let Err(merr) = mark_schedule_complete(
                        &progress_path,
                        &progress,
                        *idx,
                        result.success,
                        &result.error_message,
                    ) {
                        tracing::error!(error = %merr, idx, "Failed to mark schedule complete");
                    }
                }
                if !result.success && !result.cancelled && fail_fast {
                    fail_fast_triggered.store(true, Ordering::SeqCst);
                    cancel.cancel();
                }
                results.lock().unwrap().push(result);
            }
        }));
    }

    for h in handles {
        if let Err(e) = h.await {
            tracing::error!(error = %e, "Exhaustive worker panicked");
        }
    }

    let mut results = Arc::try_unwrap(results)
        .map(|m| m.into_inner().unwrap())
        .unwrap_or_default();
    // Fold in failures inherited from earlier (resumed) runs. Their schedules are
    // in `completed`, so they were skipped this invocation and are absent from
    // `results`; without this a resume that finishes the remaining schedules
    // cleanly would report success and hide the earlier failure.
    for f in prior_failures {
        results.push(TestResult {
            run_id: format!("earlier-run-schedule-{}", f.index),
            success: false,
            error_message: format!("{} (recorded in an earlier run)", f.error_message),
            ..Default::default()
        });
    }
    // The token is also cancelled by fail-fast; that path is a test failure, not
    // an interrupt. Only a cancellation we didn't trigger ourselves (an external
    // signal) counts as interrupted.
    let interrupted = cancel.is_cancelled() && !fail_fast_triggered.load(Ordering::SeqCst);
    (results, interrupted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "devnet-progress-test-{name}-{}.json",
            std::process::id()
        ));
        p.to_string_lossy().into_owned()
    }

    fn sample_schedules() -> Vec<Vec<RankPartitionEntry>> {
        vec![
            vec![],
            vec![RankPartitionEntry {
                rank: 0,
                partition1: vec!["a".into()],
                partition2: vec!["b".into()],
            }],
        ]
    }

    #[test]
    fn read_missing_or_empty_returns_none() {
        // Empty path means "no persistence" — nothing to resume.
        assert!(read_progress_file("", &["a".into()], 0).unwrap().is_none());
        let missing = tmp_path("missing");
        let _ = std::fs::remove_file(&missing);
        assert!(read_progress_file(&missing, &["a".into()], 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn create_then_read_roundtrip() {
        let path = tmp_path("roundtrip");
        let nodes = vec!["a".to_string(), "b".to_string()];
        let created = create_progress_file(&path, 42, &nodes, 0, sample_schedules()).unwrap();
        assert_eq!(created.seed, 42);
        assert!(created.completed.is_empty());
        assert!(created.failures.is_empty());

        let loaded = read_progress_file(&path, &nodes, 0).unwrap().unwrap();
        assert_eq!(loaded.seed, 42);
        assert_eq!(loaded.schedules.len(), 2);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn read_rejects_stop_rank_mismatch() {
        let path = tmp_path("mismatch");
        let nodes = vec!["a".to_string()];
        create_progress_file(&path, 1, &nodes, 3, sample_schedules()).unwrap();
        let err = read_progress_file(&path, &nodes, 5)
            .unwrap_err()
            .to_string();
        assert!(err.contains("stop_rank"), "unexpected error: {err}");
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn mark_complete_records_failures() {
        let path = tmp_path("mark");
        let nodes = vec!["a".to_string()];
        let state = create_progress_file(&path, 1, &nodes, 0, sample_schedules()).unwrap();
        let m = Mutex::new(state);
        mark_schedule_complete(&path, &m, 0, true, "").unwrap();
        mark_schedule_complete(&path, &m, 1, false, "boom").unwrap();

        // Both indices are completed; only the failing one is retained in
        // `failures`, and it survives a reload (the resume path reads this back).
        let reloaded = read_progress_file(&path, &nodes, 0).unwrap().unwrap();
        assert_eq!(reloaded.completed, vec![0, 1]);
        assert_eq!(reloaded.failures.len(), 1);
        assert_eq!(reloaded.failures[0].index, 1);
        assert_eq!(reloaded.failures[0].error_message, "boom");
        std::fs::remove_file(&path).unwrap();
    }
}
