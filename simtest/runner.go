package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"source.quilibrium.com/quilibrium/monorepo/simtest/rankpartitions"
	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)


type TestResult struct {
	RunID        string
	Success      bool
	ErrorMessage string
	Duration     time.Duration
	ArtifactDir  string
}

type runConfig struct {
	ExecDir                string
	BearerToken            string
	Verbose                bool
	StopFrame              int
	Nodes                  []shared.NodeInfo
	MinimumNodes           int
	RankPartitionsResolved string
	RankPartitionsOriginal []rankpartitions.RankPartitionEntry
	OutDir                 string
	SaveLogsOnSuccess      bool
	Parallel               int
}

// runAllTests handles signal setup, spawns parallel test runs, and collects results.
// Returns all results and whether execution was interrupted by a signal.
func runAllTests(ctx context.Context, cancel context.CancelFunc, cfg runConfig, router *NotificationRouter, projectRegistry *ProjectRegistry) (results []TestResult, interrupted bool) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Infow("Received interrupt signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(cfg.Parallel)
	resultsChan := make(chan TestResult, cfg.Parallel)

	for i := 0; i < cfg.Parallel; i++ {
		go func(runNumber int) {
			defer wg.Done()

			runID := uuid.New().String()
			logger.Debugw("Starting test run", "run_number", runNumber+1, "run_id", runID)

			startTime := time.Now()
			result := runSingleTest(ctx, runID, cfg, router, projectRegistry)
			result.Duration = time.Since(startTime)

			resultsChan <- result
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for result := range resultsChan {
		results = append(results, result)
	}

	return results, ctx.Err() != nil
}

func runSingleTest(ctx context.Context, runID string, cfg runConfig, router *NotificationRouter, projectRegistry *ProjectRegistry) TestResult {
	// Create notification channel for this run
	notifChan := make(chan shared.FrameNotification, 10)
	router.Register(runID, notifChan)
	defer router.Unregister(runID)
	defer close(notifChan)

	projectName := fmt.Sprintf("simtest_run_%s", runID)

	// Start compose stack
	if err := executeTest(ctx, runID, cfg.ExecDir, cfg.BearerToken, projectName, cfg.StopFrame, cfg.Verbose, cfg.Parallel, cfg.Nodes, cfg.MinimumNodes, cfg.RankPartitionsResolved); err != nil {
		logger.Errorw("Failed to start compose stack", "error", err, "run_id", runID)
		return TestResult{
			RunID:        runID,
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to start: %v", err),
		}
	}

	// Register project and ensure cleanup on all exit paths
	projectRegistry.Register(projectName)
	defer func() {
		// Use background context for cleanup so it runs even if main context cancelled
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		if err := dockerComposeDown(cleanupCtx, cfg.ExecDir, projectName, cfg.Verbose, cfg.Parallel); err != nil {
			logger.Errorw("Failed to cleanup compose stack", "error", err, "run_id", runID, "project", projectName)
		}

		projectRegistry.Unregister(projectName)
	}()

	// Wait for notification or context cancellation
	var result TestResult
	select {
	case n := <-notifChan:
		logger.Debugw("Terminal frame reached",
			"run_id", runID,
			"frame_number", n.FrameNumber,
			"nodes_reached_stop_frame", n.NodesReachedStopFrame,
			"total_nodes", n.TotalNodes)

		if n.SafetyError != "" {
			result = TestResult{
				RunID:        runID,
				Success:      false,
				ErrorMessage: n.SafetyError,
			}
		} else if n.NodesReachedStopFrame != cfg.MinimumNodes {
			result = TestResult{
				RunID:        runID,
				Success:      false,
				ErrorMessage: fmt.Sprintf("expected %d nodes to reach stop frame, but got %d", cfg.MinimumNodes, n.NodesReachedStopFrame),
			}
		} else {
			result = TestResult{
				RunID:   runID,
				Success: true,
			}
		}

	case <-ctx.Done():
		logger.Debugw("Test run cancelled", "run_id", runID, "reason", ctx.Err())
		result = TestResult{
			RunID:        runID,
			Success:      false,
			ErrorMessage: fmt.Sprintf("test run cancelled: %v", ctx.Err()),
		}
	}

	// Save artifacts for failing tests (and succeeding tests when -save-logs-on-success is set)
	if (!result.Success || cfg.SaveLogsOnSuccess) && cfg.OutDir != "" {
		tcfg := testConfig{
			RunID:          runID,
			StopFrame:      cfg.StopFrame,
			Nodes:          cfg.Nodes,
			MinimumNodes:   cfg.MinimumNodes,
			RankPartitions: cfg.RankPartitionsOriginal,
		}
		result.ArtifactDir = saveFailureArtifacts(cfg.OutDir, runID, projectName, cfg.ExecDir, result, tcfg)
	}

	return result
}

func printSummary(results []TestResult, interrupted bool) {
	passed := 0
	failed := 0

	for _, r := range results {
		logger.Debugf("Test run result, run_id=%s, success=%t, duration=%s", r.RunID, r.Success, r.Duration)
		if r.Success {
			passed++
		} else {
			failed++
		}
	}

	status := "PASSED"
	if interrupted {
		status = "INTERRUPTED"
	}
	if failed > 0 {
		status = "FAILED"
	}

	logger.Infow("Test Summary",
		"status", status,
		"total", len(results),
		"passed", passed,
		"failed", failed,
	)

	// Show details for failures
	if failed > 0 {
		logger.Info("Failed test runs:")
		for _, r := range results {
			if !r.Success {
				logger.Infow("  Run failed",
					"run_id", r.RunID,
					"error", r.ErrorMessage,
					"duration", r.Duration,
				)
			}
		}
		for _, r := range results {
			if r.ArtifactDir != "" {
				logger.Infow("Saved failure artifacts", "dir", r.ArtifactDir, "run_id", r.RunID)
			}
		}
	}
}

func hasFailures(results []TestResult) bool {
	for _, r := range results {
		if !r.Success {
			return true
		}
	}
	return false
}
