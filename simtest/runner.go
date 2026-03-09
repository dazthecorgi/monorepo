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

	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)


type TestResult struct {
	RunID        string
	Success      bool
	ErrorMessage string
	Duration     time.Duration
	ArtifactDir  string
}

// runAllTests handles signal setup, spawns parallel test runs, and collects results.
// Returns all results and whether execution was interrupted by a signal.
func runAllTests(ctx context.Context, cancel context.CancelFunc, parallel int, execDir string, bearerToken string, router *NotificationRouter, verbose bool, stopFrame int, projectRegistry *ProjectRegistry, nodes []shared.NodeInfo, minimumNodes int, rankPartitionsResolved string, rankPartitionsOriginal []shared.RankPartitionEntry, outDir string) (results []TestResult, interrupted bool) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Infow("Received interrupt signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(parallel)
	resultsChan := make(chan TestResult, parallel)

	for i := 0; i < parallel; i++ {
		go func(runNumber int) {
			defer wg.Done()

			runID := uuid.New().String()
			logger.Debugw("Starting test run", "run_number", runNumber+1, "run_id", runID)

			startTime := time.Now()
			result := runSingleTest(ctx, runID, execDir, bearerToken, router, verbose, stopFrame, projectRegistry, parallel, nodes, minimumNodes, rankPartitionsResolved, rankPartitionsOriginal, outDir)
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

func runSingleTest(ctx context.Context, runID string, execDir string, bearerToken string, router *NotificationRouter, verbose bool, stopFrame int, projectRegistry *ProjectRegistry, parallelRuns int, nodes []shared.NodeInfo, minimumNodes int, rankPartitionsResolved string, rankPartitionsOriginal []shared.RankPartitionEntry, outDir string) TestResult {
	// Create notification channel for this run
	notifChan := make(chan shared.FrameNotification, 10)
	router.Register(runID, notifChan)
	defer router.Unregister(runID)
	defer close(notifChan)

	projectName := fmt.Sprintf("simtest_run_%s", runID)

	// Start compose stack
	if err := executeTest(ctx, runID, execDir, bearerToken, projectName, stopFrame, verbose, parallelRuns, nodes, minimumNodes, rankPartitionsResolved); err != nil {
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

		if err := dockerComposeDown(cleanupCtx, execDir, projectName, verbose, parallelRuns); err != nil {
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
		} else if n.NodesReachedStopFrame != minimumNodes {
			result = TestResult{
				RunID:        runID,
				Success:      false,
				ErrorMessage: fmt.Sprintf("expected %d nodes to reach stop frame, but got %d", minimumNodes, n.NodesReachedStopFrame),
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

	// Save artifacts for failing tests before compose stack is torn down
	if !result.Success && outDir != "" {
		cfg := testConfig{
			RunID:        runID,
			StopFrame:    stopFrame,
			Nodes:        nodes,
			MinimumNodes: minimumNodes,
			RankPartitions: rankPartitionsOriginal,
		}
		result.ArtifactDir = saveFailureArtifacts(outDir, runID, projectName, execDir, result, cfg)
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
				logger.Errorw("  Run failed",
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
