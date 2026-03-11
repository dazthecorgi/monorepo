package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"source.quilibrium.com/quilibrium/monorepo/simtest/rankpartitions"
	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)

type progressState struct {
	SchemaVersion int                                   `json:"schema_version"`
	Seed          int64                                 `json:"seed"`
	Nodes         []string                              `json:"nodes"`
	StopRank      uint64                                `json:"stop_rank"`
	Schedules     [][]rankpartitions.RankPartitionEntry `json:"schedules"`
	Completed     []int                                 `json:"completed"`
}

// loadOrCreateProgressFile loads an existing progress file or creates a new one.
// Returns an error if the file exists but nodes/stopRank mismatch the current config.
func loadOrCreateProgressFile(path string, seed int64, nodes []string, stopRank uint64, schedules [][]rankpartitions.RankPartitionEntry) (*progressState, error) {
	if path == "" {
		return &progressState{
			SchemaVersion: 1,
			Seed:          seed,
			Nodes:         nodes,
			StopRank:      stopRank,
			Schedules:     schedules,
		}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read progress file: %w", err)
		}
		// File does not exist — create it.
		state := &progressState{
			SchemaVersion: 1,
			Seed:          seed,
			Nodes:         nodes,
			StopRank:      stopRank,
			Schedules:     schedules,
		}
		if err := writeProgressFile(path, state); err != nil {
			return nil, fmt.Errorf("failed to write initial progress file: %w", err)
		}
		return state, nil
	}

	// File exists — parse and validate.
	var state progressState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse progress file: %w", err)
	}

	if state.StopRank != stopRank {
		return nil, fmt.Errorf("progress file stop_rank %d does not match -partition-stop-rank %d; delete the file or use a different -progress-file path", state.StopRank, stopRank)
	}
	if len(state.Nodes) != len(nodes) {
		return nil, fmt.Errorf("progress file has %d nodes %v but current config has %d nodes %v; delete the file or use a different -progress-file path", len(state.Nodes), state.Nodes, len(nodes), nodes)
	}
	nodeSet := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n] = struct{}{}
	}
	for _, n := range state.Nodes {
		if _, ok := nodeSet[n]; !ok {
			return nil, fmt.Errorf("progress file node %q not found in current config nodes %v; delete the file or use a different -progress-file path", n, nodes)
		}
	}

	return &state, nil
}

// writeProgressFile atomically writes state to path via a temp file + rename.
func writeProgressFile(path string, state *progressState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// markScheduleComplete appends idx to state.Completed and atomically writes the
// progress file. Thread-safe via mu. If path is empty, only the in-memory state
// is updated.
func markScheduleComplete(path string, mu *sync.Mutex, state *progressState, idx int) error {
	mu.Lock()
	defer mu.Unlock()
	state.Completed = append(state.Completed, idx)
	if path == "" {
		return nil
	}
	return writeProgressFile(path, state)
}

// resolveSchedule converts a service-name schedule to peer-ID JSON suitable for
// the RANK_PARTITIONS docker env var. Returns "" for an empty schedule.
func resolveSchedule(schedule []rankpartitions.RankPartitionEntry, nodeInfoMap map[string]shared.NodeInfo) (string, error) {
	if len(schedule) == 0 {
		return "", nil
	}
	resolved := make([]rankpartitions.RankPartitionEntry, len(schedule))
	for i, e := range schedule {
		re := e
		re.Partition1 = make([]string, len(e.Partition1))
		for j, name := range e.Partition1 {
			info, ok := nodeInfoMap[name]
			if !ok {
				return "", fmt.Errorf("node %q not found in nodeInfoMap", name)
			}
			re.Partition1[j] = info.PeerID
		}
		re.Partition2 = make([]string, len(e.Partition2))
		for j, name := range e.Partition2 {
			info, ok := nodeInfoMap[name]
			if !ok {
				return "", fmt.Errorf("node %q not found in nodeInfoMap", name)
			}
			re.Partition2[j] = info.PeerID
		}
		resolved[i] = re
	}
	data, err := json.Marshal(resolved)
	if err != nil {
		return "", fmt.Errorf("failed to serialize resolved schedule: %w", err)
	}
	return string(data), nil
}

type scheduleJob struct {
	idx      int
	schedule []rankpartitions.RankPartitionEntry
}

type exhaustiveConfig struct {
	runConfig
	NodeInfoMap  map[string]shared.NodeInfo
	Schedules    [][]rankpartitions.RankPartitionEntry
	FailFast     bool
	ProgressPath string
	ProgressMu   *sync.Mutex
	ProgressSt   *progressState
}

// runExhaustive runs all schedules from cfg.Schedules in a worker pool,
// skipping indices already present in cfg.ProgressSt.Completed.
// Returns collected results and whether execution was interrupted.
func runExhaustive(ctx context.Context, cancel context.CancelFunc, cfg exhaustiveConfig, router *NotificationRouter, projectRegistry *ProjectRegistry) ([]TestResult, bool) {
	completedSet := make(map[int]struct{}, len(cfg.ProgressSt.Completed))
	for _, idx := range cfg.ProgressSt.Completed {
		completedSet[idx] = struct{}{}
	}

	jobs := make(chan scheduleJob, cfg.Parallel*2)
	resultsChan := make(chan TestResult, len(cfg.Schedules))

	var wg sync.WaitGroup
	wg.Add(cfg.Parallel)

	for i := 0; i < cfg.Parallel; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				resolved, err := resolveSchedule(job.schedule, cfg.NodeInfoMap)
				if err != nil {
					logger.Errorw("Failed to resolve schedule", "error", err, "idx", job.idx)
					result := TestResult{
						RunID:        uuid.New().String(),
						Success:      false,
						ErrorMessage: fmt.Sprintf("failed to resolve schedule: %v", err),
					}
					if merr := markScheduleComplete(cfg.ProgressPath, cfg.ProgressMu, cfg.ProgressSt, job.idx); merr != nil {
						logger.Errorw("Failed to mark schedule complete", "error", merr, "idx", job.idx)
					}
					if cfg.FailFast {
						cancel()
					}
					resultsChan <- result
					continue
				}

				runCfg := cfg.runConfig
				runCfg.RankPartitionsOriginal = job.schedule
				runCfg.RankPartitionsResolved = resolved

				runID := uuid.New().String()
				startTime := time.Now()
				result := runSingleTest(ctx, runID, runCfg, router, projectRegistry)
				result.Duration = time.Since(startTime)

				if merr := markScheduleComplete(cfg.ProgressPath, cfg.ProgressMu, cfg.ProgressSt, job.idx); merr != nil {
					logger.Errorw("Failed to mark schedule complete", "error", merr, "idx", job.idx)
				}

				if !result.Success && cfg.FailFast {
					cancel()
				}
				resultsChan <- result
			}
		}()
	}

	// Dispatcher: feed jobs to workers, stopping if context is cancelled.
	go func() {
		defer close(jobs)
		for idx, schedule := range cfg.Schedules {
			if _, done := completedSet[idx]; done {
				continue
			}
			select {
			case jobs <- scheduleJob{idx: idx, schedule: schedule}:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var results []TestResult
	for result := range resultsChan {
		results = append(results, result)
	}

	return results, ctx.Err() != nil
}
