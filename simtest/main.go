package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"source.quilibrium.com/quilibrium/monorepo/simtest/rankpartitions"
	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)

// Shared flags (persistent across all subcommands).
var (
	workingDir        string
	listenPort        string
	verbose           bool
	minNodes          int
	outDir            string
	saveLogsOnSuccess bool
)

// single-mode flags.
var (
	rankPartitions string
	stopFrame         int
)

// exhaustive-mode flags.
var (
	partitionStopRank uint
	seedFlag          int64
	parallel          int
	progressFile      string
	failFast          bool
)

var logger *zap.SugaredLogger

const (
	RunnerErrorExitCode  = 1
	TestRunErrorExitCode = 2
	InterruptExitCode    = 130
)

func initLogger(verbose bool) (*zap.Logger, error) {
	if verbose {
		config := zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		return config.Build()
	}
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	return config.Build()
}

// generateBearerToken creates a secure random bearer token.
func generateBearerToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// resolveRankPartitions parses the raw JSON rank-partitions flag and resolves
// node names to peer IDs. Returns the original entries (with service names) and
// the peer-ID-resolved JSON string for the docker env var. Both are empty/nil if rawJSON is empty.
func resolveRankPartitions(execDir, rawJSON string) (original []rankpartitions.RankPartitionEntry, resolved string, err error) {
	if rawJSON == "" {
		return nil, "", nil
	}

	parsed, err := rankpartitions.ParseRankPartitions(rawJSON)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse -rank-partitions: %w", err)
	}

	// Sort by rank for deterministic order in artifacts.
	ranks := make([]uint64, 0, len(parsed))
	for r := range parsed {
		ranks = append(ranks, r)
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i] < ranks[j] })

	original = make([]rankpartitions.RankPartitionEntry, 0, len(parsed))
	for _, r := range ranks {
		original = append(original, parsed[r])
	}

	resolvedEntries := make([]rankpartitions.RankPartitionEntry, 0, len(parsed))
	for _, e := range original {
		re := e
		if len(e.Partition1) > 0 {
			idMap, err := resolveNodeIdentities(execDir, e.Partition1)
			if err != nil {
				return nil, "", fmt.Errorf("failed to resolve rank %d partition1: %w", e.Rank, err)
			}
			re.Partition1 = make([]string, len(e.Partition1))
			for j, n := range e.Partition1 {
				re.Partition1[j] = idMap[strings.TrimSpace(n)].PeerID
			}
		}
		if len(e.Partition2) > 0 {
			idMap, err := resolveNodeIdentities(execDir, e.Partition2)
			if err != nil {
				return nil, "", fmt.Errorf("failed to resolve rank %d partition2: %w", e.Rank, err)
			}
			re.Partition2 = make([]string, len(e.Partition2))
			for j, n := range e.Partition2 {
				re.Partition2[j] = idMap[strings.TrimSpace(n)].PeerID
			}
		}
		resolvedEntries = append(resolvedEntries, re)
	}

	serialized, err := json.Marshal(resolvedEntries)
	if err != nil {
		return nil, "", fmt.Errorf("failed to serialize rank-partitions: %w", err)
	}
	return original, string(serialized), nil
}

// cleanupActiveProjects tears down any Docker Compose projects still registered
// in the registry (safety net for the interrupt case).
func cleanupActiveProjects(execDir string, registry *ProjectRegistry, verbose bool, parallelRuns int) {
	activeProjects := registry.GetAll()
	if len(activeProjects) == 0 {
		return
	}
	logger.Infow("Cleaning up remaining Docker Compose projects", "count", len(activeProjects))
	for _, projectName := range activeProjects {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := dockerComposeDown(cleanupCtx, execDir, projectName, verbose, parallelRuns); err != nil {
			logger.Errorw("Failed to cleanup project", "error", err, "project", projectName)
		}
		cleanupCancel()
	}
}

// commonState holds setup results shared by both modes.
type commonState struct {
	execDir       string
	nodeInfos []shared.NodeInfo
	minimumNodes  int
}

// prepareRun creates a cancellable context, wires SIGINT/SIGTERM to cancel it,
// resolves the working directory, discovers archive services, and computes the
// effective minimum-nodes threshold.
func prepareRun() (context.Context, context.CancelFunc, commonState, error) {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Infow("Received interrupt signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	execDir := workingDir
	if execDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			cancel()
			return nil, nil, commonState{}, fmt.Errorf("failed to get current directory: %w", err)
		}
		execDir = cwd
	}

	nodeInfos, err := getArchiveServices(ctx, execDir)
	if err != nil {
		cancel()
		return nil, nil, commonState{}, fmt.Errorf("failed to get archive services: %w", err)
	}

	mn := len(nodeInfos)
	if minNodes > 0 {
		mn = minNodes
	}
	return ctx, cancel, commonState{execDir: execDir, nodeInfos: nodeInfos, minimumNodes: mn}, nil
}

// finishRun prints the summary and exits with the appropriate code.
func finishRun(results []TestResult, interrupted bool) {
	printSummary(results, interrupted)
	if interrupted {
		logger.Warnw("Tests interrupted by signal", "completed", len(results))
		os.Exit(InterruptExitCode)
	}
	if hasFailures(results) {
		os.Exit(TestRunErrorExitCode)
	}
	os.Exit(0)
}

// runSingleMode runs a single simulation.
func runSingleMode(ctx context.Context, cancel context.CancelFunc, st commonState) {
	rankPartitionsOriginal, rankPartitionsResolved, err := resolveRankPartitions(st.execDir, rankPartitions)
	if err != nil {
		logger.Errorw("Failed to resolve rank partitions", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	if err := dockerComposeBuild(ctx, st.execDir, verbose); err != nil {
		logger.Errorw("Failed to build docker compose", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	bearerToken, err := generateBearerToken()
	if err != nil {
		logger.Errorw("Failed to generate bearer token", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	router := NewNotificationRouter()
	server := startNotificationServer(listenPort, bearerToken, router)
	projectRegistry := NewProjectRegistry()

	cfg := runConfig{
		ExecDir:                st.execDir,
		BearerToken:            bearerToken,
		Verbose:                verbose,
		StopFrame:              stopFrame,
		Nodes:                  st.nodeInfos,
		MinimumNodes:           st.minimumNodes,
		RankPartitionsResolved: rankPartitionsResolved,
		RankPartitionsOriginal: rankPartitionsOriginal,
		OutDir:                 outDir,
		SaveLogsOnSuccess:      saveLogsOnSuccess,
		Parallel:               1,
	}

	runID := uuid.New().String()
	startTime := time.Now()
	result := runSingleTest(ctx, runID, cfg, router, projectRegistry)
	result.Duration = time.Since(startTime)
	interrupted := ctx.Err() != nil

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorw("HTTP server shutdown error", "error", err)
	}

	if interrupted {
		cleanupActiveProjects(st.execDir, projectRegistry, verbose, 1)
	}

	finishRun([]TestResult{result}, interrupted)
}

// runExhaustiveMode generates every symmetry-unique rank partition schedule,
// shuffles them, and runs them in a worker pool with optional resumption.
func runExhaustiveMode(ctx context.Context, cancel context.CancelFunc, st commonState) {
	nodeNames := make([]string, len(st.nodeInfos))
	nodeInfoMap := make(map[string]shared.NodeInfo, len(st.nodeInfos))
	for i, n := range st.nodeInfos {
		nodeNames[i] = n.Name
		nodeInfoMap[n.Name] = n
	}

	var schedules [][]rankpartitions.RankPartitionEntry
	for sched := range rankpartitions.AllRankPartitions(nodeNames, uint64(partitionStopRank)) {
		schedules = append(schedules, sched)
	}

	seed := seedFlag
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	logger.Infow("Exhaustive mode", "seed", seed, "total_schedules", len(schedules))
	mrand.New(mrand.NewSource(seed)).Shuffle(len(schedules), func(i, j int) { //nolint:gosec
		schedules[i], schedules[j] = schedules[j], schedules[i]
	})

	progressSt, err := loadOrCreateProgressFile(progressFile, seed, nodeNames, uint64(partitionStopRank), schedules)
	if err != nil {
		logger.Errorw("Failed to load/create progress file", "error", err)
		os.Exit(RunnerErrorExitCode)
	}
	logger.Infow("Progress", "already_completed", len(progressSt.Completed), "total", len(schedules))

	if err := dockerComposeBuild(ctx, st.execDir, verbose); err != nil {
		logger.Errorw("Failed to build docker compose", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	bearerToken, err := generateBearerToken()
	if err != nil {
		logger.Errorw("Failed to generate bearer token", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	router := NewNotificationRouter()
	server := startNotificationServer(listenPort, bearerToken, router)
	projectRegistry := NewProjectRegistry()

	excfg := exhaustiveConfig{
		runConfig: runConfig{
			ExecDir:           st.execDir,
			BearerToken:       bearerToken,
			Verbose:           verbose,
			StopFrame:         int(partitionStopRank) + 1,
			Nodes:             st.nodeInfos,
			MinimumNodes:      st.minimumNodes,
			OutDir:            outDir,
			SaveLogsOnSuccess: saveLogsOnSuccess,
			Parallel:     parallel,
		},
		NodeInfoMap:  nodeInfoMap,
		Schedules:    progressSt.Schedules,
		FailFast:     failFast,
		ProgressPath: progressFile,
		ProgressMu:   &sync.Mutex{},
		ProgressSt:   progressSt,
	}
	results, interrupted := runExhaustive(ctx, cancel, excfg, router, projectRegistry)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorw("HTTP server shutdown error", "error", err)
	}

	if interrupted {
		cleanupActiveProjects(st.execDir, projectRegistry, verbose, parallel)
	}

	finishRun(results, interrupted)
}

func runSingleCmd(cmd *cobra.Command, args []string) error {
	ctx, cancel, st, err := prepareRun()
	defer cancel()
	if err != nil {
		logger.Errorw("Setup failed", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	runSingleMode(ctx, cancel, st)
	return nil
}

func runExhaustiveCmd(cmd *cobra.Command, args []string) error {
	ctx, cancel, st, err := prepareRun()
	defer cancel()
	if err != nil {
		logger.Errorw("Setup failed", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	runExhaustiveMode(ctx, cancel, st)
	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:           "simtest <mode>",
		Short:         "Simulation test runner",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			zapLogger, err := initLogger(verbose)
			if err != nil {
				return fmt.Errorf("failed to initialize logger: %w", err)
			}
			logger = zapLogger.Sugar()
			return nil
		},
	}

	pf := rootCmd.PersistentFlags()
	pf.StringVar(&workingDir, "dir", "", "working directory containing docker-compose.yml (defaults to current directory)")
	pf.StringVar(&listenPort, "listen", "8080", "port to listen for notifications from proxy")
	pf.BoolVar(&verbose, "verbose", false, "enable verbose logging (DEBUG level)")
	pf.StringVar(&outDir, "out", "./out", "directory to save artifacts (config, result, logs) for failing test runs")
	pf.BoolVar(&saveLogsOnSuccess, "save-logs-on-success", false, "also save artifacts (config, result, logs) for successful test runs")

	singleCmd := &cobra.Command{
		Use:   "single",
		Short: "Run a single simulation",
		RunE:  runSingleCmd,
	}
	singleCmd.Flags().IntVar(&stopFrame, "stopframe", 10, "frame number at which the simulation should stop")
	singleCmd.Flags().IntVar(&minNodes, "minnodes", 0, "minimum number of nodes that must reach the stop frame (0 = all nodes)")
	singleCmd.Flags().StringVar(&rankPartitions, "rank-partitions", "",
		`JSON array of per-rank partition configs, e.g. '[{"rank":5,"partition1":["archive-1"],"partition2":["archive-3"]}]'`)

	exhaustiveCmd := &cobra.Command{
		Use:   "exhaustive",
		Short: "Run every symmetry-unique rank partition schedule",
		RunE:  runExhaustiveCmd,
	}
	exhaustiveCmd.Flags().IntVar(&parallel, "parallel", 1, "number of schedules to run in parallel")
	exhaustiveCmd.Flags().UintVar(&partitionStopRank, "partition-stop-rank", 0,
		"max rank for AllRankPartitions; required for multi-node setups")
	exhaustiveCmd.Flags().Int64Var(&seedFlag, "seed", 0,
		"RNG seed for shuffling schedules (0 = time-based); actual seed is always logged")
	exhaustiveCmd.Flags().StringVar(&progressFile, "progress-file", "",
		"path to progress file; enables resumption of interrupted runs")
	exhaustiveCmd.Flags().BoolVar(&failFast, "fail-fast", false,
		"cancel remaining jobs after the first failure")

	rootCmd.AddCommand(singleCmd, exhaustiveCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(RunnerErrorExitCode)
	}
}
