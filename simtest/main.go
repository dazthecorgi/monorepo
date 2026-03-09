package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)

var workingDir = flag.String(
	"dir",
	"",
	"working directory containing docker-compose.yml (defaults to current directory)",
)

var listenPort = flag.String(
	"listen",
	"8080",
	"port to listen for notifications from proxy",
)

var verbose = flag.Bool(
	"verbose",
	false,
	"enable verbose logging (DEBUG level)",
)

var stopFrame = flag.Int(
	"stopframe",
	10,
	"frame number at which the simulation should stop",
)

var parallel = flag.Int(
	"parallel",
	1,
	"number of test runs to execute in parallel",
)

var minNodes = flag.Int(
	"minnodes",
	0,
	"minimum number of nodes that must reach the stop frame (0 = all nodes)",
)

var rankPartitions = flag.String(
	"rank-partitions",
	"",
	`JSON array of per-rank partition configs, e.g. '[{"rank":5,"partition1":["archive-1"],"partition2":["archive-3"]}]'`,
)

var outDir = flag.String(
	"out",
	"./out",
	"directory to save artifacts (config, result, logs) for failing test runs",
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
func resolveRankPartitions(execDir, rawJSON string) (original []shared.RankPartitionEntry, resolved string, err error) {
	if rawJSON == "" {
		return nil, "", nil
	}

	parsed, err := shared.ParseRankPartitions(rawJSON)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse -rank-partitions: %w", err)
	}

	// Sort by rank for deterministic order in artifacts.
	ranks := make([]uint64, 0, len(parsed))
	for r := range parsed {
		ranks = append(ranks, r)
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i] < ranks[j] })

	original = make([]shared.RankPartitionEntry, 0, len(parsed))
	for _, r := range ranks {
		original = append(original, parsed[r])
	}

	resolvedEntries := make([]shared.RankPartitionEntry, 0, len(parsed))
	for _, e := range original {
		re := e
		if len(e.Partition1) > 0 {
			idMap, err := resolveNodePeerIDs(execDir, e.Partition1)
			if err != nil {
				return nil, "", fmt.Errorf("failed to resolve rank %d partition1: %w", e.Rank, err)
			}
			re.Partition1 = make([]string, len(e.Partition1))
			for j, n := range e.Partition1 {
				re.Partition1[j] = idMap[strings.TrimSpace(n)]
			}
		}
		if len(e.Partition2) > 0 {
			idMap, err := resolveNodePeerIDs(execDir, e.Partition2)
			if err != nil {
				return nil, "", fmt.Errorf("failed to resolve rank %d partition2: %w", e.Rank, err)
			}
			re.Partition2 = make([]string, len(e.Partition2))
			for j, n := range e.Partition2 {
				re.Partition2[j] = idMap[strings.TrimSpace(n)]
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

func main() {
	flag.Parse()

	zapLogger, err := initLogger(*verbose)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(RunnerErrorExitCode)
	}
	defer zapLogger.Sync()
	logger = zapLogger.Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	execDir := *workingDir
	if execDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			logger.Errorw("Failed to get current directory", "error", err)
			os.Exit(RunnerErrorExitCode)
		}
		execDir = cwd
	}

	nodeAddresses, err := getArchiveServices(ctx, execDir)
	if err != nil {
		logger.Errorw("Failed to get archive services", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	minimumNodes := len(nodeAddresses)
	if *minNodes > 0 {
		minimumNodes = *minNodes
	}

	rankPartitionsOriginal, rankPartitionsResolved, err := resolveRankPartitions(execDir, *rankPartitions)
	if err != nil {
		logger.Errorw("Failed to resolve rank partitions", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	// Build images once at start (shared by all test runs)
	if err := dockerComposeBuild(ctx, execDir, *verbose); err != nil {
		logger.Errorw("Failed to build docker compose", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	router := NewNotificationRouter()

	bearerToken, err := generateBearerToken()
	if err != nil {
		logger.Errorw("Failed to generate bearer token", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	server := startNotificationServer(*listenPort, bearerToken, router)
	projectRegistry := NewProjectRegistry()

	results, interrupted := runAllTests(ctx, cancel, *parallel, execDir, bearerToken, router, *verbose, *stopFrame, projectRegistry, nodeAddresses, minimumNodes, rankPartitionsResolved, rankPartitionsOriginal, *outDir)

	// Shutdown HTTP server gracefully
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorw("HTTP server shutdown error", "error", err)
	}

	if interrupted {
		cleanupActiveProjects(execDir, projectRegistry, *verbose, *parallel)
	}

	printSummary(results, interrupted)

	if interrupted {
		logger.Warnw("Tests interrupted by signal", "completed", len(results), "expected", *parallel)
		os.Exit(InterruptExitCode)
	}
	if hasFailures(results) {
		os.Exit(TestRunErrorExitCode)
	}
	os.Exit(0)
}
