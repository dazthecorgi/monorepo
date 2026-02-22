package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
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

var partition1 = flag.String(
	"partition1",
	"",
	"comma-separated node names for partition group 1 (e.g. archive-1,archive-2)",
)

var partition2 = flag.String(
	"partition2",
	"",
	"comma-separated node names for partition group 2 (e.g. archive-3,archive-4)",
)

var logger *zap.SugaredLogger

const (
	RunnerErrorExitCode  = 1
	TestRunErrorExitCode = 2
	InterruptExitCode    = 130
)

type TestResult struct {
	RunID        string
	Success      bool
	ErrorMessage string
	Duration     time.Duration
}

type NotificationRouter struct {
	mu       sync.RWMutex
	channels map[string]chan shared.FrameNotification
}

func NewNotificationRouter() *NotificationRouter {
	return &NotificationRouter{
		channels: make(map[string]chan shared.FrameNotification),
	}
}

func (nr *NotificationRouter) Register(runID string, ch chan shared.FrameNotification) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.channels[runID] = ch
}

func (nr *NotificationRouter) Unregister(runID string) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	delete(nr.channels, runID)
}

func (nr *NotificationRouter) Route(notification shared.FrameNotification) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	if ch, ok := nr.channels[notification.RunID]; ok {
		ch <- notification
	} else {
		logger.Warnw("Notification for unknown run ID", "run_id", notification.RunID)
	}
}

type ProjectRegistry struct {
	mu       sync.Mutex
	projects map[string]bool
}

func NewProjectRegistry() *ProjectRegistry {
	return &ProjectRegistry{
		projects: make(map[string]bool),
	}
}

func (pr *ProjectRegistry) Register(projectName string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.projects[projectName] = true
}

func (pr *ProjectRegistry) Unregister(projectName string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	delete(pr.projects, projectName)
}

func (pr *ProjectRegistry) GetAll() []string {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	var result []string
	for name := range pr.projects {
		result = append(result, name)
	}
	return result
}

// generateBearerToken creates a secure random bearer token
func generateBearerToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func main() {
	flag.Parse()

	if (*partition1 == "") != (*partition2 == "") {
		fmt.Fprintf(os.Stderr, "Error: -partition1 and -partition2 must be specified together\n")
		os.Exit(RunnerErrorExitCode)
	}

	// Set up logger based on verbose flag
	var zapLogger *zap.Logger
	var err error
	if *verbose {
		// Development logger with debug level
		config := zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		zapLogger, err = config.Build()
	} else {
		// Production logger with info level
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		zapLogger, err = config.Build()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(RunnerErrorExitCode)
	}
	defer zapLogger.Sync()
	logger = zapLogger.Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get working directory
	execDir := *workingDir
	if execDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			logger.Errorw("Failed to get current directory", "error", err)
			os.Exit(RunnerErrorExitCode)
		}
		execDir = cwd
	}

	// Build images ONCE (shared by all test runs)
	if err := dockerComposeBuild(ctx, execDir, *verbose); err != nil {
		logger.Errorw("Failed to build docker compose", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	// Create notification router
	router := NewNotificationRouter()

	// Generate bearer token once (shared by all runs)
	bearerToken, err := generateBearerToken()
	if err != nil {
		logger.Errorw("Failed to generate bearer token", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	// Start shared HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/run-notification", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Validate bearer token
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Missing authorization header", http.StatusUnauthorized)
			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Invalid authorization header format", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, bearerPrefix)
		if subtle.ConstantTimeCompare([]byte(token), []byte(bearerToken)) != 1 {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}

		var notification shared.FrameNotification
		if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
			http.Error(w, fmt.Sprintf("Failed to decode notification: %v", err), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)

		logger.Debugw("Received notification", "run_id", notification.RunID, "frame_number", notification.FrameNumber, "type", notification.Type, "safety_error", notification.SafetyError)

		// Route notification to correct test run
		if notification.Type == shared.NotificationTypeTerminalFrame {
			router.Route(notification)
		}
	})

	server := &http.Server{
		Addr:    ":" + *listenPort,
		Handler: mux,
	}

	// Start server in background
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorw("HTTP server error", "error", err)
		}
	}()
	logger.Debugw("HTTP notification server started", "port", *listenPort)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	projectRegistry := NewProjectRegistry()

	// Handle signals in background
	go func() {
		sig := <-sigChan
		logger.Infow("Received interrupt signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	// Spawn parallel test runs
	var wg sync.WaitGroup
	wg.Add(*parallel)
	resultsChan := make(chan TestResult, *parallel)

	for i := 0; i < *parallel; i++ {
		go func(runNumber int) {
			defer wg.Done()

			runID := uuid.New().String()
			logger.Debugw("Starting test run", "run_number", runNumber+1, "run_id", runID)

			startTime := time.Now()
			result := runSingleTest(ctx, runID, execDir, bearerToken, router, *verbose, *stopFrame, projectRegistry)
			result.Duration = time.Since(startTime)

			resultsChan <- result
		}(i)
	}

	// Wait for all runs to complete in background
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results as they complete
	var results []TestResult
	for result := range resultsChan {
		results = append(results, result)
	}

	// Check if we were interrupted
	interrupted := ctx.Err() != nil

	// Shutdown HTTP server gracefully
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorw("HTTP server shutdown error", "error", err)
	}

	// Cleanup any remaining active projects (safety net for interrupt case)
	if interrupted {
		activeProjects := projectRegistry.GetAll()
		if len(activeProjects) > 0 {
			logger.Infow("Cleaning up remaining Docker Compose projects", "count", len(activeProjects))
			for _, projectName := range activeProjects {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := dockerComposeDown(cleanupCtx, execDir, projectName, *verbose, *parallel); err != nil {
					logger.Errorw("Failed to cleanup project", "error", err, "project", projectName)
				}
				cleanupCancel()
			}
		}
	}

	// Print summary report
	printSummary(results, interrupted)

	// Exit with appropriate code
	if interrupted {
		logger.Warnw("Tests interrupted by signal", "completed", len(results), "expected", *parallel)
		os.Exit(InterruptExitCode)
	}
	if hasFailures(results) {
		os.Exit(TestRunErrorExitCode)
	}
	os.Exit(0)
}

func runSingleTest(ctx context.Context, runID string, execDir string, bearerToken string, router *NotificationRouter, verbose bool, stopFrame int, projectRegistry *ProjectRegistry) TestResult {
	// Create notification channel for this run
	notifChan := make(chan shared.FrameNotification, 10)
	router.Register(runID, notifChan)
	defer router.Unregister(runID)
	defer close(notifChan)

	projectName := fmt.Sprintf("simtest_run_%s", runID)

	// Start compose stack
	if err := executeTest(ctx, runID, execDir, bearerToken, projectName, stopFrame, verbose, *parallel); err != nil {
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

		if err := dockerComposeDown(cleanupCtx, execDir, projectName, verbose, *parallel); err != nil {
			logger.Errorw("Failed to cleanup compose stack", "error", err, "run_id", runID, "project", projectName)
		}

		projectRegistry.Unregister(projectName)
	}()

	// Wait for notification or context cancellation
	var notification *shared.FrameNotification
	select {
	case n := <-notifChan:
		logger.Debugw("Terminal frame reached", "run_id", runID, "frame_number", n.FrameNumber)
		notification = &n
	case <-ctx.Done():
		logger.Debugw("Test run cancelled", "run_id", runID, "reason", ctx.Err())
	}

	// Determine result based on notification
	if notification != nil && notification.SafetyError != "" {
		return TestResult{
			RunID:        runID,
			Success:      false,
			ErrorMessage: notification.SafetyError,
		}
	}

	return TestResult{
		RunID:   runID,
		Success: true,
	}
}

// resolveNodePeerIDs reads the peer ID for each named node from its config.yml comment.
// Each config.yml starts with a line of the form: "# Peer id: QmXXX..."
func resolveNodePeerIDs(execDir string, nodeNames []string) ([]string, error) {
	var peerIDs []string
	for _, name := range nodeNames {
		name = strings.TrimSpace(name)
		configFile := filepath.Join(execDir, "config", name+"-config", "config.yml")
		data, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config for node %s: %w", name, err)
		}
		// First line format: "# Peer id: QmXXX..."
		firstLine := strings.SplitN(string(data), "\n", 2)[0]
		firstLine = strings.TrimSpace(firstLine)
		const prefix = "# Peer id: "
		if !strings.HasPrefix(firstLine, prefix) {
			return nil, fmt.Errorf("config for node %s does not contain peer ID on first line (expected '# Peer id: ...')", name)
		}
		peerID := strings.TrimSpace(strings.TrimPrefix(firstLine, prefix))
		if peerID == "" {
			return nil, fmt.Errorf("empty peer ID in config for node %s", name)
		}
		peerIDs = append(peerIDs, peerID)
	}
	return peerIDs, nil
}

// getArchiveServices discovers archive services from docker-compose.yml
// Returns a comma-separated list of node addresses (e.g., "archive-1:8337,archive-2:8337")
func getArchiveServices(ctx context.Context, workDir string) (string, error) {
	// Use docker compose config --services to list all services
	cmd := exec.CommandContext(ctx, "docker", "compose", "config", "--services")
	cmd.Dir = workDir

	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to list docker compose services: %w", err)
	}

	// Parse service names and filter for archive-* services
	services := strings.Split(strings.TrimSpace(string(output)), "\n")
	var archiveAddresses []string

	for _, service := range services {
		service = strings.TrimSpace(service)
		if strings.HasPrefix(service, "archive-") {
			// Archive nodes expose global consensus gRPC on port 8340
			archiveAddresses = append(archiveAddresses, fmt.Sprintf("%s:8340", service))
		}
	}

	if len(archiveAddresses) == 0 {
		return "", fmt.Errorf("no archive node addresses found")
	}

	return strings.Join(archiveAddresses, ","), nil
}

// executeTest executes "docker compose up" using CLI commands
func executeTest(ctx context.Context, runId string, execDir string, bearerToken string, projectName string, stopFrame int, verbose bool, parallel int) error {
	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}
	logger.Debugw("Found docker-compose.yml", "path", composePath, "project", projectName)

	// Discover archive services for multi-node synchronization
	nodeAddresses, err := getArchiveServices(ctx, execDir)
	if err != nil {
		return err
	}

	// Prepare environment variables for docker-compose
	env := map[string]string{
		"RUN_ID":         runId,
		"RUNNER_AUTH":    bearerToken,
		"RUNNER_ADDRESS": "host.docker.internal:" + strings.TrimPrefix(*listenPort, ":"),
		"STOP_FRAME":     fmt.Sprintf("%d", stopFrame),
		"NODE_ADDRESSES": nodeAddresses,
		"MIN_NODES":      fmt.Sprintf("%d", *minNodes),
	}

	if *partition1 != "" {
		peerIDs1, err := resolveNodePeerIDs(execDir, strings.Split(*partition1, ","))
		if err != nil {
			return fmt.Errorf("failed to resolve partition1 peer IDs: %w", err)
		}
		peerIDs2, err := resolveNodePeerIDs(execDir, strings.Split(*partition2, ","))
		if err != nil {
			return fmt.Errorf("failed to resolve partition2 peer IDs: %w", err)
		}
		env["PARTITION_1"] = strings.Join(peerIDs1, ",")
		env["PARTITION_2"] = strings.Join(peerIDs2, ",")
	}

	// Start services with environment variables
	if err := dockerComposeUp(ctx, execDir, projectName, env, verbose, parallel); err != nil {
		return fmt.Errorf("failed to start compose stack: %w", err)
	}

	return nil
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

// dockerComposeBuild executes "docker compose build" in the specified working directory.
// It respects the verbose flag for output visibility.
func dockerComposeBuild(ctx context.Context, workDir string, verbose bool) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "build")
	cmd.Dir = workDir

	if verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose build failed: %w", err)
	}
	return nil
}

// dockerComposeUp executes "docker compose up" with environment variables.
// It waits for services to be ready based on healthchecks and removes orphaned containers.
// The --no-build flag is used since build is done separately.
func dockerComposeUp(ctx context.Context, workDir string, projectName string, env map[string]string, verbose bool, parallel int) error {
	logger.Debugw("Executing docker compose up", "project", projectName, "env", env)

	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "up",
		"-d",               // detached mode
		"--wait",           // wait for services to be healthy
		"--remove-orphans", // remove orphaned containers
		"--no-build",       // don't build images (already done separately)
	)
	cmd.Dir = workDir

	cmd.Env = make([]string, 0, len(env))
	for key, value := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
	}

	// Suppress output for parallel runs to avoid interleaving logs, but show for single runs if verbose is enabled
	if verbose && parallel == 1 {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up failed: %w", err)
	}
	return nil
}

// dockerComposeDown executes "docker compose down" with cleanup flags.
// It removes containers, networks, orphaned containers, and volumes.
func dockerComposeDown(ctx context.Context, workDir string, projectName string, verbose bool, parallel int) error {
	logger.Debugw("Executing docker compose down", "project", projectName)

	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "down",
		"--remove-orphans", // remove orphaned containers
		"--volumes",        // remove named volumes
	)
	cmd.Dir = workDir

	// Suppress output for parallel runs to avoid interleaving logs, but show for single runs if verbose is enabled
	if verbose && parallel == 1 {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose down failed: %w", err)
	}
	return nil
}
