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
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var workingDir = flag.String(
	"dir",
	"",
	"working directory containing docker-compose.yml (defaults to current directory)",
)

var listenPort = flag.String(
	"listen",
	":8080",
	"port to listen for notifications from proxy",
)

var verbose = flag.Bool(
	"verbose",
	false,
	"enable verbose logging (DEBUG level)",
)

var logger *zap.SugaredLogger

const (
	RunnerErrorExitCode = 1
	TestRunErrorExitCode = 2
)

type NotificationType string

const (
	NotificationTypeTerminalFrame  NotificationType = "terminal_frame_reached"
)

type FrameNotification struct {
	RunID        string           `json:"run_id,omitempty"`
	FrameNumber  uint64           `json:"frame_number"`
	Type         NotificationType `json:"type"`
	SafetyError string           `json:"safety_error,omitempty"`
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

	// Set up logger based on verbose flag
	var zapLogger *zap.Logger
	var err error
	if *verbose {
		// Development logger with debug level
		config := zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		zapLogger, err = config.Build()
	} else {
		// Production logger with warn level
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
		zapLogger, err = config.Build()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(RunnerErrorExitCode)
	}
	defer zapLogger.Sync()
	logger = zapLogger.Sugar()

	// Create root context for all operations
	ctx := context.Background()

	// Create uuid for this test run
	runId := uuid.New().String()
	logger.Debugw("Generated run ID", "run_id", runId)

	// Generate bearer token for authentication
	bearerToken, err := generateBearerToken()
	if err != nil {
		logger.Errorw("Failed to generate bearer token", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	// Get the directory where docker-compose.yml is located
	execDir := *workingDir
	if execDir == "" {
		// Fall back to current working directory
		cwd, err := os.Getwd()
		if err != nil {
			logger.Errorw("Failed to get current directory", "error", err)
			os.Exit(RunnerErrorExitCode)
		}
		execDir = cwd
	}
	logger.Debugw("Using working directory", "dir", execDir)


	// Build docker compose services
	logger.Debug("Building docker compose")
	projectName := fmt.Sprintf("test_run_%s", runId)
	if err = dockerComposeBuild(ctx, execDir, *verbose); err != nil {
		logger.Errorw("Failed to build docker compose", "error", err)
		os.Exit(RunnerErrorExitCode)
	}

	frameNotificationChan := make(chan FrameNotification, 100)

	// Start HTTP server to listen for notifications
	logger.Debugw("Starting HTTP server", "listen", *listenPort)
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

		// Check for Bearer prefix and extract token
		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Invalid authorization header format", http.StatusUnauthorized)
			return
		}

		// Use constant time comparison to prevent timing attacks
		token := strings.TrimPrefix(authHeader, bearerPrefix)
		if subtle.ConstantTimeCompare([]byte(token), []byte(bearerToken)) != 1 {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}

		var notification FrameNotification
		if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
			http.Error(w, fmt.Sprintf("Failed to decode notification: %v", err), http.StatusBadRequest)
			return
		}

		// Verify run ID matches
		if notification.RunID != runId {
			logger.Warnw("Notification run_id does not match",
				"notification_run_id", notification.RunID,
				"expected_run_id", runId)
		}

		w.WriteHeader(http.StatusOK)

		// Signal shutdown only for terminal frame
		if notification.Type == NotificationTypeTerminalFrame {
			frameNotificationChan <- notification
		}
	})

	server := &http.Server{
		Addr:    *listenPort,
		Handler: mux,
	}

	// Start server in background
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorw("HTTP server error", "error", err)
		}
	}()

	// Start compose stack
	logger.Debug("Starting compose stack")
	if err = Run(ctx, runId, execDir, bearerToken, projectName, *verbose); err != nil {
		logger.Errorw("Failed to start compose stack", "error", err)
		os.Exit(RunnerErrorExitCode)
	}
	logger.Info("Compose stack started successfully")

	// Wait for notification or interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var notification *FrameNotification
	select {
	case n := <-frameNotificationChan:
		logger.Infow("Terminal frame reached, shutting down", "frame_number", n.FrameNumber)
		notification = &n
	case sig := <-sigChan:
		logger.Infow("Received signal, shutting down", "signal", sig)
	}

	// Shutdown HTTP server
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorw("HTTP server shutdown error", "error", err)
	}

	// Bring down compose stack
	logger.Debug("Bringing down compose stack")
	err = dockerComposeDown(ctx, execDir, projectName, *verbose)

	if notification != nil && notification.SafetyError != "" {
		logger.Errorw("Test FAILED", "error", notification.SafetyError)
		os.Exit(TestRunErrorExitCode)
	} else if err == nil {
		logger.Info("Test SUCCEEDED")
	} else {
		logger.Errorw("Failed to stop compose stack", "error", err)
		os.Exit(RunnerErrorExitCode)
	}
}

// Run executes "docker compose up" using CLI commands
func Run(ctx context.Context, runId string, execDir string, bearerToken string, projectName string, verbose bool) error {
	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}
	logger.Debugw("Found docker-compose.yml", "path", composePath, "project", projectName)

	// Prepare environment variables for docker-compose
	env := map[string]string{
		"RUN_ID":         runId,
		"RUNNER_AUTH":    bearerToken,
		"RUNNER_ADDRESS": "172.17.0.1" + *listenPort,
		"STOP_FRAME":     "2", // TODO: make this configurable
	}
	logger.Debugw("Prepared environment variables", "env", env)

	// Start services with environment variables
	if err := dockerComposeUp(ctx, execDir, projectName, env, verbose); err != nil {
		return fmt.Errorf("failed to start compose stack: %w", err)
	}

	return nil
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
func dockerComposeUp(ctx context.Context, workDir string, projectName string, env map[string]string, verbose bool) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "up",
		"-d",              // detached mode
		"--wait",          // wait for services to be healthy
		"--remove-orphans", // remove orphaned containers
		"--no-build",      // don't build images (already done separately)
	)
	cmd.Dir = workDir

	// Set environment variables by extending the current environment
	cmd.Env = os.Environ()
	for key, value := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
	}

	if verbose {
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
func dockerComposeDown(ctx context.Context, workDir string, projectName string, verbose bool) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "down",
		"--remove-orphans", // remove orphaned containers
		"--volumes",        // remove named volumes
	)
	cmd.Dir = workDir

	if verbose {
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
