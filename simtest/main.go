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
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/compose"
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


	// TODO this is required, because "docker compose up" won't rebuild changed containers when invoked via the API for some reason.
	// Run docker compose build
	logger.Debug("Building docker compose")
	buildCmd := exec.Command("docker", "compose", "build")
	buildCmd.Dir = execDir
	// Only show docker output in verbose mode
	if *verbose {
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr
	} else {
		buildCmd.Stdout = io.Discard
		buildCmd.Stderr = io.Discard
	}
	if err := buildCmd.Run(); err != nil {
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
	composeStack, err := Run(runId, execDir, bearerToken, *verbose)
	if err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Errorw("HTTP server shutdown error", "error", err)
	}

	// Bring down compose stack
	logger.Debug("Bringing down compose stack")
	err = composeStack.Down(
		context.Background(),
		compose.RemoveOrphans(true),
		compose.RemoveVolumes(true),
	)
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

// Run executes "docker compose up" using testcontainers compose module
func Run(runId string, execDir string, bearerToken string, verbose bool) (*compose.DockerCompose, error) {
	ctx := context.Background()

	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}
	logger.Debugw("Found docker-compose.yml", "path", composePath)

	// Prepare environment variables for docker-compose
	env := map[string]string{
		"RUN_ID":         runId,
		"RUNNER_AUTH":    bearerToken,
		"RUNNER_ADDRESS": "172.17.0.1" + *listenPort, // Gateway IP of proxy-network
		"STOP_FRAME":     "2", // TODO: make this configurable
	}
	logger.Debugw("Prepared environment variables", "env", env)

	var logger *log.Logger
	if verbose {
		logger = log.Default()
	} else {
		logger = log.New(io.Discard, "", 0)
	}

	// Create compose stack
	composeStack, err := compose.NewDockerComposeWith(compose.WithLogger(logger), compose.WithStackFiles(composePath))
	if err != nil {
		return nil, fmt.Errorf("failed to create compose stack: %w", err)
	}

	// TODO figure out why Up prints logs even when logger is set to discard. 
	// TODO figure out why Up won't rebuild changed containers.

	// Set environment variables and start services with build
	// testcontainers will build images automatically if they don't exist
	err = composeStack.WithEnv(env).Up(ctx, compose.RemoveOrphans(true), compose.Wait(true))
	if err != nil {
		return nil, fmt.Errorf("failed to run docker compose up: %w", err)
	}

	return composeStack, nil
}
