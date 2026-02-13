package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"source.quilibrium.com/quilibrium/monorepo/config"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/p2p"
)

var configDirectory = flag.String(
	"config",
	filepath.Join(".", ".config"),
	"the configuration directory",
)

var network = flag.Uint(
	"network",
	0,
	"sets the active network for the node (mainnet = 0, primary testnet = 1)",
)

type NotificationType string

const (
	NotificationTypeTerminalFrame NotificationType = "terminal_frame_reached"
)

type FrameNotification struct {
	RunID       string           `json:"run_id"`
	FrameNumber uint64           `json:"frame_number"`
	Type        NotificationType `json:"type"`
}

func notifyRunner(logger *zap.Logger, runnerAddress, authCredential, runID string, frameNumber uint64) error {
	notification := FrameNotification{
		RunID:       runID,
		FrameNumber: frameNumber,
		Type:        NotificationTypeTerminalFrame,
	}

	jsonData, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	url := fmt.Sprintf("http://%s/frame-notification", runnerAddress)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if authCredential != "" {
		req.Header.Set("Authorization", authCredential)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send notification to runner at %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		logger.Info("Successfully notified runner", zap.Int("status_code", resp.StatusCode))
		return nil
	}

	return fmt.Errorf("runner returned non-success status: %d", resp.StatusCode)
}

func main() {
	flag.Parse()

	// Read environment variables
	runID := os.Getenv("RUN_ID")
	runnerAddress := os.Getenv("RUNNER_ADDRESS")
	stopFrameStr := os.Getenv("STOP_FRAME")
	runnerAuthCredential := os.Getenv("RUNNER_AUTH")

	// Validate required environment variables
	if runID == "" {
		fmt.Fprintf(os.Stderr, "Error: RUN_ID environment variable is required\n")
		os.Exit(1)
	}
	if runnerAddress == "" {
		fmt.Fprintf(os.Stderr, "Error: RUNNER_ADDRESS environment variable is required\n")
		os.Exit(1)
	}
	if runnerAuthCredential == "" {
		fmt.Fprintf(os.Stderr, "Error: RUNNER_AUTH environment variable is required\n")
		os.Exit(1)
	}
	if stopFrameStr == "" {
		fmt.Fprintf(os.Stderr, "Error: STOP_FRAME environment variable is required\n")
		os.Exit(1)
	}

	fmt.Printf("Run ID: %s\n", runID)

	// Parse stopFrame
	stopFrame, err := strconv.ParseUint(stopFrameStr, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid STOP_FRAME value '%s': %v\n", stopFrameStr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
    defer cancel(nil)

	// Set up logger
	logConfig := zap.NewDevelopmentConfig()
	logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := logConfig.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Add run ID to logger context
	logger = logger.With(zap.String("run_id", runID))

	nodeConfig, err := config.LoadConfig(*configDirectory, "", false)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	nodeConfig.P2P.Network = uint8(*network)

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Starting DHT-only node...")

	globalFrameChan := make(chan *protobufs.GlobalFrame, 100)
	blossomSub := p2p.NewBlossomSubProxy(ctx, nodeConfig.P2P, nodeConfig.Engine, logger, p2p.ConfigDir(*configDirectory), globalFrameChan)

	if err := blossomSub.SubscribeToAllMessages(); err != nil {
		logger.Fatal("failed to subscribe to all messages", zap.Error(err))
	}

	logger.Info("DHT node running. Press Ctrl+C to stop.")

	// Monitor global frames until they reach the specified stop frame, then notify the runner and shut down
	go func() {
		for frame := range globalFrameChan {
			if frame.Header.FrameNumber == stopFrame {
				logger.Info("Received terminal frame number, shutting down ", zap.Uint64("frame_number", frame.Header.FrameNumber))

				err := notifyRunner(logger, runnerAddress, runnerAuthCredential, runID, frame.Header.FrameNumber)

				cancel(err)

				return
			}
		}
	}()

	select {
    case <-done:
        logger.Info("Received interrupt signal")
    case <-ctx.Done():
        logger.Info("Regular shutdown initiated")
    }

	if cause := context.Cause(ctx); cause != nil {
		logger.Info("Context cancelled with cause", zap.Error(cause))
	}

	logger.Info("Shutting down DHT node...")
	blossomSub.Close()
	os.Exit(0)
}
