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
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"source.quilibrium.com/quilibrium/monorepo/config"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/p2p"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/testing"
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
	NotificationTypeTerminalFrame  NotificationType = "terminal_frame_reached"
)

type FrameNotification struct {
	RunID        string           `json:"run_id"`
	FrameNumber  uint64           `json:"frame_number"`
	Type         NotificationType `json:"type"`
	SafetyError string           `json:"safety_error,omitempty"`
}

func notifyRunner(logger *zap.Logger, runnerAddress, authToken, runID string, frameNumber uint64, notifType NotificationType, frames []*testing.GlobalFrameWrapper) error {
	safetyError := testing.CheckSafety(frames)

	var safetyErrorMsg string
	if safetyError != nil {
		safetyErrorMsg = safetyError.Error()
		logger.Error("Safety violation detected", zap.String("error", safetyErrorMsg))
	}

	notification := FrameNotification{
		RunID:        runID,
		FrameNumber:  frameNumber,
		Type:         notifType,
		SafetyError: safetyErrorMsg,
	}

	jsonData, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	url := fmt.Sprintf("http://%s/run-notification", runnerAddress)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	authCredential := fmt.Sprintf("Bearer %s", authToken)
	req.Header.Set("Authorization", authCredential)

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
	runnerAuthToken := os.Getenv("RUNNER_AUTH")
	nodeAddressesStr := os.Getenv("NODE_ADDRESSES")

	// Validate required environment variables
	if runID == "" {
		fmt.Fprintf(os.Stderr, "Error: RUN_ID environment variable is required\n")
		os.Exit(1)
	}
	if runnerAddress == "" {
		fmt.Fprintf(os.Stderr, "Error: RUNNER_ADDRESS environment variable is required\n")
		os.Exit(1)
	}
	if runnerAuthToken == "" {
		fmt.Fprintf(os.Stderr, "Error: RUNNER_AUTH environment variable is required\n")
		os.Exit(1)
	}
	if stopFrameStr == "" {
		fmt.Fprintf(os.Stderr, "Error: STOP_FRAME environment variable is required\n")
		os.Exit(1)
	}
	if nodeAddressesStr == "" {
		fmt.Fprintf(os.Stderr, "Error: NODE_ADDRESSES environment variable is required\n")
		os.Exit(1)
	}	

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

	logger.Info("Stop frame", zap.Uint64("stop_frame", stopFrame))

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

	globalFrames := make([]*testing.GlobalFrameWrapper, 0, stopFrame)

	// TODO move these to config
	pollInterval := 5 * time.Second
	timeout := 60 * time.Second

	nodeAddresses := strings.Split(strings.TrimSpace(nodeAddressesStr), ",")

	minNodesStr := os.Getenv("MIN_NODES")
	minNodes := len(nodeAddresses)
	if minNodesStr != "" {
		n, err := strconv.Atoi(minNodesStr)
		if err != nil || n <= 0 {
			fmt.Fprintf(os.Stderr, "Error: invalid MIN_NODES value '%s'\n", minNodesStr)
			os.Exit(1)
		}
		minNodes = n
	}

	frameMonitor, err := testing.NewFrameMonitor(
		ctx,
		logger,
		stopFrame,
		nodeAddresses,
		pollInterval,
		minNodes,
		timeout,
	)

	if err != nil {
		logger.Fatal("failed to create frame monitor", zap.Error(err))
	}

	// Continue collecting frames from BlossomSub for safety checking
	go func() {
		for frame := range globalFrameChan {
			globalFrames = append(globalFrames, &testing.GlobalFrameWrapper{GlobalFrame: frame})
			logger.Debug("received global frame",
				zap.Uint64("frame_number", frame.Header.FrameNumber))
		}
	}()

	go func() {
		for frame := range globalFrameChan {
			frameNumber := frame.Header.FrameNumber
			globalFrames = append(globalFrames, &testing.GlobalFrameWrapper{GlobalFrame: frame})

			if frameNumber == stopFrame {
				logger.Info("received terminal frame over gossip network, monitoring all nodes now",
					zap.Uint64("frame_number", frameNumber))

				frameMonitor.StartMonitoring()
				logger.Info("all nodes reached terminal frame")

				err := notifyRunner(logger, runnerAddress, runnerAuthToken, runID,
					frameNumber, NotificationTypeTerminalFrame, globalFrames)

				cancel(err)
				return
			}
	}}()

	select {
    case <-done:
        logger.Info("Received interrupt signal")
    case <-ctx.Done():
        logger.Info("Regular shutdown initiated")
    }

	if cause := context.Cause(ctx); cause != nil {
		logger.Error("Context cancelled with cause", zap.Error(cause))
	}

	logger.Info("Shutting down DHT node...")

	frameMonitor.Close()
	blossomSub.Close()
	
	os.Exit(0)
}
