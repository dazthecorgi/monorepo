package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
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

var stopFrame = flag.Uint(
	"stopFrame",
	10,
	"sets the maximum frame number to reach before shutting down",
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

	// Set up logger
	logConfig := zap.NewDevelopmentConfig()
	logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := logConfig.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

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

	// Monitor global frames for frame number 10
	go func() {
		for frame := range globalFrameChan {
			if frame.Header.FrameNumber == uint64(*stopFrame) {
				logger.Info("Received terminal frame number, shutting down ", zap.Uint64("frame_number", frame.Header.FrameNumber))
				cancel()
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

	logger.Info("Shutting down DHT node...")
	blossomSub.Close()
	os.Exit(0)
}
