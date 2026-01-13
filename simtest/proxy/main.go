package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"source.quilibrium.com/quilibrium/monorepo/config"
	"source.quilibrium.com/quilibrium/monorepo/node/p2p"
	p2ptypes "source.quilibrium.com/quilibrium/monorepo/types/p2p"
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

type ProxyNode struct {
	pubSub p2ptypes.PubSub
}

func NewProxyNode(
	pubSub p2ptypes.PubSub,
) (*ProxyNode, error) {
	return &ProxyNode{
		pubSub: pubSub,
	}, nil
}

func (pn *ProxyNode) Stop() {
	go func() {
		pn.pubSub.Close()
	}()
}

func main() {
	flag.Parse()

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

	blossomSub := p2p.NewBlossomSub(nodeConfig.P2P, nodeConfig.Engine, logger, 0, p2p.ConfigDir(*configDirectory))
	proxyNode, err := NewProxyNode(blossomSub)
	if err != nil {
		logger.Fatal("failed to start proxy node", zap.Error(err))
	}

	logger.Info("DHT node running. Press Ctrl+C to stop.")

	<-done

	logger.Info("Shutting down DHT node...")
	proxyNode.Stop()
}
