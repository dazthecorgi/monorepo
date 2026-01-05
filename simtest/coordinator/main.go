package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"source.quilibrium.com/quilibrium/monorepo/config"
	"source.quilibrium.com/quilibrium/monorepo/node/p2p/testutil"
)

var defaultListenAddress = "0.0.0.0:8340"

func createP2PConfig(addr string, network uint8) (*config.P2PConfig, error) {
	p2pConfig := (&config.P2PConfig{}).WithDefaults()

	// Generate an Ed448 private key
	privKey, _, err := crypto.GenerateEd448Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create private key: %w", err)
	}

	privKeyBytes, err := privKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("failed to get raw private key bytes: %w", err)
	}
	p2pConfig.PeerPrivKey = hex.EncodeToString(privKeyBytes)

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to split host and port: %w", err)
	}
	p2pConfig.StreamListenMultiaddr = fmt.Sprintf("/ip4/%s/tcp/%s", host, port)

	if network != 0 {
		p2pConfig.Network = network
	}

	return &p2pConfig, nil
}

func main() {
	// Define CLI flags
	listenAddr := flag.String("listen", "", "Pubsub proxy server listen address, defaults to "+defaultListenAddress)
	network := flag.Uint("network", 0, "Network ID")
	flag.Parse()

	// Set default listen address if not provided
	addr := *listenAddr
	if addr == "" {
		addr = defaultListenAddress
	}

	// Set up logger
	logConfig := zap.NewDevelopmentConfig()
	logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := logConfig.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting PubSubProxy archive node",
		zap.String("listen", addr),
		zap.String("network", fmt.Sprintf("%d", *network)))

	// Create P2P config
	p2pConfig, err := createP2PConfig(addr, uint8(*network))
	if err != nil {
		logger.Fatal("Failed to create P2P config", zap.Error(err))
	}

	// Start PubSubProxy server
	mockPubSub := testutil.NewMockPubSub()
	cleanup, err := testutil.StartPubSubProxyServer(mockPubSub, p2pConfig, addr)
	if err != nil {
		logger.Fatal("Failed to start PubSubProxy server", zap.Error(err))
	}
	defer cleanup()

	logger.Info("PubSubProxy archive node is running", zap.String("address", addr))

	// Keep the server running
	select {}
}
