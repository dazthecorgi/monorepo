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

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"source.quilibrium.com/quilibrium/monorepo/config"
	proxygrpc "source.quilibrium.com/quilibrium/monorepo/simtest/proxy/grpc"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/p2p"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/testing"
	"source.quilibrium.com/quilibrium/monorepo/simtest/rankpartitions"
	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
	"source.quilibrium.com/quilibrium/monorepo/types/channel"
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

func notifyRunner(logger *zap.Logger, runnerAddress, authToken, runID string, stopFrame uint64, notifType shared.NotificationType, frames []*testing.GlobalFrameWrapper, nodesReachedStopFrame, totalNodes int) error {
	var safetyError error
	if len(frames) > 0 || notifType == shared.NotificationTypeTerminalFrame {
		safetyError = testing.CheckSafety(frames)
	}

	var safetyErrorMsg string
	if safetyError != nil {
		safetyErrorMsg = safetyError.Error()
		logger.Error("Safety violation detected", zap.String("error", safetyErrorMsg))
	}

	notification := shared.FrameNotification{
		RunID:                 runID,
		StopFrame:             stopFrame,
		Type:                  notifType,
		SafetyError:           safetyErrorMsg,
		NodesReachedStopFrame: nodesReachedStopFrame,
		TotalNodes:            totalNodes,
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
	nodeInfosStr := os.Getenv("NODE_INFOS")

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
	if nodeInfosStr == "" {
		fmt.Fprintf(os.Stderr, "Error: NODE_INFOS environment variable is required\n")
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

	nodeConfig, err := config.LoadConfig(*configDirectory, "", false)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	nodeConfig.P2P.Network = uint8(*network)

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Starting DHT-only node...")

	globalConsensusChan := make(chan p2p.ConsensusEvent, 100)

	partitioner := p2p.NewNetworkPartitioner()
	blossomSub := p2p.NewBlossomSubProxy(ctx, nodeConfig.P2P, nodeConfig.Engine, logger, p2p.ConfigDir(*configDirectory), globalConsensusChan, partitioner)

	// Parse per-rank partition schedule from RANK_PARTITIONS env var
	var rankPartitions map[uint64]rankpartitions.RankPartitionEntry
	if rpStr := os.Getenv("RANK_PARTITIONS"); rpStr != "" {
		var err error
		rankPartitions, err = rankpartitions.ParseRankPartitions(rpStr)
		if err != nil {
			logger.Fatal("failed to parse RANK_PARTITIONS", zap.Error(err))
		}
		// Validate that all peer IDs are decodable
		for _, e := range rankPartitions {
			for _, p := range e.Partition1 {
				if _, err := peer.Decode(strings.TrimSpace(p)); err != nil {
					logger.Fatal("invalid peer ID in RANK_PARTITIONS partition1",
						zap.String("peer_id", p), zap.Error(err))
				}
			}
			for _, p := range e.Partition2 {
				if _, err := peer.Decode(strings.TrimSpace(p)); err != nil {
					logger.Fatal("invalid peer ID in RANK_PARTITIONS partition2",
						zap.String("peer_id", p), zap.Error(err))
				}
			}
		}
		logger.Info("loaded rank partition schedule",
			zap.Int("entries", len(rankPartitions)))

		// Apply rank-0 entry immediately at startup
		if entry, ok := rankPartitions[0]; ok {
			blossomSub.ApplyPartition(entry.Partition1, entry.Partition2)
		}
	}

	if err := blossomSub.SubscribeToAllMessages(); err != nil {
		logger.Fatal("failed to subscribe to all messages", zap.Error(err))
	}

	// Parse NODE_INFOS JSON
	var nodeInfos []shared.NodeInfo
	if err := json.Unmarshal([]byte(nodeInfosStr), &nodeInfos); err != nil {
		logger.Fatal("failed to parse NODE_INFOS", zap.Error(err))
	}

	for _, n := range nodeInfos {
		if n.PeerID == "" {
			logger.Fatal("node info missing peer ID", zap.String("name", n.Name))
		}
		if n.PeerPrivKey == "" {
			logger.Fatal("node info missing peer private key", zap.String("name", n.Name))
		}
	}

	// Build a PeerAuthenticator for the proxy itself (used for the frame
	// monitor's client-side TLS when polling backend nodes directly).
	proxyAuth := p2p.NewPeerAuthenticator(
		logger,
		nodeConfig.P2P,
		nil, nil, nil, nil, nil,
		map[string]channel.AllowedPeerPolicyType{},
		map[string]channel.AllowedPeerPolicyType{},
	)

	// Pre-build a PeerAuthenticator per node so the proxy can impersonate any
	// caller when forwarding to a backend.
	type nodeIdent struct {
		PeerID peer.ID
		Auth   *p2p.PeerAuthenticator
	}
	nodeIdents := make([]nodeIdent, len(nodeInfos))
	for i, n := range nodeInfos {
		pid, err := peer.Decode(n.PeerID)
		if err != nil {
			logger.Fatal("invalid peer ID in NODE_INFOS",
				zap.String("name", n.Name), zap.String("peer_id", n.PeerID), zap.Error(err))
		}
		cfg := &config.P2PConfig{PeerPrivKey: n.PeerPrivKey}
		nodeIdents[i] = nodeIdent{
			PeerID: pid,
			Auth: p2p.NewPeerAuthenticator(
				logger, cfg,
				nil, nil, nil, nil, nil,
				map[string]channel.AllowedPeerPolicyType{},
				map[string]channel.AllowedPeerPolicyType{},
			),
		}
	}

	var grpcProxy *proxygrpc.GRPCProxy
	if len(nodeInfos) > 0 {
		const grpcBasePort = 9000

		backends := make([]proxygrpc.BackendEntry, 0, len(nodeInfos))
		peerIDToHostname := make(map[peer.ID]string, len(nodeInfos))

		for i, n := range nodeInfos {
			backend := nodeIdents[i]
			peerIDToHostname[backend.PeerID] = n.Hostname

			// Server-side: impersonate the backend node using its private key.
			serverCreds, err := backend.Auth.CreateServerTLSCredentials()
			if err != nil {
				logger.Fatal("failed to create server TLS credentials for backend",
					zap.String("name", n.Name), zap.Error(err))
			}

			// Client-side: for each potential caller, create credentials that
			// carry the caller's identity when connecting to this backend.
			clientCredsPerCaller := make(map[peer.ID]credentials.TransportCredentials, len(nodeIdents))
			for _, caller := range nodeIdents {
				creds, err := caller.Auth.CreateClientTLSCredentials([]byte(backend.PeerID))
				if err != nil {
					logger.Fatal("failed to create client TLS credentials",
						zap.String("caller", caller.PeerID.String()),
						zap.String("backend", n.Name), zap.Error(err))
				}
				clientCredsPerCaller[caller.PeerID] = creds
			}

			ordinal, err := n.Ordinal()
			if err != nil {
				logger.Fatal("failed to extract ordinal from node name",
					zap.String("name", n.Name), zap.Error(err))
			}

			backends = append(backends, proxygrpc.BackendEntry{
				ListenPort:           grpcBasePort + ordinal,
				BackendAddr:          n.StreamAddress(),
				PeerID:               backend.PeerID,
				ServerCreds:          serverCreds,
				ClientCredsPerCaller: clientCredsPerCaller,
			})
		}

		grpcProxy = proxygrpc.NewGRPCProxy(logger, partitioner, backends, peerIDToHostname)
		if err := grpcProxy.Serve(); err != nil {
			logger.Fatal("failed to start gRPC proxy", zap.Error(err))
		}
		logger.Info("gRPC proxy started",
			zap.Int("backends", len(backends)),
			zap.Int("base_port", grpcBasePort))
	}

	logger.Info("DHT node running. Press Ctrl+C to stop.")

	minNodesStr := os.Getenv("MIN_NODES")
	if minNodesStr == "" {
		fmt.Fprintf(os.Stderr, "Error: MIN_NODES environment variable is required\n")
		os.Exit(1)
	}
	minNodes, err := strconv.Atoi(minNodesStr)
	if err != nil || minNodes <= 0 {
		fmt.Fprintf(os.Stderr, "Error: invalid MIN_NODES value '%s'\n", minNodesStr)
		os.Exit(1)
	}

	globalTimeoutStr := os.Getenv("GLOBAL_TIMEOUT")
	nodeCatchupTimeoutStr := os.Getenv("NODE_CATCHUP_TIMEOUT")

	globalTimeout := 2 * time.Minute
	if globalTimeoutStr != "" {
		if d, err := time.ParseDuration(globalTimeoutStr); err == nil {
			globalTimeout = d
		}
	}

	nodeCatchupTimeout := 30 * time.Second
	if nodeCatchupTimeoutStr != "" {
		if d, err := time.ParseDuration(nodeCatchupTimeoutStr); err == nil {
			nodeCatchupTimeout = d
		}
	}

	pollInterval := 5 * time.Second

	logger.Info("Stop conditions", zap.Uint64("stop_frame", stopFrame), zap.Int("min_nodes", minNodes))

	// Build frame monitor targets with TLS credentials for each node.
	targets := make([]testing.NodeTarget, len(nodeInfos))
	for i, n := range nodeInfos {
		pid, _ := peer.Decode(n.PeerID) // already validated above
		creds, err := proxyAuth.CreateClientTLSCredentials([]byte(pid))
		if err != nil {
			logger.Fatal("failed to create frame monitor TLS credentials",
				zap.String("name", n.Name), zap.Error(err))
		}
		targets[i] = testing.NodeTarget{
			Address:  n.StreamAddress(),
			DialOpts: []grpc.DialOption{grpc.WithTransportCredentials(creds)},
		}
	}

	frameMonitor, err := testing.NewFrameMonitor(
		ctx,
		logger,
		stopFrame,
		targets,
		pollInterval,
		minNodes,
		nodeCatchupTimeout,
	)

	if err != nil {
		logger.Fatal("failed to create frame monitor", zap.Error(err))
	}

	// applyRankPartition checks whether there is a partition entry for the given
	// rank and applies it if the rank hasn't been seen before.
	ranksApplied := make(map[uint64]struct{})
	applyRankPartition := func(rank uint64) {
		if rankPartitions == nil {
			return
		}
		if _, seen := ranksApplied[rank]; seen {
			logger.Debug("already applied partition for rank", zap.Uint64("rank", rank))
			return
		}
		ranksApplied[rank] = struct{}{}
		if entry, ok := rankPartitions[rank]; ok {
			logger.Info("applying rank partition",
				zap.Uint64("rank", rank))
			blossomSub.ApplyPartition(entry.Partition1, entry.Partition2)
		} else {
			logger.Info("no rank partition entry found for rank, clearing partitions",
				zap.Uint64("rank", rank))
			blossomSub.ClearPartitions()
		}
	}

	globalTimer := time.NewTimer(globalTimeout)

	// timeoutSendersPerRank tracks unique TimeoutState senders (by prover filter)
	// per rank to detect timeout-based rank advancement conditions.
	timeoutSendersPerRank := make(map[uint64]map[string]struct{})

	go func() {
		for {
			select {
			case event, ok := <-globalConsensusChan:
				if !ok {
					return
				}
				if !event.IsTimeout {
					applyRankPartition(event.Rank)
				} else {
					rank := event.Rank
					senderKey := string(event.SenderAddress)
					if len(senderKey) == 0 {
						break
					}
					senders := timeoutSendersPerRank[rank]
					if senders == nil {
						senders = make(map[string]struct{})
						timeoutSendersPerRank[rank] = senders
					}
					senders[senderKey] = struct{}{}
					count := len(senders)
					n := len(nodeInfos)
					allNodesTimedOut := count >= n
					if allNodesTimedOut {
						logger.Info("advancing rank due to timeout condition",
							zap.Uint64("rank", rank),
							zap.Int("timeout_count", count),
							zap.Int("total_nodes", n),
							zap.Bool("all_nodes_timed_out", allNodesTimedOut),
						)
						applyRankPartition(rank + 1)
					}
				}

				if event.FrameNumber >= stopFrame+1 {
					logger.Info("observed proposal past stop frame, monitoring all nodes now",
						zap.Uint64("event_frame_number", event.FrameNumber),
						zap.Uint64("stop_frame", stopFrame))

					globalTimer.Stop()

					nodesReachedStopFrame, totalNodes := frameMonitor.StartMonitoring()
					logger.Info("all nodes reached terminal frame",
						zap.Int("nodes_reached_stop_frame", nodesReachedStopFrame),
						zap.Int("total_nodes", totalNodes))

					committedFrames := frameMonitor.FetchCommittedFrames()

					err := notifyRunner(logger, runnerAddress, runnerAuthToken, runID,
						stopFrame, shared.NotificationTypeTerminalFrame, committedFrames,
						nodesReachedStopFrame, totalNodes)

					cancel(err)
					return
				}
			case <-globalTimer.C:
				logger.Warn("global timeout expired without seeing stop frame via gossip",
					zap.Duration("global_timeout", globalTimeout),
					zap.Uint64("stop_frame", stopFrame))
				err := notifyRunner(logger, runnerAddress, runnerAuthToken, runID,
					stopFrame, shared.NotificationTypeGlobalTimeout, nil,
					0, len(nodeInfos))
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

	if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
		logger.Error("Context cancelled with cause", zap.Error(cause))
	}

	logger.Info("Shutting down DHT node...")

	frameMonitor.Close()
	blossomSub.Close()
	if grpcProxy != nil {
		grpcProxy.Close()
	}

	os.Exit(0)
}
