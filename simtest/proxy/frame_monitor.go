package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
)

// NodeFrameStatus tracks the frame status for a single node
type NodeFrameStatus struct {
	address             string
	lastGlobalHeadFrame uint64
	lastPolled          time.Time
	lastSuccessfulPoll  time.Time
	consecutiveFailures int
	err                 error
}

// FrameMonitor polls multiple nodes and tracks when they all reach a target frame
type FrameMonitor struct {
	ctx             context.Context
	logger          *zap.Logger
	stopFrame       uint64
	nodeAddresses   []string
	pollInterval    time.Duration
	timeout         time.Duration
	requireAllNodes bool

	clients      map[string]protobufs.GlobalServiceClient
	connections  map[string]*grpc.ClientConn
	nodeStatuses map[string]*NodeFrameStatus
	statusMutex  sync.RWMutex
}

// NewFrameMonitor creates a new FrameMonitor instance
func NewFrameMonitor(
	ctx context.Context,
	logger *zap.Logger,
	stopFrame uint64,
	nodeAddresses []string,
	pollInterval time.Duration,
	requireAllNodes bool,
	timeout time.Duration,
) (*FrameMonitor, error) {
	fm := FrameMonitor{
		ctx:             ctx,
		logger:          logger,
		stopFrame:       stopFrame,
		nodeAddresses:   nodeAddresses,
		pollInterval:    pollInterval,
		timeout:         timeout,
		requireAllNodes: requireAllNodes,
		clients:         make(map[string]protobufs.GlobalServiceClient),
		connections:     make(map[string]*grpc.ClientConn),
		nodeStatuses:    make(map[string]*NodeFrameStatus),
	}

	for _, addr := range fm.nodeAddresses {
		fm.logger.Info("creating gRPC client", zap.String("address", addr))

		// Create gRPC connection with insecure credentials (for local simulation)
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", addr, err)
		}

		client := protobufs.NewGlobalServiceClient(conn)
		fm.connections[addr] = conn
		fm.clients[addr] = client

		// Initialize node status
		fm.nodeStatuses[addr] = &NodeFrameStatus{
			address: addr,
		}
	}
	return &fm, nil
}

// NewFrameMonitorWithClients creates a FrameMonitor with provided clients for testing
func NewFrameMonitorWithClients(
	ctx context.Context,
	logger *zap.Logger,
	stopFrame uint64,
	nodeAddresses []string,
	pollInterval time.Duration,
	requireAllNodes bool,
	timeout time.Duration,
	clients map[string]protobufs.GlobalServiceClient,
) *FrameMonitor {
	fm := &FrameMonitor{
		ctx:             ctx,
		logger:          logger,
		stopFrame:       stopFrame,
		nodeAddresses:   nodeAddresses,
		pollInterval:    pollInterval,
		timeout:         timeout,
		requireAllNodes: requireAllNodes,
		clients:         clients,
		connections:     make(map[string]*grpc.ClientConn), // empty for mocks
		nodeStatuses:    make(map[string]*NodeFrameStatus),
	}

	// Initialize node statuses
	for _, addr := range nodeAddresses {
		fm.nodeStatuses[addr] = &NodeFrameStatus{
			address: addr,
		}
	}

	return fm
}

// pollNode queries a single node's current frame status
func (fm *FrameMonitor) pollNode(addr string) {
	client, exists := fm.clients[addr]
	if !exists {
		fm.logger.Error("no client found for address", zap.String("address", addr))
		return
	}

	// Create context with timeout for this specific request
	ctx, cancel := context.WithTimeout(fm.ctx, 5*time.Second)
	defer cancel()

	resp, err := client.GetGlobalFrame(ctx, &protobufs.GetGlobalFrameRequest{FrameNumber: fm.stopFrame})

	fm.statusMutex.Lock()
	defer fm.statusMutex.Unlock()

	status := fm.nodeStatuses[addr]
	status.lastPolled = time.Now()

	if err != nil {
		status.err = err
		status.consecutiveFailures++
		fm.logger.Warn("failed to poll node",
			zap.String("address", addr),
			zap.Int("consecutive_failures", status.consecutiveFailures),
			zap.Error(err))
		return
	}

	// Success - update status
	status.lastGlobalHeadFrame = resp.Frame.GetFrameNumber()
	status.lastSuccessfulPoll = time.Now()
	status.consecutiveFailures = 0
	status.err = nil

	fm.logger.Debug("polled node",
		zap.String("address", addr),
		zap.Uint64("last_global_head_frame", resp.Frame.GetFrameNumber()),
		zap.Uint64("stop_frame", fm.stopFrame))
}

// pollAllNodes polls all configured nodes in parallel
func (fm *FrameMonitor) pollAllNodes() {
	var wg sync.WaitGroup

	for _, addr := range fm.nodeAddresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			fm.pollNode(address)
		}(addr)
	}

	wg.Wait()
}

// checkAllNodesReachedStopFrame checks if all nodes have reached the stop frame
// Returns true if the condition is met and monitoring should stop
func (fm *FrameMonitor) checkAllNodesReachedStopFrame() bool {
	fm.statusMutex.RLock()
	defer fm.statusMutex.RUnlock()

	now := time.Now()
	allReady := true
	readyCount := 0
	failedCount := 0
	var notReadyNodes []string

	for _, status := range fm.nodeStatuses {
		// Check if node has reached stop frame
		if status.lastGlobalHeadFrame >= fm.stopFrame && status.err == nil {
			readyCount++
			fm.logger.Info("node reached stop frame",
				zap.String("address", status.address),
				zap.Uint64("frame", status.lastGlobalHeadFrame))
			continue
		}

		// Check if node is within grace period
		if status.err != nil {
			timeSinceLastSuccess := now.Sub(status.lastSuccessfulPoll)

			// If we've never successfully polled this node, use lastPolled instead
			if status.lastSuccessfulPoll.IsZero() {
				timeSinceLastSuccess = now.Sub(status.lastPolled)
			}

			if timeSinceLastSuccess > fm.timeout {
				fm.logger.Error("node exceeded timeout",
					zap.String("address", status.address),
					zap.Duration("time_since_success", timeSinceLastSuccess),
					zap.Duration("timeout", fm.timeout),
					zap.Error(status.err))

				if fm.requireAllNodes {
					// If we require all nodes, this is a failure condition
					failedCount++
				}
			} else {
				fm.logger.Debug("error from node, but within timeout",
					zap.String("address", status.address),
					zap.Duration("time_since_success", timeSinceLastSuccess),
					zap.Duration("timeout", fm.timeout),
					zap.Error(status.err))
			}
		}

		notReadyNodes = append(notReadyNodes, status.address)
		allReady = false
	}

	// Log summary
	fm.logger.Info("frame convergence check",
		zap.Int("ready", readyCount),
		zap.Int("not_ready", len(notReadyNodes)),
		zap.Int("failed", failedCount),
		zap.Strings("not_ready_nodes", notReadyNodes),
		zap.Bool("all_ready", allReady))

	// If we require all nodes and any have failed beyond grace period, fail immediately
	if fm.requireAllNodes && failedCount > 0 {
		fm.logger.Error("required nodes failed, aborting")
		return true // Stop monitoring, will be handled as error
	}

	return allReady
}

// startMonitoring begins the polling loop and blocks until all nodes reach stop frame or context is cancelled
func (fm *FrameMonitor) startMonitoring() {
	fm.logger.Info("starting frame monitoring",
		zap.Int("node_count", len(fm.nodeAddresses)),
		zap.Uint64("stop_frame", fm.stopFrame),
		zap.Duration("poll_interval", fm.pollInterval))

	ticker := time.NewTicker(fm.pollInterval)
	defer ticker.Stop()

	// Do an initial poll immediately
	fm.pollAllNodes()
	if fm.checkAllNodesReachedStopFrame() {
		fm.logger.Info("all nodes reached stop frame on initial poll")
		return
	}

	for {
		select {
		case <-ticker.C:
			fm.pollAllNodes()
			if fm.checkAllNodesReachedStopFrame() {
				return
			}
		case <-fm.ctx.Done():
			fm.logger.Info("monitoring cancelled")
			return
		}
	}
}

// Close closes all gRPC connections
func (fm *FrameMonitor) Close() {
	for addr, conn := range fm.connections {
		if err := conn.Close(); err != nil {
			fm.logger.Warn("failed to close connection",
				zap.String("address", addr),
				zap.Error(err))
		}
	}
}
