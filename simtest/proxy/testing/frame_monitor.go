package testing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
)

// NodeFrameStatus tracks the frame status for a single node
type NodeFrameStatus struct {
	address             string
	firstPolledAt       time.Time
	lastGlobalHeadFrame uint64
	lastPolled          time.Time
	lastSuccessfulPoll  time.Time
	consecutiveFailures int
	err                 error
}

// FrameMonitor polls multiple nodes and tracks when they all reach a target frame
type FrameMonitor struct {
	ctx           context.Context
	logger        *zap.Logger
	stopFrame     uint64
	nodeAddresses []string
	pollInterval  time.Duration
	timeout       time.Duration
	minNodes      int

	clients      map[string]protobufs.GlobalServiceClient
	connections  map[string]*grpc.ClientConn
	nodeStatuses map[string]*NodeFrameStatus
	statusMutex  sync.RWMutex
}

// NodeTarget pairs a node address with gRPC dial options (e.g. TLS credentials).
type NodeTarget struct {
	Address  string
	DialOpts []grpc.DialOption
}

// NewFrameMonitor creates a new FrameMonitor instance. Each NodeTarget
// specifies the address and dial options (including TLS credentials) to use.
func NewFrameMonitor(
	ctx context.Context,
	logger *zap.Logger,
	stopFrame uint64,
	targets []NodeTarget,
	pollInterval time.Duration,
	minNodes int,
	timeout time.Duration,
) (*FrameMonitor, error) {
	nodeAddresses := make([]string, len(targets))
	for i, t := range targets {
		nodeAddresses[i] = t.Address
	}

	fm := FrameMonitor{
		ctx:           ctx,
		logger:        logger,
		stopFrame:     stopFrame,
		nodeAddresses: nodeAddresses,
		pollInterval:  pollInterval,
		timeout:       timeout,
		minNodes:      minNodes,
		clients:       make(map[string]protobufs.GlobalServiceClient),
		connections:   make(map[string]*grpc.ClientConn),
		nodeStatuses:  make(map[string]*NodeFrameStatus),
	}

	for _, target := range targets {
		fm.logger.Debug("creating gRPC client", zap.String("address", target.Address))

		conn, err := grpc.NewClient(target.Address, target.DialOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", target.Address, err)
		}

		client := protobufs.NewGlobalServiceClient(conn)
		fm.connections[target.Address] = conn
		fm.clients[target.Address] = client

		// Initialize node status
		fm.nodeStatuses[target.Address] = &NodeFrameStatus{
			address: target.Address,
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
	minNodes int,
	timeout time.Duration,
	clients map[string]protobufs.GlobalServiceClient,
) *FrameMonitor {
	fm := &FrameMonitor{
		ctx:           ctx,
		logger:        logger,
		stopFrame:     stopFrame,
		nodeAddresses: nodeAddresses,
		pollInterval:  pollInterval,
		timeout:       timeout,
		minNodes:      minNodes,
		clients:       clients,
		connections:   make(map[string]*grpc.ClientConn), // empty for mocks
		nodeStatuses:  make(map[string]*NodeFrameStatus),
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
	if status.firstPolledAt.IsZero() {
		status.firstPolledAt = time.Now()
	}

	if err != nil {
		status.err = err
		status.consecutiveFailures++
		fm.logger.Debug("failed to poll node for stop frame",
			zap.String("address", addr),
			zap.Uint64("stop_frame", fm.stopFrame),
			zap.Int("consecutive_failures", status.consecutiveFailures),
			zap.Error(err))
		return
	}

	// Success - update status
	status.lastGlobalHeadFrame = resp.Frame.GetFrameNumber()
	status.lastSuccessfulPoll = time.Now()
	status.consecutiveFailures = 0
	status.err = nil

	fm.logger.Debug("successfully polled node for stop frame",
		zap.String("address", addr),
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

// checkAllNodesReachedStopFrame checks if enough nodes have reached the stop frame
// Returns true if the condition is met and monitoring should stop
func (fm *FrameMonitor) checkAllNodesReachedStopFrame() bool {
	now := time.Now()
	readyCount := fm.countNodesReachedStopFrame()
	failedCount := 0
	var notReadyNodes []string

	// Enough nodes already reached the stop frame
	if readyCount >= fm.minNodes {
		return true
	}

	for _, status := range fm.nodeStatuses {
		if status.err != nil {
			timeSinceLastSuccess := now.Sub(status.lastSuccessfulPoll)

			// If we've never successfully polled this node, measure from first poll attempt
			if status.lastSuccessfulPoll.IsZero() {
				timeSinceLastSuccess = now.Sub(status.firstPolledAt)
			}

			// Check if node is within grace period
			if timeSinceLastSuccess > fm.timeout {
				fm.logger.Error("node exceeded timeout",
					zap.String("address", status.address),
					zap.Duration("time_since_success", timeSinceLastSuccess),
					zap.Duration("timeout", fm.timeout),
					zap.Error(status.err))
				failedCount++
			} else {
				fm.logger.Debug("error from node, but within timeout",
					zap.String("address", status.address),
					zap.Duration("time_since_success", timeSinceLastSuccess),
					zap.Duration("timeout", fm.timeout),
					zap.Error(status.err))
			}

			notReadyNodes = append(notReadyNodes, status.address)
		}
	}

	// Log summary
	fm.logger.Info("frame convergence check",
		zap.Int("ready", readyCount),
		zap.Int("not_ready", len(notReadyNodes)),
		zap.Int("failed", failedCount),
		zap.Int("min_nodes", fm.minNodes),
		zap.Strings("not_ready_nodes", notReadyNodes))

	// Impossible to reach minimum even if all remaining non-failed nodes succeed
	if len(fm.nodeAddresses)-failedCount < fm.minNodes {
		fm.logger.Error("insufficient nodes available to reach minimum, aborting",
			zap.Int("min_nodes", fm.minNodes),
			zap.Int("failed", failedCount),
			zap.Int("total", len(fm.nodeAddresses)))
		return true
	}

	return false
}

// countNodesReachedStopFrame returns the number of nodes that have reached the stop frame.
// Caller must not hold statusMutex.
func (fm *FrameMonitor) countNodesReachedStopFrame() int {
	fm.statusMutex.RLock()
	defer fm.statusMutex.RUnlock()
	count := 0
	for _, status := range fm.nodeStatuses {
		if status.lastGlobalHeadFrame >= fm.stopFrame && status.err == nil {
			count++
		}
	}
	return count
}

// StartMonitoring begins the polling loop and blocks until all nodes reach stop frame or context is cancelled.
// Returns the number of nodes that reached the stop frame and the total number of nodes.
func (fm *FrameMonitor) StartMonitoring() (int, int) {
	total := len(fm.nodeAddresses)
	fm.logger.Info("starting frame monitoring",
		zap.Int("node_count", total),
		zap.Uint64("stop_frame", fm.stopFrame),
		zap.Duration("poll_interval", fm.pollInterval))

	ticker := time.NewTicker(fm.pollInterval)
	defer ticker.Stop()

	// Do an initial poll immediately
	fm.pollAllNodes()
	if fm.checkAllNodesReachedStopFrame() {
		fm.logger.Debug("all nodes reached stop frame on initial poll")
		return fm.countNodesReachedStopFrame(), total
	}

	for {
		select {
		case <-ticker.C:
			fm.pollAllNodes()
			if fm.checkAllNodesReachedStopFrame() {
				return fm.countNodesReachedStopFrame(), total
			}
		case <-fm.ctx.Done():
			fm.logger.Debug("monitoring cancelled")
			return fm.countNodesReachedStopFrame(), total
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

// FetchCommittedFrames fetches all frames 1..stopFrame from every node that has
// reached the stop frame. 
// Call this after StartMonitoring() returns.
func (fm *FrameMonitor) FetchCommittedFrames() []*GlobalFrameWrapper {
	// Collect addresses of nodes that succeeded.
	fm.statusMutex.RLock()
	var readyAddrs []string
	for _, status := range fm.nodeStatuses {
		if status.lastGlobalHeadFrame >= fm.stopFrame && status.err == nil {
			readyAddrs = append(readyAddrs, status.address)
		}
	}
	fm.statusMutex.RUnlock()

	var (
		mu     sync.Mutex
		frames []*GlobalFrameWrapper
		wg     sync.WaitGroup
	)

	for _, addr := range readyAddrs {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			client := fm.clients[address]
			for frameNum := uint64(1); frameNum <= fm.stopFrame; frameNum++ {
				ctx, cancel := context.WithTimeout(fm.ctx, 5*time.Second)
				resp, err := client.GetGlobalFrame(ctx, &protobufs.GetGlobalFrameRequest{FrameNumber: frameNum})
				cancel()
				if err != nil {
					fm.logger.Warn("failed to fetch committed frame",
						zap.String("address", address),
						zap.Uint64("frame_number", frameNum),
						zap.Error(err))
					continue
				}
				wrapper := &GlobalFrameWrapper{GlobalFrame: resp.Frame}
				if err != nil {
					fm.logger.Warn("failed to get frame identity",
						zap.String("address", address),
						zap.Uint64("frame_number", frameNum),
						zap.Error(err))
					continue
				}
				mu.Lock()
				frames = append(frames, wrapper)
				mu.Unlock()
			}
		}(addr)
	}

	wg.Wait()
	fm.logger.Info("fetched committed frames from nodes",
		zap.Int("node_count", len(readyAddrs)),
		zap.Int("frame_count", len(frames)))
	return frames
}
