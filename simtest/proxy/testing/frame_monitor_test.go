package testing

import (
	"context"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/mocks"
)

func TestFrameMonitorHappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	stopFrame := uint64(100)
	nodeAddresses := []string{"node1:8337", "node2:8337"}
	pollInterval := 50 * time.Millisecond
	timeout := 5 * time.Second

	// Create mock clients
	mockClient1 := mocks.NewMockGlobalServiceClient(ctrl)
	mockClient2 := mocks.NewMockGlobalServiceClient(ctrl)

	// Setup expectations: both clients return frames >= stopFrame immediately
	mockClient1.EXPECT().
		GetGlobalFrame(gomock.Any(), gomock.Any()).
		Return(&protobufs.GlobalFrameResponse{
			Frame: &protobufs.GlobalFrame{
				Header: &protobufs.GlobalFrameHeader{
					FrameNumber: stopFrame,
				},
			},
		}, nil).
		AnyTimes()

	mockClient2.EXPECT().
		GetGlobalFrame(gomock.Any(), gomock.Any()).
		Return(&protobufs.GlobalFrameResponse{
			Frame: &protobufs.GlobalFrame{
				Header: &protobufs.GlobalFrameHeader{
					FrameNumber: stopFrame,
				},
			},
		}, nil).
		AnyTimes()

	clients := map[string]protobufs.GlobalServiceClient{
		nodeAddresses[0]: mockClient1,
		nodeAddresses[1]: mockClient2,
	}

	monitor := NewFrameMonitorWithClients(
		ctx,
		logger,
		stopFrame,
		nodeAddresses,
		pollInterval,
		true, // requireAllNodes
		timeout,
		clients,
	)

	// Start monitoring in a goroutine and wait for completion
	done := make(chan struct{})
	start := time.Now()

	go func() {
		monitor.StartMonitoring()
		close(done)
	}()

	// Wait for monitoring to complete or timeout
	select {
	case <-done:
		elapsed := time.Since(start)
		// Should complete quickly (within 500ms), not hitting the timeout
		if elapsed > 500*time.Millisecond {
			t.Errorf("monitoring took too long: %v (expected < 500ms)", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitoring did not complete in time")
	}

	// Verify all nodes reached stopFrame
	monitor.statusMutex.RLock()
	defer monitor.statusMutex.RUnlock()

	for _, addr := range nodeAddresses {
		status, exists := monitor.nodeStatuses[addr]
		if !exists {
			t.Errorf("no status found for node %s", addr)
			continue
		}

		if status.lastGlobalHeadFrame != stopFrame {
			t.Errorf("node %s: expected frame %d, got %d",
				addr, stopFrame, status.lastGlobalHeadFrame)
		}

		if status.err != nil {
			t.Errorf("node %s: expected no error, got %v", addr, status.err)
		}
	}
}

func TestFrameMonitorTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger, _ := zap.NewDevelopment()
	timeout := 200 * time.Millisecond // Short timeout for fast test
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stopFrame := uint64(100)
	nodeAddresses := []string{"node1:8337", "node2:8337"}
	pollInterval := 50 * time.Millisecond

	// Create mock clients that always fail
	mockClient1 := mocks.NewMockGlobalServiceClient(ctrl)
	mockClient2 := mocks.NewMockGlobalServiceClient(ctrl)

	// Setup expectations: both clients always return errors
	failureErr := status.Error(codes.Unavailable, "connection refused")

	mockClient1.EXPECT().
		GetGlobalFrame(gomock.Any(), gomock.Any()).
		Return(nil, failureErr).
		AnyTimes()

	mockClient2.EXPECT().
		GetGlobalFrame(gomock.Any(), gomock.Any()).
		Return(nil, failureErr).
		AnyTimes()

	clients := map[string]protobufs.GlobalServiceClient{
		nodeAddresses[0]: mockClient1,
		nodeAddresses[1]: mockClient2,
	}

	monitor := NewFrameMonitorWithClients(
		ctx,
		logger,
		stopFrame,
		nodeAddresses,
		pollInterval,
		true, // requireAllNodes
		timeout,
		clients,
	)

	// Start monitoring in a goroutine and wait for completion
	done := make(chan struct{})
	start := time.Now()

	go func() {
		monitor.StartMonitoring()
		close(done)
	}()

	// Wait for monitoring to complete
	select {
	case <-done:
		elapsed := time.Since(start)
		// Should take at least the timeout duration
		if elapsed < timeout {
			t.Errorf("monitoring completed too quickly: %v (expected >= %v)", elapsed, timeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitoring did not complete in time")
	}

	// Verify all nodes have errors recorded
	monitor.statusMutex.RLock()
	defer monitor.statusMutex.RUnlock()

	for _, addr := range nodeAddresses {
		status, exists := monitor.nodeStatuses[addr]
		if !exists {
			t.Errorf("no status found for node %s", addr)
			continue
		}

		if status.err == nil {
			t.Errorf("node %s: expected error, got nil", addr)
		}

		if status.lastGlobalHeadFrame >= stopFrame {
			t.Errorf("node %s: expected frame < %d, got %d",
				addr, stopFrame, status.lastGlobalHeadFrame)
		}

		if status.consecutiveFailures == 0 {
			t.Errorf("node %s: expected consecutive failures > 0, got %d",
				addr, status.consecutiveFailures)
		}
	}
}
