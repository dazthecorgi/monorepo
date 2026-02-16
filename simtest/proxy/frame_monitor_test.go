package main

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestFrameMonitorCreation(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	nodeAddresses := []string{"localhost:8337", "localhost:8338"}
	stopFrame := uint64(100)
	pollInterval := 5 * time.Second
	gracePeriod := 60 * time.Second

	monitor := NewFrameMonitor(
		ctx,
		logger,
		stopFrame,
		nodeAddresses,
		pollInterval,
		gracePeriod,
		true,
	)

	if monitor == nil {
		t.Fatal("expected monitor to be created")
	}

	if monitor.stopFrame != stopFrame {
		t.Errorf("expected stopFrame %d, got %d", stopFrame, monitor.stopFrame)
	}

	if len(monitor.nodeAddresses) != 2 {
		t.Errorf("expected 2 node addresses, got %d", len(monitor.nodeAddresses))
	}

	if monitor.pollInterval != pollInterval {
		t.Errorf("expected poll interval %v, got %v", pollInterval, monitor.pollInterval)
	}

	if monitor.gracePeriod != gracePeriod {
		t.Errorf("expected grace period %v, got %v", gracePeriod, monitor.gracePeriod)
	}

	if !monitor.requireAllNodes {
		t.Error("expected requireAllNodes to be true")
	}
}

func TestNodeFrameStatusInitialization(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	monitor := NewFrameMonitor(
		ctx,
		logger,
		100,
		[]string{"node1:8337", "node2:8337"},
		5*time.Second,
		60*time.Second,
		true,
	)

	// Create clients should initialize node statuses
	// Note: This will fail to connect since nodes don't exist, but should still initialize structures
	_ = monitor.createNodeClients()

	if len(monitor.nodeStatuses) != 2 {
		t.Errorf("expected 2 node statuses, got %d", len(monitor.nodeStatuses))
	}

	for addr, status := range monitor.nodeStatuses {
		if status.address != addr {
			t.Errorf("expected address %s, got %s", addr, status.address)
		}
		if status.lastGlobalHeadFrame != 0 {
			t.Errorf("expected initial frame to be 0, got %d", status.lastGlobalHeadFrame)
		}
	}

	monitor.Close()
}
