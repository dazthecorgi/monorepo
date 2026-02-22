package shared

import (
	"encoding/json"
	"fmt"
)

type NotificationType string

const (
	NotificationTypeTerminalFrame NotificationType = "terminal_frame_reached"
)

// FramePartitionEntry defines a partition configuration to apply when a specific
// frame number is observed over gossip.
type FramePartitionEntry struct {
	Frame      uint64   `json:"frame"`
	Partition1 []string `json:"partition1"`
	Partition2 []string `json:"partition2"`
}

// ParseFramePartitions parses a JSON-encoded list of FramePartitionEntry values
// and returns a lookup map keyed by frame number. It rejects duplicate frame numbers.
func ParseFramePartitions(raw string) (map[uint64]FramePartitionEntry, error) {
	var entries []FramePartitionEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	m := make(map[uint64]FramePartitionEntry, len(entries))
	for _, e := range entries {
		if _, dup := m[e.Frame]; dup {
			return nil, fmt.Errorf("duplicate frame number %d", e.Frame)
		}
		m[e.Frame] = e
	}
	return m, nil
}

type FrameNotification struct {
	RunID                 string           `json:"run_id,omitempty"`
	FrameNumber           uint64           `json:"frame_number"`
	Type                  NotificationType `json:"type"`
	SafetyError           string           `json:"safety_error,omitempty"`
	NodesReachedStopFrame int              `json:"nodes_reached_stop_frame"`
	TotalNodes            int              `json:"total_nodes"`
}
