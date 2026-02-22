package shared

import (
	"encoding/json"
	"fmt"
	"iter"
	"sort"
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

func allBipartitions(nodes []string) [][2][]string {
	if len(nodes) < 2 {
		return nil
	}
	sorted := make([]string, len(nodes))
	copy(sorted, nodes)
	sort.Strings(sorted)

	rest := sorted[1:]
	total := 1 << len(rest) // 2^(n-1)
	result := make([][2][]string, 0, total-1)

	for mask := 0; mask < total-1; mask++ {
		// mask < total-1 excludes the all-ones case (p2 would be empty)
		p1 := []string{sorted[0]}
		var p2 []string
		for i, node := range rest {
			if mask>>i&1 == 1 {
				p1 = append(p1, node)
			} else {
				p2 = append(p2, node)
			}
		}
		result = append(result, [2][]string{p1, p2})
	}
	return result
}

// AllFramePartitions returns a lazy iterator over every possible complete
// partition schedule for the given nodes across frames 0..stopFrame.
// Each schedule is a slice of FramePartitionEntry (one per frame that has a
// partition; frames with no partition are omitted).
func AllFramePartitions(nodes []string, stopFrame uint64) iter.Seq[[]FramePartitionEntry] {
	bipartitions := allBipartitions(nodes)
	B := len(bipartitions)
	numFrames := int(stopFrame) + 1

	return func(yield func([]FramePartitionEntry) bool) {
		counter := make([]int, numFrames)
		for {
			var schedule []FramePartitionEntry
			for i, digit := range counter {
				if digit > 0 {
					bp := bipartitions[digit-1]
					schedule = append(schedule, FramePartitionEntry{
						Frame:      uint64(i),
						Partition1: bp[0],
						Partition2: bp[1],
					})
				}
			}
			if !yield(schedule) {
				return
			}
			// increment mixed-radix counter
			pos := 0
			for pos < len(counter) {
				counter[pos]++
				if counter[pos] <= B {
					break
				}
				counter[pos] = 0
				pos++
			}
			if pos == len(counter) {
				return
			}
		}
	}
}

type FrameNotification struct {
	RunID                 string           `json:"run_id,omitempty"`
	FrameNumber           uint64           `json:"frame_number"`
	Type                  NotificationType `json:"type"`
	SafetyError           string           `json:"safety_error,omitempty"`
	NodesReachedStopFrame int              `json:"nodes_reached_stop_frame"`
	TotalNodes            int              `json:"total_nodes"`
}
