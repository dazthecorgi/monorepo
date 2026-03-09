package shared

import (
	"encoding/json"
	"fmt"
	"iter"
	"sort"
	"strings"
)

type NotificationType string

const (
	NotificationTypeTerminalFrame NotificationType = "terminal_frame_reached"
)

// RankPartitionEntry defines a partition configuration to apply when a specific
// rank number is observed over gossip.
type RankPartitionEntry struct {
	Rank       uint64   `json:"rank"`
	Partition1 []string `json:"partition1"`
	Partition2 []string `json:"partition2"`
}

// ParseRankPartitions parses a JSON-encoded list of RankPartitionEntry values
// and returns a lookup map keyed by rank number. It rejects duplicate rank numbers
// and requires all fields (rank, partition1, partition2) to be present in each entry.
func ParseRankPartitions(raw string) (map[uint64]RankPartitionEntry, error) {
	var rawEntries []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &rawEntries); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	m := make(map[uint64]RankPartitionEntry, len(rawEntries))
	for i, rawEntry := range rawEntries {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(rawEntry, &fields); err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		for _, required := range []string{"rank", "partition1", "partition2"} {
			if _, ok := fields[required]; !ok {
				return nil, fmt.Errorf("entry %d: missing required field %q", i, required)
			}
		}
		var e RankPartitionEntry
		if err := json.Unmarshal(rawEntry, &e); err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		if _, dup := m[e.Rank]; dup {
			return nil, fmt.Errorf("duplicate rank number %d", e.Rank)
		}
		m[e.Rank] = e
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

// allRankPartitions returns a lazy iterator over every possible complete
// partition schedule for the given nodes across ranks 0..stopRank.
// Each schedule is a slice of RankPartitionEntry (one per rank that has a
// partition; ranks with no partition are omitted).
func allRankPartitions(nodes []string, stopRank uint64) iter.Seq[[]RankPartitionEntry] {
	bipartitions := allBipartitions(nodes)
	B := len(bipartitions)
	numRanks := int(stopRank) + 1

	return func(yield func([]RankPartitionEntry) bool) {
		counter := make([]int, numRanks)
		for {
			var schedule []RankPartitionEntry
			for i, digit := range counter {
				if digit > 0 {
					bp := bipartitions[digit-1]
					schedule = append(schedule, RankPartitionEntry{
						Rank:       uint64(i),
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

func generatePermutations(nodes []string) [][]string {
	if len(nodes) == 0 {
		return [][]string{{}}
	}
	var result [][]string
	for i, node := range nodes {
		rest := make([]string, 0, len(nodes)-1)
		rest = append(rest, nodes[:i]...)
		rest = append(rest, nodes[i+1:]...)
		for _, perm := range generatePermutations(rest) {
			result = append(result, append([]string{node}, perm...))
		}
	}
	return result
}

func canonicalForm(schedule []RankPartitionEntry, nodes []string) string {
	perms := generatePermutations(nodes)
	var minForm string
	for k, perm := range perms {
		mapping := make(map[string]string, len(nodes))
		for j := range nodes {
			mapping[nodes[j]] = perm[j]
		}
		var sb strings.Builder
		for _, entry := range schedule {
			p1 := make([]string, len(entry.Partition1))
			for j, n := range entry.Partition1 {
				p1[j] = mapping[n]
			}
			p2 := make([]string, len(entry.Partition2))
			for j, n := range entry.Partition2 {
				p2[j] = mapping[n]
			}
			sort.Strings(p1)
			sort.Strings(p2)
			if p1[0] > p2[0] {
				p1, p2 = p2, p1
			}
			fmt.Fprintf(&sb, "%d:", entry.Rank)
			for j, n := range p1 {
				if j > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(n)
			}
			sb.WriteByte('|')
			for j, n := range p2 {
				if j > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(n)
			}
			sb.WriteByte(';')
		}
		if s := sb.String(); k == 0 || s < minForm {
			minForm = s
		}
	}
	return minForm
}

// AllRankPartitions returns a lazy iterator over one representative
// schedule per symmetry class (equivalence under any permutation of node names).
func AllRankPartitions(nodes []string, stopRank uint64) iter.Seq[[]RankPartitionEntry] {
	sorted := make([]string, len(nodes))
	copy(sorted, nodes)
	sort.Strings(sorted)

	return func(yield func([]RankPartitionEntry) bool) {
		seen := make(map[string]struct{})
		for schedule := range allRankPartitions(sorted, stopRank) {
			cf := canonicalForm(schedule, sorted)
			if _, ok := seen[cf]; ok {
				continue
			}
			seen[cf] = struct{}{}
			if !yield(schedule) {
				return
			}
		}
	}
}

// NodeInfo holds per-node address and identity information.
type NodeInfo struct {
	Name       string `json:"name"`        // service name, e.g. "archive-1"
	IpAddress   string `json:"ip_address"`    // IP address, e.g. "172.20.1.10"
	StreamPort int    `json:"stream_port"` // TCP stream port, e.g. 8340
	PeerID     string `json:"peer_id"`     // base58-encoded peer ID, empty if unknown
}

func (n NodeInfo) StreamAddress() string { return fmt.Sprintf("%s:%d", n.IpAddress, n.StreamPort) }

type FrameNotification struct {
	RunID                 string           `json:"run_id,omitempty"`
	FrameNumber           uint64           `json:"frame_number"`
	Type                  NotificationType `json:"type"`
	SafetyError           string           `json:"safety_error,omitempty"`
	NodesReachedStopFrame int              `json:"nodes_reached_stop_frame"`
	TotalNodes            int              `json:"total_nodes"`
}
