package shared

import (
	"fmt"
	"reflect"
	"testing"
)

func TestAllBipartitions_TwoNodes(t *testing.T) {
	bps := allBipartitions([]string{"A", "B"})
	if len(bps) != 1 {
		t.Fatalf("expected 1 bipartition, got %d", len(bps))
	}
	if !reflect.DeepEqual(bps[0], [2][]string{{"A"}, {"B"}}) {
		t.Errorf("unexpected bipartition: %v", bps[0])
	}
}

func TestAllBipartitions_ThreeNodesUnsorted(t *testing.T) {
	bps := allBipartitions([]string{"C", "A", "B"})
	if len(bps) != 3 {
		t.Fatalf("expected 3 bipartitions, got %d", len(bps))
	}
	for _, bp := range bps {
		if bp[0][0] != "A" {
			t.Errorf("expected 'A' as first element of Partition1, got %q in %v", bp[0][0], bp)
		}
	}
}

func TestAllFramePartitions_TwoNodes_StopFrame1(t *testing.T) {
	nodes := []string{"A", "B"}
	want := [][]FramePartitionEntry{
		nil,
		{{Frame: 0, Partition1: []string{"A"}, Partition2: []string{"B"}}},
		{{Frame: 1, Partition1: []string{"A"}, Partition2: []string{"B"}}},
		{
			{Frame: 0, Partition1: []string{"A"}, Partition2: []string{"B"}},
			{Frame: 1, Partition1: []string{"A"}, Partition2: []string{"B"}},
		},
	}

	var got [][]FramePartitionEntry
	for schedule := range AllFramePartitions(nodes, 1) {
		got = append(got, schedule)
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d schedules, got %d", len(want), len(got))
	}
	for i, w := range want {
		if !reflect.DeepEqual(got[i], w) {
			t.Errorf("schedule[%d]: got %v, want %v", i, got[i], w)
		}
	}
}

func TestAllFramePartitions_ThreeNodes_StopFrame2_Count(t *testing.T) {
	count := 0
	for p := range AllFramePartitions([]string{"A", "B", "C"}, 2) {
		if len(p) == 2 {
		fmt.Printf("\n")
		}
		for _, entry := range p {
			if len(p) == 2 {
				fmt.Printf("  Frame=%d Partition1=%v Partition2=%v\n", entry.Frame, entry.Partition1, entry.Partition2)
			}
			if entry.Frame > 2 {
				t.Errorf("got entry with frame %d, expected max frame 2", entry.Frame)
			}
			if len(entry.Partition1)+len(entry.Partition2) != 3 {
				t.Errorf("partitions do not cover all nodes: %v + %v", entry.Partition1, entry.Partition2)
			}
			nodeSet := make(map[string]bool)
			for _, n := range entry.Partition1 {
				nodeSet[n] = true
			}
			for _, n := range entry.Partition2 {
				nodeSet[n] = true
			}
			if len(nodeSet) != 3 {
				t.Errorf("partitions do not cover all nodes: %v + %v", entry.Partition1, entry.Partition2)
			}
		}
		count++
	}
	// B=3 bipartitions, numFrames=3, total = (3+1)^3 = 64
	if count != 64 {
		t.Errorf("expected 64 schedules, got %d", count)
	}
}

func TestAllBipartitions_Empty(t *testing.T) {
	if got := allBipartitions(nil); got != nil {
		t.Errorf("expected nil for nil input, got %v", got)
	}
	if got := allBipartitions([]string{}); got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestAllBipartitions_SingleNode(t *testing.T) {
	if got := allBipartitions([]string{"A"}); got != nil {
		t.Errorf("expected nil for single node, got %v", got)
	}
}

func TestAllFramePartitions_EmptyNodes(t *testing.T) {
	// No bipartitions possible; only one schedule (always empty) should be yielded.
	count := 0
	for schedule := range AllFramePartitions(nil, 5) {
		if len(schedule) != 0 {
			t.Errorf("expected empty schedule, got %v", schedule)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 schedule for empty nodes, got %d", count)
	}
}

func TestAllFramePartitions_SingleNode(t *testing.T) {
	// Single node cannot form a bipartition; only one empty schedule.
	count := 0
	for schedule := range AllFramePartitions([]string{"A"}, 3) {
		if len(schedule) != 0 {
			t.Errorf("expected empty schedule, got %v", schedule)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 schedule for single node, got %d", count)
	}
}

func TestAllFramePartitions_TwoNodes_StopFrame0(t *testing.T) {
	// stopFrame=0 means one frame; with 1 bipartition: (1+1)^1 = 2 schedules.
	want := [][]FramePartitionEntry{
		nil,
		{{Frame: 0, Partition1: []string{"A"}, Partition2: []string{"B"}}},
	}
	var got [][]FramePartitionEntry
	for schedule := range AllFramePartitions([]string{"A", "B"}, 0) {
		got = append(got, schedule)
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d schedules, got %d", len(want), len(got))
	}
	for i, w := range want {
		if !reflect.DeepEqual(got[i], w) {
			t.Errorf("schedule[%d]: got %v, want %v", i, got[i], w)
		}
	}
}
