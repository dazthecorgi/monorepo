package shared

import (
	"fmt"
	"reflect"
	"slices"
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
	for schedule := range allFramePartitions(nodes, 1) {
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
	for p := range allFramePartitions([]string{"A", "B", "C"}, 2) {
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
	for schedule := range allFramePartitions(nil, 5) {
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
	for schedule := range allFramePartitions([]string{"A"}, 3) {
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
	for schedule := range allFramePartitions([]string{"A", "B"}, 0) {
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

func TestAllFramePartitions_UniqueCount(t *testing.T) {
	// Burnside's lemma for n=3, stopFrame=2:
	// (64 + 8+8+8 + 1+1) / 6 = 15 unique schedules.
	count := 0
	for range AllFramePartitions([]string{"A", "B", "C"}, 2) {
		count++
	}
	if count != 15 {
		t.Errorf("expected 15 unique schedules, got %d", count)
	}
}

func TestAllFramePartitions_SymmetricDedup(t *testing.T) {
	// The two schedules:
	//   s1: Frame0:[A,B]|[C], Frame1:[A]|[B,C]
	//   s2: Frame0:[A,C]|[B], Frame1:[A]|[B,C]
	// are symmetric (swap B and C), so at most one should appear.
	s1 := normalizeSchedule([]FramePartitionEntry{
		{Frame: 0, Partition1: []string{"A", "B"}, Partition2: []string{"C"}},
		{Frame: 1, Partition1: []string{"A"}, Partition2: []string{"B", "C"}},
	})
	s2 := normalizeSchedule([]FramePartitionEntry{
		{Frame: 0, Partition1: []string{"A", "C"}, Partition2: []string{"B"}},
		{Frame: 1, Partition1: []string{"A"}, Partition2: []string{"B", "C"}},
	})

	foundS1, foundS2 := false, false
	for schedule := range AllFramePartitions([]string{"A", "B", "C"}, 2) {
		ns := normalizeSchedule(schedule)
		if reflect.DeepEqual(ns, s1) {
			foundS1 = true
		}
		if reflect.DeepEqual(ns, s2) {
			foundS2 = true
		}
	}
	if foundS1 && foundS2 {
		t.Error("both symmetric schedules appear; expected only one representative")
	}
	if !foundS1 && !foundS2 {
		t.Error("neither symmetric schedule appears; expected exactly one representative")
	}
}

func TestAllFramePartitions_TwoNodes_NoDedup(t *testing.T) {
	// For 2 nodes, the only swap maps the single bipartition to itself,
	// so deduplication does not reduce the count. Expect 4 schedules.
	count := 0
	for range AllFramePartitions([]string{"A", "B"}, 1) {
		count++
	}
	if count != 4 {
		t.Errorf("expected 4 schedules for 2 nodes / stopFrame=1, got %d", count)
	}
}

func TestAllFramePartitions_EmptyNodes_Unique(t *testing.T) {
	// No bipartitions possible; one empty schedule, deduplicated to one.
	count := 0
	for schedule := range AllFramePartitions(nil, 5) {
		if len(schedule) != 0 {
			t.Errorf("expected empty schedule, got %v", schedule)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 schedule for nil nodes, got %d", count)
	}
}

func normalizeSchedule(s []FramePartitionEntry) []FramePartitionEntry {
	out := make([]FramePartitionEntry, len(s))
	for i, e := range s {
		out[i] = FramePartitionEntry{
			Frame:      e.Frame,
			Partition1: slices.Sorted(slices.Values(e.Partition1)),
			Partition2: slices.Sorted(slices.Values(e.Partition2)),
		}
	}
	return out
}
