package shared

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
)

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

func TestAllRankPartitions_EmptyNodes(t *testing.T) {
	// No bipartitions possible; only one schedule (always empty) should be yielded.
	count := 0
	for schedule := range AllRankPartitions(nil, 5) {
		if len(schedule) != 0 {
			t.Errorf("expected empty schedule, got %v", schedule)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 schedule for empty nodes, got %d", count)
	}
}

func TestAllRankPartitions_SingleNode(t *testing.T) {
	// Single node cannot form a bipartition; only one empty schedule.
	count := 0
	for schedule := range AllRankPartitions([]string{"A"}, 3) {
		if len(schedule) != 0 {
			t.Errorf("expected empty schedule, got %v", schedule)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 schedule for single node, got %d", count)
	}
}

func TestAllRankPartitions_TwoNodes_StopRank1(t *testing.T) {
	nodes := []string{"A", "B"}
	want := [][]RankPartitionEntry{
		nil,
		{{Rank: 0, Partition1: []string{"A"}, Partition2: []string{"B"}}},
		{{Rank: 1, Partition1: []string{"A"}, Partition2: []string{"B"}}},
		{
			{Rank: 0, Partition1: []string{"A"}, Partition2: []string{"B"}},
			{Rank: 1, Partition1: []string{"A"}, Partition2: []string{"B"}},
		},
	}

	var got [][]RankPartitionEntry
	for schedule := range AllRankPartitions(nodes, 1) {
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

func TestAllRankPartitions_ThreeNodes_StopRank2_Count(t *testing.T) {
	count := 0
	for p := range AllRankPartitions([]string{"A", "B", "C"}, 2) {
		fmt.Printf("\n")
		for _, entry := range p {
			fmt.Printf("  Rank=%d Partition1=%v Partition2=%v\n", entry.Rank, entry.Partition1, entry.Partition2)
			if entry.Rank > 2 {
				t.Errorf("got entry with rank %d, expected max rank 2", entry.Rank)
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
	if count != 15 {
		t.Errorf("expected 15 schedules, got %d", count)
	}
}

func TestAllRankPartitions_TwoNodes_StopRank0(t *testing.T) {
	// stopRank=0 means one rank; with 1 bipartition: (1+1)^1 = 2 schedules.
	want := [][]RankPartitionEntry{
		nil,
		{{Rank: 0, Partition1: []string{"A"}, Partition2: []string{"B"}}},
	}
	var got [][]RankPartitionEntry
	for schedule := range AllRankPartitions([]string{"A", "B"}, 0) {
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

func TestAllRankPartitions_SymmetricDedup(t *testing.T) {
	// The two schedules:
	//   s1: Rank0:[A,B]|[C], Rank1:[A]|[B,C]
	//   s2: Rank0:[A,C]|[B], Rank1:[A]|[B,C]
	// are symmetric (swap B and C), so at most one should appear.
	s1 := normalizeSchedule([]RankPartitionEntry{
		{Rank: 0, Partition1: []string{"A", "B"}, Partition2: []string{"C"}},
		{Rank: 1, Partition1: []string{"A"}, Partition2: []string{"B", "C"}},
	})
	s2 := normalizeSchedule([]RankPartitionEntry{
		{Rank: 0, Partition1: []string{"A", "C"}, Partition2: []string{"B"}},
		{Rank: 1, Partition1: []string{"A"}, Partition2: []string{"B", "C"}},
	})

	foundS1, foundS2 := false, false
	for schedule := range AllRankPartitions([]string{"A", "B", "C"}, 2) {
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

func TestAllRankPartitions_SymmetricDedup_TwoPairSwap(t *testing.T) {
	// With 4 nodes, the two schedules:
	//   s1: Rank0:[A]|[B,C,D],   Rank1:[A,B]|[C,D]
	//   s2: Rank0:[A,B,D]|[C],   Rank1:[A,B]|[C,D]
	// are symmetric under the double transposition A↔C, B↔D,
	// so at most one should appear.
	s1 := normalizeSchedule([]RankPartitionEntry{
		{Rank: 0, Partition1: []string{"A"}, Partition2: []string{"B", "C", "D"}},
		{Rank: 1, Partition1: []string{"A", "B"}, Partition2: []string{"C", "D"}},
	})
	s2 := normalizeSchedule([]RankPartitionEntry{
		{Rank: 0, Partition1: []string{"A", "B", "D"}, Partition2: []string{"C"}},
		{Rank: 1, Partition1: []string{"A", "B"}, Partition2: []string{"C", "D"}},
	})

	foundS1, foundS2 := false, false
	for schedule := range AllRankPartitions([]string{"A", "B", "C", "D"}, 1) {
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

func normalizeSchedule(s []RankPartitionEntry) []RankPartitionEntry {
	out := make([]RankPartitionEntry, len(s))
	for i, e := range s {
		out[i] = RankPartitionEntry{
			Rank:       e.Rank,
			Partition1: slices.Sorted(slices.Values(e.Partition1)),
			Partition2: slices.Sorted(slices.Values(e.Partition2)),
		}
	}
	return out
}
