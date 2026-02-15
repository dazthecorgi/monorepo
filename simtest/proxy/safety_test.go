package main

import (
	"errors"
	"testing"
)

// mockFrame is a test implementation of FrameFields
type mockFrame struct {
	identity       [32]byte
	parentSelector [32]byte
}

func (m *mockFrame) Identity() ([32]byte, error) {
	return m.identity, nil
}

func (m *mockFrame) ParentSelector() ([32]byte, error) {
	return m.parentSelector, nil
}

// Helper to create a mock frame with specific identity and parent
func newMockFrame(identity, parent [32]byte) *mockFrame {
	return &mockFrame{
		identity:       identity,
		parentSelector: parent,
	}
}

// Helper to create an array from a byte value (for easier test setup)
func byteArray(vals ...byte) [32]byte {
	var arr [32]byte
	copy(arr[:], vals)
	return arr
}

func TestCheckSafety_EmptySequence(t *testing.T) {
	frames := []*mockFrame{}
	err := CheckSafety(frames)

	if err == nil {
		t.Fatal("expected error for empty sequence, got nil")
	}
	if !errors.Is(err, ErrEmptyFrameSequence) {
		t.Errorf("expected ErrEmptyFrameSequence, got %v", err)
	}
}

func TestCheckSafety_SingleFrame(t *testing.T) {
	frame := newMockFrame(byteArray(1), byteArray(0))
	frames := []*mockFrame{frame}

	err := CheckSafety(frames)
	if err != nil {
		t.Errorf("expected no error for single frame, got %v", err)
	}
}

func TestCheckSafety_ValidLinearChain(t *testing.T) {
	// Create a valid chain: frame1 <- frame2 <- frame3 <- frame4
	// (where <- means "is parent of")
	frame1 := newMockFrame(byteArray(1), byteArray(0))           // root
	frame2 := newMockFrame(byteArray(2), byteArray(1))   // parent is frame1
	frame3 := newMockFrame(byteArray(3), byteArray(2))   // parent is frame2
	frame4 := newMockFrame(byteArray(4), byteArray(3))   // parent is frame3

	frames := []*mockFrame{frame1, frame2, frame3, frame4}

	err := CheckSafety(frames)
	if err != nil {
		t.Errorf("expected no error for valid linear chain, got %v", err)
	}
}

func TestCheckSafety_ValidLinearChainOutOfOrder(t *testing.T) {
	// Same chain as above but provided in different order
	frame1 := newMockFrame(byteArray(1), byteArray(0))
	frame2 := newMockFrame(byteArray(2), byteArray(1))
	frame3 := newMockFrame(byteArray(3), byteArray(2))
	frame4 := newMockFrame(byteArray(4), byteArray(3))

	// Provide frames out of order
	frames := []*mockFrame{frame3, frame1, frame4, frame2}

	err := CheckSafety(frames)
	if err != nil {
		t.Errorf("expected no error for valid linear chain (out of order), got %v", err)
	}
}

func TestCheckSafety_TreeStructure(t *testing.T) {
	// Create a tree: frame1 has two children (frame2 and frame3)
	//     frame1
	//     /    \
	// frame2  frame3
	frame1 := newMockFrame(byteArray(1), byteArray(0))
	frame2 := newMockFrame(byteArray(2), byteArray(1))  // parent is frame1
	frame3 := newMockFrame(byteArray(3), byteArray(1))  // parent is also frame1

	frames := []*mockFrame{frame1, frame2, frame3}

	err := CheckSafety(frames)
	if err == nil {
		t.Fatal("expected error for tree structure, got nil")
	}
	if !errors.Is(err, ErrFork) {
		t.Errorf("expected ErrFork, got %v", err)
	}
}

func TestCheckSafety_DisjointedChains(t *testing.T) {
	// Create two separate chains:
	// Chain 1: frame1 <- frame2
	// Chain 2: frame3 <- frame4
	frame1 := newMockFrame(byteArray(1), byteArray(0))
	frame2 := newMockFrame(byteArray(2), byteArray(1))
	frame3 := newMockFrame(byteArray(4), byteArray(3))          // another root
	frame4 := newMockFrame(byteArray(5), byteArray(4))

	frames := []*mockFrame{frame1, frame2, frame3, frame4}

	err := CheckSafety(frames)
	if err == nil {
		t.Fatal("expected error for disjointed chains, got nil")
	}
	if !errors.Is(err, ErrFork) {
		t.Errorf("expected ErrFork, got %v", err)
	}
}

func TestCheckSafety_GapInChain(t *testing.T) {
	// Create a chain with a gap: frame1 <- frame3 (frame2 is missing)
	frame1 := newMockFrame(byteArray(1), byteArray(0))
	frame3 := newMockFrame(byteArray(3), byteArray(2))  // references missing frame2

	frames := []*mockFrame{frame1, frame3}

	err := CheckSafety(frames)
	if err == nil {
		t.Fatal("expected error for gap in chain, got nil")
	}
	if !errors.Is(err, ErrFork) {
		t.Errorf("expected ErrFork, got %v", err)
	}
}

func TestCheckSafety_Cycle(t *testing.T) {
	// Create a cycle: frame1 <- frame2 <- frame3 <- frame1
	frame1 := newMockFrame(byteArray(1), byteArray(3))  // parent is frame3
	frame2 := newMockFrame(byteArray(2), byteArray(1))  // parent is frame1
	frame3 := newMockFrame(byteArray(3), byteArray(2))  // parent is frame2

	frames := []*mockFrame{frame1, frame2, frame3}

	err := CheckSafety(frames)
	if err == nil {
		t.Fatal("expected error for cycle, got nil")
	}
	if !errors.Is(err, ErrCycle) {
		t.Errorf("expected ErrCycle, got %v", err)
	}
}

func TestCheckSafety_SingleFrameChain(t *testing.T) {
	// Minimal valid chain: frame1
	frame1 := newMockFrame(byteArray(1), byteArray(0))

	err := CheckSafety([]*mockFrame{frame1})
	if err != nil {
		t.Errorf("expected no error for single-frame chain, got %v", err)
	}
}

func TestCheckSafety_ValidChain(t *testing.T) {
	// Create a longer chain to test scalability
	frames := make([]*mockFrame, 10)

	frames[0] = newMockFrame(byteArray(1), byteArray(0))
	for i := 1; i < 10; i++ {
		frames[i] = newMockFrame(byteArray(byte(i+1)), byteArray(byte(i)))
	}

	err := CheckSafety(frames)
	if err != nil {
		t.Errorf("expected no error for long valid chain, got %v", err)
	}
}

func TestCheckSafety_ComplexTree(t *testing.T) {
	// More complex tree structure
	//       frame1
	//      /      \
	//  frame2    frame3
	//            /      \
	//        frame4    frame5
	frame1 := newMockFrame(byteArray(1), byteArray(0))
	frame2 := newMockFrame(byteArray(2), byteArray(1))
	frame3 := newMockFrame(byteArray(3), byteArray(1))  // second child of frame1
	frame4 := newMockFrame(byteArray(4), byteArray(3))
	frame5 := newMockFrame(byteArray(5), byteArray(3))  // second child of frame3

	frames := []*mockFrame{frame1, frame2, frame3, frame4, frame5}

	err := CheckSafety(frames)
	if err == nil {
		t.Fatal("expected error for complex tree structure, got nil")
	}
	if !errors.Is(err, ErrFork) {
		t.Errorf("expected ErrFork, got %v", err)
	}
}

func TestCheckSafety_DuplicateFrameIDs(t *testing.T) {
	// Create two frames with the same identity but different parents
	// This represents a conflict where the same frame ID appears twice
	// with inconsistent parent relationships
	frame1 := newMockFrame(byteArray(1), byteArray(0))
	frame2a := newMockFrame(byteArray(2), byteArray(1))  // frame2 with parent=frame1
	frame2b := newMockFrame(byteArray(2), byteArray(3))  // frame2 with parent=frame3 (duplicate ID!)
	frame3 := newMockFrame(byteArray(3), byteArray(1))

	frames := []*mockFrame{frame1, frame2a, frame2b, frame3}

	err := CheckSafety(frames)
	if err == nil {
		t.Fatal("expected error for duplicate frame IDs with different parents, got nil")
	}
	// Note: The current implementation may not have a specific error for this case,
	// but it should fail in some way (likely ErrFork or other validation error)
}
