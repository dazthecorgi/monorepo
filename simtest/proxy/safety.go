package main

import (
	"encoding/hex"
	"errors"
	"fmt"

	"source.quilibrium.com/quilibrium/monorepo/protobufs"
)

var (
	ErrCycle              = errors.New("cycle detected in frame chain")
	ErrDuplicateFrame     = errors.New("duplicate frame detected")
	ErrEmptyFrameSequence = errors.New("empty frame sequence")
	ErrFork               = errors.New("fork detected")
	ErrInvalidParentSelector = errors.New("parent selector is not 32 bytes")
	ErrNilFrame              = errors.New("frame is nil")
	ErrNilHeader             = errors.New("frame header is nil")
	ErrInvalidIdentity       = errors.New("identity is not 32 bytes")
)

// CheckSafety verifies that the given frames form a single non-empty linear chain.
// That is there should be exactly one root,
// each frame should have at most one child, and all frames should be connected without
// any gaps (missing parents) or cycles.
func CheckSafety[FrameT FrameFields](frames []FrameT) error {
	if len(frames) == 0 {
		return ErrEmptyFrameSequence
	}

	// Single frame is always valid
	if len(frames) == 1 {
		return nil
	}

	childToParent, err := buildFrameParents(frames)
	if err != nil {
		return err
	}

	parentToChildren := buildParentToChildrenMap(childToParent)

	// Find leaves and detect forks at the same time
	leaf_ids := make([][32]byte, 0)
	for parent_id, children_ids := range parentToChildren {
		if len(children_ids) > 1 {
			return fmt.Errorf("%w: parent %s has %d children", ErrFork, hex.EncodeToString(parent_id[:]), len(children_ids))
		}
		if len(children_ids) == 0 {
			leaf_ids = append(leaf_ids, parent_id)
		}
	}

	if len(leaf_ids) == 0 {
		return fmt.Errorf("%w: no leaf frames detected", ErrCycle)
	}

	if len(leaf_ids) > 1 {
		hexLeaves := make([]string, len(leaf_ids))
		for i, leaf_id := range leaf_ids {
			hexLeaves[i] = hex.EncodeToString(leaf_id[:])
		}
		return fmt.Errorf("%w: multiple leaves detected: %v", ErrFork, hexLeaves)
	}

	// Check for cycles and disjoint frames by traversing from the leaf to the root
	visited := make(map[[32]byte]bool)
	current_id := leaf_ids[0]

	for {
		if visited[current_id] {
			return fmt.Errorf("%w: cycle detected at frame %s", ErrCycle, hex.EncodeToString(current_id[:]))
		}
		visited[current_id] = true

		parent_id, exists := childToParent[current_id]

		// Reached a root frame
		if !exists {
			break
		}

		current_id = parent_id
	}

	// If we haven't visited all frames, there are disjoint frames.
	// +1, because the genesis frame's parent will not be in the frames list.
	if len(visited) != len(frames) + 1 {
		return fmt.Errorf("%w: only %d out of %d frames are connected", ErrFork, len(visited), len(frames))
	}

	return nil
}

type FrameFields interface {
	Identity() ([32]byte, error)
	ParentSelector() ([32]byte, error)
}

// GlobalFrameWrapper wraps a protobufs.GlobalFrame to implement FrameFields
type GlobalFrameWrapper struct {
	*protobufs.GlobalFrame
}

// Identity returns the frame's id as a 32-byte array.
func (gf *GlobalFrameWrapper) Identity() ([32]byte, error) {
	if gf.GlobalFrame == nil {
		return [32]byte{}, ErrNilFrame
	}

	identity := gf.GlobalFrame.Identity()
	if len(identity) != 32 {
		return [32]byte{}, fmt.Errorf("%w: got %d bytes", ErrInvalidIdentity, len(identity))
	}

	var result [32]byte
	copy(result[:], []byte(identity))

	return result, nil
}

// ParentSelector returns the parent id as a 32-byte array.
func (gf *GlobalFrameWrapper) ParentSelector() ([32]byte, error) {
	if gf.GlobalFrame == nil {
		return [32]byte{}, ErrNilFrame
	}
	if gf.Header == nil {
		return [32]byte{}, ErrNilHeader
	}

	if len(gf.Header.ParentSelector) != 32 {
		return [32]byte{}, fmt.Errorf("%w: got %d bytes", ErrInvalidParentSelector, len(gf.Header.ParentSelector))
	}

	var result [32]byte
	copy(result[:], gf.Header.ParentSelector)
	return result, nil
}

// buildFrameParents takes a sequence of global frames and returns a map
// where each key is a frame selector and the value is its parent selector.
// Returns an error for duplicate parents.
func buildFrameParents[FrameT FrameFields](frames []FrameT) (map[[32]byte][32]byte, error) {
	parents := make(map[[32]byte][32]byte)

	for _, frame := range frames {
		identity, err := frame.Identity()
		if err != nil {
			return nil, fmt.Errorf("failed to get identity: %w", err)
		}

		parentSelector, err := frame.ParentSelector()
		if err != nil {
			return nil, fmt.Errorf("failed to get parent selector: %w", err)
		}

		if _, exists := parents[identity]; exists {
			return nil, fmt.Errorf("%w: frame %s appears multiple times", ErrDuplicateFrame, hex.EncodeToString(identity[:]))
		}

		parents[identity] = parentSelector
	}

	return parents, nil
}

// buildParentToChildrenMap builds a reverse map from parent frame selectors to their children.
// Entries are added for all parents and children, so even frames without children will have an entry with an empty slice.
func buildParentToChildrenMap(parents map[[32]byte][32]byte) map[[32]byte][][32]byte {
	children := make(map[[32]byte][][32]byte)

	for child_id, parent_id := range parents {
		if _, exists := children[parent_id]; !exists {
			children[parent_id] = make([][32]byte, 0)
		}
		if _, exists := children[child_id]; !exists {
			children[child_id] = make([][32]byte, 0)
		}
		children[parent_id] = append(children[parent_id], child_id)
	}

	return children
}
