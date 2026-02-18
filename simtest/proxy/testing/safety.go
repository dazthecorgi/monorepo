package testing

import (
	"encoding/hex"
	"errors"
	"fmt"
)

var (
	ErrCycle              = errors.New("cycle detected in frame chain")
	ErrDuplicateFrame     = errors.New("duplicate frame detected with different parents")
	ErrEmptyFrameSequence = errors.New("empty frame sequence")
	ErrFork               = errors.New("fork detected")
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
	// parentToChildren includes entries for all frames, so we can compare its length to the number of visited frames.
	if len(visited) != len(parentToChildren) {
		return fmt.Errorf("%w: only %d out of %d frames are connected", ErrFork, len(visited), len(parentToChildren))
	}

	return nil
}

// buildFrameParents takes a sequence of global frames and returns a map
// where each key is a frame selector and the value is its parent selector.
// Returns an error if an id is mapped to different parents.
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

		if _, exists := parents[identity]; exists && parents[identity] != parentSelector {
			return nil, fmt.Errorf("%w: frame %s", ErrDuplicateFrame, hex.EncodeToString(identity[:]))
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
