package main

import (
	"errors"
	"fmt"

	"source.quilibrium.com/quilibrium/monorepo/protobufs"
)

var (
	ErrNilFrame              = errors.New("frame is nil")
	ErrNilHeader             = errors.New("frame header is nil")
	ErrInvalidIdentity       = errors.New("identity is not 32 bytes")
	ErrInvalidParentSelector = errors.New("parent selector is not 32 bytes")
)

// FrameFields defines the interface for accessing frame identity and parent relationships.
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
