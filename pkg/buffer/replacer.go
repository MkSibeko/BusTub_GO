// Package buffer provides buffer pool management for BusTub-Go.
//
// The buffer pool is a critical component that sits between the disk and
// the rest of the system. It caches pages in memory to reduce disk I/O.
//
// ASSIGNMENT NOTES:
// ==================
// This is a key part of Assignment 1. The Replacer interface defines
// the eviction policy used by the buffer pool manager.
//
// Available implementations:
// - LRUKReplacer: LRU-K algorithm (main assignment)
// - LRUReplacer: Simple LRU (simpler, for testing)
// - ARCReplacer: Adaptive Replacement Cache (advanced)
// - ClockReplacer: Clock algorithm (alternative)
//
// This mirrors: bustub/src/include/buffer/replacer.h
package buffer

import "github.com/bustub-go/pkg/common"

// Replacer is the interface for page replacement algorithms.
//
// The buffer pool manager uses the replacer to decide which page to
// evict when it needs to free a frame for a new page.
//
// Key concepts:
// - Frame: A slot in the buffer pool that can hold one page
// - Evictable: A frame that can be removed (pin count = 0)
// - Pin: Reference count preventing eviction
//
// All methods must be thread-safe.
type Replacer interface {
	// Evict selects a victim frame for eviction according to the
	// replacement policy.
	//
	// Returns:
	// - FrameID of the victim frame
	// - true if a victim was found, false if no evictable frames
	//
	// This is the core method that implements the replacement policy.
	Evict() (common.FrameID, bool)

	// RecordAccess records that a frame has been accessed.
	//
	// This should be called by the buffer pool manager whenever a
	// page is read or written.
	//
	// Parameters:
	// - frameID: The frame that was accessed
	// - accessType: The type of access (affects some policies)
	RecordAccess(frameID common.FrameID, accessType common.AccessType)

	// SetEvictable sets whether a frame is eligible for eviction.
	//
	// A frame with pin count > 0 should not be evictable.
	// When pin count drops to 0, the frame becomes evictable.
	//
	// Parameters:
	// - frameID: The frame to update
	// - evictable: true if the frame can be evicted
	SetEvictable(frameID common.FrameID, evictable bool)

	// Remove removes a frame from the replacer entirely.
	//
	// Called when a page is deleted and its frame should no longer
	// be tracked by the replacer.
	//
	// Parameters:
	// - frameID: The frame to remove
	Remove(frameID common.FrameID)

	// Size returns the number of evictable frames in the replacer.
	Size() int
}

// EvictionPolicy represents different replacement algorithms.
type EvictionPolicy int

const (
	// LRU is the Least Recently Used policy.
	LRU EvictionPolicy = iota

	// LRUK is the LRU-K policy (considers K-th previous access).
	LRUK

	// ARC is the Adaptive Replacement Cache policy.
	ARC

	// Clock is the clock/second-chance algorithm.
	Clock
)

// String returns the name of the eviction policy.
func (p EvictionPolicy) String() string {
	switch p {
	case LRU:
		return "LRU"
	case LRUK:
		return "LRU-K"
	case ARC:
		return "ARC"
	case Clock:
		return "Clock"
	default:
		return "Unknown"
	}
}
