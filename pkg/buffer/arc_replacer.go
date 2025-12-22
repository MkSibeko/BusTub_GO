// Package buffer provides buffer pool management for BusTub-Go.
//
// This file implements the Adaptive Replacement Cache (ARC) algorithm.
// ARC dynamically balances between recency and frequency to adapt to workload patterns.
//
// This mirrors: bustub/src/buffer/arc_replacer.cpp
package buffer

import (
	"container/list"
	"sync"

	"github.com/bustub-go/pkg/common"
)

// ARCReplacer implements the Adaptive Replacement Cache replacement policy.
//
// ARC maintains four lists:
//   - mru: Recently accessed pages (recency-focused)
//   - mfu: Frequently accessed pages (frequency-focused)
//   - mruGhost: Ghost entries for recently evicted mru pages
//   - mfuGhost: Ghost entries for recently evicted mfu pages
//
// The target size parameter adapts based on ghost list hits to balance
// between recency and frequency based on observed access patterns.
type ARCReplacer struct {
	// replacerSize is the maximum number of frames that can be cached.
	replacerSize int

	// targetSize is the target size for the mru list (adapts dynamically).
	targetSize int

	// mru tracks recently used pages (recency list T1 in paper).
	mru *list.List

	// mfu tracks frequently used pages (frequency list T2 in paper).
	mfu *list.List

	// mruGhost tracks ghost entries from evicted mru pages (B1 in paper).
	mruGhost *list.List

	// mfuGhost tracks ghost entries from evicted mfu pages (B2 in paper).
	mfuGhost *list.List

	// frameToElement maps frame_id to its list element in mru or mfu.
	frameToElement map[common.FrameID]*list.Element

	// pageToGhostElement maps page_id to its ghost list element.
	pageToGhostElement map[common.PageID]*list.Element

	// frameInfo stores metadata about each frame.
	frameInfo map[common.FrameID]*arcFrameInfo

	// evictableCount tracks the number of evictable frames.
	evictableCount int

	mu sync.Mutex
}

// arcFrameInfo stores metadata for a frame in the ARC replacer.
type arcFrameInfo struct {
	frameID   common.FrameID
	pageID    common.PageID
	evictable bool
	inMFU     bool // true if in mfu, false if in mru
}

// arcGhostEntry stores metadata for a ghost list entry.
type arcGhostEntry struct {
	pageID common.PageID
	inMFU  bool // true if came from mfu, false if from mru
}

// NewARCReplacer creates a new ARC replacer.
func NewARCReplacer(numFrames int) *ARCReplacer {
	return &ARCReplacer{
		replacerSize:       numFrames,
		targetSize:         0,
		mru:                list.New(),
		mfu:                list.New(),
		mruGhost:           list.New(),
		mfuGhost:           list.New(),
		frameToElement:     make(map[common.FrameID]*list.Element),
		pageToGhostElement: make(map[common.PageID]*list.Element),
		frameInfo:          make(map[common.FrameID]*arcFrameInfo),
		evictableCount:     0,
	}
}

// Evict selects a victim frame according to the ARC balancing policy.
//
// Evicts from mru if mru.size >= targetSize, otherwise from mfu.
// Skips non-evictable frames. If the preferred list has no evictable frames,
// tries the other list. Moves evicted frame to corresponding ghost list.
func (r *ARCReplacer) Evict() (common.FrameID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: Implement ARC eviction
	//
	// Steps:
	// 1. If no evictable frames, return false
	// 2. Determine which list to evict from based on targetSize
	// 3. Find first evictable frame in preferred list (from back/LRU end)
	// 4. If none found, try the other list
	// 5. Remove frame from its list
	// 6. Add page_id to corresponding ghost list
	// 7. Trim ghost list if exceeds capacity
	// 8. Return the evicted frame_id

	return common.InvalidFrameID, false
}

// RecordAccess records that a frame was accessed.
//
// Handles four cases:
//  1. Hit in mru or mfu: Move to front of mfu
//  2. Hit in mruGhost: Increase targetSize, move to front of mfu
//  3. Hit in mfuGhost: Decrease targetSize, move to front of mfu
//  4. Miss all lists: Add to front of mru
func (r *ARCReplacer) RecordAccess(frameID common.FrameID, accessType common.AccessType) {
	r.RecordAccessWithPageID(frameID, common.InvalidPageID, accessType)
}

// RecordAccessWithPageID records access with both frame and page identifiers.
// Page ID is needed to track ghost entries after eviction.
func (r *ARCReplacer) RecordAccessWithPageID(frameID common.FrameID, pageID common.PageID, accessType common.AccessType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: Implement ARC access recording
	//
	// Case 1: Frame exists in mru or mfu
	//   - Remove from current list
	//   - Add to front of mfu (promotes to frequency list)
	//   - Update frameInfo.inMFU = true
	//
	// Case 2: Page exists in mruGhost
	//   - Adapt: increase targetSize (favor recency)
	//   - Remove from ghost list
	//   - Add frame to front of mfu
	//
	// Case 3: Page exists in mfuGhost
	//   - Adapt: decrease targetSize (favor frequency)
	//   - Remove from ghost list
	//   - Add frame to front of mfu
	//
	// Case 4: Complete miss
	//   - Add frame to front of mru (new entry starts in recency list)
	//   - Create new frameInfo
}

// SetEvictable sets whether a frame can be evicted.
func (r *ARCReplacer) SetEvictable(frameID common.FrameID, evictable bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: Implement evictable status toggle
	//
	// Steps:
	// 1. Look up frame in frameInfo
	// 2. If not found, panic or return (invalid frame)
	// 3. If status unchanged, return
	// 4. Update evictable flag
	// 5. Adjust evictableCount accordingly
}

// Remove removes a frame from the replacer entirely.
func (r *ARCReplacer) Remove(frameID common.FrameID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO: Implement frame removal
	//
	// Steps:
	// 1. Look up frame in frameInfo
	// 2. If not found, return
	// 3. If not evictable, panic (cannot remove pinned frame)
	// 4. Remove from mru or mfu list
	// 5. Delete from frameToElement map
	// 6. Delete from frameInfo map
	// 7. Decrement evictableCount
}

// Size returns the number of evictable frames.
func (r *ARCReplacer) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evictableCount
}

// adaptTargetSize adjusts targetSize based on ghost list hit.
func (r *ARCReplacer) adaptTargetSize(hitMRUGhost bool) {
	// TODO: Implement target size adaptation
	//
	// If hit mruGhost: increase targetSize (give more space to recency)
	//   delta = max(1, mfuGhost.size / mruGhost.size)
	//   targetSize = min(targetSize + delta, replacerSize)
	//
	// If hit mfuGhost: decrease targetSize (give more space to frequency)
	//   delta = max(1, mruGhost.size / mfuGhost.size)
	//   targetSize = max(targetSize - delta, 0)
}

// trimGhostLists ensures ghost lists don't exceed capacity.
func (r *ARCReplacer) trimGhostLists() {
	// TODO: Implement ghost list trimming
	//
	// Total ghost capacity = replacerSize
	// If mruGhost.size + mfuGhost.size > replacerSize:
	//   - Remove oldest entries from the larger ghost list
}

// moveToMFU moves a frame from mru to the front of mfu.
func (r *ARCReplacer) moveToMFU(frameID common.FrameID) {
	// TODO: Implement list movement
}

// addToGhostList adds a page to the appropriate ghost list.
func (r *ARCReplacer) addToGhostList(pageID common.PageID, fromMFU bool) {
	// TODO: Implement ghost list addition
}

// Ensure ARCReplacer implements Replacer interface.
var _ Replacer = (*ARCReplacer)(nil)
