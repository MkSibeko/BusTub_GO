// Package buffer provides buffer pool management for BusTub-Go.
//
// This file implements the Adaptive Replacement Cache (ARC) algorithm.
// ARC dynamically balances between recency and frequency to adapt to workload patterns.
//
// This mirrors: bustub/src/buffer/arc_replacer.cpp
package buffer

import (
	"container/list"
	"fmt"
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
	mru cacheFrame

	// mfu tracks frequently used pages (frequency list T2 in paper).
	mfu cacheFrame

	// mruGhost tracks ghost entries from evicted mru pages (B1 in paper).
	mruGhost cacheFrame

	// mfuGhost tracks ghost entries from evicted mfu pages (B2 in paper).
	mfuGhost cacheFrame

	// frameInfo stores metadata about each frame.
	frameInfo map[common.FrameID]*arcFrameInfo

	// evictableCount tracks the number of evictable frames.
	evictableCount int

	mu sync.Mutex
}

type cacheFrame struct {
	list   *list.List
	lookup map[common.PageID]*list.Element
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
		replacerSize:   numFrames,
		targetSize:     0,
		mru:            cacheFrame{list.New(), make(map[common.FrameID]*list.Element)},
		mfu:            cacheFrame{list.New(), make(map[common.FrameID]*list.Element)},
		mruGhost:       cacheFrame{list.New(), make(map[common.FrameID]*list.Element)},
		mfuGhost:       cacheFrame{list.New(), make(map[common.FrameID]*list.Element)},
		frameInfo:      make(map[common.FrameID]*arcFrameInfo),
		evictableCount: 0,
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

	if r.evictableCount == 0 {
		return common.InvalidFrameID, false
	}

	// r.targetSize

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
	// check mru
	frame := r.mru.lookup[frameID]
	if frame != nil {
		// L1 hit
		r.mru.list.Remove(frame)
		delete(r.mru.lookup, frame.Value.(common.PageID))
		r.mfu.lookup[frameID] = r.mfu.list.PushFront(frame)
		return
	}

	//check mfu
	frame = r.mfu.lookup[frameID]
	if frame != nil {
		// L2 hit bring to front of mfu
		r.mfu.list.Remove(frame)
		r.mfu.list.PushFront(frame)
		return
	}

	mruGhostListSize := r.mruGhost.list.Len()
	mfuGhostListSize := r.mfuGhost.list.Len()

	//miss mru & mfu
	frame = r.mruGhost.lookup[pageID]
	if frame != nil {
		if mruGhostListSize >= mfuGhostListSize && r.targetSize < r.replacerSize {
			r.targetSize++
		} else if newTarget := r.targetSize + int(mfuGhostListSize/mruGhostListSize); newTarget <= r.replacerSize {
			r.targetSize = newTarget
		}

		r.mruGhost.list.Remove(frame)
		r.mfu.list.PushFront(frame)
	}

	//miss mru & mfu & mruGhost
	frame = r.mfuGhost.lookup[pageID]
	if frame != nil {
		if mfuGhostListSize > mruGhostListSize && r.targetSize > 0 {
			r.targetSize--
		} else if newTarget := r.targetSize - int(mruGhostListSize/mfuGhostListSize); newTarget >= 0 {
			r.targetSize = newTarget
		}

		r.mfuGhost.list.Remove(frame)
		r.mfu.list.PushFront(frame)
	}
	mruSize := r.mru.list.Len()
	mfuSize := r.mfu.list.Len()

	//cache miss make space in L1 or B1
	if mruSize+mruGhostListSize == r.replacerSize {
		// kick out last mruGhost frame
		frame = r.mruGhost.list.Back()
		r.mruGhost.list.Remove(frame)
		delete(r.mruGhost.lookup, frame.Value.(common.PageID))
	}

	if mruSize+mruGhostListSize+mfuSize+mfuGhostListSize == 2*r.replacerSize {
		// kick out last mfuGhost frame
		frame = r.mfuGhost.list.Back()
		r.mfuGhost.list.Remove(frame)
		delete(r.mfuGhost.lookup, frame.Value.(common.PageID))
	}

	r.mru.lookup[frameID] = r.mru.list.PushFront(frameID) // add to mru

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

// trimGhostLists ensures ghost lists don't exceed capacity.
func (r *ARCReplacer) trimGhostLists() {
	mfuGhostLen := r.mfuGhost.list.Len()
	mruGhostLen := r.mruGhost.list.Len()
	if mfuGhostLen+mruGhostLen > r.replacerSize {
		if mfuGhostLen > mruGhostLen {
			removeFrameFromGhostList(&r.mfuGhost)
			return
		}
		removeFrameFromGhostList(&r.mruGhost)
	}
}

func removeFrameFromGhostList(ghostList *cacheFrame) {
	ghostFrame := ghostList.list.Back() //dereference
	ghostList.list.Remove(ghostFrame)
	delete(ghostList.lookup, ghostFrame.Value.(common.PageID))
}

// moveToMFU moves a frame from mru to the front of mfu.
func (r *ARCReplacer) moveToMFU(frameID common.FrameID) {
	frame := r.mru.lookup[frameID]
	if frame != nil {
		r.mru.list.Remove(frame)
		r.mfu.list.PushBack(frameID)
		return
	}
	panic(fmt.Errorf("frameID: %d is not in mru list", frameID))
}

// addToGhostList adds a page to the appropriate ghost list.
func (r *ARCReplacer) addToGhostList(pageID common.PageID, fromMFU bool) {
	if fromMFU {
		r.mfuGhost.list.PushBack(pageID)
		return
	}
	r.mruGhost.list.PushBack(pageID)
}

// Ensure ARCReplacer implements Replacer interface.
var _ Replacer = (*ARCReplacer)(nil)
