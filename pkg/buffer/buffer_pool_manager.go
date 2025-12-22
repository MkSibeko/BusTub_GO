// Package buffer - Buffer Pool Manager implementation.
//
// The Buffer Pool Manager (BPM) is responsible for:
// - Managing a pool of frames in memory
// - Fetching pages from disk into memory
// - Evicting pages when the pool is full
// - Flushing dirty pages back to disk
//
// ASSIGNMENT 1 - BUFFER POOL MANAGER:
// =====================================
// This is a main component you need to implement for Assignment 1.
//
// Key responsibilities:
// 1. NewPage() - Allocate a new page in the buffer pool
// 2. DeletePage() - Remove a page from the buffer pool
// 3. ReadPage() - Fetch a page for reading (with read latch)
// 4. WritePage() - Fetch a page for writing (with write latch)
// 5. FlushPage() - Write a page's contents to disk
// 6. Eviction - Select and evict pages when pool is full
//
// This mirrors: bustub/src/include/buffer/buffer_pool_manager.h
package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/bustub-go/pkg/common"
	"github.com/bustub-go/pkg/storage/disk"
)

// BufferPoolManager manages the buffer pool.
//
// It sits between the disk and the rest of the system, caching pages
// in memory and managing their lifecycle.
//
// TODO (ASSIGNMENT 1 - BUFFER POOL MANAGER):
// ==========================================
// You need to implement these methods:
//
// 1. NewPage() - Create a new page
//   - Find or evict a frame
//   - Allocate a new page ID
//   - Initialize the frame
//   - Return the new page ID
//
// 2. DeletePage() - Delete a page
//   - Check if the page is in the pool
//   - If pinned, return false
//   - Flush if dirty
//   - Remove from page table and replacer
//
// 3. ReadPage() / CheckedReadPage() - Get a page for reading
//   - If page is in pool, pin and return
//   - If not, find/evict a frame, load from disk
//   - Acquire read latch
//
// 4. WritePage() / CheckedWritePage() - Get a page for writing
//   - Same as ReadPage but with write latch
//
// 5. FlushPage() - Write page to disk
//   - Find the page in the pool
//   - Write to disk using disk scheduler
//   - Clear dirty flag
//
// Key Data Structures:
// - frames: Array of FrameHeader (the actual memory slots)
// - pageTable: Map from PageID to FrameID
// - freeFrames: List of available frame IDs
// - replacer: Tracks eviction candidates
type BufferPoolManager struct {
	// numFrames is the total number of frames in the pool.
	numFrames int

	// nextPageID is the next page ID to allocate.
	nextPageID atomic.Int32

	// bpmLatch protects the buffer pool's internal data structures.
	// Must be held when modifying pageTable, freeFrames, etc.
	bpmLatch sync.Mutex

	// frames holds all the frame headers.
	// Indexed by FrameID (0 to numFrames-1).
	frames []*FrameHeader

	// pageTable maps page IDs to frame IDs.
	// Allows O(1) lookup of where a page is stored.
	pageTable map[common.PageID]common.FrameID

	// freeFrames is a list of frame IDs that are not currently in use.
	// Used for fast allocation when creating new pages.
	freeFrames []common.FrameID

	// replacer implements the page replacement policy.
	// Used to select a victim frame when the pool is full.
	replacer Replacer

	// diskScheduler handles async I/O operations.
	diskScheduler *disk.DiskScheduler
}

// NewBufferPoolManager creates a new buffer pool manager.
//
// Parameters:
// - numFrames: Number of frames in the pool
// - diskManager: The disk manager for I/O operations
//
// TODO (ASSIGNMENT): Initialize all data structures
func NewBufferPoolManager(numFrames int, diskManager *disk.DiskManager) *BufferPoolManager {
	bpm := &BufferPoolManager{
		numFrames:     numFrames,
		frames:        make([]*FrameHeader, numFrames),
		pageTable:     make(map[common.PageID]common.FrameID),
		freeFrames:    make([]common.FrameID, 0, numFrames),
		replacer:      NewLRUKReplacer(numFrames, common.LRUKReplacerK),
		diskScheduler: disk.NewDiskScheduler(diskManager),
	}

	// Initialize all frames and add to free list
	for i := 0; i < numFrames; i++ {
		frameID := common.FrameID(i)
		bpm.frames[i] = NewFrameHeader(frameID)
		bpm.freeFrames = append(bpm.freeFrames, frameID)
	}

	bpm.nextPageID.Store(0)

	return bpm
}

// Size returns the number of frames in the buffer pool.
func (bpm *BufferPoolManager) Size() int {
	return bpm.numFrames
}

// NewPage allocates a new page in the buffer pool.
//
// TODO (ASSIGNMENT 1):
// =====================
// Create a new page in the buffer pool.
//
// Steps:
// 1. Acquire bpmLatch
// 2. Find a free frame or evict a victim
//   - If freeFrames is not empty, use one
//   - Otherwise, use replacer.Evict() to find a victim
//   - If victim is dirty, flush it first
//
// 3. Allocate a new page ID (nextPageID++)
// 4. Initialize the frame:
//   - Reset the frame
//   - Set the page ID
//   - Pin count = 1
//   - Add to page table
//
// 5. Record access in replacer (not evictable since pinned)
// 6. Return the new page ID
//
// Returns:
// - The new page ID, or InvalidPageID if no frames available
//
// Thread Safety: Must be thread-safe.
func (bpm *BufferPoolManager) NewPage() common.PageID {
	bpm.bpmLatch.Lock()
	defer bpm.bpmLatch.Unlock()

	// TODO (ASSIGNMENT): Implement NewPage
	//
	// Step 1: Find a free frame or evict
	// var frameID FrameID
	// if len(bpm.freeFrames) > 0 {
	//     // Pop from free list
	//     frameID = bpm.freeFrames[len(bpm.freeFrames)-1]
	//     bpm.freeFrames = bpm.freeFrames[:len(bpm.freeFrames)-1]
	// } else {
	//     // Try to evict
	//     var ok bool
	//     frameID, ok = bpm.replacer.Evict()
	//     if !ok {
	//         return InvalidPageID // No evictable frames
	//     }
	//     // Get the evicted frame and flush if dirty
	//     frame := bpm.frames[frameID]
	//     if frame.IsDirty() {
	//         // Flush to disk
	//     }
	//     // Remove old page from page table
	//     delete(bpm.pageTable, frame.GetPageID())
	// }
	//
	// Step 2: Allocate page ID
	// pageID := PageID(bpm.nextPageID.Add(1) - 1)
	//
	// Step 3: Initialize frame
	// frame := bpm.frames[frameID]
	// frame.Reset()
	// frame.SetPageID(pageID)
	// frame.IncrementPinCount()
	//
	// Step 4: Update tracking
	// bpm.pageTable[pageID] = frameID
	// bpm.replacer.RecordAccess(frameID, AccessTypeUnknown)
	// bpm.replacer.SetEvictable(frameID, false) // Pinned
	//
	// return pageID

	panic("TODO: Implement BufferPoolManager.NewPage()")
}

// DeletePage deletes a page from the buffer pool.
//
// TODO (ASSIGNMENT 1):
// =====================
// Remove a page from the buffer pool and disk.
//
// Steps:
// 1. Acquire bpmLatch
// 2. Check if page is in the page table
//   - If not, return true (nothing to delete)
//
// 3. Check if the page is pinned
//   - If pinned (pin count > 0), return false
//
// 4. If dirty, DO NOT flush (we're deleting anyway)
// 5. Remove from page table
// 6. Remove from replacer
// 7. Add frame to free list
// 8. Request disk scheduler to deallocate
//
// Returns:
// - true if the page was deleted or didn't exist
// - false if the page is pinned and cannot be deleted
func (bpm *BufferPoolManager) DeletePage(pageID common.PageID) bool {
	bpm.bpmLatch.Lock()
	defer bpm.bpmLatch.Unlock()

	// TODO (ASSIGNMENT): Implement DeletePage
	//
	// frameID, exists := bpm.pageTable[pageID]
	// if !exists {
	//     return true // Page not in pool
	// }
	//
	// frame := bpm.frames[frameID]
	// if frame.GetPinCount() > 0 {
	//     return false // Cannot delete pinned page
	// }
	//
	// // Remove from tracking
	// delete(bpm.pageTable, pageID)
	// bpm.replacer.Remove(frameID)
	//
	// // Reset frame and add to free list
	// frame.Reset()
	// bpm.freeFrames = append(bpm.freeFrames, frameID)
	//
	// // Deallocate from disk
	// bpm.diskScheduler.DeallocatePage(pageID)
	//
	// return true

	panic("TODO: Implement BufferPoolManager.DeletePage()")
}

// CheckedReadPage attempts to get a page for reading.
//
// Returns an optional guard - empty if the page couldn't be fetched.
// Use this when the page might not exist.
//
// TODO (ASSIGNMENT 1):
// Same as ReadPage but returns optional guard instead of panicking.
func (bpm *BufferPoolManager) CheckedReadPage(pageID common.PageID, accessType common.AccessType) OptionalReadPageGuard {
	// TODO (ASSIGNMENT): Implement CheckedReadPage
	// Return None() if page can't be fetched
	// Return Some(guard) if successful

	panic("TODO: Implement BufferPoolManager.CheckedReadPage()")
}

// ReadPage gets a page for reading.
//
// TODO (ASSIGNMENT 1):
// =====================
// Fetch a page and return a read guard.
//
// Steps:
// 1. Acquire bpmLatch
// 2. Check if page is already in the pool
//   - If yes, pin it and record access
//   - If no, fetch from disk:
//     a. Find a free frame or evict
//     b. Read page from disk
//     c. Initialize frame with page data
//     d. Add to page table
//
// 3. Acquire read latch on the frame
// 4. Return ReadPageGuard
//
// Panics if the page cannot be fetched.
func (bpm *BufferPoolManager) ReadPage(pageID common.PageID, accessType common.AccessType) *ReadPageGuard {
	// TODO (ASSIGNMENT): Implement ReadPage
	// Hint: Call CheckedReadPage and unwrap, or panic

	panic("TODO: Implement BufferPoolManager.ReadPage()")
}

// CheckedWritePage attempts to get a page for writing.
//
// Returns an optional guard - empty if the page couldn't be fetched.
//
// TODO (ASSIGNMENT 1):
// Same as WritePage but returns optional guard instead of panicking.
func (bpm *BufferPoolManager) CheckedWritePage(pageID common.PageID, accessType common.AccessType) OptionalWritePageGuard {
	// TODO (ASSIGNMENT): Implement CheckedWritePage

	panic("TODO: Implement BufferPoolManager.CheckedWritePage()")
}

// WritePage gets a page for writing.
//
// TODO (ASSIGNMENT 1):
// =====================
// Fetch a page and return a write guard.
//
// Same as ReadPage but:
// - Acquires write latch instead of read latch
// - Returns WritePageGuard
//
// Panics if the page cannot be fetched.
func (bpm *BufferPoolManager) WritePage(pageID common.PageID, accessType common.AccessType) *WritePageGuard {
	// TODO (ASSIGNMENT): Implement WritePage

	panic("TODO: Implement BufferPoolManager.WritePage()")
}

// FlushPage writes a specific page to disk.
//
// TODO (ASSIGNMENT 1):
// =====================
// Write the page's contents to disk if it exists in the pool.
//
// Steps:
// 1. Acquire bpmLatch
// 2. Find the frame containing the page
// 3. If not found, return false
// 4. Acquire the frame's latch
// 5. Write to disk via disk scheduler
// 6. Clear the dirty flag
// 7. Return true
//
// Note: This is "unsafe" in C++ terminology because it doesn't
// check the latch - the caller is responsible for synchronization.
func (bpm *BufferPoolManager) FlushPage(pageID common.PageID) bool {
	bpm.bpmLatch.Lock()
	defer bpm.bpmLatch.Unlock()

	// TODO (ASSIGNMENT): Implement FlushPage
	//
	// frameID, exists := bpm.pageTable[pageID]
	// if !exists {
	//     return false
	// }
	//
	// frame := bpm.frames[frameID]
	//
	// // Create disk request
	// promise := bpm.diskScheduler.CreatePromise()
	// request := DiskRequest{
	//     IsWrite:  true,
	//     Data:     frame.GetData(),
	//     PageID:   pageID,
	//     Callback: promise,
	// }
	// bpm.diskScheduler.Schedule([]DiskRequest{request})
	//
	// // Wait for completion
	// <-promise
	//
	// frame.SetDirty(false)
	// return true

	panic("TODO: Implement BufferPoolManager.FlushPage()")
}

// FlushAllPages writes all dirty pages to disk.
//
// TODO (ASSIGNMENT 1):
// Iterate over all frames and flush any that are dirty.
func (bpm *BufferPoolManager) FlushAllPages() {
	bpm.bpmLatch.Lock()
	defer bpm.bpmLatch.Unlock()

	// TODO (ASSIGNMENT): Implement FlushAllPages
	// for pageID := range bpm.pageTable {
	//     bpm.FlushPageUnsafe(pageID)
	// }

	panic("TODO: Implement BufferPoolManager.FlushAllPages()")
}

// GetPinCount returns the pin count for a page.
//
// Returns:
// - The pin count and true if the page is in the pool
// - 0 and false if the page is not in the pool
func (bpm *BufferPoolManager) GetPinCount(pageID common.PageID) (int, bool) {
	bpm.bpmLatch.Lock()
	defer bpm.bpmLatch.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return 0, false
	}

	return bpm.frames[frameID].GetPinCount(), true
}

// =============================================================================
// HELPER METHODS (Private)
// =============================================================================

// getFrame returns the frame for a given frame ID.
func (bpm *BufferPoolManager) getFrame(frameID common.FrameID) *FrameHeader {
	if frameID < 0 || int(frameID) >= len(bpm.frames) {
		return nil
	}
	return bpm.frames[frameID]
}

// findFrame attempts to find or create a frame for a page.
//
// This is a helper that:
// 1. Checks free list
// 2. Tries eviction if no free frames
// 3. Returns InvalidFrameID if nothing available
func (bpm *BufferPoolManager) findFrame() (common.FrameID, error) {
	// Try free list first
	if len(bpm.freeFrames) > 0 {
		frameID := bpm.freeFrames[len(bpm.freeFrames)-1]
		bpm.freeFrames = bpm.freeFrames[:len(bpm.freeFrames)-1]
		return frameID, nil
	}

	// Try eviction
	frameID, ok := bpm.replacer.Evict()
	if !ok {
		return common.InvalidFrameID, common.ErrBufferPoolFull
	}

	return frameID, nil
}
