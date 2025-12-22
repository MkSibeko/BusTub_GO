// Package buffer - Frame Header for buffer pool management.
//
// FrameHeader represents a slot in the buffer pool that can hold one page.
// It contains metadata about the frame and a pointer to the page data.
//
// ASSIGNMENT NOTES:
// ==================
// FrameHeader is used by BufferPoolManager to track frame state.
// Key responsibilities:
// - Track which page (if any) is stored in this frame
// - Manage pin count (reference counting)
// - Track dirty status
// - Provide locking for concurrent access
//
// This mirrors: bustub/src/include/buffer/buffer_pool_manager.h (FrameHeader class)
package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/bustub-go/pkg/common"
)

// FrameHeader manages a frame of memory in the buffer pool.
//
// A frame is a slot that can hold exactly one page's worth of data.
// The FrameHeader tracks:
// - The frame's ID (position in the buffer pool)
// - The page currently stored (if any)
// - Pin count (how many users are accessing this page)
// - Dirty status (whether the page has been modified)
//
// Thread Safety:
// - pinCount and isDirty use atomic operations
// - rwLatch provides read/write locking for data access
// - The buffer pool manager's latch protects structural changes
type FrameHeader struct {
	// frameID is the index of this frame in the buffer pool.
	frameID common.FrameID

	// pageID is the ID of the page currently stored in this frame.
	// InvalidPageID if the frame is empty.
	pageID common.PageID

	// rwLatch provides read/write locking for the frame's data.
	rwLatch sync.RWMutex

	// pinCount tracks how many references exist to this frame.
	// A frame with pinCount > 0 cannot be evicted.
	pinCount atomic.Int64

	// isDirty is true if the page has been modified since loading.
	isDirty bool

	// data holds the actual page content.
	// Allocated as a separate slice to help with memory safety.
	data []byte
}

// NewFrameHeader creates a new frame header with the given ID.
func NewFrameHeader(frameID common.FrameID) *FrameHeader {
	return &FrameHeader{
		frameID: frameID,
		pageID:  common.InvalidPageID,
		isDirty: false,
		data:    make([]byte, common.PageSize),
	}
}

// GetData returns a read-only view of the frame's data.
func (f *FrameHeader) GetData() []byte {
	return f.data
}

// GetDataMut returns a mutable reference to the frame's data.
func (f *FrameHeader) GetDataMut() []byte {
	return f.data
}

// GetFrameID returns the frame ID.
func (f *FrameHeader) GetFrameID() common.FrameID {
	return f.frameID
}

// GetPageID returns the page ID currently stored in this frame.
func (f *FrameHeader) GetPageID() common.PageID {
	return f.pageID
}

// SetPageID sets the page ID for this frame.
func (f *FrameHeader) SetPageID(pageID common.PageID) {
	f.pageID = pageID
}

// GetPinCount returns the current pin count.
func (f *FrameHeader) GetPinCount() int {
	return int(f.pinCount.Load())
}

// IncrementPinCount atomically increments the pin count.
func (f *FrameHeader) IncrementPinCount() {
	f.pinCount.Add(1)
}

// DecrementPinCount atomically decrements the pin count.
// Returns the new pin count.
func (f *FrameHeader) DecrementPinCount() int {
	return int(f.pinCount.Add(-1))
}

// IsDirty returns true if the frame's data has been modified.
func (f *FrameHeader) IsDirty() bool {
	return f.isDirty
}

// SetDirty marks the frame as dirty or clean.
func (f *FrameHeader) SetDirty(dirty bool) {
	f.isDirty = dirty
}

// Reset clears the frame for reuse.
//
// This should be called when a page is evicted and the frame
// is being prepared for a new page.
func (f *FrameHeader) Reset() {
	// Zero out the data
	for i := range f.data {
		f.data[i] = 0
	}
	f.pageID = common.InvalidPageID
	f.pinCount.Store(0)
	f.isDirty = false
}

// RLock acquires the read lock.
func (f *FrameHeader) RLock() {
	f.rwLatch.RLock()
}

// RUnlock releases the read lock.
func (f *FrameHeader) RUnlock() {
	f.rwLatch.RUnlock()
}

// Lock acquires the write lock.
func (f *FrameHeader) Lock() {
	f.rwLatch.Lock()
}

// Unlock releases the write lock.
func (f *FrameHeader) Unlock() {
	f.rwLatch.Unlock()
}

// TryRLock attempts to acquire the read lock without blocking.
// Returns true if the lock was acquired.
func (f *FrameHeader) TryRLock() bool {
	return f.rwLatch.TryRLock()
}

// TryLock attempts to acquire the write lock without blocking.
// Returns true if the lock was acquired.
func (f *FrameHeader) TryLock() bool {
	return f.rwLatch.TryLock()
}
