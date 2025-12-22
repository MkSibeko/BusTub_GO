// Package buffer - Page Guards for safe page access.
//
// Page Guards implement a RAII-like pattern in Go for safe page access.
// They automatically handle:
// - Latching (acquiring read/write locks)
// - Pinning (preventing eviction while in use)
// - Unlatching and unpinning when dropped
//
// ASSIGNMENT NOTES:
// ==================
// Page guards are CRITICAL for correct buffer pool usage.
// They ensure:
// 1. Pages are properly locked during access
// 2. Pages are unpinned when no longer needed
// 3. Dirty pages are marked for write-back
//
// Usage pattern:
//
//	guard := bpm.ReadPage(pageID, accessType)
//	defer guard.Drop()
//	data := guard.GetData()
//	// ... use data ...
//
// Or for writing:
//
//	guard := bpm.WritePage(pageID, accessType)
//	defer guard.Drop()
//	data := guard.GetDataMut()
//	// ... modify data ...
//	// Page is automatically marked dirty
//
// This mirrors: bustub/src/include/storage/page/page_guard.h
package buffer

import (
	"github.com/bustub-go/pkg/common"
	"github.com/bustub-go/pkg/storage/disk"
)

// ReadPageGuard provides read-only access to a page.
//
// The guard holds:
// - A read latch on the frame (shared access)
// - A pin on the frame (prevents eviction)
//
// When dropped:
// - The read latch is released
// - The pin count is decremented
// - The frame may become evictable
type ReadPageGuard struct {
	// bpm is the buffer pool manager that owns the page.
	// Needed for flushing and updating replacer.
	bpm *BufferPoolManager

	// frame is the frame header containing the page.
	frame *FrameHeader

	// dropped indicates if Drop() has been called.
	dropped bool
}

// NewReadPageGuard creates a new read page guard.
//
// The frame should already have:
// - Read latch acquired
// - Pin count incremented
func NewReadPageGuard(bpm *BufferPoolManager, frame *FrameHeader) *ReadPageGuard {
	return &ReadPageGuard{
		bpm:     bpm,
		frame:   frame,
		dropped: false,
	}
}

// GetPageID returns the page ID of the guarded page.
func (g *ReadPageGuard) GetPageID() common.PageID {
	if g.dropped || g.frame == nil {
		return common.InvalidPageID
	}
	return g.frame.GetPageID()
}

// GetData returns a read-only view of the page data.
//
// Panics if the guard has been dropped.
func (g *ReadPageGuard) GetData() []byte {
	if g.dropped {
		panic("ReadPageGuard: accessing dropped guard")
	}
	return g.frame.GetData()
}

// Drop releases the read latch and decrements the pin count.
//
// After calling Drop, the guard should not be used.
// It's safe to call Drop multiple times (subsequent calls are no-ops).
//
// Typically called via defer:
//
//	guard := bpm.ReadPage(pageID, accessType)
//	defer guard.Drop()
func (g *ReadPageGuard) Drop() {
	if g.dropped || g.frame == nil {
		return
	}

	// Release the read latch
	g.frame.RUnlock()

	// Decrement pin count and possibly mark evictable
	// (This should be handled by the BPM, but we simulate it here)
	newPinCount := g.frame.DecrementPinCount()

	// Notify replacer if pin count reaches 0
	if g.bpm != nil && newPinCount == 0 {
		// The frame is now evictable
		// bpm.replacer.SetEvictable(g.frame.GetFrameID(), true)
	}

	g.dropped = true
}

// IsValid returns true if the guard is valid (not dropped).
func (g *ReadPageGuard) IsValid() bool {
	return !g.dropped && g.frame != nil
}

// WritePageGuard provides read-write access to a page.
//
// The guard holds:
// - A write latch on the frame (exclusive access)
// - A pin on the frame (prevents eviction)
//
// When modified through GetDataMut(), the page is automatically marked dirty.
//
// When dropped:
// - The write latch is released
// - The pin count is decremented
// - The frame may become evictable
type WritePageGuard struct {
	// bpm is the buffer pool manager that owns the page.
	bpm *BufferPoolManager

	// diskScheduler for flushing pages.
	diskScheduler *disk.DiskScheduler

	// frame is the frame header containing the page.
	frame *FrameHeader

	// dropped indicates if Drop() has been called.
	dropped bool
}

// NewWritePageGuard creates a new write page guard.
//
// The frame should already have:
// - Write latch acquired
// - Pin count incremented
func NewWritePageGuard(bpm *BufferPoolManager, frame *FrameHeader, scheduler *disk.DiskScheduler) *WritePageGuard {
	return &WritePageGuard{
		bpm:           bpm,
		diskScheduler: scheduler,
		frame:         frame,
		dropped:       false,
	}
}

// GetPageID returns the page ID of the guarded page.
func (g *WritePageGuard) GetPageID() common.PageID {
	if g.dropped || g.frame == nil {
		return common.InvalidPageID
	}
	return g.frame.GetPageID()
}

// GetData returns a read-only view of the page data.
//
// For read-only access within a write guard context.
func (g *WritePageGuard) GetData() []byte {
	if g.dropped {
		panic("WritePageGuard: accessing dropped guard")
	}
	return g.frame.GetData()
}

// GetDataMut returns a mutable reference to the page data.
//
// Automatically marks the page as dirty.
// Panics if the guard has been dropped.
func (g *WritePageGuard) GetDataMut() []byte {
	if g.dropped {
		panic("WritePageGuard: accessing dropped guard")
	}
	g.frame.SetDirty(true)
	return g.frame.GetDataMut()
}

// IsDirty returns true if the page has been modified.
func (g *WritePageGuard) IsDirty() bool {
	if g.dropped || g.frame == nil {
		return false
	}
	return g.frame.IsDirty()
}

// Drop releases the write latch and decrements the pin count.
//
// After calling Drop, the guard should not be used.
// It's safe to call Drop multiple times (subsequent calls are no-ops).
//
// Typically called via defer:
//
//	guard := bpm.WritePage(pageID, accessType)
//	defer guard.Drop()
func (g *WritePageGuard) Drop() {
	if g.dropped || g.frame == nil {
		return
	}

	// Release the write latch
	g.frame.Unlock()

	// Decrement pin count
	newPinCount := g.frame.DecrementPinCount()

	// Notify replacer if pin count reaches 0
	if g.bpm != nil && newPinCount == 0 {
		// The frame is now evictable
		// bpm.replacer.SetEvictable(g.frame.GetFrameID(), true)
	}

	g.dropped = true
}

// IsValid returns true if the guard is valid (not dropped).
func (g *WritePageGuard) IsValid() bool {
	return !g.dropped && g.frame != nil
}

// =============================================================================
// Optional/Checked Page Guards
// =============================================================================
// These are "maybe" types that represent optional page guards.
// Used when a page might not exist or might not be accessible.

// OptionalReadPageGuard wraps a ReadPageGuard that may or may not exist.
type OptionalReadPageGuard struct {
	guard    *ReadPageGuard
	hasValue bool
}

// Some creates an OptionalReadPageGuard containing a guard.
func Some(guard *ReadPageGuard) OptionalReadPageGuard {
	return OptionalReadPageGuard{guard: guard, hasValue: true}
}

// None creates an empty OptionalReadPageGuard.
func None() OptionalReadPageGuard {
	return OptionalReadPageGuard{guard: nil, hasValue: false}
}

// HasValue returns true if the optional contains a guard.
func (o OptionalReadPageGuard) HasValue() bool {
	return o.hasValue
}

// Value returns the contained guard.
// Panics if HasValue() is false.
func (o OptionalReadPageGuard) Value() *ReadPageGuard {
	if !o.hasValue {
		panic("OptionalReadPageGuard: no value")
	}
	return o.guard
}

// OptionalWritePageGuard wraps a WritePageGuard that may or may not exist.
type OptionalWritePageGuard struct {
	guard    *WritePageGuard
	hasValue bool
}

// SomeWrite creates an OptionalWritePageGuard containing a guard.
func SomeWrite(guard *WritePageGuard) OptionalWritePageGuard {
	return OptionalWritePageGuard{guard: guard, hasValue: true}
}

// NoneWrite creates an empty OptionalWritePageGuard.
func NoneWrite() OptionalWritePageGuard {
	return OptionalWritePageGuard{guard: nil, hasValue: false}
}

// HasValue returns true if the optional contains a guard.
func (o OptionalWritePageGuard) HasValue() bool {
	return o.hasValue
}

// Value returns the contained guard.
// Panics if HasValue() is false.
func (o OptionalWritePageGuard) Value() *WritePageGuard {
	if !o.hasValue {
		panic("OptionalWritePageGuard: no value")
	}
	return o.guard
}
