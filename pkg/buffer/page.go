// Package buffer - Page representation.
//
// Page is the basic unit of storage in the database system.
// Pages are fixed-size blocks of data (8KB) that are:
// - Stored on disk
// - Cached in the buffer pool
// - Used by all higher-level components
//
// This mirrors: bustub/src/include/storage/page/page.h
package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/bustub-go/pkg/common"
)

// Page represents a page of data in the database.
//
// Each page contains:
// - Raw data (PageSize bytes)
// - Metadata for buffer pool management (page ID, pin count, dirty flag)
// - Latch for concurrent access control
//
// Note: This is the in-memory representation. On disk, only the data is stored.
type Page struct {
	// data holds the actual page content.
	// Must be exactly PageSize bytes.
	data []byte

	// pageID identifies this page on disk.
	pageID common.PageID

	// pinCount tracks how many operations are using this page.
	// A page with pinCount > 0 cannot be evicted.
	pinCount atomic.Int32

	// isDirty indicates if the page has been modified since loading.
	// Dirty pages must be written back to disk before eviction.
	isDirty atomic.Bool

	// rwLatch protects the page data from concurrent access.
	// - Read latch: Multiple readers allowed
	// - Write latch: Exclusive access
	rwLatch sync.RWMutex
}

// NewPage creates a new page with zeroed data.
func NewPage() *Page {
	p := &Page{
		data:   make([]byte, common.PageSize),
		pageID: common.InvalidPageID,
	}
	p.pinCount.Store(0)
	p.isDirty.Store(false)
	return p
}

// GetData returns a read-only view of the page data.
//
// The caller should hold at least a read latch before accessing the data.
func (p *Page) GetData() []byte {
	return p.data
}

// GetDataMut returns a mutable reference to the page data.
//
// The caller MUST hold a write latch before modifying data.
// After modification, the caller should mark the page dirty.
func (p *Page) GetDataMut() []byte {
	return p.data
}

// GetPageID returns the page ID.
func (p *Page) GetPageID() common.PageID {
	return p.pageID
}

// GetPinCount returns the current pin count.
func (p *Page) GetPinCount() int {
	return int(p.pinCount.Load())
}

// IsDirty returns true if the page has been modified.
func (p *Page) IsDirty() bool {
	return p.isDirty.Load()
}

// SetDirty marks the page as dirty (modified).
func (p *Page) SetDirty(dirty bool) {
	p.isDirty.Store(dirty)
}

// WLatch acquires the write latch (exclusive access).
func (p *Page) WLatch() {
	p.rwLatch.Lock()
}

// WUnlatch releases the write latch.
func (p *Page) WUnlatch() {
	p.rwLatch.Unlock()
}

// RLatch acquires the read latch (shared access).
func (p *Page) RLatch() {
	p.rwLatch.RLock()
}

// RUnlatch releases the read latch.
func (p *Page) RUnlatch() {
	p.rwLatch.RUnlock()
}

// ResetMemory zeros out the page data and resets all fields.
//
// Called when a frame is being reused for a new page.
func (p *Page) ResetMemory() {
	for i := range p.data {
		p.data[i] = 0
	}
	p.pageID = common.InvalidPageID
	p.pinCount.Store(0)
	p.isDirty.Store(false)
}

// =============================================================================
// PAGE HEADER LAYOUT
// =============================================================================
// Pages may have a header at the beginning. The exact format depends on
// the page type (table page, index page, etc.), but common fields include:
//
// Offset 0-3: Page ID (4 bytes)
// Offset 4-7: LSN - Log Sequence Number (4 bytes)
//
// The rest of the layout is page-type specific.

const (
	// PageHeaderSize is the size of the common page header.
	PageHeaderSize = 8

	// OffsetPageStart is the offset where page content starts.
	OffsetPageStart = 0

	// OffsetLSN is the offset of the LSN in the page header.
	OffsetLSN = 4
)

// GetLSN reads the Log Sequence Number from the page header.
func (p *Page) GetLSN() common.LSN {
	// LSN is stored at offset 4 as a 4-byte integer
	if len(p.data) < 8 {
		return common.InvalidLSN
	}
	return common.LSN(
		int32(p.data[4]) |
			int32(p.data[5])<<8 |
			int32(p.data[6])<<16 |
			int32(p.data[7])<<24,
	)
}

// SetLSN writes the Log Sequence Number to the page header.
func (p *Page) SetLSN(lsn common.LSN) {
	if len(p.data) < 8 {
		return
	}
	p.data[4] = byte(lsn)
	p.data[5] = byte(lsn >> 8)
	p.data[6] = byte(lsn >> 16)
	p.data[7] = byte(lsn >> 24)
}
