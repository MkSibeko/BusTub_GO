// Package disk provides disk I/O management for BusTub-Go.
//
// DiskManager handles reading and writing pages to/from disk.
// It provides a logical file layer within the context of the database system.
//
// ASSIGNMENT NOTES:
// ==================
// The DiskManager is typically PROVIDED to you (not an assignment).
// However, understanding how it works is crucial:
//
// 1. Pages are the unit of transfer between disk and memory
// 2. Each page has a unique PageID
// 3. Pages are stored at specific offsets in the database file
// 4. DiskManager uses lazy allocation - only allocates space when needed
// 5. Deleted pages can be reused
//
// This mirrors: bustub/src/include/storage/disk/disk_manager.h
package disk

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/bustub-go/pkg/common"
)

// DiskManager handles the reading and writing of pages to and from disk.
//
// Key responsibilities:
// - Read pages from disk into memory buffers
// - Write pages from memory to disk
// - Manage free space when pages are deleted
// - Provide atomic page I/O operations
//
// Implementation uses lazy allocation: pages are only written to disk
// when they are first accessed/written.
type DiskManager struct {
	// dbFilePath is the path to the database file.
	dbFilePath string

	// dbFile is the file handle for the database file.
	dbFile *os.File

	// logFilePath is the path to the write-ahead log file.
	logFilePath string

	// logFile is the file handle for the log file.
	logFile *os.File

	// mu protects concurrent access to the file.
	mu sync.Mutex

	// pageCapacity is the current capacity in pages.
	pageCapacity int

	// pages maps page IDs to their offsets in the file.
	// This allows non-contiguous page storage.
	pages map[common.PageID]int64

	// freeSlots tracks available slots from deleted pages.
	// These can be reused for new pages.
	freeSlots []int64

	// Statistics for monitoring/debugging
	numFlushes int
	numWrites  int
	numDeletes int
	numReads   int
}

// NewDiskManager creates a new DiskManager for the given database file.
//
// If the file doesn't exist, it will be created.
// If it exists, it will be opened for read/write.
//
// TODO (ASSIGNMENT - DISK MANAGER):
// ===================================
// Students typically don't implement this, but should understand:
// 1. How file I/O works at the OS level
// 2. How pages map to file offsets
// 3. Why we need synchronization for concurrent access
func NewDiskManager(dbFilePath string) (*DiskManager, error) {
	// Open or create the database file
	dbFile, err := os.OpenFile(dbFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	// Create log file path (same name with .log extension)
	logFilePath := dbFilePath + ".log"
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		dbFile.Close()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	dm := &DiskManager{
		dbFilePath:   dbFilePath,
		dbFile:       dbFile,
		logFilePath:  logFilePath,
		logFile:      logFile,
		pageCapacity: common.DefaultDBIOSize,
		pages:        make(map[common.PageID]int64),
		freeSlots:    make([]int64, 0),
	}

	return dm, nil
}

// ReadPage reads the contents of a page from disk into the provided buffer.
//
// Parameters:
// - pageID: The ID of the page to read
// - data: Buffer to store the page data (must be PageSize bytes)
//
// Returns error if:
// - pageID is invalid
// - I/O error occurs
// - Page doesn't exist on disk
//
// TODO (ASSIGNMENT):
// ==================
// This is typically provided. Key points to understand:
// 1. Calculate file offset from pageID
// 2. Seek to that position
// 3. Read exactly PageSize bytes
// 4. Handle partial reads
func (dm *DiskManager) ReadPage(pageID common.PageID, data []byte) error {
	if len(data) != common.PageSize {
		return fmt.Errorf("buffer size must be %d bytes", common.PageSize)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Find the offset for this page
	offset, exists := dm.pages[pageID]
	if !exists {
		// Page doesn't exist - return zero-filled page
		for i := range data {
			data[i] = 0
		}
		return nil
	}

	// Seek to the page's position
	_, err := dm.dbFile.Seek(offset, io.SeekStart)
	if err != nil {
		return common.NewPageError(pageID, "seek", err)
	}

	// Read the page data
	bytesRead, err := dm.dbFile.Read(data)
	if err != nil && err != io.EOF {
		return common.NewPageError(pageID, "read", err)
	}

	// If we read less than a full page, zero-fill the rest
	for i := bytesRead; i < common.PageSize; i++ {
		data[i] = 0
	}

	dm.numReads++
	return nil
}

// WritePage writes a page from memory to disk.
//
// Parameters:
// - pageID: The ID of the page to write
// - data: Buffer containing the page data (must be PageSize bytes)
//
// Returns error if:
// - pageID is invalid
// - I/O error occurs
//
// TODO (ASSIGNMENT):
// ==================
// This is typically provided. Key points to understand:
// 1. Calculate or allocate file offset for pageID
// 2. Seek to that position
// 3. Write exactly PageSize bytes
// 4. Ensure durability (fsync in production)
func (dm *DiskManager) WritePage(pageID common.PageID, data []byte) error {
	if len(data) != common.PageSize {
		return fmt.Errorf("buffer size must be %d bytes", common.PageSize)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Find or allocate offset for this page
	offset, exists := dm.pages[pageID]
	if !exists {
		offset = dm.allocatePage()
		dm.pages[pageID] = offset
	}

	// Seek to the page's position
	_, err := dm.dbFile.Seek(offset, io.SeekStart)
	if err != nil {
		return common.NewPageError(pageID, "seek", err)
	}

	// Write the page data
	bytesWritten, err := dm.dbFile.Write(data)
	if err != nil {
		return common.NewPageError(pageID, "write", err)
	}
	if bytesWritten != common.PageSize {
		return common.NewPageError(pageID, "write", fmt.Errorf("short write: %d bytes", bytesWritten))
	}

	dm.numWrites++
	return nil
}

// DeletePage marks a page as deleted and reclaims its space.
//
// The page's slot becomes available for future allocations.
//
// TODO (ASSIGNMENT):
// ==================
// Key points:
// 1. Remove the page from the pages map
// 2. Add the slot to freeSlots for reuse
func (dm *DiskManager) DeletePage(pageID common.PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset, exists := dm.pages[pageID]
	if !exists {
		return common.ErrPageNotFound
	}

	// Remove from pages map
	delete(dm.pages, pageID)

	// Add to free slots for reuse
	dm.freeSlots = append(dm.freeSlots, offset)

	dm.numDeletes++
	return nil
}

// allocatePage returns the file offset for a new page.
// It reuses deleted slots if available, otherwise allocates at the end.
func (dm *DiskManager) allocatePage() int64 {
	// Try to reuse a free slot
	if len(dm.freeSlots) > 0 {
		offset := dm.freeSlots[len(dm.freeSlots)-1]
		dm.freeSlots = dm.freeSlots[:len(dm.freeSlots)-1]
		return offset
	}

	// Allocate at the end of the file
	offset := int64(dm.pageCapacity) * int64(common.PageSize)
	dm.pageCapacity++
	return offset
}

// Shutdown closes the disk manager and releases resources.
func (dm *DiskManager) Shutdown() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var err error
	if dm.dbFile != nil {
		if syncErr := dm.dbFile.Sync(); syncErr != nil {
			err = syncErr
		}
		if closeErr := dm.dbFile.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		dm.dbFile = nil
	}

	if dm.logFile != nil {
		if syncErr := dm.logFile.Sync(); syncErr != nil && err == nil {
			err = syncErr
		}
		if closeErr := dm.logFile.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		dm.logFile = nil
	}

	return err
}

// WriteLog writes log data to the write-ahead log file.
func (dm *DiskManager) WriteLog(logData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	_, err := dm.logFile.Write(logData)
	if err != nil {
		return fmt.Errorf("failed to write log: %w", err)
	}

	dm.numFlushes++
	return nil
}

// ReadLog reads log data from the specified offset.
func (dm *DiskManager) ReadLog(data []byte, offset int64) (int, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	_, err := dm.logFile.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return dm.logFile.Read(data)
}

// GetNumFlushes returns the number of flush operations.
func (dm *DiskManager) GetNumFlushes() int {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.numFlushes
}

// GetNumWrites returns the number of write operations.
func (dm *DiskManager) GetNumWrites() int {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.numWrites
}

// GetNumDeletes returns the number of delete operations.
func (dm *DiskManager) GetNumDeletes() int {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.numDeletes
}

// GetDbFileSize returns the current size of the database file.
func (dm *DiskManager) GetDbFileSize() (int64, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	info, err := dm.dbFile.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
