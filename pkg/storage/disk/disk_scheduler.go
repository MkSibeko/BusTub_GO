// Package disk - DiskScheduler for async I/O operations.
//
// The DiskScheduler manages a background worker that processes disk read/write
// requests asynchronously. This allows the buffer pool manager to continue
// processing while I/O operations complete in the background.
//
// ASSIGNMENT NOTES:
// ==================
// You may need to implement parts of DiskScheduler as part of Assignment 1.
// Key concepts:
//
// 1. Request Queue: Disk requests are queued for background processing
// 2. Worker Goroutine: A background goroutine processes requests
// 3. Promise/Future Pattern: Callers wait on a channel for completion
// 4. Graceful Shutdown: Worker must stop when scheduler is destroyed
//
// This mirrors: bustub/src/include/storage/disk/disk_scheduler.h
package disk

import (
	"sync"

	"github.com/bustub-go/pkg/common"
)

// DiskRequest represents a single read or write request to the disk.
//
// The request includes:
// - IsWrite: Whether this is a write (true) or read (false)
// - Data: Pointer to the memory buffer
// - PageID: Which page to read/write
// - Callback: Channel to signal completion
type DiskRequest struct {
	// IsWrite is true for write operations, false for reads.
	IsWrite bool

	// Data is the buffer to read into or write from.
	// Must be exactly PageSize bytes.
	Data []byte

	// PageID is the page to read from or write to.
	PageID common.PageID

	// Callback is signaled when the request completes.
	// The boolean indicates success (true) or failure (false).
	Callback chan bool
}

// DiskScheduler schedules disk read and write operations.
//
// It maintains a background worker goroutine that processes requests from
// a queue. The worker reads from the request channel and performs I/O
// using the provided DiskManager.
//
// TODO (ASSIGNMENT 1 - DISK SCHEDULER):
// ======================================
// You need to implement:
// 1. StartWorkerThread() - Create and run the background worker
// 2. Schedule() - Add requests to the queue
// 3. Worker logic - Process requests from the queue
// 4. Shutdown - Gracefully stop the worker
//
// Key implementation details:
// - Use a channel as the request queue
// - The worker should loop, getting requests from the channel
// - For each request, call DiskManager.ReadPage or WritePage
// - Signal completion on the request's Callback channel
// - When shutting down, close the channel and wait for worker to finish
type DiskScheduler struct {
	// diskManager is used to perform actual I/O operations.
	diskManager *DiskManager

	// requestQueue is the channel for incoming disk requests.
	// nil is sent to signal shutdown.
	requestQueue chan *DiskRequest

	// backgroundThread tracks the worker goroutine.
	// Using sync.WaitGroup to wait for it during shutdown.
	wg sync.WaitGroup

	// shuttingDown indicates the scheduler is being destroyed.
	shuttingDown bool

	// mu protects shuttingDown flag.
	mu sync.Mutex
}

// NewDiskScheduler creates a new DiskScheduler.
//
// Parameters:
// - diskManager: The underlying DiskManager to use for I/O
//
// TODO (ASSIGNMENT):
// Initialize the request queue channel and start the worker thread.
func NewDiskScheduler(diskManager *DiskManager) *DiskScheduler {
	ds := &DiskScheduler{
		diskManager:  diskManager,
		requestQueue: make(chan *DiskRequest, 128), // Buffered channel
		shuttingDown: false,
	}

	// Start the background worker
	ds.StartWorkerThread()

	return ds
}

// Schedule adds disk requests to the queue for background processing.
//
// Parameters:
// - requests: Slice of DiskRequest to process
//
// TODO (ASSIGNMENT):
// ==================
// For each request in the slice:
// 1. Add it to the request queue channel
// 2. Handle the case where the scheduler is shutting down
//
// Note: In C++ this takes a vector. In Go, we use a slice.
// Consider whether to block or return error if queue is full.
func (ds *DiskScheduler) Schedule(requests []DiskRequest) {
	ds.mu.Lock()
	if ds.shuttingDown {
		ds.mu.Unlock()
		// Signal failure on all callbacks
		for i := range requests {
			if requests[i].Callback != nil {
				requests[i].Callback <- false
			}
		}
		return
	}
	ds.mu.Unlock()

	// TODO (ASSIGNMENT): Implement request scheduling
	// Hint: Send each request to the requestQueue channel
	//
	// for i := range requests {
	//     ds.requestQueue <- &requests[i]
	// }
	panic("TODO: Implement Schedule()")
}

// StartWorkerThread creates and starts the background worker goroutine.
//
// TODO (ASSIGNMENT):
// ==================
// The worker should:
// 1. Loop forever, reading requests from the queue
// 2. For each request:
//   - If nil, shutdown (break the loop)
//   - If IsWrite, call diskManager.WritePage()
//   - If !IsWrite, call diskManager.ReadPage()
//   - Signal completion on the Callback channel
//
// 3. Decrement WaitGroup when done
//
// Example worker logic:
//
//	go func() {
//	    defer ds.wg.Done()
//	    for request := range ds.requestQueue {
//	        if request == nil {
//	            return // Shutdown signal
//	        }
//	        var err error
//	        if request.IsWrite {
//	            err = ds.diskManager.WritePage(request.PageID, request.Data)
//	        } else {
//	            err = ds.diskManager.ReadPage(request.PageID, request.Data)
//	        }
//	        if request.Callback != nil {
//	            request.Callback <- (err == nil)
//	        }
//	    }
//	}()
func (ds *DiskScheduler) StartWorkerThread() {
	ds.wg.Add(1)

	// TODO (ASSIGNMENT): Implement the background worker goroutine
	// See the hints above for implementation details
	go func() {
		defer ds.wg.Done()
		// TODO: Implement worker loop
		panic("TODO: Implement background worker")
	}()
}

// CreatePromise creates a new promise channel for async completion.
//
// Returns a channel that will receive true on success or false on failure.
// The caller should wait on this channel after scheduling a request.
func (ds *DiskScheduler) CreatePromise() chan bool {
	return make(chan bool, 1)
}

// DeallocatePage requests the disk manager to delete a page.
//
// This is a convenience method that delegates to DiskManager.DeletePage.
func (ds *DiskScheduler) DeallocatePage(pageID common.PageID) error {
	return ds.diskManager.DeletePage(pageID)
}

// Shutdown gracefully stops the disk scheduler.
//
// TODO (ASSIGNMENT):
// 1. Set shuttingDown flag
// 2. Send nil to the queue to signal the worker to stop
// 3. Close the channel
// 4. Wait for the worker to finish (using WaitGroup)
func (ds *DiskScheduler) Shutdown() {
	ds.mu.Lock()
	if ds.shuttingDown {
		ds.mu.Unlock()
		return
	}
	ds.shuttingDown = true
	ds.mu.Unlock()

	// Send shutdown signal and close channel
	close(ds.requestQueue)

	// Wait for worker to finish
	ds.wg.Wait()
}
