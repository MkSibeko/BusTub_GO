// Package buffer_test provides tests for the buffer pool manager.
//
// These tests verify the correct implementation of:
// - LRU-K Replacer
// - Buffer Pool Manager
//
// TESTING STRATEGY:
// =================
// 1. Unit tests for each component in isolation
// 2. Integration tests that combine components
// 3. Stress tests for concurrent access
//
// RUNNING TESTS:
// ==============
//
//	go test ./pkg/buffer/... -v
//
// Or run specific tests:
//
//	go test ./pkg/buffer/... -v -run TestLRUKReplacer
package buffer_test

import (
	"testing"

	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/common"
)

// =============================================================================
// LRU-K REPLACER TESTS
// =============================================================================

// TestLRUKReplacer_Basic tests basic LRU-K replacement operations.
//
// TODO: Uncomment and implement when LRU-K replacer is complete.
func TestLRUKReplacer_Basic(t *testing.T) {
	t.Skip("Implement LRU-K Replacer first")

	// Create replacer with k=2 and capacity for 7 frames
	replacer := buffer.NewLRUKReplacer(7, 2)

	// Initially empty, eviction should fail
	_, ok := replacer.Evict()
	if ok {
		t.Error("Evict should fail on empty replacer")
	}

	// Record accesses for frames 1, 2, 3, 4, 5
	replacer.RecordAccess(common.FrameID(1), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(2), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(3), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(4), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(5), common.AccessTypeUnknown)

	// Set all as evictable
	replacer.SetEvictable(common.FrameID(1), true)
	replacer.SetEvictable(common.FrameID(2), true)
	replacer.SetEvictable(common.FrameID(3), true)
	replacer.SetEvictable(common.FrameID(4), true)
	replacer.SetEvictable(common.FrameID(5), true)

	// Size should be 5
	if replacer.Size() != 5 {
		t.Errorf("Expected size 5, got %d", replacer.Size())
	}

	// Record more accesses to give some frames k accesses
	replacer.RecordAccess(common.FrameID(1), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(1), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(2), common.AccessTypeUnknown)

	// Now frame 1 has 3 accesses, frame 2 has 2, frames 3,4,5 have 1
	// With k=2, frames 3,4,5 have infinite backward k-distance
	// They should be evicted first (FIFO among infinite)

	// Evict should return frame 3 (first with <k accesses)
	frameID, ok := replacer.Evict()
	if !ok || frameID != 3 {
		t.Errorf("Expected to evict frame 3, got frame %d", frameID)
	}

	// Continue evicting...
	frameID, ok = replacer.Evict()
	if !ok || frameID != 4 {
		t.Errorf("Expected to evict frame 4, got frame %d", frameID)
	}

	frameID, ok = replacer.Evict()
	if !ok || frameID != 5 {
		t.Errorf("Expected to evict frame 5, got frame %d", frameID)
	}

	// Now only frames 1 and 2 remain (both have >= k accesses)
	// Frame 2 has older k-th access, should be evicted first
	frameID, ok = replacer.Evict()
	if !ok || frameID != 2 {
		t.Errorf("Expected to evict frame 2, got frame %d", frameID)
	}

	// Only frame 1 remains
	if replacer.Size() != 1 {
		t.Errorf("Expected size 1, got %d", replacer.Size())
	}
}

// TestLRUKReplacer_SetEvictable tests the evictable flag.
func TestLRUKReplacer_SetEvictable(t *testing.T) {
	t.Skip("Implement LRU-K Replacer first")

	replacer := buffer.NewLRUKReplacer(5, 2)

	// Record access and set as evictable
	replacer.RecordAccess(common.FrameID(1), common.AccessTypeUnknown)
	replacer.SetEvictable(common.FrameID(1), true)

	// Size should be 1
	if replacer.Size() != 1 {
		t.Errorf("Expected size 1, got %d", replacer.Size())
	}

	// Set as not evictable
	replacer.SetEvictable(common.FrameID(1), false)

	// Size should be 0 (no evictable frames)
	if replacer.Size() != 0 {
		t.Errorf("Expected size 0, got %d", replacer.Size())
	}

	// Evict should fail
	_, ok := replacer.Evict()
	if ok {
		t.Error("Evict should fail when no evictable frames")
	}
}

// TestLRUKReplacer_Remove tests removing frames.
func TestLRUKReplacer_Remove(t *testing.T) {
	t.Skip("Implement LRU-K Replacer first")

	replacer := buffer.NewLRUKReplacer(5, 2)

	// Add frames
	replacer.RecordAccess(common.FrameID(1), common.AccessTypeUnknown)
	replacer.RecordAccess(common.FrameID(2), common.AccessTypeUnknown)
	replacer.SetEvictable(common.FrameID(1), true)
	replacer.SetEvictable(common.FrameID(2), true)

	// Remove frame 1
	replacer.Remove(common.FrameID(1))

	// Size should be 1
	if replacer.Size() != 1 {
		t.Errorf("Expected size 1, got %d", replacer.Size())
	}

	// Evict should return frame 2
	frameID, ok := replacer.Evict()
	if !ok || frameID != 2 {
		t.Errorf("Expected to evict frame 2, got frame %d", frameID)
	}
}

// =============================================================================
// BUFFER POOL MANAGER TESTS
// =============================================================================

// TestBufferPoolManager_Basic tests basic buffer pool operations.
func TestBufferPoolManager_Basic(t *testing.T) {
	t.Skip("Implement Buffer Pool Manager first")

	// TODO: Create a temporary database file
	// dm, _ := disk.NewDiskManager("test.db")
	// defer os.Remove("test.db")
	// bpm := buffer.NewBufferPoolManager(10, dm)

	// Test NewPage
	// page, _ := bpm.NewPage()
	// if page.GetPageID() != 0 {
	//     t.Error("First page should have ID 0")
	// }

	// Test writing data
	// copy(page.GetData(), "Hello BusTub-Go")

	// Test unpinning
	// bpm.UnpinPage(page.GetPageID(), true)

	// Test fetching
	// page2 := bpm.FetchPage(0)
	// if string(page2.GetData()[:15]) != "Hello BusTub-Go" {
	//     t.Error("Data mismatch after fetch")
	// }
}

// TestBufferPoolManager_Eviction tests page eviction.
func TestBufferPoolManager_Eviction(t *testing.T) {
	t.Skip("Implement Buffer Pool Manager first")

	// Create a small buffer pool (3 frames)
	// Fill it up and verify eviction works correctly

	// TODO: Implement when BPM is complete
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

// TestBufferPoolManager_Concurrent tests concurrent access.
func TestBufferPoolManager_Concurrent(t *testing.T) {
	t.Skip("Implement Buffer Pool Manager first")

	// Launch multiple goroutines that:
	// - Create new pages
	// - Fetch existing pages
	// - Write data
	// - Unpin pages

	// Verify no data corruption or deadlocks

	// TODO: Implement when BPM is complete
}
