// Package index_test provides tests for index structures.
//
// These tests verify the correct implementation of:
// - B+ Tree Index
// - Extendible Hash Table
//
// TESTING STRATEGY:
// =================
// 1. Test basic operations (insert, lookup, delete)
// 2. Test edge cases (empty tree, single element)
// 3. Test bulk operations
// 4. Test concurrent access
// 5. Test iterator functionality
//
// RUNNING TESTS:
// ==============
//
//	go test ./test/index/... -v
package index_test

import (
	"testing"
)

// =============================================================================
// B+ TREE TESTS
// =============================================================================

// TestBPlusTree_Insert tests basic insertion.
func TestBPlusTree_Insert(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// TODO: Create buffer pool manager and B+ tree
	// tree := index.NewBPlusTree[int64, common.RID](bpm, comparator, 4, 4)

	// Insert some keys
	// tree.Insert(1, common.NewRID(0, 0))
	// tree.Insert(2, common.NewRID(0, 1))
	// tree.Insert(3, common.NewRID(0, 2))

	// Verify lookups
	// result, found := tree.GetValue(2)
	// if !found || result.GetSlotNum() != 1 {
	//     t.Error("Key 2 not found or wrong value")
	// }
}

// TestBPlusTree_Delete tests deletion.
func TestBPlusTree_Delete(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// Insert keys, delete some, verify state
}

// TestBPlusTree_Iterator tests the iterator.
func TestBPlusTree_Iterator(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// Insert keys in random order
	// Iterate and verify sorted order
}

// TestBPlusTree_Split tests node splitting.
func TestBPlusTree_Split(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// Insert enough keys to cause splits
	// Verify tree structure is correct
}

// TestBPlusTree_Merge tests node merging.
func TestBPlusTree_Merge(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// Insert keys, delete to cause underflow
	// Verify merge/redistribution
}

// TestBPlusTree_LargeScale tests with many keys.
func TestBPlusTree_LargeScale(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// Insert 10000 keys
	// Delete random 5000
	// Verify remaining 5000
}

// TestBPlusTree_Concurrent tests concurrent operations.
func TestBPlusTree_Concurrent(t *testing.T) {
	t.Skip("Implement B+ Tree first")

	// Launch goroutines that insert/delete/lookup
	// Verify correctness
}

// =============================================================================
// EXTENDIBLE HASH TABLE TESTS
// =============================================================================

// TestHashTable_Insert tests basic insertion.
func TestHashTable_Insert(t *testing.T) {
	t.Skip("Implement Hash Table first")

	// Create hash table
	// Insert key-value pairs
	// Verify lookups
}

// TestHashTable_Delete tests deletion.
func TestHashTable_Delete(t *testing.T) {
	t.Skip("Implement Hash Table first")
}

// TestHashTable_BucketSplit tests bucket splitting.
func TestHashTable_BucketSplit(t *testing.T) {
	t.Skip("Implement Hash Table first")

	// Insert keys that hash to same bucket
	// Verify bucket splits correctly
}

// TestHashTable_DirectoryGrow tests directory growth.
func TestHashTable_DirectoryGrow(t *testing.T) {
	t.Skip("Implement Hash Table first")

	// Insert enough keys to cause directory growth
}

// TestHashTable_LargeScale tests with many keys.
func TestHashTable_LargeScale(t *testing.T) {
	t.Skip("Implement Hash Table first")
}
