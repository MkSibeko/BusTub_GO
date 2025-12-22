// Package hash provides hash table implementations for BusTub-Go.
//
// The Extendible Hash Table is a disk-based hash table that:
// - Grows dynamically as data is inserted
// - Supports concurrent access
// - Uses the buffer pool for page management
//
// ASSIGNMENT 2 - EXTENDIBLE HASH TABLE:
// ======================================
// Implement a disk-based extendible hash table.
//
// Structure:
// - Header Page: Top-level, maps hash prefix to directory pages
// - Directory Pages: Map hash values to bucket pages
// - Bucket Pages: Store actual key-value pairs
//
// This mirrors: bustub/src/include/container/disk/hash/disk_extendible_hash_table.h
package hash

import (
	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/common"
)

// HashFunction computes a hash value for a key.
type HashFunction[K any] func(key K) uint32

// KeyComparator compares two keys for equality.
type KeyComparator[K any] func(a, b K) bool

// DiskExtendibleHashTable is a disk-based extendible hash table.
//
// TODO (ASSIGNMENT 2 - EXTENDIBLE HASH TABLE):
// =============================================
// Implement the following methods:
//
// Core Operations:
// 1. Insert(key, value) - Insert a key-value pair
// 2. Remove(key) - Remove a key
// 3. GetValue(key) - Find value(s) for a key
//
// For Insert, handle bucket overflow:
//  1. Try to insert into the bucket
//  2. If bucket is full:
//     a. If local depth == global depth, double directory
//     b. Split the bucket
//     c. Redistribute entries
//     d. Retry insert
//
// For Remove, handle bucket underflow (optional):
// - Merge buckets if both are less than half full
// - Shrink directory if all local depths allow
type DiskExtendibleHashTable[K comparable, V any] struct {
	// indexName is the name of this hash table.
	indexName string

	// bpm is the buffer pool manager.
	bpm *buffer.BufferPoolManager

	// cmp compares keys for equality.
	cmp KeyComparator[K]

	// hashFn computes hash values for keys.
	hashFn HashFunction[K]

	// headerMaxDepth is the maximum depth for the header.
	headerMaxDepth uint32

	// directoryMaxDepth is the maximum depth for directories.
	directoryMaxDepth uint32

	// bucketMaxSize is the maximum entries per bucket.
	bucketMaxSize uint32

	// headerPageID is the page ID of the header page.
	headerPageID common.PageID
}

// NewDiskExtendibleHashTable creates a new extendible hash table.
//
// Parameters:
// - name: Name of the hash table
// - bpm: Buffer pool manager
// - cmp: Key comparison function
// - hashFn: Hash function
// - headerMaxDepth: Maximum depth for header (default 9)
// - directoryMaxDepth: Maximum depth for directories (default 9)
// - bucketMaxSize: Maximum entries per bucket
func NewDiskExtendibleHashTable[K comparable, V any](
	name string,
	bpm *buffer.BufferPoolManager,
	cmp KeyComparator[K],
	hashFn HashFunction[K],
	headerMaxDepth uint32,
	directoryMaxDepth uint32,
	bucketMaxSize uint32,
) *DiskExtendibleHashTable[K, V] {
	ht := &DiskExtendibleHashTable[K, V]{
		indexName:         name,
		bpm:               bpm,
		cmp:               cmp,
		hashFn:            hashFn,
		headerMaxDepth:    headerMaxDepth,
		directoryMaxDepth: directoryMaxDepth,
		bucketMaxSize:     bucketMaxSize,
	}

	// TODO: Create and initialize header page
	// ht.headerPageID = bpm.NewPage()
	// headerGuard := bpm.WritePage(ht.headerPageID, AccessTypeIndex)
	// headerPage := NewExtendibleHTableHeaderPage(headerGuard.GetDataMut())
	// headerPage.Init(headerMaxDepth)

	return ht
}

// GetHeaderPageID returns the header page ID.
func (ht *DiskExtendibleHashTable[K, V]) GetHeaderPageID() common.PageID {
	return ht.headerPageID
}

// Hash computes the hash of a key.
func (ht *DiskExtendibleHashTable[K, V]) Hash(key K) uint32 {
	return ht.hashFn(key)
}

// Insert inserts a key-value pair into the hash table.
//
// TODO (ASSIGNMENT 2):
// =====================
// Insert a key-value pair, handling splits as needed.
//
// Algorithm:
//  1. Compute hash of the key
//  2. Use hash to find the directory page
//  3. Use hash to find the bucket page
//  4. Try to insert into the bucket
//  5. If bucket is full, split it:
//     a. If local_depth == global_depth, double the directory
//     b. Create a new bucket page
//     c. Increment local depths
//     d. Redistribute entries between old and new buckets
//     e. Update directory pointers
//     f. Retry insert
//
// Returns true if inserted, false if duplicate key.
func (ht *DiskExtendibleHashTable[K, V]) Insert(key K, value V, txn interface{}) bool {
	// TODO (ASSIGNMENT): Implement Insert
	//
	// hash := ht.Hash(key)
	//
	// // Get header page
	// headerGuard := ht.bpm.ReadPage(ht.headerPageID, AccessTypeIndex)
	// headerPage := NewExtendibleHTableHeaderPage(headerGuard.GetData())
	//
	// // Find directory page
	// directoryIdx := headerPage.HashToDirectoryIndex(hash)
	// directoryPageID := headerPage.GetDirectoryPageID(directoryIdx)
	//
	// if directoryPageID == InvalidPageID {
	//     // Need to create a new directory
	//     return ht.InsertToNewDirectory(headerPage, directoryIdx, hash, key, value)
	// }
	//
	// // Get directory page
	// dirGuard := ht.bpm.WritePage(directoryPageID, AccessTypeIndex)
	// dirPage := NewExtendibleHTableDirectoryPage(dirGuard.GetDataMut())
	//
	// // Find bucket page
	// bucketIdx := dirPage.HashToBucketIndex(hash)
	// bucketPageID := dirPage.GetBucketPageID(bucketIdx)
	//
	// if bucketPageID == InvalidPageID {
	//     // Need to create a new bucket
	//     return ht.InsertToNewBucket(dirPage, bucketIdx, key, value)
	// }
	//
	// // Get bucket page and try to insert
	// bucketGuard := ht.bpm.WritePage(bucketPageID, AccessTypeIndex)
	// bucketPage := NewExtendibleHTableBucketPage[K, V](bucketGuard.GetDataMut())
	//
	// if !bucketPage.IsFull() {
	//     return bucketPage.Insert(key, value, ht.cmp)
	// }
	//
	// // Bucket is full, need to split
	// // ... implement split logic ...

	panic("TODO: Implement DiskExtendibleHashTable.Insert()")
}

// Remove removes a key from the hash table.
//
// TODO (ASSIGNMENT 2):
// =====================
// Remove a key-value pair.
//
// Algorithm:
// 1. Compute hash and find bucket
// 2. Remove the key from the bucket
// 3. Optionally: merge buckets if too empty
// 4. Optionally: shrink directory if possible
//
// Returns true if removed, false if key not found.
func (ht *DiskExtendibleHashTable[K, V]) Remove(key K, txn interface{}) bool {
	// TODO (ASSIGNMENT): Implement Remove
	panic("TODO: Implement DiskExtendibleHashTable.Remove()")
}

// GetValue finds all values associated with a key.
//
// TODO (ASSIGNMENT 2):
// =====================
// Look up a key and return its value(s).
//
// Algorithm:
// 1. Compute hash of the key
// 2. Find the directory page
// 3. Find the bucket page
// 4. Search the bucket for the key
// 5. If found, add to result
//
// Returns true if key was found, false otherwise.
func (ht *DiskExtendibleHashTable[K, V]) GetValue(key K, result *[]V, txn interface{}) bool {
	// TODO (ASSIGNMENT): Implement GetValue
	//
	// hash := ht.Hash(key)
	//
	// // Navigate through header -> directory -> bucket
	// ...
	//
	// // Search bucket for key
	// return bucketPage.Lookup(key, ht.cmp, result)

	panic("TODO: Implement DiskExtendibleHashTable.GetValue()")
}

// VerifyIntegrity checks the integrity of the hash table.
//
// This is useful for debugging and testing.
func (ht *DiskExtendibleHashTable[K, V]) VerifyIntegrity() {
	// TODO: Implement integrity verification
	// - Check that all bucket page IDs are valid
	// - Check that local depths are <= global depth
	// - Check that sibling buckets have correct local depths
}

// PrintHT prints the hash table structure for debugging.
func (ht *DiskExtendibleHashTable[K, V]) PrintHT() {
	// TODO: Implement debug printing
}

// =============================================================================
// HELPER METHODS
// =============================================================================

// insertToNewDirectory creates a new directory and bucket for insertion.
//
// Called when the header points to an invalid directory.
func (ht *DiskExtendibleHashTable[K, V]) insertToNewDirectory(
	directoryIdx uint32,
	hash uint32,
	key K,
	value V,
) bool {
	// TODO: Implement
	panic("TODO: Implement insertToNewDirectory()")
}

// insertToNewBucket creates a new bucket for insertion.
//
// Called when the directory points to an invalid bucket.
func (ht *DiskExtendibleHashTable[K, V]) insertToNewBucket(
	bucketIdx uint32,
	key K,
	value V,
) bool {
	// TODO: Implement
	panic("TODO: Implement insertToNewBucket()")
}

// splitBucket splits a full bucket.
//
// Called when a bucket overflows during insertion.
func (ht *DiskExtendibleHashTable[K, V]) splitBucket(
	bucketIdx uint32,
	localDepth uint32,
) {
	// TODO: Implement bucket splitting
	// 1. Create new bucket page
	// 2. Increment local depths
	// 3. Update directory pointers
	// 4. Redistribute entries
	panic("TODO: Implement splitBucket()")
}

// migrateEntries moves entries between buckets during a split.
func (ht *DiskExtendibleHashTable[K, V]) migrateEntries(
	oldBucket interface{},
	newBucket interface{},
	newBucketIdx uint32,
	localDepthMask uint32,
) {
	// TODO: Implement entry migration
	panic("TODO: Implement migrateEntries()")
}
