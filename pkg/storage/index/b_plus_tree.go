// Package index - B+ Tree implementation.
//
// The B+ Tree is an ordered index structure that supports:
// - Point queries (exact key lookup)
// - Range queries (scan between keys)
// - Efficient insert and delete
//
// ASSIGNMENT 2 - B+ TREE:
// ========================
// This is a major assignment. You need to implement:
//
// 1. Search: Traverse from root to leaf to find a key
// 2. Insert: Add a key-value pair, handling splits
// 3. Delete: Remove a key-value pair, handling merges
// 4. Iterator: Support for range scans
//
// Key Concepts:
// - Root is stored in a header page
// - Internal pages have keys and child pointers
// - Leaf pages have keys and values (RIDs)
// - Leaves are linked for range scans
// - Pages split when full, merge when underflow
//
// This mirrors: bustub/src/include/storage/index/b_plus_tree.h
package index

import (
	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/common"
)

// KeyComparator compares two keys.
// Returns:
// - negative if a < b
// - zero if a == b
// - positive if a > b
type KeyComparator func(a, b []byte) int

// BPlusTree is an ordered index using a B+ tree structure.
//
// TODO (ASSIGNMENT 2 - B+ TREE):
// ==============================
// Implement the following methods:
//
// Core Operations:
// 1. Insert(key, value) - Insert a key-value pair
// 2. Remove(key) - Remove a key
// 3. GetValue(key) - Find value(s) for a key
//
// Helper Methods:
// 4. IsEmpty() - Check if tree is empty
// 5. GetRootPageId() - Get the root page ID
//
// Iterator Methods:
// 6. Begin() - Iterator at first key
// 7. End() - Iterator past last key
// 8. Begin(key) - Iterator at specific key
//
// Implementation Strategy:
//
// For Insert:
//  1. If tree is empty, create a new leaf as root
//  2. Find the leaf page where the key should go
//  3. Insert into the leaf
//  4. If leaf overflows, split it:
//     a. Create a new leaf page
//     b. Move half the entries to new page
//     c. Insert separator key into parent
//     d. Recursively handle parent overflow
//
// For Remove:
//  1. Find the leaf containing the key
//  2. Remove the key from the leaf
//  3. If leaf underflows, handle it:
//     a. Try to borrow from sibling
//     b. If can't borrow, merge with sibling
//     c. Recursively handle parent underflow
//
// For Search:
// 1. Start at root
// 2. For internal pages: find child to follow
// 3. For leaf page: search for key
type BPlusTree struct {
	// indexName is the name of this index.
	indexName string

	// headerPageID is the page ID of the header page.
	// The header page stores the root page ID.
	headerPageID common.PageID

	// bpm is the buffer pool manager for page access.
	bpm *buffer.BufferPoolManager

	// comparator compares keys.
	comparator KeyComparator

	// leafMaxSize is the maximum entries in a leaf page.
	leafMaxSize int

	// internalMaxSize is the maximum entries in an internal page.
	internalMaxSize int
}

// NewBPlusTree creates a new B+ tree.
//
// Parameters:
// - name: Name of the index
// - headerPageID: Page ID for the header page (must be pre-allocated)
// - bpm: Buffer pool manager
// - comparator: Key comparison function
// - leafMaxSize: Maximum entries per leaf (optional, calculated if 0)
// - internalMaxSize: Maximum entries per internal page (optional)
func NewBPlusTree(
	name string,
	headerPageID common.PageID,
	bpm *buffer.BufferPoolManager,
	comparator KeyComparator,
	leafMaxSize int,
	internalMaxSize int,
) *BPlusTree {
	return &BPlusTree{
		indexName:       name,
		headerPageID:    headerPageID,
		bpm:             bpm,
		comparator:      comparator,
		leafMaxSize:     leafMaxSize,
		internalMaxSize: internalMaxSize,
	}
}

// IsEmpty returns true if the tree contains no keys.
//
// TODO (ASSIGNMENT): Implement this method.
// Check if the root page ID in the header is invalid.
func (t *BPlusTree) IsEmpty() bool {
	// TODO: Read header page and check root page ID
	panic("TODO: Implement BPlusTree.IsEmpty()")
}

// GetRootPageID returns the root page ID.
//
// TODO (ASSIGNMENT): Implement this method.
// Read the header page to get the root page ID.
func (t *BPlusTree) GetRootPageID() common.PageID {
	// TODO: Read header page and return root page ID
	panic("TODO: Implement BPlusTree.GetRootPageID()")
}

// Insert inserts a key-value pair into the tree.
//
// TODO (ASSIGNMENT 2):
// =====================
// Insert a key-value pair, handling splits as needed.
//
// Algorithm:
//  1. If tree is empty:
//     a. Create a new leaf page
//     b. Insert the key-value pair
//     c. Update header to point to new root
//     d. Return true
//
//  2. Find the leaf page for this key:
//     a. Start at root
//     b. For internal pages, find child to follow
//     c. Continue until reaching a leaf
//
//  3. Insert into the leaf:
//     a. Find the correct position
//     b. If key exists, return false (no duplicates)
//     c. Insert the key-value pair
//
//  4. If leaf overflows (size > max_size):
//     a. Split the leaf
//     b. Insert separator into parent
//     c. Handle parent overflow recursively
//
// Returns true if inserted, false if duplicate key.
func (t *BPlusTree) Insert(key []byte, value common.RID) bool {
	// TODO (ASSIGNMENT): Implement Insert
	//
	// Pseudocode:
	//
	// // Step 1: Handle empty tree
	// headerGuard := t.bpm.WritePage(t.headerPageID, AccessTypeIndex)
	// defer headerGuard.Drop()
	// headerPage := NewBPlusTreeHeaderPage(headerGuard.GetDataMut())
	//
	// if headerPage.GetRootPageID() == InvalidPageID {
	//     // Create new root leaf
	//     rootPageID := t.bpm.NewPage()
	//     rootGuard := t.bpm.WritePage(rootPageID, AccessTypeIndex)
	//     leafPage := NewBPlusTreeLeafPage(rootGuard.GetDataMut(), keySize, valueSize)
	//     leafPage.Init(t.leafMaxSize)
	//     leafPage.Insert(key, value)
	//     headerPage.SetRootPageID(rootPageID)
	//     return true
	// }
	//
	// // Step 2: Find leaf page
	// ctx := NewContext()
	// ctx.rootPageID = headerPage.GetRootPageID()
	// // ... traverse to leaf ...
	//
	// // Step 3: Insert into leaf
	// // ... insert and handle splits ...

	panic("TODO: Implement BPlusTree.Insert()")
}

// Remove removes a key from the tree.
//
// TODO (ASSIGNMENT 2):
// =====================
// Remove a key, handling merges/redistribution as needed.
//
// Algorithm:
//  1. Find the leaf containing the key
//  2. Remove the key from the leaf
//  3. If leaf underflows (size < min_size):
//     a. Try to borrow from sibling
//     b. If can't borrow, merge with sibling
//     c. Update parent (remove separator)
//     d. Handle parent underflow recursively
//
// Special cases:
// - Removing from root: If root becomes empty, update header
// - Merging with root: If internal root has one child, make child new root
func (t *BPlusTree) Remove(key []byte) {
	// TODO (ASSIGNMENT): Implement Remove
	panic("TODO: Implement BPlusTree.Remove()")
}

// GetValue finds all values associated with a key.
//
// TODO (ASSIGNMENT 2):
// =====================
// Search for a key and return its value(s).
//
// Algorithm:
//  1. If tree is empty, return false
//  2. Traverse from root to leaf:
//     a. For internal pages: find child whose key range contains our key
//     b. Continue until reaching leaf
//  3. Search leaf for the key
//  4. If found, add value(s) to result and return true
//  5. If not found, return false
//
// Parameters:
// - key: The key to search for
// - result: Slice to append found values to
//
// Returns true if key was found, false otherwise.
func (t *BPlusTree) GetValue(key []byte, result *[]common.RID) bool {
	// TODO (ASSIGNMENT): Implement GetValue
	panic("TODO: Implement BPlusTree.GetValue()")
}

// Begin returns an iterator pointing to the first key.
//
// TODO (ASSIGNMENT): Implement iterator support.
func (t *BPlusTree) Begin() *IndexIterator {
	// TODO: Find leftmost leaf and return iterator at first entry
	panic("TODO: Implement BPlusTree.Begin()")
}

// End returns an iterator pointing past the last key.
//
// TODO (ASSIGNMENT): Implement iterator support.
func (t *BPlusTree) End() *IndexIterator {
	// TODO: Return an "end" iterator
	panic("TODO: Implement BPlusTree.End()")
}

// BeginAt returns an iterator starting at the given key.
//
// TODO (ASSIGNMENT): Implement iterator support.
func (t *BPlusTree) BeginAt(key []byte) *IndexIterator {
	// TODO: Find the key (or first key >= it) and return iterator
	panic("TODO: Implement BPlusTree.BeginAt()")
}

// =============================================================================
// B+ TREE CONTEXT
// =============================================================================

// Context tracks state during B+ tree operations.
//
// This is useful for holding page guards during tree traversal.
// It helps implement safe locking protocols.
type Context struct {
	// headerPage holds the header page guard during operations.
	headerPage *buffer.WritePageGuard

	// rootPageID is cached for convenience.
	rootPageID common.PageID

	// writeSet holds write-locked pages during modifications.
	writeSet []*buffer.WritePageGuard

	// readSet holds read-locked pages during searches.
	readSet []*buffer.ReadPageGuard
}

// NewContext creates a new empty context.
func NewContext() *Context {
	return &Context{
		writeSet: make([]*buffer.WritePageGuard, 0),
		readSet:  make([]*buffer.ReadPageGuard, 0),
	}
}

// IsRootPage checks if a page ID is the root.
func (c *Context) IsRootPage(pageID common.PageID) bool {
	return pageID == c.rootPageID
}
