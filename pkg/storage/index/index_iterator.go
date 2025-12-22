// Package index - Index Iterator for B+ Tree.
//
// The IndexIterator provides a way to iterate over key-value pairs
// in a B+ tree in sorted order. This is essential for range scans.
//
// ASSIGNMENT 2 - INDEX ITERATOR:
// ===============================
// Implement the iterator to support range queries.
//
// Usage:
//
//	for it := tree.Begin(); !it.IsEnd(); it = it.Next() {
//	    key := it.Key()
//	    value := it.Value()
//	    // process key-value pair
//	}
//
// This mirrors: bustub/src/include/storage/index/index_iterator.h
package index

import (
	"github.com/bustub-go/pkg/buffer"
	"github.com/bustub-go/pkg/common"
)

// IndexIterator iterates over B+ tree entries in sorted order.
//
// The iterator maintains:
// - Current leaf page (via page guard)
// - Current position within the leaf
// - Buffer pool manager for fetching next pages
//
// TODO (ASSIGNMENT 2):
// =====================
// Implement the following methods:
// - IsEnd() - Check if at end
// - Key() - Get current key
// - Value() - Get current value
// - Next() - Move to next entry
//
// Key points:
// - Leaf pages are linked via next page pointers
// - When reaching end of a leaf, follow the next pointer
// - Iterator becomes "end" when there are no more entries
type IndexIterator struct {
	// bpm is the buffer pool manager.
	bpm *buffer.BufferPoolManager

	// pageGuard holds the current leaf page.
	pageGuard *buffer.ReadPageGuard

	// index is the current position within the leaf.
	index int

	// isEnd indicates if the iterator is at the end.
	isEnd bool
}

// NewIndexIterator creates a new iterator.
//
// Parameters:
// - bpm: Buffer pool manager
// - pageGuard: Guard for the current leaf page
// - index: Starting index within the page
func NewIndexIterator(bpm *buffer.BufferPoolManager, pageGuard *buffer.ReadPageGuard, index int) *IndexIterator {
	return &IndexIterator{
		bpm:       bpm,
		pageGuard: pageGuard,
		index:     index,
		isEnd:     false,
	}
}

// NewEndIterator creates an "end" iterator.
func NewEndIterator() *IndexIterator {
	return &IndexIterator{
		isEnd: true,
	}
}

// IsEnd returns true if the iterator is at the end.
func (it *IndexIterator) IsEnd() bool {
	return it.isEnd
}

// Key returns the key at the current position.
//
// TODO (ASSIGNMENT): Implement this method.
// Read the key from the current leaf page at the current index.
func (it *IndexIterator) Key() []byte {
	if it.isEnd {
		panic("IndexIterator: accessing end iterator")
	}

	// TODO: Get key from leaf page
	// leafPage := NewBPlusTreeLeafPage(it.pageGuard.GetData(), keySize, valueSize)
	// return leafPage.KeyAt(it.index)

	panic("TODO: Implement IndexIterator.Key()")
}

// Value returns the value (RID) at the current position.
//
// TODO (ASSIGNMENT): Implement this method.
func (it *IndexIterator) Value() common.RID {
	if it.isEnd {
		panic("IndexIterator: accessing end iterator")
	}

	// TODO: Get value from leaf page
	panic("TODO: Implement IndexIterator.Value()")
}

// Next advances the iterator to the next entry.
//
// TODO (ASSIGNMENT): Implement this method.
//
// Algorithm:
//  1. Increment the index
//  2. If index is within current page, done
//  3. If index exceeds page size:
//     a. Get next page ID from current leaf
//     b. If next page is invalid, set isEnd = true
//     c. Otherwise, drop current guard and fetch next page
//     d. Set index = 0
//
// Returns the iterator (for chaining).
func (it *IndexIterator) Next() *IndexIterator {
	if it.isEnd {
		return it
	}

	// TODO: Implement iterator advancement
	//
	// it.index++
	//
	// leafPage := NewBPlusTreeLeafPage(it.pageGuard.GetData(), keySize, valueSize)
	// if it.index >= leafPage.GetSize() {
	//     nextPageID := leafPage.GetNextPageID()
	//     if nextPageID == InvalidPageID {
	//         it.isEnd = true
	//         it.pageGuard.Drop()
	//         it.pageGuard = nil
	//     } else {
	//         it.pageGuard.Drop()
	//         it.pageGuard = it.bpm.ReadPage(nextPageID, AccessTypeIndex)
	//         it.index = 0
	//     }
	// }
	//
	// return it

	panic("TODO: Implement IndexIterator.Next()")
}

// Equals checks if two iterators are at the same position.
func (it *IndexIterator) Equals(other *IndexIterator) bool {
	if it.isEnd && other.isEnd {
		return true
	}
	if it.isEnd != other.isEnd {
		return false
	}

	// Compare page IDs and indices
	return it.pageGuard.GetPageID() == other.pageGuard.GetPageID() &&
		it.index == other.index
}
