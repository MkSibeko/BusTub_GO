// Package index provides index data structures for BusTub-Go.
//
// Indexes provide fast lookup of tuples by key. BusTub supports:
// - B+ Tree Index: Ordered index for range queries
// - Hash Table Index: Unordered index for point queries
//
// ASSIGNMENT 2 - INDEXES:
// ========================
// You need to implement B+ Tree and/or Extendible Hash Table.
//
// This file defines the common Index interface that all indexes implement.
//
// This mirrors: bustub/src/include/storage/index/index.h
package index

import (
	"github.com/bustub-go/pkg/common"
)

// Index is the interface for all index types.
//
// An index maps keys to RIDs (Record Identifiers).
// The RID can then be used to fetch the actual tuple from the table heap.
type Index interface {
	// GetIndexName returns the name of the index.
	GetIndexName() string

	// GetKeySchema returns the schema of the index key.
	// GetKeySchema() *Schema

	// InsertEntry inserts a key-RID pair into the index.
	// Returns true if successful, false if duplicate key (for unique indexes).
	InsertEntry(key []byte, rid common.RID, txn interface{}) bool

	// DeleteEntry removes a key-RID pair from the index.
	DeleteEntry(key []byte, rid common.RID, txn interface{})

	// ScanKey finds all RIDs with the given key.
	// For unique indexes, there should be at most one.
	ScanKey(key []byte, result *[]common.RID, txn interface{})
}

// IndexType represents the type of index.
type IndexType int

const (
	// IndexTypeBPlusTree is a B+ tree index.
	IndexTypeBPlusTree IndexType = iota

	// IndexTypeHashTable is a hash table index.
	IndexTypeHashTable

	// IndexTypeSTLOrdered is an ordered STL container (for testing).
	IndexTypeSTLOrdered

	// IndexTypeSTLUnordered is an unordered STL container (for testing).
	IndexTypeSTLUnordered
)

// String returns the name of the index type.
func (t IndexType) String() string {
	switch t {
	case IndexTypeBPlusTree:
		return "BPlusTree"
	case IndexTypeHashTable:
		return "HashTable"
	case IndexTypeSTLOrdered:
		return "STLOrdered"
	case IndexTypeSTLUnordered:
		return "STLUnordered"
	default:
		return "Unknown"
	}
}

// IndexMetadata contains metadata about an index.
type IndexMetadata struct {
	// Name is the name of the index.
	Name string

	// TableName is the name of the table this index is on.
	TableName string

	// KeyAttrs are the column indices that form the key.
	KeyAttrs []int

	// IsPrimaryKey indicates if this is a primary key index.
	IsPrimaryKey bool

	// IndexType is the type of index.
	Type IndexType
}
