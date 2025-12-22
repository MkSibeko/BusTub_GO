// Package common provides shared types, constants, and utilities for BusTub-Go.
//
// This package mirrors the C++ bustub/src/include/common/ directory and contains:
// - Configuration constants (page size, buffer pool size, etc.)
// - Type aliases for identifiers (PageID, FrameID, TxnID, etc.)
// - Common error types
// - Utility functions
//
// ASSIGNMENT NOTES:
// This file provides the foundation types used throughout the database system.
// Students should understand these types before implementing any components.
package common

// =============================================================================
// CONFIGURATION CONSTANTS
// =============================================================================

// PageSize is the size of a database page in bytes.
// This is the fundamental unit of data storage and transfer between disk and memory.
// All pages in BusTub are fixed-size for simplicity.
// Value: 8192 bytes (8 KB) - common in real database systems
const PageSize = 8192

// BufferPoolSize is the default number of frames in the buffer pool.
// Each frame can hold one page in memory.
const BufferPoolSize = 128

// DefaultDBIOSize is the starting size of the database file on disk in pages.
const DefaultDBIOSize = 16

// LogBufferSize is the size of the log buffer in bytes.
const LogBufferSize = (BufferPoolSize + 1) * PageSize

// BucketSize is the default size of hash table buckets.
const BucketSize = 50

// LRUKReplacerK is the default K value for the LRU-K replacement algorithm.
// K=10 means we track the last 10 accesses for each frame.
const LRUKReplacerK = 10

// BatchSize is the number of tuples processed in a batch for vectorized execution.
const BatchSize = 20

// VarcharDefaultLength is the default length for VARCHAR columns.
const VarcharDefaultLength = 128

// =============================================================================
// TYPE ALIASES - Identifier Types
// =============================================================================
// These type aliases improve code readability and type safety.
// Each represents a different kind of identifier in the system.

// PageID uniquely identifies a page on disk.
// Valid page IDs are non-negative. InvalidPageID indicates no page.
type PageID = int32

// FrameID identifies a frame (slot) in the buffer pool.
// Valid frame IDs are non-negative. InvalidFrameID indicates no frame.
type FrameID = int32

// TxnID uniquely identifies a transaction.
type TxnID = int64

// LSN (Log Sequence Number) identifies a position in the write-ahead log.
type LSN = int32

// SlotOffset is the position of a tuple within a page.
type SlotOffset = uint16

// TableOID is the object identifier for a table in the catalog.
type TableOID = uint32

// IndexOID is the object identifier for an index in the catalog.
type IndexOID = uint32

// ColumnOID is the object identifier for a column.
type ColumnOID = uint32

// Timestamp represents a logical timestamp for MVCC.
type Timestamp = uint64

// TxnTimestamp represents a transaction timestamp (for MVCC).
type TxnTimestamp = uint64

// TypeID represents the type of a column/value.
type TypeID = int

// Type ID constants
const (
	TypeInvalid   TypeID = 0
	TypeBoolean   TypeID = 1
	TypeTinyInt   TypeID = 2
	TypeSmallInt  TypeID = 3
	TypeInteger   TypeID = 4
	TypeBigInt    TypeID = 5
	TypeDecimal   TypeID = 6
	TypeVarchar   TypeID = 7
	TypeTimestamp TypeID = 8
)

// =============================================================================
// INVALID/SENTINEL VALUES
// =============================================================================
// These constants represent invalid or uninitialized identifiers.

const (
	// InvalidFrameID represents an invalid frame identifier.
	InvalidFrameID FrameID = -1

	// InvalidPageID represents an invalid page identifier.
	InvalidPageID PageID = -1

	// InvalidTxnID represents an invalid transaction identifier.
	InvalidTxnID TxnID = -1

	// InvalidLSN represents an invalid log sequence number.
	InvalidLSN LSN = -1

	// TxnStartID is the starting transaction ID (high value to avoid collisions).
	TxnStartID TxnID = 1 << 62
)

// =============================================================================
// ACCESS TYPE ENUMERATION
// =============================================================================
// AccessType is used by the replacer to track how pages are accessed.
// Different access patterns may affect replacement decisions.

type AccessType int

const (
	// AccessTypeUnknown indicates unknown or default access type.
	AccessTypeUnknown AccessType = iota

	// AccessTypeLookup indicates a point lookup access.
	AccessTypeLookup

	// AccessTypeScan indicates a sequential scan access.
	AccessTypeScan

	// AccessTypeIndex indicates an index access.
	AccessTypeIndex
)

// String returns a human-readable string for the access type.
func (a AccessType) String() string {
	switch a {
	case AccessTypeLookup:
		return "Lookup"
	case AccessTypeScan:
		return "Scan"
	case AccessTypeIndex:
		return "Index"
	default:
		return "Unknown"
	}
}
