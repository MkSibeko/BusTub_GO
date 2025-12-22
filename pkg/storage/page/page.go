// Package page provides page layout implementations for BusTub-Go.
//
// Pages are the fundamental unit of storage. Different page types have
// different layouts optimized for their use case:
// - TablePage: Stores table tuples
// - B+Tree pages: Store index nodes
// - Hash table pages: Store hash table data
//
// ASSIGNMENT NOTES:
// ==================
// Page layouts are provided but understanding them is crucial.
// Each page type has a specific binary layout that must be followed
// for compatibility between components.
//
// This mirrors: bustub/src/include/storage/page/page.h
package page

import (
	"github.com/bustub-go/pkg/common"
)

// PageType identifies the type of a page.
type PageType uint8

const (
	// PageTypeInvalid is an uninitialized page.
	PageTypeInvalid PageType = iota

	// PageTypeTable is a table heap page storing tuples.
	PageTypeTable

	// PageTypeBPlusTreeLeaf is a B+ tree leaf page.
	PageTypeBPlusTreeLeaf

	// PageTypeBPlusTreeInternal is a B+ tree internal page.
	PageTypeBPlusTreeInternal

	// PageTypeBPlusTreeHeader is a B+ tree header page.
	PageTypeBPlusTreeHeader

	// PageTypeHashTableHeader is a hash table header page.
	PageTypeHashTableHeader

	// PageTypeHashTableDirectory is a hash table directory page.
	PageTypeHashTableDirectory

	// PageTypeHashTableBucket is a hash table bucket page.
	PageTypeHashTableBucket
)

// String returns the name of the page type.
func (pt PageType) String() string {
	switch pt {
	case PageTypeTable:
		return "TablePage"
	case PageTypeBPlusTreeLeaf:
		return "BPlusTreeLeaf"
	case PageTypeBPlusTreeInternal:
		return "BPlusTreeInternal"
	case PageTypeBPlusTreeHeader:
		return "BPlusTreeHeader"
	case PageTypeHashTableHeader:
		return "HashTableHeader"
	case PageTypeHashTableDirectory:
		return "HashTableDirectory"
	case PageTypeHashTableBucket:
		return "HashTableBucket"
	default:
		return "Invalid"
	}
}

// =============================================================================
// COMMON PAGE HEADER
// =============================================================================
// All pages share a common header at the beginning.
// This provides a consistent way to identify page types and versions.

// PageHeaderSize is the size of the common page header in bytes.
const PageHeaderSize = 8

// Page header offsets
const (
	// OffsetPageType is where the page type is stored (1 byte).
	OffsetPageType = 0

	// OffsetReserved is reserved for future use (3 bytes).
	OffsetReserved = 1

	// OffsetLSN is where the Log Sequence Number is stored (4 bytes).
	OffsetLSN = 4
)

// =============================================================================
// PAGE UTILITY FUNCTIONS
// =============================================================================

// GetPageType reads the page type from raw page data.
func GetPageType(data []byte) PageType {
	if len(data) < 1 {
		return PageTypeInvalid
	}
	return PageType(data[OffsetPageType])
}

// SetPageType writes the page type to raw page data.
func SetPageType(data []byte, pageType PageType) {
	if len(data) < 1 {
		return
	}
	data[OffsetPageType] = byte(pageType)
}

// GetLSN reads the LSN from raw page data.
func GetLSN(data []byte) common.LSN {
	if len(data) < PageHeaderSize {
		return common.InvalidLSN
	}
	return common.LSN(
		int32(data[OffsetLSN]) |
			int32(data[OffsetLSN+1])<<8 |
			int32(data[OffsetLSN+2])<<16 |
			int32(data[OffsetLSN+3])<<24,
	)
}

// SetLSN writes the LSN to raw page data.
func SetLSN(data []byte, lsn common.LSN) {
	if len(data) < PageHeaderSize {
		return
	}
	data[OffsetLSN] = byte(lsn)
	data[OffsetLSN+1] = byte(lsn >> 8)
	data[OffsetLSN+2] = byte(lsn >> 16)
	data[OffsetLSN+3] = byte(lsn >> 24)
}

// =============================================================================
// BINARY ENCODING UTILITIES
// =============================================================================
// These functions help read/write integers from/to byte slices.
// All integers are stored in little-endian format.

// ReadUint16 reads a little-endian uint16 from the given offset.
func ReadUint16(data []byte, offset int) uint16 {
	return uint16(data[offset]) | uint16(data[offset+1])<<8
}

// WriteUint16 writes a little-endian uint16 to the given offset.
func WriteUint16(data []byte, offset int, value uint16) {
	data[offset] = byte(value)
	data[offset+1] = byte(value >> 8)
}

// ReadUint32 reads a little-endian uint32 from the given offset.
func ReadUint32(data []byte, offset int) uint32 {
	return uint32(data[offset]) |
		uint32(data[offset+1])<<8 |
		uint32(data[offset+2])<<16 |
		uint32(data[offset+3])<<24
}

// WriteUint32 writes a little-endian uint32 to the given offset.
func WriteUint32(data []byte, offset int, value uint32) {
	data[offset] = byte(value)
	data[offset+1] = byte(value >> 8)
	data[offset+2] = byte(value >> 16)
	data[offset+3] = byte(value >> 24)
}

// ReadInt32 reads a little-endian int32 from the given offset.
func ReadInt32(data []byte, offset int) int32 {
	return int32(ReadUint32(data, offset))
}

// WriteInt32 writes a little-endian int32 to the given offset.
func WriteInt32(data []byte, offset int, value int32) {
	WriteUint32(data, offset, uint32(value))
}

// ReadUint64 reads a little-endian uint64 from the given offset.
func ReadUint64(data []byte, offset int) uint64 {
	return uint64(data[offset]) |
		uint64(data[offset+1])<<8 |
		uint64(data[offset+2])<<16 |
		uint64(data[offset+3])<<24 |
		uint64(data[offset+4])<<32 |
		uint64(data[offset+5])<<40 |
		uint64(data[offset+6])<<48 |
		uint64(data[offset+7])<<56
}

// WriteUint64 writes a little-endian uint64 to the given offset.
func WriteUint64(data []byte, offset int, value uint64) {
	data[offset] = byte(value)
	data[offset+1] = byte(value >> 8)
	data[offset+2] = byte(value >> 16)
	data[offset+3] = byte(value >> 24)
	data[offset+4] = byte(value >> 32)
	data[offset+5] = byte(value >> 40)
	data[offset+6] = byte(value >> 48)
	data[offset+7] = byte(value >> 56)
}
