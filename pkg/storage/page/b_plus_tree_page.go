// Package page - B+ Tree Page implementations.
//
// B+ Tree pages store index data in a tree structure.
// There are three types of B+ tree pages:
// - Header Page: Stores the root page ID
// - Internal Page: Stores keys and child pointers
// - Leaf Page: Stores keys and values (RIDs)
//
// ASSIGNMENT 2 - B+ TREE INDEX:
// ==============================
// You need to implement these page layouts and the B+ tree operations.
//
// Page Layout:
// All B+ tree pages share a common header, then have type-specific data.
//
// Common Header:
// +----------------+
// | Page Header    | <- Common page header (8 bytes)
// +----------------+
// | Page Type      | <- Leaf or Internal (4 bytes)
// | Size           | <- Number of keys (4 bytes)
// | Max Size       | <- Maximum keys (4 bytes)
// | Parent Page ID | <- Parent in tree (4 bytes)
// +----------------+
//
// This mirrors:
// - bustub/src/include/storage/page/b_plus_tree_page.h
// - bustub/src/include/storage/page/b_plus_tree_internal_page.h
// - bustub/src/include/storage/page/b_plus_tree_leaf_page.h
// - bustub/src/include/storage/page/b_plus_tree_header_page.h
package page

import "github.com/bustub-go/pkg/common"

// =============================================================================
// B+ TREE PAGE CONSTANTS
// =============================================================================

// B+ tree page types (stored in the page)
const (
	BPlusTreePageTypeInvalid  = 0
	BPlusTreePageTypeLeaf     = 1
	BPlusTreePageTypeInternal = 2
)

// B+ tree common header offsets (after page header)
const (
	// BPlusTreeHeaderSize is the size of the B+ tree common header.
	BPlusTreeHeaderSize = 16

	// OffsetBPlusTreePageType is where the B+ tree page type is stored.
	OffsetBPlusTreePageType = PageHeaderSize + 0

	// OffsetBPlusTreeSize is where the current size (key count) is stored.
	OffsetBPlusTreeSize = PageHeaderSize + 4

	// OffsetBPlusTreeMaxSize is where the maximum size is stored.
	OffsetBPlusTreeMaxSize = PageHeaderSize + 8

	// OffsetBPlusTreeParentPageID is where the parent page ID is stored.
	OffsetBPlusTreeParentPageID = PageHeaderSize + 12

	// BPlusTreeDataStart is where key/value data begins.
	BPlusTreeDataStart = PageHeaderSize + BPlusTreeHeaderSize
)

// =============================================================================
// B+ TREE PAGE (BASE)
// =============================================================================

// BPlusTreePage provides common functionality for all B+ tree pages.
//
// This is the base "class" that internal and leaf pages build upon.
// It provides access to the common header fields.
type BPlusTreePage struct {
	data []byte
}

// NewBPlusTreePage creates a B+ tree page view over raw data.
func NewBPlusTreePage(data []byte) *BPlusTreePage {
	return &BPlusTreePage{data: data}
}

// IsLeafPage returns true if this is a leaf page.
func (p *BPlusTreePage) IsLeafPage() bool {
	return ReadInt32(p.data, OffsetBPlusTreePageType) == BPlusTreePageTypeLeaf
}

// IsInternalPage returns true if this is an internal page.
func (p *BPlusTreePage) IsInternalPage() bool {
	return ReadInt32(p.data, OffsetBPlusTreePageType) == BPlusTreePageTypeInternal
}

// SetPageType sets the B+ tree page type.
func (p *BPlusTreePage) SetPageType(pageType int32) {
	WriteInt32(p.data, OffsetBPlusTreePageType, pageType)
}

// GetSize returns the number of keys in this page.
func (p *BPlusTreePage) GetSize() int {
	return int(ReadInt32(p.data, OffsetBPlusTreeSize))
}

// SetSize sets the number of keys.
func (p *BPlusTreePage) SetSize(size int) {
	WriteInt32(p.data, OffsetBPlusTreeSize, int32(size))
}

// IncreaseSize increases the size by the given amount.
func (p *BPlusTreePage) IncreaseSize(amount int) {
	p.SetSize(p.GetSize() + amount)
}

// GetMaxSize returns the maximum number of keys.
func (p *BPlusTreePage) GetMaxSize() int {
	return int(ReadInt32(p.data, OffsetBPlusTreeMaxSize))
}

// SetMaxSize sets the maximum number of keys.
func (p *BPlusTreePage) SetMaxSize(maxSize int) {
	WriteInt32(p.data, OffsetBPlusTreeMaxSize, int32(maxSize))
}

// GetMinSize returns the minimum number of keys (for underflow detection).
func (p *BPlusTreePage) GetMinSize() int {
	if p.IsLeafPage() {
		return p.GetMaxSize() / 2
	}
	return (p.GetMaxSize() + 1) / 2
}

// GetParentPageID returns the parent page ID.
func (p *BPlusTreePage) GetParentPageID() common.PageID {
	return common.PageID(ReadInt32(p.data, OffsetBPlusTreeParentPageID))
}

// SetParentPageID sets the parent page ID.
func (p *BPlusTreePage) SetParentPageID(parentPageID common.PageID) {
	WriteInt32(p.data, OffsetBPlusTreeParentPageID, int32(parentPageID))
}

// IsRootPage returns true if this page has no parent.
func (p *BPlusTreePage) IsRootPage() bool {
	return p.GetParentPageID() == common.InvalidPageID
}

// =============================================================================
// B+ TREE HEADER PAGE
// =============================================================================

// B+ tree header page layout
const (
	// OffsetRootPageID is where the root page ID is stored.
	OffsetRootPageID = PageHeaderSize + 0

	// BPlusTreeHeaderPageSize is the size of the header page data.
	BPlusTreeHeaderPageSize = 4
)

// BPlusTreeHeaderPage stores the root page ID for a B+ tree.
//
// This is a simple page that just tracks where the tree's root is.
// It's needed because the root page ID can change during splits/merges.
type BPlusTreeHeaderPage struct {
	data []byte
}

// NewBPlusTreeHeaderPage creates a header page view over raw data.
func NewBPlusTreeHeaderPage(data []byte) *BPlusTreeHeaderPage {
	return &BPlusTreeHeaderPage{data: data}
}

// Init initializes the header page.
func (p *BPlusTreeHeaderPage) Init() {
	SetPageType(p.data, PageTypeBPlusTreeHeader)
	p.SetRootPageID(common.InvalidPageID)
}

// GetRootPageID returns the root page ID.
func (p *BPlusTreeHeaderPage) GetRootPageID() common.PageID {
	return common.PageID(ReadInt32(p.data, OffsetRootPageID))
}

// SetRootPageID sets the root page ID.
func (p *BPlusTreeHeaderPage) SetRootPageID(rootPageID common.PageID) {
	WriteInt32(p.data, OffsetRootPageID, int32(rootPageID))
}

// =============================================================================
// B+ TREE INTERNAL PAGE
// =============================================================================

// BPlusTreeInternalPage stores keys and child page pointers.
//
// Internal pages form the upper levels of the B+ tree.
// They contain keys that guide searches and pointers to child pages.
//
// Layout (after common header):
// +------------+------------+------------+------------+
// | Value[0]   | Key[0]     | Value[1]   | Key[1]     | ...
// | (child ptr)| (separator)| (child ptr)| (separator)|
// +------------+------------+------------+------------+
//
// Note: There's always one more value (child pointer) than keys.
// Value[i] points to subtree with keys < Key[i]
// Value[i+1] points to subtree with keys >= Key[i]
//
// TODO (ASSIGNMENT 2 - B+ TREE):
// ==============================
// Implement the following methods:
// - Init() - Initialize the page
// - KeyAt(index) - Get key at index
// - SetKeyAt(index, key) - Set key at index
// - ValueAt(index) - Get child page ID at index
// - SetValueAt(index, value) - Set child page ID
// - Insert(key, value) - Insert key-value pair
// - Remove(index) - Remove entry at index
// - Lookup(key) - Find child for a key
type BPlusTreeInternalPage[K any] struct {
	*BPlusTreePage
	keySize   int
	valueSize int // Size of PageID (4 bytes)
}

// NewBPlusTreeInternalPage creates an internal page view.
func NewBPlusTreeInternalPage[K any](data []byte, keySize int) *BPlusTreeInternalPage[K] {
	return &BPlusTreeInternalPage[K]{
		BPlusTreePage: NewBPlusTreePage(data),
		keySize:       keySize,
		valueSize:     4, // PageID is int32
	}
}

// Init initializes the internal page.
func (p *BPlusTreeInternalPage[K]) Init(maxSize int) {
	SetPageType(p.data, PageTypeBPlusTreeInternal)
	p.SetPageType(BPlusTreePageTypeInternal)
	p.SetSize(0)
	p.SetMaxSize(maxSize)
	p.SetParentPageID(common.InvalidPageID)
}

// GetEntrySize returns the size of one key-value entry.
func (p *BPlusTreeInternalPage[K]) GetEntrySize() int {
	return p.keySize + p.valueSize
}

// GetKeyOffset returns the byte offset for a key at the given index.
func (p *BPlusTreeInternalPage[K]) GetKeyOffset(index int) int {
	// First entry is just a value (no key), subsequent entries have key+value
	if index == 0 {
		panic("internal page: key index 0 is invalid (no key at index 0)")
	}
	return BPlusTreeDataStart + p.valueSize + (index-1)*(p.keySize+p.valueSize)
}

// GetValueOffset returns the byte offset for a value (child pointer) at index.
func (p *BPlusTreeInternalPage[K]) GetValueOffset(index int) int {
	if index == 0 {
		return BPlusTreeDataStart
	}
	return BPlusTreeDataStart + p.valueSize + (index-1)*(p.keySize+p.valueSize) + p.keySize
}

// ValueAt returns the child page ID at the given index.
//
// TODO (ASSIGNMENT): Implement this method.
// Read the PageID from the appropriate offset.
func (p *BPlusTreeInternalPage[K]) ValueAt(index int) common.PageID {
	offset := p.GetValueOffset(index)
	return common.PageID(ReadInt32(p.data, offset))
}

// SetValueAt sets the child page ID at the given index.
//
// TODO (ASSIGNMENT): Implement this method.
func (p *BPlusTreeInternalPage[K]) SetValueAt(index int, value common.PageID) {
	offset := p.GetValueOffset(index)
	WriteInt32(p.data, offset, int32(value))
}

// =============================================================================
// B+ TREE LEAF PAGE
// =============================================================================

// Leaf page specific offsets
const (
	// OffsetNextPageID is where the next leaf pointer is stored.
	OffsetLeafNextPageID = PageHeaderSize + BPlusTreeHeaderSize
)

// BPlusTreeLeafPage stores key-value pairs (keys and RIDs).
//
// Leaf pages form the bottom level of the B+ tree.
// They contain the actual data (or RIDs pointing to data).
// All leaf pages are linked together for range scans.
//
// Layout (after common header):
// +----------------+
// | Next Page ID   | <- Pointer to next leaf (4 bytes)
// +----------------+
// | Key[0] | Val[0]|
// | Key[1] | Val[1]|
// | ...            |
// +----------------+
//
// TODO (ASSIGNMENT 2 - B+ TREE):
// ==============================
// Implement the following methods:
// - Init() - Initialize the page
// - GetNextPageID() - Get next leaf page
// - SetNextPageID() - Set next leaf page
// - KeyAt(index) - Get key at index
// - ValueAt(index) - Get RID at index
// - Insert(key, value) - Insert key-value pair
// - Remove(key) - Remove entry by key
// - Lookup(key) - Find value for key
type BPlusTreeLeafPage[K any, V any] struct {
	*BPlusTreePage
	keySize   int
	valueSize int
}

// NewBPlusTreeLeafPage creates a leaf page view.
func NewBPlusTreeLeafPage[K any, V any](data []byte, keySize int, valueSize int) *BPlusTreeLeafPage[K, V] {
	return &BPlusTreeLeafPage[K, V]{
		BPlusTreePage: NewBPlusTreePage(data),
		keySize:       keySize,
		valueSize:     valueSize,
	}
}

// LeafDataStart returns where key-value data begins (after next page ID).
func (p *BPlusTreeLeafPage[K, V]) LeafDataStart() int {
	return OffsetLeafNextPageID + 4
}

// Init initializes the leaf page.
func (p *BPlusTreeLeafPage[K, V]) Init(maxSize int) {
	SetPageType(p.data, PageTypeBPlusTreeLeaf)
	p.SetPageType(BPlusTreePageTypeLeaf)
	p.SetSize(0)
	p.SetMaxSize(maxSize)
	p.SetParentPageID(common.InvalidPageID)
	p.SetNextPageID(common.InvalidPageID)
}

// GetNextPageID returns the next leaf page ID.
func (p *BPlusTreeLeafPage[K, V]) GetNextPageID() common.PageID {
	return common.PageID(ReadInt32(p.data, OffsetLeafNextPageID))
}

// SetNextPageID sets the next leaf page ID.
func (p *BPlusTreeLeafPage[K, V]) SetNextPageID(nextPageID common.PageID) {
	WriteInt32(p.data, OffsetLeafNextPageID, int32(nextPageID))
}

// GetEntrySize returns the size of one key-value entry.
func (p *BPlusTreeLeafPage[K, V]) GetEntrySize() int {
	return p.keySize + p.valueSize
}

// GetKeyOffset returns the byte offset for a key at the given index.
func (p *BPlusTreeLeafPage[K, V]) GetKeyOffset(index int) int {
	return p.LeafDataStart() + index*(p.keySize+p.valueSize)
}

// GetValueOffset returns the byte offset for a value at the given index.
func (p *BPlusTreeLeafPage[K, V]) GetValueOffset(index int) int {
	return p.LeafDataStart() + index*(p.keySize+p.valueSize) + p.keySize
}

// TODO (ASSIGNMENT): Implement the remaining methods for leaf pages
// - KeyAt(index int) K
// - SetKeyAt(index int, key K)
// - ValueAt(index int) V
// - SetValueAt(index int, value V)
// - Insert(key K, value V, comparator) int
// - Lookup(key K, comparator) (V, bool)
